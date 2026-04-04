"""
Historical backfill runner.

For each in-bounds station channel, walks the date range from history.start
to today (or a user-specified end), requesting one day at a time.

Skips:
  - Days already present in the SDS archive (if skip_existing=True)
  - Days with a "success" record in the DB
  - Days where retry_after is in the future

Records every attempt in fetch_requests, regardless of outcome.

Concurrency:
  Uses ThreadPoolExecutor with archive_config.max_concurrent_requests workers.
  Each worker creates its own FDSNClient to avoid shared mutable state in the
  ObsPy client cache. Server health and fetch_request writes go through
  session_scope() which creates a fresh SQLAlchemy session per call — SQLite
  serialises concurrent writes safely at small thread counts.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from sds_archive_builder.archive.sds_writer import day_file_exists, write_stream
from sds_archive_builder.clients.base import FetchError, NoDataError, RateLimitedError
from sds_archive_builder.clients.fdsn_client import FDSNClient
from sds_archive_builder.config import ArchiveConfig, NetworkConfig, RetryPolicy
from sds_archive_builder.database import (
    FetchRequest, Station,
    get_fetch_request, get_stations_in_bounds, init_db,
    session_scope, upsert_fetch_request,
)

logger = logging.getLogger(__name__)


def _staging_root(archive_config: ArchiveConfig, testing: bool, test_output_dir: Optional[Path]) -> Path:
    if testing and test_output_dir:
        return test_output_dir
    return archive_config.local_staging


def _should_skip(
    session,
    net: str, sta: str, loc: str, cha: str,
    day: date,
    server: str,
    staging_root: Path,
    skip_existing: bool,
    recheck_days: int = 0,
) -> bool:
    """Return True if this (station, day) should be skipped.

    Within the recheck window (day >= today - recheck_days), success records
    and file-existence checks are bypassed so that new data can be merged into
    existing SDS files. retry_after and max_attempts are also ignored, since the
    daily sweep should be aggressive about recent days.
    """
    in_recheck_window = (
        recheck_days > 0 and day >= date.today() - timedelta(days=recheck_days)
    )

    req = get_fetch_request(session, net, sta, loc, cha, day, server)

    # Already successfully written — skip unless in recheck window
    if req and req.status == "success" and not in_recheck_window:
        return True

    # SDS file already present — skip unless in recheck window (where we merge)
    if skip_existing and not in_recheck_window and day_file_exists(staging_root, net, sta, loc, cha, day):
        return True

    # In recheck window: always attempt regardless of retry schedule or attempt count
    if in_recheck_window:
        return False

    # Scheduled for future retry
    if req and req.retry_after and req.retry_after > date.today():
        return True

    # Exhausted max attempts
    if req and req.attempt_count >= 5:
        return True

    return False


def _record_attempt(
    session,
    net: str, sta: str, loc: str, cha: str,
    day: date, server: str,
    status: str,
    bytes_written: int = 0,
    error_message: Optional[str] = None,
    retry_after: Optional[date] = None,
    attempt_count: int = 1,
):
    upsert_fetch_request(
        session,
        network=net,
        station=sta,
        location=loc,
        channel=cha,
        day=day,
        server=server,
        status=status,
        attempt_count=attempt_count,
        last_attempt=datetime.utcnow(),
        bytes_written=bytes_written,
        error_message=error_message,
        retry_after=retry_after,
    )


# ── Per-request worker ────────────────────────────────────────────────────────

@dataclass
class _WorkContext:
    """Shared (read-only) context passed to every worker thread."""
    archive_config: ArchiveConfig
    net_cfg: NetworkConfig
    engine: Any
    staging: Path
    server: str
    skip_existing: bool
    recheck_days: int
    retry_policy: RetryPolicy
    inter_request_delay_s: float
    # Thread-safe progress tracking
    progress: dict   # {"n": int, "total": int}
    lock: threading.Lock


def _process_one(
    ctx: _WorkContext,
    net: str, sta: str, loc: str, cha: str,
    day: date,
) -> str:
    """
    Fetch and record one (channel, day).  Returns the status key for totals.

    Creates its own FDSNClient so the ObsPy client cache (_clients dict) is
    not shared between threads.
    """
    engine = ctx.engine
    staging = ctx.staging
    server = ctx.server
    recheck_days = ctx.recheck_days

    with ctx.lock:
        ctx.progress["n"] += 1
        req_num = ctx.progress["n"]
        total = ctx.progress["total"]

    in_recheck_window = (
        recheck_days > 0 and day >= date.today() - timedelta(days=recheck_days)
    )

    with session_scope(engine) as session:
        if _should_skip(session, net, sta, loc, cha, day, server, staging,
                        ctx.skip_existing, recheck_days):
            return "skipped"

        existing_req = get_fetch_request(session, net, sta, loc, cha, day, server)
        attempt_count = (existing_req.attempt_count + 1) if existing_req else 1
        was_success = existing_req is not None and existing_req.status == "success"

    logger.info("[%d/%d] %s.%s.%s.%s %s", req_num, total, net, sta, loc, cha, day)

    # Each worker gets its own FDSNClient (thread-safe: no shared mutable cache)
    client = FDSNClient(ctx.net_cfg, ctx.archive_config, engine)

    try:
        stream = client.get_waveforms(net, sta, loc, cha, day)
        if sum(tr.stats.npts for tr in stream) == 0:
            raise NoDataError(
                f"Stream returned but contained no samples for "
                f"{net}.{sta}.{loc}.{cha} {day}"
            )
        results = write_stream(stream, staging)
        nbytes = sum(results.values())

        with session_scope(engine) as session:
            _record_attempt(
                session, net, sta, loc, cha, day, server,
                status="success",
                bytes_written=nbytes,
                attempt_count=attempt_count,
            )
        logger.info("  ✓ %d bytes", nbytes)
        return "success"

    except NoDataError as exc:
        if in_recheck_window and was_success:
            logger.debug(
                "  ↺ recheck: no new data for %s.%s.%s.%s %s — keeping success",
                net, sta, loc, cha, day,
            )
            return "skipped"
        retry_days = ctx.retry_policy.no_data_retry_days(attempt_count)
        retry_after = date.today() + timedelta(days=retry_days)
        with session_scope(engine) as session:
            _record_attempt(
                session, net, sta, loc, cha, day, server,
                status="no_data",
                error_message=str(exc),
                retry_after=retry_after,
                attempt_count=attempt_count,
            )
        logger.info("  ∅ no data (retry after %s)", retry_after)
        return "no_data"

    except RateLimitedError as exc:
        retry_after = date.today() + timedelta(days=1)
        with session_scope(engine) as session:
            _record_attempt(
                session, net, sta, loc, cha, day, server,
                status="rate_limited",
                error_message=str(exc),
                retry_after=retry_after,
                attempt_count=attempt_count,
            )
        logger.warning("Rate limited on %s — server backed off", server)
        # Do not sleep here: backoff_until is set in server_health; other workers
        # will see the server as unavailable and skip it automatically.
        return "rate_limited"

    except FetchError as exc:
        if in_recheck_window and was_success:
            logger.debug(
                "  ↺ recheck: server error for %s.%s.%s.%s %s — keeping success",
                net, sta, loc, cha, day,
            )
            return "skipped"
        retry_days = ctx.retry_policy.error_backoff_days(attempt_count)
        retry_after = date.today() + timedelta(days=retry_days)
        with session_scope(engine) as session:
            _record_attempt(
                session, net, sta, loc, cha, day, server,
                status="error",
                error_message=str(exc),
                retry_after=retry_after,
                attempt_count=attempt_count,
            )
        logger.warning("✗ %s.%s.%s.%s %s — error: %s", net, sta, loc, cha, day, exc)
        return "error"

    finally:
        if ctx.inter_request_delay_s > 0:
            time.sleep(ctx.inter_request_delay_s)


# ── Public entry point ────────────────────────────────────────────────────────

def run_backfill(
    archive_config: ArchiveConfig,
    network_configs: dict[str, NetworkConfig],
    *,
    start: Optional[date] = None,
    end: Optional[date] = None,
    networks: Optional[list[str]] = None,
    testing: bool = False,
    test_output_dir: Optional[Path] = None,
    inter_request_delay_s: float = 1.0,
    recheck_days: int = 0,
) -> dict:
    """
    Run a backfill across all in-bounds stations.

    Args:
        start: Override the per-network history.start. If None, uses network config.
        end: Last day to fetch (inclusive). Defaults to yesterday.
        networks: Limit to these network codes. None = all configured.
        testing: If True, write to test_output_dir instead of local_staging.
        test_output_dir: Required when testing=True.
        inter_request_delay_s: Seconds to sleep between requests (per worker).
        recheck_days: Days from today within which existing success records are
            re-fetched and merged rather than skipped. 0 = archive mode (never
            re-fetch successes). Set to daily lookback window for daily sweeps.

    Returns:
        Summary dict with counts of success, no_data, error, skipped.

    Concurrency:
        Uses archive_config.max_concurrent_requests threads. With N workers and
        inter_request_delay_s=1.0, throughput is ~N requests/second.
    """
    if testing and test_output_dir is None:
        raise ValueError("test_output_dir must be set when testing=True")

    engine = init_db(archive_config.db_path)
    staging = _staging_root(archive_config, testing, test_output_dir)
    staging.mkdir(parents=True, exist_ok=True)

    target_nets = networks or list(network_configs.keys())
    end_date = end or (date.today() - timedelta(days=1))

    totals = {"success": 0, "no_data": 0, "error": 0, "skipped": 0, "rate_limited": 0}
    lock = threading.Lock()
    max_workers = archive_config.max_concurrent_requests

    for net_code in target_nets:
        if net_code not in network_configs:
            logger.warning("Network %s not in config — skipping", net_code)
            continue

        net_cfg = network_configs[net_code]
        retry_policy = net_cfg.effective_retry_policy(archive_config)
        server = net_cfg.servers.primary
        skip_existing = archive_config.backfill.skip_existing

        net_start = start or date.fromisoformat(net_cfg.history_start)
        if net_start > end_date:
            logger.info(
                "Network %s: history start %s is after end date %s — skipping",
                net_code, net_start, end_date,
            )
            continue

        with session_scope(engine) as session:
            stations = [
                (s.network, s.station, s.location, s.channel)
                for s in get_stations_in_bounds(session, network=net_code)
            ]

        if not stations:
            logger.warning(
                "No in-bounds stations found for %s — run inventory sync first", net_code
            )
            continue

        # Build the full work list for this network
        date_list = list(_date_range(net_start, end_date, archive_config.backfill.mode))
        work_items = [
            (net, sta, loc, cha, day)
            for day in date_list
            for (net, sta, loc, cha) in stations
        ]
        total_requests = len(work_items)

        logger.info(
            "Backfill %s: %d channels × %d days = %d requests (%s, workers=%d)",
            net_code, len(stations), len(date_list), total_requests,
            archive_config.backfill.mode, max_workers,
        )

        ctx = _WorkContext(
            archive_config=archive_config,
            net_cfg=net_cfg,
            engine=engine,
            staging=staging,
            server=server,
            skip_existing=skip_existing,
            recheck_days=recheck_days,
            retry_policy=retry_policy,
            inter_request_delay_s=inter_request_delay_s,
            progress={"n": 0, "total": total_requests},
            lock=lock,
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_process_one, ctx, net, sta, loc, cha, day): (net, sta, loc, cha, day)
                for (net, sta, loc, cha, day) in work_items
            }
            for future in as_completed(futures):
                try:
                    status = future.result()
                except Exception as exc:
                    item = futures[future]
                    logger.error("Unhandled exception for %s: %s", item, exc)
                    status = "error"
                with lock:
                    totals[status] += 1

    logger.info(
        "Backfill complete: success=%d no_data=%d error=%d rate_limited=%d skipped=%d",
        totals["success"], totals["no_data"], totals["error"],
        totals["rate_limited"], totals["skipped"],
    )
    return totals


def _date_range(start: date, end: date, mode: str):
    """Generate dates from start to end (inclusive), in specified direction."""
    if mode == "newest_first":
        current = end
        while current >= start:
            yield current
            current -= timedelta(days=1)
    else:  # oldest_first
        current = start
        while current <= end:
            yield current
            current += timedelta(days=1)
