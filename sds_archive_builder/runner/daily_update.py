"""
Daily update runner.

On each run:
  1. For each network: run sds-verify over the recent window to reset any
     suspect files (empty stubs, partial downloads) so they are re-fetched.
  2. Pull waveforms for the last N days per network. Skips existing successes;
     ignores server backoff / retry_after state (daily data should always be
     attempted regardless of historical fill backoff).
  3. Optionally rsync local_staging → sds_root.

The daily job does NOT process historical retries (run_retries: false). That
belongs in sds-backfill, which sweeps retry_after records naturally on each pass.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from sds_archive_builder.archive.sds_writer import sync_to_archive
from sds_archive_builder.config import ArchiveConfig, NetworkConfig
from sds_archive_builder.database import init_db, get_due_retries, session_scope
from sds_archive_builder.runner.backfill import run_backfill

logger = logging.getLogger(__name__)


def run_daily(
    archive_config: ArchiveConfig,
    network_configs: dict[str, NetworkConfig],
    *,
    networks: Optional[list[str]] = None,
    rsync: bool = False,
    testing: bool = False,
    test_output_dir: Optional[Path] = None,
) -> dict:
    """
    Run the daily update.

    Args:
        networks: Limit to these network codes. None = all configured.
        rsync: If True, rsync local_staging → sds_root after fetching.
        testing: Write to test_output_dir; skip rsync and verify.
        test_output_dir: Required when testing=True.

    Returns:
        Summary dict.
    """
    today = date.today()
    summary = {"date": today.isoformat(), "networks": {}, "rsync": None}

    target_nets = networks or list(network_configs.keys())
    staging = archive_config.local_staging

    for net_code in target_nets:
        if net_code not in network_configs:
            logger.warning("Network %s not in config — skipping", net_code)
            continue

        net_cfg = network_configs[net_code]
        lookback = max(archive_config.daily.lookback_days, net_cfg.data_lag_days + 2)
        start = today - timedelta(days=lookback)
        end = today - timedelta(days=1)

        # ── 1. Verify recent files before fetching ────────────────────────────
        # Reset any suspect files within the lookback window so they will be
        # re-fetched in the fetch step below. Uses a stricter threshold than
        # standalone sds-verify to catch materially partial files, not just stubs.
        if archive_config.daily.verify_before_run and not testing:
            _run_verify_for_network(
                archive_config, net_cfg, net_code, staging, start,
            )

        # ── 2. Fetch recent days ──────────────────────────────────────────────
        logger.info("Daily update for %s: %s → %s", net_code, start, end)

        result = run_backfill(
            archive_config,
            {net_code: net_cfg},
            start=start,
            end=end,
            networks=[net_code],
            testing=testing,
            test_output_dir=test_output_dir,
            ignore_retry_schedule=True,
        )
        summary["networks"][net_code] = result

    # ── 3. rsync ──────────────────────────────────────────────────────────────
    if rsync and not testing:
        ok = sync_to_archive(
            archive_config.local_staging,
            archive_config.sds_root,
            archive_config.daily.rsync_options,
        )
        summary["rsync"] = "success" if ok else "failed"
    elif testing:
        summary["rsync"] = "skipped (testing mode)"

    return summary


def _run_verify_for_network(
    archive_config: ArchiveConfig,
    net_cfg: NetworkConfig,
    net_code: str,
    staging: Path,
    since: date,
) -> None:
    """
    Run a windowed sds-verify scan for one network and reset suspect files.

    Uses archive_config.daily.verify_threshold (default 0.70) — stricter than
    the standalone sds-verify default (0.20) to catch materially partial files.
    """
    from sds_archive_builder.archive.sds_verify import run_verify, fix_db_records

    engine = init_db(archive_config.db_path)

    suspects = run_verify(
        staging,
        network=net_code,
        relative_threshold=archive_config.daily.verify_threshold,
        since=since,
    )

    if suspects:
        logger.info(
            "sds-verify: %d suspect files since %s for %s — resetting for re-fetch",
            len(suspects), since, net_code,
        )
        fix_db_records(suspects, engine, net_cfg.servers.primary)
    else:
        logger.debug("sds-verify: no suspect files since %s for %s", since, net_code)
