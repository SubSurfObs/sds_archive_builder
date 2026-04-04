"""
Verify SDS archive integrity by scanning for suspect files.

Two-pass algorithm:

Pass 1 (fast, always):
    For each SDS file, compare its size against the median of ±7 neighbouring
    days for the same channel. Flag if:
      - Below absolute floor (4096 bytes), OR
      - Below relative_threshold × median (default 0.20 for standalone,
        configurable — daily workflow uses a stricter value e.g. 0.70).
    Channel size index is built from ALL files so medians are accurate even
    when a date window (since=) is applied.

Pass 2 (slow, --full only):
    For each flagged file from pass 1, read with ObsPy and count actual
    samples. Separates genuinely empty files from legitimately short ones
    (e.g. newly deployed instruments, known data gaps).

--fix:
    Reset DB records for all flagged files:
      status=error, retry_after=today, attempt_count=1
    Does NOT delete files. write_stream will merge on re-fetch.
    Without --fix, pure audit mode — safe to run any time.
"""

from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator, Optional

from sds_archive_builder.archive.sds_writer import sds_path

logger = logging.getLogger(__name__)

# Default thresholds
ABSOLUTE_FLOOR_BYTES = 4096
DEFAULT_RELATIVE_THRESHOLD = 0.20   # standalone sds-verify default
MEDIAN_WINDOW_DAYS = 7              # ± this many days around the candidate day


@dataclass
class SuspectFile:
    net: str
    sta: str
    loc: str
    cha: str
    day: date
    path: Path
    file_bytes: int
    median_bytes: Optional[float]
    reason: str          # "below_floor" | "below_relative" | "zero_samples" | "unreadable"
    sample_count: int = 0  # filled in by pass 2 if --full


def _iter_sds_files(sds_root: Path) -> Iterator[tuple[str, str, str, str, int, int, Path]]:
    """
    Walk an SDS root and yield (net, sta, loc, cha, year, julday, path) for each file.

    SDS layout: {root}/{year}/{net}/{sta}/{cha}.D/{net}.{sta}.{loc}.{cha}.D.{year}.{julday}
    """
    for year_dir in sorted(sds_root.iterdir()):
        if not year_dir.is_dir():
            continue
        try:
            year = int(year_dir.name)
        except ValueError:
            continue

        for net_dir in sorted(year_dir.iterdir()):
            if not net_dir.is_dir():
                continue
            net = net_dir.name

            for sta_dir in sorted(net_dir.iterdir()):
                if not sta_dir.is_dir():
                    continue
                sta = sta_dir.name

                for cha_dir in sorted(sta_dir.iterdir()):
                    if not cha_dir.is_dir() or not cha_dir.name.endswith(".D"):
                        continue
                    cha = cha_dir.name[:-2]  # strip ".D"

                    for fpath in sorted(cha_dir.iterdir()):
                        if not fpath.is_file():
                            continue
                        parts = fpath.name.split(".")
                        # Expected: net.sta.loc.cha.D.year.julday (7 parts)
                        if len(parts) != 7:
                            continue
                        try:
                            file_year = int(parts[5])
                            julday = int(parts[6])
                            loc = parts[2]
                        except (ValueError, IndexError):
                            continue
                        yield net, sta, loc, cha, file_year, julday, fpath


def _rolling_median(sizes: dict[date, int], day: date, window: int) -> Optional[float]:
    """
    Compute median file size across ±window days (excluding the candidate day itself).
    Returns None if fewer than 3 neighbours exist.
    """
    neighbours = [
        sizes[day + timedelta(days=offset)]
        for offset in range(-window, window + 1)
        if offset != 0 and (day + timedelta(days=offset)) in sizes
    ]
    if len(neighbours) < 3:
        return None
    return statistics.median(neighbours)


def run_verify(
    sds_root: Path,
    *,
    full: bool = False,
    network: Optional[str] = None,
    relative_threshold: float = DEFAULT_RELATIVE_THRESHOLD,
    since: Optional[date] = None,
) -> list[SuspectFile]:
    """
    Pass 1: walk SDS root and flag files below the size thresholds.
    Pass 2 (if full=True): read flagged files with ObsPy to count samples.

    Args:
        sds_root:           Root of the SDS archive.
        full:               If True, run ObsPy sample-count check on flagged files.
        network:            Limit scan to this network code. None = all.
        relative_threshold: Flag if file < this fraction of the ±7-day rolling
                            median. Default 0.20 (standalone); use 0.70 for the
                            daily workflow to catch materially partial files.
        since:              Only flag files on or after this date. The channel
                            size index is still built from all files so medians
                            are accurate. None = flag all dates.

    Returns:
        List of SuspectFile objects.
    """
    # Build size index from ALL files for accurate medians, but only add
    # files within the date window to the flagging list.
    channel_sizes: dict[tuple, dict[date, int]] = {}
    all_files: list[tuple[str, str, str, str, date, Path, int]] = []

    logger.info("Building file size index for %s ...", sds_root)

    for net, sta, loc, cha, year, julday, fpath in _iter_sds_files(sds_root):
        if network and net != network:
            continue
        try:
            nbytes = fpath.stat().st_size
            d = date(year, 1, 1) + timedelta(days=julday - 1)
        except (OSError, ValueError):
            continue

        key = (net, sta, loc, cha)
        if key not in channel_sizes:
            channel_sizes[key] = {}
        channel_sizes[key][d] = nbytes

        # Only flag files within the requested window
        if since is None or d >= since:
            all_files.append((net, sta, loc, cha, d, fpath, nbytes))

    logger.info(
        "Indexed %d channels; checking %d files%s",
        len(channel_sizes),
        len(all_files),
        f" since {since}" if since else "",
    )

    # Flag suspects
    suspects: list[SuspectFile] = []

    for net, sta, loc, cha, d, fpath, nbytes in all_files:
        key = (net, sta, loc, cha)
        sizes = channel_sizes[key]

        reason: Optional[str] = None

        if nbytes < ABSOLUTE_FLOOR_BYTES:
            reason = "below_floor"
        else:
            median = _rolling_median(sizes, d, MEDIAN_WINDOW_DAYS)
            if median is not None and nbytes < relative_threshold * median:
                reason = "below_relative"

        if reason:
            median_val = _rolling_median(sizes, d, MEDIAN_WINDOW_DAYS)
            suspects.append(SuspectFile(
                net=net, sta=sta, loc=loc, cha=cha,
                day=d, path=fpath,
                file_bytes=nbytes,
                median_bytes=median_val,
                reason=reason,
            ))

    logger.info("Pass 1: %d suspect files found", len(suspects))

    # Optional pass 2: ObsPy sample count
    if full and suspects:
        logger.info("Pass 2: reading %d suspect files with ObsPy ...", len(suspects))
        try:
            from obspy import read as obspy_read
        except ImportError:
            logger.error("ObsPy not available — skipping pass 2")
            return suspects

        still_suspect: list[SuspectFile] = []
        for sf in suspects:
            try:
                st = obspy_read(str(sf.path))
                sf.sample_count = sum(tr.stats.npts for tr in st)
                if sf.sample_count == 0:
                    sf.reason = "zero_samples"
                still_suspect.append(sf)
                if sf.sample_count > 0:
                    logger.debug(
                        "  %s %s — %d bytes but %d samples (may be OK)",
                        sf.path.name, sf.day, sf.file_bytes, sf.sample_count,
                    )
            except Exception as exc:
                sf.reason = "unreadable"
                still_suspect.append(sf)
                logger.warning("  Could not read %s: %s", sf.path, exc)

        suspects = still_suspect
        logger.info("Pass 2: %d files remain flagged", len(suspects))

    return suspects


def fix_db_records(
    suspects: list[SuspectFile],
    engine,
    server: str,
) -> int:
    """
    Reset DB records for all suspect files so they will be re-fetched.

    Sets: status=error, retry_after=today, attempt_count=1.
    Does not delete files.

    Args:
        suspects:  List from run_verify().
        engine:    SQLAlchemy engine from init_db().
        server:    Server identifier to match in fetch_requests rows.

    Returns:
        Number of records updated.
    """
    from datetime import datetime
    from sds_archive_builder.database import session_scope, upsert_fetch_request

    today = date.today()
    updated = 0

    with session_scope(engine) as session:
        for sf in suspects:
            upsert_fetch_request(
                session,
                network=sf.net,
                station=sf.sta,
                location=sf.loc,
                channel=sf.cha,
                day=sf.day,
                server=server,
                status="error",
                attempt_count=1,
                last_attempt=datetime.utcnow(),
                bytes_written=sf.file_bytes,
                error_message=f"sds-verify: {sf.reason} ({sf.file_bytes} bytes)",
                retry_after=today,
            )
            updated += 1
            logger.debug(
                "Reset DB record: %s.%s.%s.%s %s (%s)",
                sf.net, sf.sta, sf.loc, sf.cha, sf.day, sf.reason,
            )

    logger.info("Reset %d DB records — all will be retried at next backfill", updated)
    return updated
