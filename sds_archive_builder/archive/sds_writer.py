"""
Write ObsPy Stream objects to SDS (SeisComP Data Structure) format.

SDS path format:
    {root}/{year}/{net}/{sta}/{cha}.D/{net}.{sta}.{loc}.{cha}.D.{year}.{julday}

Write strategy:
    1. Write to local_staging first (fast local disk)
    2. Merge with any existing data for the same day
    3. rsync to sds_root (SMB mount) separately via sync_to_archive()

In testing mode, writes go to a temporary test_output/ directory and
never touch local_staging or sds_root.
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import date
from pathlib import Path
from typing import Optional

from obspy import Stream, UTCDateTime, read
from obspy.core.trace import Trace

logger = logging.getLogger(__name__)


def sds_path(root: Path, net: str, sta: str, loc: str, cha: str, year: int, julday: int) -> Path:
    """Construct the SDS file path for a given SEED channel and day."""
    filename = f"{net}.{sta}.{loc}.{cha}.D.{year:04d}.{julday:03d}"
    return root / str(year) / net / sta / f"{cha}.D" / filename


def _split_stream_by_day(stream: Stream) -> dict[date, Stream]:
    """
    Split a Stream into per-day sub-Streams (UTC day boundaries).

    An FDSN response for day D may contain samples from the previous or
    next day if the instrument samples across midnight.
    """
    days: dict[date, Stream] = {}
    for trace in stream:
        # Slice the trace at each day boundary it overlaps
        t = UTCDateTime(trace.stats.starttime.date)
        t_end = trace.stats.endtime
        while t <= t_end:
            day_start = t
            day_end = t + 86400
            sliced = trace.slice(day_start, day_end - trace.stats.delta)
            if sliced is not None and sliced.stats.npts > 0:
                d = t.date
                if d not in days:
                    days[d] = Stream()
                days[d].append(sliced)
            t += 86400
    return days


def write_stream(
    stream: Stream,
    staging_root: Path,
    *,
    merge_existing: bool = True,
) -> dict[date, int]:
    """
    Write a Stream to SDS under staging_root.

    Splits by day, merges with any existing SDS files, and writes MSEED.

    Returns:
        Dict mapping day → bytes written.
    """
    if not stream:
        return {}

    results: dict[date, int] = {}
    per_day = _split_stream_by_day(stream)

    for day, day_stream in per_day.items():
        year = day.year
        julday = day.timetuple().tm_yday

        # Group traces by SEED ID so each file gets its own channel
        by_channel: dict[tuple, Stream] = {}
        for trace in day_stream:
            key = (
                trace.stats.network,
                trace.stats.station,
                trace.stats.location,
                trace.stats.channel,
            )
            if key not in by_channel:
                by_channel[key] = Stream()
            by_channel[key].append(trace)

        for (net, sta, loc, cha), chan_stream in by_channel.items():
            path = sds_path(staging_root, net, sta, loc, cha, year, julday)
            path.parent.mkdir(parents=True, exist_ok=True)

            if merge_existing and path.exists():
                try:
                    existing = read(str(path))
                    combined = existing + chan_stream
                    # merge(method=1) handles overlaps; split() converts any
                    # masked-array gaps back to separate traces so MSEED can
                    # write them without error (gap info is preserved, not filled).
                    combined.merge(method=1)
                    combined = combined.split()
                    combined.sort()
                    combined.write(str(path), format="MSEED")
                    nbytes = path.stat().st_size
                    logger.debug("Merged %s.%s.%s.%s day %s → %s bytes", net, sta, loc, cha, day, nbytes)
                except Exception as exc:
                    logger.warning(
                        "Failed to merge with existing %s: %s — overwriting", path, exc
                    )
                    chan_stream.write(str(path), format="MSEED")
                    nbytes = path.stat().st_size
            else:
                chan_stream.write(str(path), format="MSEED")
                nbytes = path.stat().st_size
                logger.debug("Wrote %s.%s.%s.%s day %s → %s bytes", net, sta, loc, cha, day, nbytes)

            results[day] = results.get(day, 0) + nbytes

    return results


def day_file_exists(staging_root: Path, net: str, sta: str, loc: str, cha: str, day: date) -> bool:
    """Return True if an SDS file exists and is non-empty for this channel/day."""
    year = day.year
    julday = day.timetuple().tm_yday
    path = sds_path(staging_root, net, sta, loc, cha, year, julday)
    return path.exists() and path.stat().st_size > 0


def sync_to_archive(staging_root: Path, sds_root: Path, rsync_options: str = "--checksum") -> bool:
    """
    rsync from local staging → SMB archive.

    Returns True on success, False on failure.
    """
    cmd = [
        "rsync",
        "-av",
        *rsync_options.split(),
        f"{staging_root}/",
        f"{sds_root}/",
    ]
    logger.info("rsync: %s → %s", staging_root, sds_root)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        if result.returncode == 0:
            logger.info("rsync completed successfully")
            return True
        else:
            logger.error("rsync failed (exit %d): %s", result.returncode, result.stderr)
            return False
    except subprocess.TimeoutExpired:
        logger.error("rsync timed out after 1 hour")
        return False
    except FileNotFoundError:
        logger.error("rsync binary not found")
        return False
