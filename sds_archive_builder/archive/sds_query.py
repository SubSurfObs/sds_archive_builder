"""Query the local SDS archive for coverage information."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator

from sds_archive_builder.archive.sds_writer import sds_path

logger = logging.getLogger(__name__)


def iter_missing_days(
    staging_root: Path,
    net: str,
    sta: str,
    loc: str,
    cha: str,
    start: date,
    end: date,
) -> Iterator[date]:
    """
    Yield each day in [start, end] for which no SDS file exists or the file is empty.
    """
    current = start
    while current <= end:
        year = current.year
        julday = current.timetuple().tm_yday
        path = sds_path(staging_root, net, sta, loc, cha, year, julday)
        if not path.exists() or path.stat().st_size == 0:
            yield current
        current += timedelta(days=1)


def coverage_summary(
    staging_root: Path,
    net: str,
    sta: str,
    loc: str,
    cha: str,
    start: date,
    end: date,
) -> dict:
    """
    Return a summary dict of archive coverage for a channel over a date range.

    Keys:
        total_days, days_present, days_missing, coverage_pct, missing_days (list)
    """
    total = 0
    present = 0
    missing_days = []

    current = start
    while current <= end:
        total += 1
        year = current.year
        julday = current.timetuple().tm_yday
        path = sds_path(staging_root, net, sta, loc, cha, year, julday)
        if path.exists() and path.stat().st_size > 0:
            present += 1
        else:
            missing_days.append(current)
        current += timedelta(days=1)

    pct = (present / total * 100) if total > 0 else 0.0
    return {
        "network": net,
        "station": sta,
        "location": loc,
        "channel": cha,
        "start": start,
        "end": end,
        "total_days": total,
        "days_present": present,
        "days_missing": total - present,
        "coverage_pct": round(pct, 1),
        "missing_days": missing_days,
    }


def list_available_channels(staging_root: Path, net: str, sta: str) -> list[str]:
    """Return list of channel codes present in the SDS archive for a station."""
    channels = set()
    sta_root = staging_root / "*" / net / sta
    for cha_dir in staging_root.glob(f"*/{net}/{sta}/*.D"):
        cha = cha_dir.name.replace(".D", "")
        channels.add(cha)
    return sorted(channels)
