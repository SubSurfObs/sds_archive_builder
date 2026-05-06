"""Pre-flight checks for cron entry points."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sds_archive_builder.config import ArchiveConfig


def archive_paths_unavailable(archive_cfg: "ArchiveConfig") -> str | None:
    """Return a reason string if sds_root or local_staging is not accessible.

    Catches the case where an SMB share fails to remount after a VM reboot —
    callers can log a clean error and exit instead of crashing deep in a
    filesystem walk.
    """
    for label in ("sds_root", "local_staging"):
        path = getattr(archive_cfg, label)
        if not path.exists():
            return f"{label} not accessible: {path}"
    return None
