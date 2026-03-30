"""Shared logging setup for CLI scripts."""

from __future__ import annotations

import logging
import logging.handlers
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sds_archive_builder.config import ArchiveConfig


def setup_logging(archive_cfg: "ArchiveConfig", *, verbose: bool = False) -> None:
    """Configure root logger from archive config. Called once at CLI entry."""
    level_name = "DEBUG" if verbose else archive_cfg.logging.level
    level = getattr(logging, level_name.upper(), logging.INFO)

    handlers: list[logging.Handler] = [logging.StreamHandler()]

    log_path = archive_cfg.log_path
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(
            logging.handlers.RotatingFileHandler(
                log_path,
                maxBytes=archive_cfg.logging.max_bytes,
                backupCount=archive_cfg.logging.backup_count,
            )
        )

    fmt = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
    logging.basicConfig(level=level, handlers=handlers, format=fmt, force=True)
