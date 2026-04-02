"""Load and validate instance configuration from YAML files."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class GeoBounds:
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float
    buffer_deg: float = 0.5

    def buffered(self) -> "GeoBounds":
        """Return bounds expanded by buffer_deg for inventory queries."""
        return GeoBounds(
            min_lat=self.min_lat - self.buffer_deg,
            max_lat=self.max_lat + self.buffer_deg,
            min_lon=self.min_lon - self.buffer_deg,
            max_lon=self.max_lon + self.buffer_deg,
            buffer_deg=0.0,
        )

    def contains(self, lat: float, lon: float) -> bool:
        return (self.min_lat <= lat <= self.max_lat and
                self.min_lon <= lon <= self.max_lon)


@dataclass
class RetryPolicy:
    max_attempts: int = 5
    retry_after_days: list[int] = field(default_factory=lambda: [7, 30, 90])
    error_backoff_max_days: int = 14

    def no_data_retry_days(self, attempt_count: int) -> int:
        """Days to wait before retrying a no_data response."""
        idx = min(attempt_count, len(self.retry_after_days) - 1)
        return self.retry_after_days[idx]

    def error_backoff_days(self, attempt_count: int) -> int:
        """Exponential backoff in days for error responses."""
        return min(2 ** attempt_count, self.error_backoff_max_days)


@dataclass
class ServerConfig:
    primary: str
    fallback: Optional[str] = None


@dataclass
class BackfillConfig:
    mode: str = "newest_first"   # "oldest_first" | "newest_first"
    skip_existing: bool = True


@dataclass
class DailyConfig:
    lookback_days: int = 3
    recheck_days: int = 3
    run_retries: bool = True
    auto_rsync: bool = False
    rsync_options: str = "--checksum --delete"


@dataclass
class LoggingConfig:
    level: str = "INFO"
    file: str = "logs/archive.log"
    max_bytes: int = 10_485_760
    backup_count: int = 5


@dataclass
class ArchiveConfig:
    instance_dir: Path
    sds_root: Path
    local_staging: Path
    geo_bounds: GeoBounds
    chunk_size_days: int = 1
    request_timeout_s: int = 120
    max_concurrent_requests: int = 2
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    backfill: BackfillConfig = field(default_factory=BackfillConfig)
    daily: DailyConfig = field(default_factory=DailyConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    @property
    def db_path(self) -> Path:
        return self.instance_dir / "archive.db"

    @property
    def log_path(self) -> Optional[Path]:
        if self.logging.file:
            return self.instance_dir / self.logging.file
        return None


@dataclass
class NetworkConfig:
    network: str
    description: str
    channels: list[str]
    location_codes: list[str]
    servers: ServerConfig
    history_start: str
    data_lag_days: int = 2
    retry_policy: Optional[RetryPolicy] = None    # None → inherit from ArchiveConfig
    geo_bounds: Optional[GeoBounds] = None        # None → inherit from ArchiveConfig

    def effective_retry_policy(self, archive: ArchiveConfig) -> RetryPolicy:
        return self.retry_policy or archive.retry_policy

    def effective_geo_bounds(self, archive: ArchiveConfig) -> GeoBounds:
        return self.geo_bounds or archive.geo_bounds


# ── Loaders ───────────────────────────────────────────────────────────────────

def _parse_geo_bounds(d: dict) -> GeoBounds:
    return GeoBounds(
        min_lat=float(d["min_lat"]),
        max_lat=float(d["max_lat"]),
        min_lon=float(d["min_lon"]),
        max_lon=float(d["max_lon"]),
        buffer_deg=float(d.get("buffer_deg", 0.5)),
    )


def _parse_retry_policy(d: dict) -> RetryPolicy:
    return RetryPolicy(
        max_attempts=int(d.get("max_attempts", 5)),
        retry_after_days=list(d.get("retry_after_days", [7, 30, 90])),
        error_backoff_max_days=int(d.get("error_backoff_max_days", 14)),
    )


def load_archive_config(instance_dir: Path) -> ArchiveConfig:
    """Load archive.yaml from the instance directory."""
    path = instance_dir / "archive.yaml"
    if not path.exists():
        raise FileNotFoundError(
            f"No archive.yaml found in instance directory: {instance_dir}\n"
            f"Run 'sds-init {instance_dir}' to scaffold a new instance."
        )

    with open(path) as f:
        raw = yaml.safe_load(f)

    retry = _parse_retry_policy(raw.get("retry_policy", {}))
    bounds = _parse_geo_bounds(raw["geo_bounds"])

    bf_raw = raw.get("backfill", {})
    daily_raw = raw.get("daily", {})
    log_raw = raw.get("logging", {})

    return ArchiveConfig(
        instance_dir=instance_dir,
        sds_root=Path(raw["sds_root"]),
        local_staging=Path(raw["local_staging"]),
        geo_bounds=bounds,
        chunk_size_days=int(raw.get("chunk_size_days", 1)),
        request_timeout_s=int(raw.get("request_timeout_s", 120)),
        max_concurrent_requests=int(raw.get("max_concurrent_requests", 2)),
        retry_policy=retry,
        backfill=BackfillConfig(
            mode=bf_raw.get("mode", "newest_first"),
            skip_existing=bool(bf_raw.get("skip_existing", True)),
        ),
        daily=DailyConfig(
            lookback_days=int(daily_raw.get("lookback_days", 3)),
            recheck_days=int(daily_raw.get("recheck_days", 3)),
            run_retries=bool(daily_raw.get("run_retries", True)),
            auto_rsync=bool(daily_raw.get("auto_rsync", False)),
            rsync_options=daily_raw.get("rsync_options", "--checksum --delete"),
        ),
        logging=LoggingConfig(
            level=log_raw.get("level", "INFO"),
            file=log_raw.get("file", "logs/archive.log"),
            max_bytes=int(log_raw.get("max_bytes", 10_485_760)),
            backup_count=int(log_raw.get("backup_count", 5)),
        ),
    )


def load_network_configs(instance_dir: Path) -> dict[str, NetworkConfig]:
    """Load all network YAML files from <instance_dir>/networks/."""
    networks_dir = instance_dir / "networks"
    if not networks_dir.exists():
        raise FileNotFoundError(
            f"No networks/ directory found in instance directory: {instance_dir}"
        )

    configs: dict[str, NetworkConfig] = {}
    for yaml_path in sorted(networks_dir.glob("*.yaml")):
        if yaml_path.stem.startswith("_"):
            continue  # skip template files (_template.yaml etc.)
        with open(yaml_path) as f:
            raw = yaml.safe_load(f)

        net_code = raw["network"]

        servers_raw = raw.get("servers", {})
        servers = ServerConfig(
            primary=servers_raw["primary"],
            fallback=servers_raw.get("fallback") or None,
        )

        retry = None
        if "retry_policy" in raw:
            retry = _parse_retry_policy(raw["retry_policy"])

        bounds = None
        if "geo_bounds" in raw:
            bounds = _parse_geo_bounds(raw["geo_bounds"])

        history_raw = raw.get("history", {})

        configs[net_code] = NetworkConfig(
            network=net_code,
            description=raw.get("description", ""),
            channels=list(raw.get("channels", [])),
            location_codes=list(raw.get("location_codes", [""])),
            servers=servers,
            history_start=history_raw.get("start", "2000-01-01"),
            data_lag_days=int(history_raw.get("data_lag_days", 2)),
            retry_policy=retry,
            geo_bounds=bounds,
        )
        logger.debug("Loaded network config: %s from %s", net_code, yaml_path.name)

    if not configs:
        raise ValueError(f"No network YAML files found in {networks_dir}")

    return configs


def load_instance(instance_dir: str | Path) -> tuple[ArchiveConfig, dict[str, NetworkConfig]]:
    """Load full instance configuration. Returns (archive_config, network_configs)."""
    instance_dir = Path(instance_dir).resolve()
    if not instance_dir.exists():
        raise FileNotFoundError(f"Instance directory does not exist: {instance_dir}")

    archive = load_archive_config(instance_dir)
    networks = load_network_configs(instance_dir)

    logger.info(
        "Loaded instance: %s | networks: %s | bounds: lat [%.1f, %.1f] lon [%.1f, %.1f]",
        instance_dir.name,
        list(networks.keys()),
        archive.geo_bounds.min_lat,
        archive.geo_bounds.max_lat,
        archive.geo_bounds.min_lon,
        archive.geo_bounds.max_lon,
    )
    return archive, networks
