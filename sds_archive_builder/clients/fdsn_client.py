"""
FDSN client with server fallback, adaptive rate limiting, and server health tracking.

Each FDSNClient instance is bound to a single network's configuration.
It tries the primary server first, then the fallback if available.
Server health is tracked in the database and used to skip backed-off servers.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Optional

from obspy import Inventory, Stream, UTCDateTime
from obspy.clients.fdsn import Client as ObspyClient
from obspy.clients.fdsn.header import (
    FDSNNoDataException,
    FDSNException,
    FDSNNoAuthenticationServiceException,
)
from sqlalchemy.orm import Session

from sds_archive_builder.clients.base import (
    BaseClient, FetchError, NoDataError, RateLimitedError
)
from sds_archive_builder.config import NetworkConfig, ArchiveConfig
from sds_archive_builder.database import (
    ServerHealth, get_or_create_server_health, session_scope
)

logger = logging.getLogger(__name__)

# Backoff schedule for consecutive server failures (seconds)
_FAILURE_BACKOFF_S = [60, 300, 900, 3600, 7200]


class FDSNClient(BaseClient):
    """
    FDSN waveform and inventory client for a single network.

    Maintains two ObsPy clients (primary + optional fallback) and uses
    server health records to decide which server to use.
    """

    def __init__(
        self,
        network_config: NetworkConfig,
        archive_config: ArchiveConfig,
        engine,
    ):
        self.net_cfg = network_config
        self.arc_cfg = archive_config
        self.engine = engine
        self._clients: dict[str, ObspyClient] = {}

    def _get_obspy_client(self, server: str) -> ObspyClient:
        """Return a cached ObsPy Client for the given server URL/shortname."""
        if server not in self._clients:
            logger.debug("Creating ObsPy FDSN client for %s", server)
            self._clients[server] = ObspyClient(
                server,
                timeout=self.arc_cfg.request_timeout_s,
            )
        return self._clients[server]

    def _server_is_available(self, server: str) -> bool:
        """Check if the server is currently backed off."""
        with session_scope(self.engine) as session:
            health = get_or_create_server_health(session, server)
            if health.is_backed_off():
                logger.debug(
                    "Server %s is backed off until %s — skipping",
                    server, health.backoff_until,
                )
                return False
        return True

    def _record_success(self, server: str) -> None:
        with session_scope(self.engine) as session:
            health = get_or_create_server_health(session, server)
            health.last_success = datetime.utcnow()
            health.consecutive_failures = 0
            health.backoff_until = None
            today = date.today()
            if health.requests_today_date != today:
                health.requests_today = 1
                health.requests_today_date = today
            else:
                health.requests_today += 1

    def _record_failure(self, server: str, rate_limited: bool = False) -> None:
        with session_scope(self.engine) as session:
            health = get_or_create_server_health(session, server)
            health.last_failure = datetime.utcnow()
            health.consecutive_failures = (health.consecutive_failures or 0) + 1

            if rate_limited:
                # Back off for 1 hour on rate limit
                backoff_s = 3600
            else:
                idx = min(
                    health.consecutive_failures - 1,
                    len(_FAILURE_BACKOFF_S) - 1
                )
                backoff_s = _FAILURE_BACKOFF_S[idx]

            health.backoff_until = datetime.utcnow() + timedelta(seconds=backoff_s)
            logger.info(
                "Server %s: %d consecutive failures — backing off %ds until %s",
                server,
                health.consecutive_failures,
                backoff_s,
                health.backoff_until.strftime("%Y-%m-%d %H:%M"),
            )

    def _server_order(self) -> list[str]:
        """Return [primary, fallback] filtered to available servers."""
        candidates = [self.net_cfg.servers.primary]
        if self.net_cfg.servers.fallback:
            candidates.append(self.net_cfg.servers.fallback)
        return [s for s in candidates if self._server_is_available(s)]

    # ── Public interface ──────────────────────────────────────────────────────

    def get_inventory(
        self,
        network: str,
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float,
    ) -> Inventory:
        servers = self._server_order()
        if not servers:
            raise FetchError(
                f"All servers for network {network} are currently backed off."
            )

        last_exc: Optional[Exception] = None
        for server in servers:
            try:
                client = self._get_obspy_client(server)
                logger.debug(
                    "Fetching inventory for %s from %s (lat [%.2f, %.2f] lon [%.2f, %.2f])",
                    network, server, min_lat, max_lat, min_lon, max_lon,
                )
                inv = client.get_stations(
                    network=network,
                    minlatitude=min_lat,
                    maxlatitude=max_lat,
                    minlongitude=min_lon,
                    maxlongitude=max_lon,
                    level="channel",
                )
                self._record_success(server)
                return inv
            except FDSNNoDataException:
                # No stations match — not a server error
                self._record_success(server)
                return Inventory(networks=[], source="empty")
            except FDSNException as exc:
                logger.warning("FDSN error from %s for %s inventory: %s", server, network, exc)
                self._record_failure(server)
                last_exc = exc
            except Exception as exc:
                logger.warning("Unexpected error from %s for %s inventory: %s", server, network, exc)
                self._record_failure(server)
                last_exc = exc

        raise FetchError(
            f"All servers failed for {network} inventory. Last error: {last_exc}"
        )

    def get_waveforms(
        self,
        network: str,
        station: str,
        location: str,
        channel: str,
        day: date,
    ) -> Stream:
        """
        Fetch one day of waveforms. Tries primary then fallback server.

        Raises:
            NoDataError: server returned no data for this request.
            RateLimitedError: server rate-limited this client.
            FetchError: all servers failed.
        """
        servers = self._server_order()
        if not servers:
            raise FetchError(
                f"All servers for {network} are currently backed off."
            )

        starttime = UTCDateTime(day.isoformat())
        endtime = starttime + 86400  # full day

        # Normalise location: ObsPy uses "--" for blank loc in some contexts
        loc = location if location else "*"

        last_exc: Optional[Exception] = None
        for server in servers:
            try:
                client = self._get_obspy_client(server)
                logger.debug(
                    "Fetching %s.%s.%s.%s %s from %s",
                    network, station, location, channel, day, server,
                )
                stream = client.get_waveforms(
                    network=network,
                    station=station,
                    location=loc,
                    channel=channel,
                    starttime=starttime,
                    endtime=endtime,
                )
                self._record_success(server)
                if len(stream) == 0:
                    raise NoDataError(f"Empty stream returned by {server}")
                return stream

            except FDSNNoDataException:
                self._record_success(server)  # Server is fine, just no data
                raise NoDataError(
                    f"No data on {server} for "
                    f"{network}.{station}.{location}.{channel} {day}"
                )

            except FDSNException as exc:
                msg = str(exc)
                if "429" in msg or "rate" in msg.lower() or "throttle" in msg.lower():
                    logger.warning("Rate limited by %s: %s", server, exc)
                    self._record_failure(server, rate_limited=True)
                    raise RateLimitedError(str(exc), retry_after_s=3600)
                if "503" in msg or "502" in msg or "504" in msg:
                    logger.warning("Server unavailable %s: %s", server, exc)
                    self._record_failure(server)
                    last_exc = exc
                    continue  # try fallback
                # Other FDSN errors (400 bad request etc.) — don't retry fallback
                self._record_failure(server)
                last_exc = exc
                continue

            except Exception as exc:
                logger.warning("Unexpected error from %s: %s", server, exc)
                self._record_failure(server)
                last_exc = exc
                continue

        raise FetchError(
            f"All servers failed for "
            f"{network}.{station}.{location}.{channel} {day}. "
            f"Last error: {last_exc}"
        )
