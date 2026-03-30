"""Geographic filtering utilities."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from obspy.core.inventory import Inventory, Network, Station as ObspyStation
    from sds_archive_builder.config import GeoBounds

logger = logging.getLogger(__name__)


def station_in_bounds(lat: float, lon: float, bounds: "GeoBounds") -> bool:
    """Return True if (lat, lon) falls within the strict (non-buffered) bounds."""
    return bounds.contains(lat, lon)


def filter_inventory(inventory: "Inventory", bounds: "GeoBounds") -> "Inventory":
    """
    Return a copy of the inventory containing only stations within strict bounds.

    The inventory query is made with buffered bounds; this function applies the
    tighter operational filter afterwards.
    """
    from obspy import Inventory as ObspyInventory

    kept_networks = []
    total_in = 0
    total_kept = 0

    for net in inventory.networks:
        kept_stations = []
        for sta in net.stations:
            total_in += 1
            if sta.latitude is None or sta.longitude is None:
                logger.warning(
                    "Station %s.%s has no coordinates — excluding",
                    net.code, sta.code,
                )
                continue
            if station_in_bounds(sta.latitude, sta.longitude, bounds):
                kept_stations.append(sta)
                total_kept += 1
            else:
                logger.debug(
                    "Station %s.%s (%.3f, %.3f) outside bounds — excluded",
                    net.code, sta.code, sta.latitude, sta.longitude,
                )

        if kept_stations:
            from obspy.core.inventory import Network
            filtered_net = net.copy()
            filtered_net.stations = kept_stations
            kept_networks.append(filtered_net)

    result = ObspyInventory(networks=kept_networks, source="filtered")
    logger.info(
        "Geo filter: %d → %d stations kept (bounds lat [%.1f, %.1f] lon [%.1f, %.1f])",
        total_in, total_kept,
        bounds.min_lat, bounds.max_lat,
        bounds.min_lon, bounds.max_lon,
    )
    return result
