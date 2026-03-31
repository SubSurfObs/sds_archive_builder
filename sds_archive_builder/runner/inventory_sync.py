"""
Fetch and sync station inventory from FDSN servers into the archive database.

For each configured network:
  1. Query the FDSN server with buffered geo bounds
  2. Apply the strict geo filter
  3. Upsert station rows into the database
  4. Flag stations outside the operational bounds (in_geo_bounds=False)

Stations are never deleted — they are flagged as out-of-bounds if they no longer
appear in the inventory or move outside the operational area.
"""

from __future__ import annotations

import logging
from datetime import datetime

from obspy.core.inventory import Channel

from sds_archive_builder.clients.fdsn_client import FDSNClient
from sds_archive_builder.config import ArchiveConfig, NetworkConfig
from sds_archive_builder.database import init_db, session_scope, upsert_station
from sds_archive_builder.geo_filter import filter_inventory

logger = logging.getLogger(__name__)


def sync_network_inventory(
    network_config: NetworkConfig,
    archive_config: ArchiveConfig,
    engine,
    *,
    dry_run: bool = False,
) -> dict:
    """
    Sync inventory for one network. Returns a summary dict.
    """
    net = network_config.network
    bounds = network_config.effective_geo_bounds(archive_config)
    buffered = bounds.buffered()

    logger.info("Syncing inventory for network %s ...", net)

    client = FDSNClient(network_config, archive_config, engine)

    inv = client.get_inventory(
        network=net,
        min_lat=buffered.min_lat,
        max_lat=buffered.max_lat,
        min_lon=buffered.min_lon,
        max_lon=buffered.max_lon,
    )

    # Apply strict geo filter
    filtered_inv = filter_inventory(inv, bounds)

    # Collect all SEED IDs in the filtered inventory (in-bounds).
    # A single (net.sta.loc.cha) may have multiple epochs (sensor changes etc.) —
    # deduplicate by SEED ID, keeping earliest start_date and latest end_date.
    seed_rows: dict[tuple, dict] = {}

    for fdsn_net in filtered_inv.networks:
        for fdsn_sta in fdsn_net.stations:
            for cha in fdsn_sta.channels:
                seed_id = (fdsn_net.code, fdsn_sta.code, cha.location_code or "", cha.code)

                start_date = cha.start_date.date if cha.start_date else None
                end_date = cha.end_date.date if cha.end_date else None

                if seed_id not in seed_rows:
                    seed_rows[seed_id] = {
                        "network": fdsn_net.code,
                        "station": fdsn_sta.code,
                        "location": cha.location_code or "",
                        "channel": cha.code,
                        "latitude": fdsn_sta.latitude,
                        "longitude": fdsn_sta.longitude,
                        "elevation": fdsn_sta.elevation,
                        "start_date": start_date,
                        "end_date": end_date,
                        "in_geo_bounds": True,
                        "last_inventory_sync": datetime.utcnow(),
                    }
                else:
                    existing = seed_rows[seed_id]
                    # Keep earliest start_date (useful as a backfill lower bound)
                    if start_date and (existing["start_date"] is None or start_date < existing["start_date"]):
                        existing["start_date"] = start_date
                    # Keep latest end_date; None means currently active
                    if end_date is None:
                        existing["end_date"] = None
                    elif existing["end_date"] is not None and end_date > existing["end_date"]:
                        existing["end_date"] = end_date

    station_rows = list(seed_rows.values())
    n_epochs_raw = sum(
        len(fdsn_sta.channels)
        for fdsn_net in filtered_inv.networks
        for fdsn_sta in fdsn_net.stations
    )
    if n_epochs_raw != len(station_rows):
        logger.debug(
            "%s: deduplicated %d channel-epochs → %d unique SEED IDs",
            net, n_epochs_raw, len(station_rows),
        )

    n_upserted = 0
    if not dry_run:
        with session_scope(engine) as session:
            for row in station_rows:
                upsert_station(session, **row)
                n_upserted += 1

    summary = {
        "network": net,
        "total_channels": len(station_rows),
        "upserted": n_upserted,
        "dry_run": dry_run,
    }
    logger.info(
        "Inventory sync %s: %d channels in bounds%s",
        net, len(station_rows), " (dry run)" if dry_run else "",
    )
    return summary


def sync_all_networks(
    archive_config: ArchiveConfig,
    network_configs: dict[str, NetworkConfig],
    *,
    networks: list[str] | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """
    Sync inventory for all (or selected) networks. Returns list of summary dicts.
    """
    engine = init_db(archive_config.db_path)

    target_nets = networks or list(network_configs.keys())
    summaries = []

    for net_code in target_nets:
        if net_code not in network_configs:
            logger.warning("Network %s not in config — skipping", net_code)
            continue
        try:
            summary = sync_network_inventory(
                network_configs[net_code],
                archive_config,
                engine,
                dry_run=dry_run,
            )
            summaries.append(summary)
        except Exception as exc:
            logger.error("Failed to sync inventory for %s: %s", net_code, exc)
            summaries.append({"network": net_code, "error": str(exc)})

    return summaries
