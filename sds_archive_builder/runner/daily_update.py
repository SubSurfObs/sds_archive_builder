"""
Daily update runner.

On each run:
  1. Pull waveforms for the last `lookback_days` days per network
     (accounts for data_lag_days and late-arriving data)
  2. Process any scheduled retries (no_data / error requests where retry_after <= today)
  3. Optionally rsync local_staging → sds_root

This is designed to be called from cron. It is safe to run multiple times per day.
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
        testing: Write to test_output_dir; skip rsync.
        test_output_dir: Required when testing=True.

    Returns:
        Summary dict.
    """
    today = date.today()
    summary = {"date": today.isoformat(), "networks": {}, "rsync": None}

    # ── 1. Fetch recent days per network ──────────────────────────────────────
    target_nets = networks or list(network_configs.keys())

    for net_code in target_nets:
        if net_code not in network_configs:
            logger.warning("Network %s not in config — skipping", net_code)
            continue

        net_cfg = network_configs[net_code]
        # Pull back at least lookback_days, and also cover the network's data_lag
        lookback = max(archive_config.daily.lookback_days, net_cfg.data_lag_days + 2)
        start = today - timedelta(days=lookback)
        end = today - timedelta(days=1)  # don't request today (incomplete day)

        logger.info("Daily update for %s: %s → %s", net_code, start, end)

        result = run_backfill(
            archive_config,
            {net_code: net_cfg},
            start=start,
            end=end,
            networks=[net_code],
            testing=testing,
            test_output_dir=test_output_dir,
            recheck_days=archive_config.daily.recheck_days,
        )
        summary["networks"][net_code] = result

    # ── 2. Process scheduled retries ──────────────────────────────────────────
    if archive_config.daily.run_retries and not testing:
        engine = init_db(archive_config.db_path)
        with session_scope(engine) as session:
            due = [(r.network, r.day) for r in get_due_retries(session, today)]

        if due:
            logger.info("Processing %d scheduled retries", len(due))
            retry_by_net: dict[str, list] = {}
            for (net_code, day) in due:
                retry_by_net.setdefault(net_code, []).append(day)

            for net_code, days in retry_by_net.items():
                if net_code not in network_configs:
                    continue
                days = sorted(set(days))
                logger.info("Retrying %d days for %s", len(days), net_code)
                # Backfill handles the per-request skip logic, so just run
                # over the distinct days; it will re-check retry_after per row
                retry_result = run_backfill(
                    archive_config,
                    {net_code: network_configs[net_code]},
                    start=min(days),
                    end=max(days),
                    networks=[net_code],
                )
                summary.setdefault("retries", {})[net_code] = retry_result
        else:
            logger.debug("No retries due today")

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
