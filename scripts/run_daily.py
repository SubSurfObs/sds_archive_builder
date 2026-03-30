"""
Daily update — pull recent data and process scheduled retries.

Designed to be called from cron. Safe to run multiple times per day.

Usage:
    sds-daily --instance ~/instances/victoria
    sds-daily --instance ~/instances/victoria --rsync
    SDS_ARCHIVE_INSTANCE=~/instances/victoria sds-daily --rsync

Crontab example (06:00 UTC daily):
    0 6 * * * SDS_ARCHIVE_INSTANCE=/home/user/instances/victoria \
        /path/to/conda/envs/sds_archive_builder/bin/sds-daily --rsync
"""

import logging

import click

from sds_archive_builder.config import load_instance
from sds_archive_builder.database import init_db
from sds_archive_builder.runner.daily_update import run_daily
from scripts._logging import setup_logging


@click.command()
@click.option(
    "--instance", envvar="SDS_ARCHIVE_INSTANCE", required=True,
    type=click.Path(exists=True, file_okay=False),
    help="Path to instance directory.",
)
@click.option("--network", "-n", multiple=True,
              help="Limit to specific network code(s). Repeatable.")
@click.option("--rsync", is_flag=True,
              help="rsync local_staging → sds_root after fetching.")
@click.option("--testing", is_flag=True,
              help="Write to a temp dir; skip rsync.")
@click.option("--verbose", "-v", is_flag=True)
def main(instance, network, rsync, testing, verbose):
    """Daily waveform update and retry processing."""
    archive_cfg, network_cfgs = load_instance(instance)
    setup_logging(archive_cfg, verbose=verbose)

    init_db(archive_cfg.db_path)

    networks = list(network) if network else None

    result = run_daily(
        archive_cfg, network_cfgs,
        networks=networks,
        rsync=rsync or archive_cfg.daily.auto_rsync,
        testing=testing,
    )

    click.echo(f"\n── Daily Update {result['date']} ──")
    for net_code, net_result in result.get("networks", {}).items():
        parts = []
        for key in ["success", "no_data", "error", "skipped"]:
            parts.append(f"{key}={net_result.get(key, 0)}")
        click.echo(f"  {net_code:6s}  {', '.join(parts)}")

    if result.get("rsync"):
        click.echo(f"  rsync: {result['rsync']}")


if __name__ == "__main__":
    main()
