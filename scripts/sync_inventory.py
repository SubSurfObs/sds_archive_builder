"""
Sync station inventory from FDSN servers into the archive database.

Usage:
    python scripts/sync_inventory.py --instance ~/instances/victoria
    sds-inventory --instance ~/instances/victoria
    SDS_ARCHIVE_INSTANCE=~/instances/victoria sds-inventory
"""

import logging

import click

from sds_archive_builder.config import load_instance
from sds_archive_builder.database import init_db
from sds_archive_builder.runner.inventory_sync import sync_all_networks
from scripts._logging import setup_logging


@click.command()
@click.option(
    "--instance", envvar="SDS_ARCHIVE_INSTANCE", required=True,
    type=click.Path(exists=True, file_okay=False),
    help="Path to instance directory (or set SDS_ARCHIVE_INSTANCE).",
)
@click.option(
    "--network", "-n", multiple=True,
    help="Limit to specific network code(s). Repeatable. Default: all.",
)
@click.option("--dry-run", is_flag=True, help="Fetch inventory but do not write to DB.")
@click.option("--verbose", "-v", is_flag=True, help="Enable DEBUG logging.")
def main(instance: str, network: tuple, dry_run: bool, verbose: bool):
    """Fetch and sync station metadata from FDSN servers."""
    archive_cfg, network_cfgs = load_instance(instance)
    setup_logging(archive_cfg, verbose=verbose)

    logger = logging.getLogger(__name__)

    engine = init_db(archive_cfg.db_path)

    networks = list(network) if network else None
    summaries = sync_all_networks(
        archive_cfg, network_cfgs,
        networks=networks,
        dry_run=dry_run,
    )

    click.echo("\n── Inventory Sync Summary ──")
    for s in summaries:
        if "error" in s:
            click.echo(f"  {s['network']:6s}  ERROR: {s['error']}")
        else:
            dr = " [dry run]" if s.get("dry_run") else ""
            click.echo(f"  {s['network']:6s}  {s['total_channels']:4d} channels in bounds{dr}")


if __name__ == "__main__":
    main()
