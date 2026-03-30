"""
Run a historical backfill.

Usage:
    # Production (writes to local_staging)
    sds-backfill --instance ~/instances/victoria --start 2020-01-01

    # Testing mode (writes to a temp dir, summary printed on exit)
    sds-backfill --instance ~/instances/victoria \
        --start 2020-01-01 --end 2020-01-31 --testing

    # Single network
    sds-backfill --instance ~/instances/victoria --network OZ --start 2022-01-01
"""

import logging
import tempfile
from datetime import date
from pathlib import Path

import click

from sds_archive_builder.config import load_instance
from sds_archive_builder.database import init_db
from sds_archive_builder.runner.backfill import run_backfill
from scripts._logging import setup_logging


@click.command()
@click.option(
    "--instance", envvar="SDS_ARCHIVE_INSTANCE", required=True,
    type=click.Path(exists=True, file_okay=False),
    help="Path to instance directory.",
)
@click.option("--start", required=True, type=click.DateTime(formats=["%Y-%m-%d"]),
              help="Start date (YYYY-MM-DD).")
@click.option("--end", default=None, type=click.DateTime(formats=["%Y-%m-%d"]),
              help="End date (YYYY-MM-DD). Default: yesterday.")
@click.option("--network", "-n", multiple=True,
              help="Limit to specific network code(s). Repeatable.")
@click.option("--testing", is_flag=True,
              help="Write to a temp dir; never touches main archive.")
@click.option("--delay", default=1.0, type=float, show_default=True,
              help="Seconds between requests.")
@click.option("--verbose", "-v", is_flag=True)
def main(instance, start, end, network, testing, delay, verbose):
    """Historical waveform backfill."""
    archive_cfg, network_cfgs = load_instance(instance)
    setup_logging(archive_cfg, verbose=verbose)
    logger = logging.getLogger(__name__)

    init_db(archive_cfg.db_path)

    start_date = start.date()
    end_date = end.date() if end else None
    networks = list(network) if network else None

    if testing:
        with tempfile.TemporaryDirectory(prefix="sds_test_") as tmpdir:
            test_path = Path(tmpdir)
            click.echo(f"Testing mode — output in: {test_path}")
            result = run_backfill(
                archive_cfg, network_cfgs,
                start=start_date,
                end=end_date,
                networks=networks,
                testing=True,
                test_output_dir=test_path,
                inter_request_delay_s=delay,
            )
    else:
        result = run_backfill(
            archive_cfg, network_cfgs,
            start=start_date,
            end=end_date,
            networks=networks,
            inter_request_delay_s=delay,
        )

    _print_summary(result, testing)


def _print_summary(result: dict, testing: bool) -> None:
    click.echo("\n── Backfill Summary ──")
    if testing:
        click.echo("  Mode: TESTING (no data written to archive)")
    for key in ["success", "no_data", "error", "rate_limited", "skipped"]:
        click.echo(f"  {key:<14s} {result.get(key, 0):>6d}")


if __name__ == "__main__":
    main()
