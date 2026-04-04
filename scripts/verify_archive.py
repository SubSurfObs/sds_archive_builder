"""
Scan the SDS archive for suspect files (empty or anomalously small).

Usage:

    # Audit only — no DB changes
    sds-verify --instance ~/instances/gippsland

    # Audit with ObsPy sample-count check on flagged files
    sds-verify --instance ~/instances/gippsland --full

    # Audit + reset DB records for flagged files (re-fetch at next backfill)
    sds-verify --instance ~/instances/gippsland --fix

    # Limit to one network
    sds-verify --instance ~/instances/gippsland --network OZ

    # Testing: scan a specific directory (bypasses instance sds_root)
    sds-verify --instance ~/instances/gippsland --sds-dir /path/to/test/archive
"""

import logging
import sys
from pathlib import Path

import click

from sds_archive_builder.archive.sds_verify import run_verify, fix_db_records
from sds_archive_builder.config import load_instance
from sds_archive_builder.database import init_db
from scripts._logging import setup_logging


@click.command()
@click.option(
    "--instance", envvar="SDS_ARCHIVE_INSTANCE", required=True,
    type=click.Path(exists=True, file_okay=False),
    help="Path to instance directory.",
)
@click.option(
    "--full", is_flag=True,
    help="Run ObsPy sample-count check on flagged files (slower).",
)
@click.option(
    "--fix", is_flag=True,
    help="Reset DB records for flagged files so they are re-fetched at next backfill.",
)
@click.option(
    "--network", "-n", default=None,
    help="Limit scan to this network code.",
)
@click.option(
    "--sds-dir", default=None,
    type=click.Path(exists=True, file_okay=False),
    help="Override the sds_root from archive.yaml (useful for testing).",
)
@click.option("--verbose", "-v", is_flag=True)
def main(instance, full, fix, network, sds_dir, verbose):
    """Scan SDS archive for suspect files."""
    archive_cfg, network_cfgs = load_instance(instance)
    setup_logging(archive_cfg, verbose=verbose)
    logger = logging.getLogger(__name__)

    scan_root = Path(sds_dir) if sds_dir else archive_cfg.local_staging

    click.echo(f"Scanning: {scan_root}")
    if network:
        click.echo(f"Network filter: {network}")
    if full:
        click.echo("Mode: full (ObsPy sample-count check enabled)")
    if fix:
        click.echo("Mode: --fix (DB records will be reset)")
    click.echo()

    suspects = run_verify(scan_root, full=full, network=network)

    if not suspects:
        click.echo("No suspect files found.")
        return

    # Print summary table
    click.echo(f"{'Channel':<28} {'Day':<12} {'Bytes':>10}  {'Median':>10}  {'Reason'}")
    click.echo("-" * 80)
    for sf in sorted(suspects, key=lambda s: (s.net, s.sta, s.cha, s.day)):
        seed = f"{sf.net}.{sf.sta}.{sf.loc}.{sf.cha}"
        median_str = f"{sf.median_bytes:,.0f}" if sf.median_bytes is not None else "n/a"
        samples_str = f"  ({sf.sample_count} samp)" if sf.sample_count > 0 else ""
        click.echo(
            f"{seed:<28} {str(sf.day):<12} {sf.file_bytes:>10,}  {median_str:>10}  {sf.reason}{samples_str}"
        )

    click.echo()
    click.echo(f"Total suspect files: {len(suspects)}")

    if fix:
        # Determine server for DB records — use primary server from any network
        # (verify resets regardless of which server fetched, since we only store
        # one record per channel/day/server and the primary is canonical)
        server = next(iter(network_cfgs.values())).servers.primary if network_cfgs else "unknown"

        # If --network specified, use that network's server
        if network and network in network_cfgs:
            server = network_cfgs[network].servers.primary

        engine = init_db(archive_cfg.db_path)
        n_reset = fix_db_records(suspects, engine, server)
        click.echo(f"\nReset {n_reset} DB records — will be retried at next sds-backfill run.")
    else:
        click.echo("\nRun with --fix to reset DB records for re-fetch.")


if __name__ == "__main__":
    main()
