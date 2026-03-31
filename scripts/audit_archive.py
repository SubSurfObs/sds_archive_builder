"""
Read-only audit of archive coverage and pending retries.

Usage:
    sds-audit --instance ~/instances/victoria
    sds-audit --instance ~/instances/victoria --network OZ --start 2023-01-01
    sds-audit --instance ~/instances/victoria --show-retries
"""

import logging
from datetime import date, timedelta

import click
from sqlalchemy import func, select

from sds_archive_builder.archive.sds_query import coverage_summary
from sds_archive_builder.config import load_instance
from sds_archive_builder.database import (
    FetchRequest, Station,
    get_due_retries, get_stations_in_bounds,
    init_db, session_scope,
)
from scripts._logging import setup_logging


@click.command()
@click.option(
    "--instance", envvar="SDS_ARCHIVE_INSTANCE", required=True,
    type=click.Path(exists=True, file_okay=False),
)
@click.option("--network", "-n", multiple=True)
@click.option("--start", default=None, type=click.DateTime(formats=["%Y-%m-%d"]),
              help="Start of coverage window. Default: 30 days ago.")
@click.option("--end", default=None, type=click.DateTime(formats=["%Y-%m-%d"]),
              help="End of coverage window. Default: yesterday.")
@click.option("--show-retries", is_flag=True,
              help="List requests due for retry.")
@click.option("--show-missing", is_flag=True,
              help="List all missing days per channel (can be verbose).")
@click.option("--verbose", "-v", is_flag=True)
def main(instance, network, start, end, show_retries, show_missing, verbose):
    """Audit SDS archive coverage and retry queue."""
    archive_cfg, network_cfgs = load_instance(instance)
    setup_logging(archive_cfg, verbose=verbose)

    engine = init_db(archive_cfg.db_path)

    today = date.today()
    start_date = start.date() if start else (today - timedelta(days=30))
    end_date = end.date() if end else (today - timedelta(days=1))
    networks = list(network) if network else None

    staging = archive_cfg.local_staging

    # ── Coverage report ───────────────────────────────────────────────────────
    click.echo(f"\n── Coverage Report: {start_date} → {end_date} ──")
    click.echo(f"   Staging: {staging}")
    click.echo()

    with session_scope(engine) as session:
        stations = [
            (s.network, s.station, s.location, s.channel)
            for s in get_stations_in_bounds(session)
        ]

    if networks:
        stations = [(n, s, l, c) for (n, s, l, c) in stations if n in networks]

    if not stations:
        click.echo("  No in-bounds stations found. Run sds-inventory first.")
        return

    total_days = total_present = 0
    for (net, sta, loc, cha) in stations:
        summary = coverage_summary(
            staging, net, sta, loc, cha, start_date, end_date,
        )
        pct = summary["coverage_pct"]
        flag = "  " if pct >= 80 else "! " if pct >= 50 else "✗ "
        click.echo(
            f"  {flag}{net:6s} {sta:8s} {loc:4s} {cha:4s} "
            f"  {summary['days_present']:4d}/{summary['total_days']:4d} days  "
            f"({pct:5.1f}%)"
        )
        if show_missing and summary["missing_days"]:
            ranges = _compress_dates(summary["missing_days"])
            click.echo(f"       missing: {ranges}")

        total_days += summary["total_days"]
        total_present += summary["days_present"]

    overall = (total_present / total_days * 100) if total_days > 0 else 0
    click.echo(f"\n  Overall: {total_present}/{total_days} days ({overall:.1f}%)")

    # ── DB status summary ─────────────────────────────────────────────────────
    click.echo(f"\n── Request Status (DB) ──")
    with session_scope(engine) as session:
        rows = session.execute(
            select(FetchRequest.status, func.count().label("n"))
            .group_by(FetchRequest.status)
        ).all()
    for row in sorted(rows, key=lambda r: r.status):
        click.echo(f"  {row.status:<16s} {row.n:>8d}")

    # ── Retries ───────────────────────────────────────────────────────────────
    if show_retries:
        with session_scope(engine) as session:
            due = [
                (r.network, r.station, r.location, r.channel,
                 r.day, r.status, r.attempt_count, r.server)
                for r in get_due_retries(session, today)
            ]
        click.echo(f"\n── Retries Due Today ({len(due)}) ──")
        for (rnet, rsta, rloc, rcha, rday, rstatus, rattempt, rserver) in due[:50]:
            click.echo(
                f"  {rnet}.{rsta}.{rloc}.{rcha}"
                f"  {rday}  {rstatus}  attempt#{rattempt}"
                f"  {rserver}"
            )
        if len(due) > 50:
            click.echo(f"  ... and {len(due) - 50} more")


def _compress_dates(days: list[date]) -> str:
    """Compress a list of dates into a readable range string."""
    if not days:
        return ""
    days = sorted(days)
    ranges = []
    start = end = days[0]
    for d in days[1:]:
        if (d - end).days == 1:
            end = d
        else:
            ranges.append(f"{start}" if start == end else f"{start}→{end}")
            start = end = d
    ranges.append(f"{start}" if start == end else f"{start}→{end}")
    return ", ".join(ranges)


if __name__ == "__main__":
    main()
