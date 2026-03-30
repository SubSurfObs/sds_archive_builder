# SDS Archive Builder

A Python toolkit for building and maintaining a local seismic waveform archive in **SDS (SeisComP Data Structure)** format, targeted at a geographically-bounded region in SE Australia.

Data is retrieved from remote **FDSN servers** via [ObsPy](https://docs.obspy.org/) and written to a local SDS archive, with optional sync to network-attached storage. The system tracks every request attempt, handles server failures and rate limits gracefully, and supports retrospective re-fetching as historical data becomes available on upstream servers.

---

## Features

- Pull waveform data from multiple FDSN servers with per-network priority/fallback configuration
- Store data in SDS format compatible with SeisComP, ObsPy, and other seismic tooling
- SQLite-backed request tracking: know what has been attempted, what succeeded, and when to retry
- Adaptive rate limiting: learns server behaviour and backs off automatically
- Geographic filtering: inventory is filtered to a configurable bounding box (with buffer)
- Two-step write: stage locally, then rsync to SMB/NAS archive
- Testing mode: run against a bounded time window without touching the main archive
- Pluggable client architecture: FDSN now, ASDF (Phase 2) via the same interface

---

## Package vs. Instance

This repository is a **generic, reusable tool**. A specific deployment (e.g. "Victoria seismic archive") is an **instance** — a separate directory on your server containing only the configuration files and the live SQLite database for that deployment.

```
# Generic tool — this repo (public, version controlled here)
~/sds_archive_builder/

# Deployment instance — lives on your VM (not in this repo)
~/instances/victoria/
    archive.yaml          # Paths, geo bounds, retry policy
    networks/
        AU.yaml
        GE.yaml
    archive.db            # Live request-tracking DB
    logs/
```

All scripts accept `--instance <path>` (or set `SDS_ARCHIVE_INSTANCE` in the environment). The instance directory is never committed to this repository.

---

## Quick Start

### 1. Install the package

```bash
conda env create -f environment.yml
conda activate sds_archive_builder
pip install -e .
```

### 2. Initialise a new instance

```bash
python scripts/init_instance.py ~/instances/victoria
```

This scaffolds the instance directory from templates with inline documentation.

### 3. Edit the instance config

Key settings in `~/instances/victoria/archive.yaml`:

```yaml
sds_root: "/mnt/mediaflux/SDS"        # Final archive location (SMB mount)
local_staging: "/scratch/sds_staging"  # Local VM disk — fast, small

geo_bounds:
  min_lat: -39.5
  max_lat: -33.0
  min_lon: 140.0
  max_lon: 150.5
  buffer_deg: 0.5                      # Expand inventory query by this amount
```

### 4. Configure your networks

Edit or add files in `~/instances/victoria/networks/`. Each network gets its own YAML:

```bash
cp ~/instances/victoria/networks/template.yaml ~/instances/victoria/networks/AU.yaml
# edit AU.yaml for your network
```

### 5. Sync station inventory

```bash
python scripts/sync_inventory.py --instance ~/instances/victoria
```

This fetches station metadata from all configured FDSN servers, applies the geographic filter, and populates the database.

### 6. Run a backfill (testing mode)

```bash
python scripts/run_backfill.py \
  --instance ~/instances/victoria \
  --start 2020-01-01 \
  --end 2020-01-31 \
  --mode testing
```

Testing mode writes to a temporary directory, never touches the main archive, and prints a coverage summary on exit.

### 7. Run production backfill

```bash
python scripts/run_backfill.py --instance ~/instances/victoria --start 2015-01-01
```

### 8. Schedule daily updates

```bash
# Add to crontab — runs at 06:00 UTC daily
0 6 * * * SDS_ARCHIVE_INSTANCE=~/instances/victoria \
  /path/to/conda/envs/sds_archive_builder/bin/python \
  /path/to/scripts/run_daily.py --rsync
```

---

## Repository Structure

```
sds_archive_builder/             # This repo — generic tool only
│
├── config/
│   └── templates/
│       ├── archive.yaml         # Instance config template (copy to your instance dir)
│       └── network.yaml         # Network config template
│
├── sds_archive_builder/         # Main Python package
│   ├── config.py                # Config loading and validation
│   ├── database.py              # SQLite models (SQLAlchemy)
│   ├── geo_filter.py            # Geographic bounds filtering
│   ├── clients/
│   │   ├── base.py              # Abstract client interface
│   │   ├── fdsn_client.py       # ObsPy FDSN client + server fallback
│   │   └── asdf_client.py       # pyasdf client (Phase 2)
│   ├── archive/
│   │   ├── sds_writer.py        # Write Stream → SDS + staging/rsync logic
│   │   └── sds_query.py         # Query SDS coverage by station/day
│   └── runner/
│       ├── inventory_sync.py    # Station metadata refresh
│       ├── backfill.py          # Historical fill logic
│       └── daily_update.py      # Daily pull + scheduled retries
│
├── scripts/
│   ├── init_instance.py         # Scaffold a new instance directory
│   ├── sync_inventory.py        # CLI: refresh station inventory
│   ├── run_backfill.py          # CLI: historical fill
│   ├── run_daily.py             # CLI: daily update + rsync
│   └── audit_archive.py         # CLI: coverage report
│
├── environment.yml
├── pyproject.toml
├── CLAUDE.md
└── README.md

# Instance directory (on your VM — NOT in this repo)
~/instances/victoria/
    archive.yaml
    networks/
        AU.yaml
        GE.yaml
    archive.db               # Created automatically on first run
    logs/
```

---

## Network Configuration

Each file in `<instance_dir>/networks/` configures one seismic network:

```yaml
network: AU
description: "Geoscience Australia broadband network"

channels: ["HHZ", "HHN", "HHE", "BHZ"]
location_codes: ["", "00", "10"]

servers:
  primary: AUSPASS
  fallback: IRIS

# Override global geo_bounds if needed
geo_filter:
  min_lat: -39.5
  max_lat: -33.0
  min_lon: 140.0
  max_lon: 150.5

history:
  start: "2000-01-01"        # Earliest date to attempt backfill
  data_lag_days: 2           # Data may appear up to N days after real-time

retry_policy:
  max_attempts: 5
  # Re-attempt "no_data" responses after these intervals (days)
  retry_after_days: [7, 30, 90]
```

---

## Operating Modes

| Mode | Description |
|------|-------------|
| `testing` | Bounded time window, verbose output, writes to temp dir only |
| `backfill` | Fill historical data from `history.start` to present |
| `daily` | Pull recent days; process scheduled retries; optionally rsync |
| `audit` | Read-only coverage report and retry candidate list |

---

## Request Tracking

Every waveform request (station × day × server) is logged to an SQLite database. Possible statuses:

| Status | Meaning |
|--------|---------|
| `success` | Data received and written to SDS |
| `no_data` | Server responded but returned no waveforms |
| `error` | Request failed (timeout, HTTP error, etc.) |
| `rate_limited` | Server returned 429 or equivalent |
| `pending` | Queued but not yet attempted |

**`no_data` is never treated as permanent.** Upstream servers for some networks are actively adding historical data. Requests with `no_data` are automatically scheduled for retry at 7, 30, and 90 day intervals.

---

## Write Strategy

```
FDSN Server
    │
    ▼
Local VM disk (staging)
    │
    ▼  rsync (on demand or scheduled)
SMB Mount (MediaFlux) — main SDS archive
```

The local staging directory holds days–weeks of data. The rsync step is triggered after each daily run (or manually). This protects against SMB mount unavailability during data acquisition.

---

## Scaling

For a 10-year backfill of ~50 stations across 4 networks:

- ~182,500 day-requests total
- At 1 request/second: ~2 days of wall time
- In practice, server rate limits spread this over days to weeks
- The retry scheduler handles this without intervention

---

## Phase 2: ASDF Support

Geoscience Australia ran a 2-year deployment across Victoria stored in ASDF format on a supercomputer. The `asdf_client.py` module (Phase 2) will read these files via [pyasdf](https://github.com/SeismicData/pyasdf) and convert waveforms to the same SDS archive via the same pipeline.

---

## Dependencies

- [ObsPy](https://docs.obspy.org/) — FDSN client, waveform I/O, SDS writing
- [SQLAlchemy](https://www.sqlalchemy.org/) — database ORM
- [PyYAML](https://pyyaml.org/) — configuration
- [Click](https://click.palletsprojects.com/) — CLI
- [pyasdf](https://github.com/SeismicData/pyasdf) — ASDF support (Phase 2)

---

## License

MIT
