# CLAUDE.md — SDS Archive Builder

## Project Purpose

Python tooling to build and maintain a local seismic waveform archive in **SDS (SeisComP Data Structure)** format. Data is pulled from remote **FDSN servers** (via ObsPy) and, in Phase 2, from **ASDF** (Adaptable Seismic Data Format) files. The archive covers a geographically-bounded region in SE Australia / Victoria.

---

## Deployment Context

- **VM:** `dsand@172.26.149.194` (rs-l-pg2zyo.desktop.cloud.unimelb.edu.au), passwordless SSH configured
- **Conda env:** `sds_archive_builder` at `~/miniconda3`
- **Repo on VM:** `~/projects/sds_archive_builder`
- **Production instance:** `~/instances/gippsland`
- **Archive storage:** 2 TB SMB mount at `/mnt/sds_other_nets/gippsland` — writing directly, no local staging
- **Write strategy:** Write directly to SMB (sufficient space, no staging needed). `local_staging` and `sds_root` both point to `/mnt/sds_other_nets/gippsland`
- **Backfill start date:** 2025-01-01 (current scope; extend later)
- **Background jobs:** Run inside `tmux` session named `backfill`
- **Environment management:** **conda only** — do not suggest venv or pip-only workflows
- **Git remote:** `https://github.com/SubSurfObs/sds_archive_builder.git`

---

## Package vs. Instance

The codebase is a **generic, installable tool**. A specific deployment (e.g. "Victoria seismic archive") is an **instance** — a separate directory containing only config files and the live SQLite database.

```
# The generic tool (this repo — public)
~/sds_archive_builder/

# A deployment instance (lives on the VM — not in this repo)
~/instances/victoria/
    archive.yaml          # Paths, geo bounds, retry policy for this deployment
    networks/
        AU.yaml
        GE.yaml
    archive.db            # Live request-tracking DB — never committed anywhere
    logs/                 # Log files — never committed
```

All CLI scripts accept `--instance <path>` (or `SDS_ARCHIVE_INSTANCE` env var) pointing to the instance directory. No config files live in this repo except templates.

---

## Architecture Overview

```
config/
  templates/
    archive.yaml          # Template: copy to instance dir and edit
    network.yaml          # Template: one copy per network in instance/networks/

sds_archive_builder/      # Main package (installed via conda)
  config.py               # Load + validate instance YAML configs
  database.py             # SQLite via SQLAlchemy — stations, requests, server health
  geo_filter.py           # Bounding box / polygon station filtering

  clients/
    base.py               # Abstract: fetch_waveforms(), fetch_inventory()
    fdsn_client.py        # ObsPy FDSN with fallback servers + rate limiting
    asdf_client.py        # pyasdf → ObsPy Stream (Phase 2)

  archive/
    sds_writer.py         # Write ObsPy Stream to SDS (local-first)
    sds_query.py          # Query local SDS for day-level coverage

  runner/
    inventory_sync.py     # Pull station metadata, populate DB, apply geo filter
    backfill.py           # Walk time range, skip existing, log attempts
    daily_update.py       # Pull recent days; re-attempt scheduled retries

scripts/
  init_instance.py        # Scaffold a new instance directory from templates
  sync_inventory.py       # CLI: refresh station metadata
  run_backfill.py         # CLI: historical fill
  run_daily.py            # CLI: cron-friendly daily pull (+ rsync)
  audit_archive.py        # CLI: coverage report, retry candidates
```

All scripts take `--instance <path>` or read `SDS_ARCHIVE_INSTANCE` from the environment.

---

## Database Schema

Three tables in SQLite (`archive.db` in the **instance directory** — never in the package repo):

### `stations`
| Column | Notes |
|--------|-------|
| `net`, `sta`, `loc`, `cha` | SEED identifiers |
| `latitude`, `longitude`, `elevation` | From inventory |
| `start_date`, `end_date` | Metadata epoch — treated as a **hint only**, not ground truth |
| `in_geo_bounds` | Boolean; set at inventory sync |
| `last_inventory_sync` | Timestamp |

### `fetch_requests`
| Column | Notes |
|--------|-------|
| `net`, `sta`, `loc`, `cha`, `day` | What was requested |
| `server` | Which server was tried |
| `status` | `pending` \| `success` \| `no_data` \| `error` \| `rate_limited` |
| `attempt_count` | Incremented on each try |
| `last_attempt` | Timestamp |
| `bytes_written` | 0 if no data |
| `error_message` | Last error string |
| `retry_after` | Next scheduled retry date (for `no_data` / `error`) |

### `server_health`
| Column | Notes |
|--------|-------|
| `server` | Server name |
| `last_success`, `last_failure` | Timestamps |
| `consecutive_failures` | Reset on success |
| `requests_today` | Rolling count |
| `backoff_until` | Don't request before this time |

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Day-by-day request granularity | Natural SDS unit; enables per-day tracking and retry |
| `no_data` ≠ permanent | Retrospective data uploads are expected; retry at 7d / 30d / 90d intervals |
| Metadata start date = hint | Station epochs in metadata are sometimes wrong; treat as lower-bound hint |
| Server fallback chain | Per-network primary + fallback server; skip server if `backoff_until` is in future |
| Local-first writes | SMB may be slow or offline; always write to local staging, rsync separately |
| Geographic filter at inventory sync | Filter once on metadata; don't re-check coordinates on every waveform fetch |
| Adaptive rate limiting | Track `server_health`; back off exponentially on 429 / timeout |
| Testing mode | Bounded date range + dry-run flag; does not write to archive or DB |

---

## Geographic Buffering

When requesting station inventory, apply a **buffer** (configurable, default 0.5°) around the target bounding box. This accounts for:
- Metadata coordinates that are slightly off
- Stations just outside the target region that may still record relevant signals

The tighter operational filter is applied post-fetch from the `stations` table (`in_geo_bounds`).

---

## Operating Modes

| Mode | Description |
|------|-------------|
| `testing` | Bounded time window, verbose logging, no writes to main archive |
| `backfill` | Historical fill from `history.start` to present, oldest- or newest-first |
| `daily` | Pull last N days; run scheduled retries; optionally rsync to SMB |
| `audit` | Read-only; report coverage gaps and retry candidates |

---

## Conventions

- All times in **UTC**
- SEED channel codes (e.g. `HHZ`, `BHN`) — never assume a channel exists without checking inventory
- SDS path format: `{sds_root}/{year}/{network}/{station}/{channel}.D/{net}.{sta}.{loc}.{cha}.D.{year}.{julday}`
- Config files: **YAML** (not TOML, not JSON)
- No hardcoded network names, server URLs, or date ranges in Python source — always from config
- Log with Python `logging` module; scripts set up handlers; library code uses `getLogger(__name__)`

---

## Environment Setup

```bash
conda env create -f environment.yml
conda activate sds_archive_builder
```

Key dependencies: `obspy`, `sqlalchemy`, `pyyaml`, `click` (CLI), `pyasdf` (Phase 2).

---

## Phase 2: ASDF Integration

Geoscience Australia ran a 2-year deployment across Victoria stored as ASDF on a supercomputer. `asdf_client.py` will:
1. Accept a path (local filesystem or SSH-mounted) to an ASDF HDF5 file
2. Use `pyasdf.ASDFDataSet` to iterate waveforms
3. Return `obspy.Stream` — identical interface to `fdsn_client.py`
4. Log to `fetch_requests` with `server = "ASDF:<filename>"`

---

## What NOT to Do

- Do not trust metadata `start_date` / `end_date` as definitive waveform availability
- Do not treat a failed or empty FDSN response as proof that no data exists
- Do not hardcode geographic bounds — read from the instance `archive.yaml`
- Do not use `pip install` alone — always update `environment.yml`
- Do not put real instance configs (`archive.yaml`, `networks/*.yaml`, `archive.db`) into this repo — they belong in the instance directory on the VM
- Do not delete `archive.db` in production — it is the source of truth for all request history
- Do not assume local staging is in use — the Gippsland instance writes directly to SMB
