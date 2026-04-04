# CLAUDE.md — SDS Archive Builder

## Project Purpose

Python tooling to build and maintain a local seismic waveform archive in **SDS (SeisComP Data Structure)** format. Data is pulled from remote **FDSN servers** (via ObsPy) and, in Phase 2, from **ASDF** files. The archive covers a geographically-bounded region in SE Australia / Victoria.

---

## Deployment Context

- **VM:** `dsand@172.26.149.194` (rs-l-pg2zyo.desktop.cloud.unimelb.edu.au), passwordless SSH
- **Conda env:** `sds_archive_builder` at `~/miniconda3`
- **Repo on VM:** `~/projects/sds_archive_builder`
- **Production instance:** `~/instances/gippsland`
- **Archive storage:** 2 TB SMB at `/mnt/sds_other_nets/gippsland` — direct write (no staging). `local_staging` and `sds_root` both point to `/mnt/sds_other_nets/gippsland`
- **Backfill start date:** 2025-01-01 — set in each `~/instances/gippsland/networks/*.yaml` as `history.start`; never pass `--start` on CLI
- **Current phase:** Active Archive Building — backfill every 3 days, daily 3×/day; switch to monthly BAU ~June 2026
- **Log timestamps:** AEDT (system local time), not UTC. Cron times also AEDT.
- **sqlite3:** Not on PATH — use `~/miniconda3/bin/sqlite3`
- **Environment management:** conda only — do not suggest venv or pip-only workflows
- **Git remote:** `https://github.com/SubSurfObs/sds_archive_builder.git`

---

## Two Primary Workflows

### 1. Historical archive build — `sds-backfill`

Walks a date range for all in-bounds stations. Respects both `success` records and `retry_after` scheduling. Re-runs are efficient — successful days are skipped immediately; `no_data`/`error` records past their retry window are retried. Run inside tmux.

### 2. Daily update — `sds-daily`

Pulls the last N days (`lookback_days: 3`) on each run. Key distinctions from backfill:

- **Respects `success`** — skips days already marked good
- **Ignores `retry_after` and `attempt_count`** (`ignore_retry_schedule=True`) — server backoff state accumulated during historical fills is irrelevant for yesterday's data
- **Runs `sds-verify` first** — scans the lookback window at a strict threshold (default 70% of median); resets partial files so they are re-fetched in the same run
- **Does not sweep historical retries** — `run_retries: false`; that belongs in `sds-backfill`

The daily job must not become a vehicle for re-processing the historical backlog. It handles only the recent window.

---

## Package vs. Instance

Generic installable tool (this repo). A deployment instance is a separate directory on the VM:

```
~/instances/gippsland/
    archive.yaml          # Paths, geo bounds, retry policy, daily settings
    networks/
        AM.yaml  AU.yaml  OZ.yaml  S1.yaml
    archive.db            # Request-tracking DB — never commit
    logs/
```

No instance config lives in this repo — templates only (`config/templates/`).

---

## Architecture

```
sds_archive_builder/
  config.py               # Load + validate instance YAML configs
  database.py             # SQLite via SQLAlchemy — stations, requests, server health
  geo_filter.py           # Bounding box station filtering

  clients/
    base.py               # Abstract: get_waveforms(), get_inventory()
    fdsn_client.py        # ObsPy FDSN with fallback + server health tracking
    asdf_client.py        # pyasdf → ObsPy Stream (Phase 2)

  archive/
    sds_writer.py         # Write ObsPy Stream → SDS; merge with existing files
    sds_query.py          # Query local SDS for day-level coverage
    sds_verify.py         # File integrity scan (size vs rolling median)

  runner/
    inventory_sync.py     # Fetch station metadata; apply geo filter; populate DB
    backfill.py           # Historical fill; ThreadPoolExecutor; respects retry schedule
    daily_update.py       # Daily pull; runs verify first; ignores retry schedule

scripts/
  init_instance.py        # Scaffold instance directory from templates
  sync_inventory.py       # CLI: sds-inventory
  run_backfill.py         # CLI: sds-backfill
  run_daily.py            # CLI: sds-daily
  verify_archive.py       # CLI: sds-verify
  audit_archive.py        # CLI: sds-audit
```

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Day-by-day granularity | Natural SDS unit; enables per-day tracking and retry |
| `no_data` ≠ permanent | Retrospective uploads expected; retry at 7d / 30d / 90d |
| `ignore_retry_schedule` in daily | Server backoff from historical fills must not block recent-day fetches |
| `sds-verify` in daily pipeline | Catches partial files before re-fetching; threshold 0.70 (stricter than standalone 0.20) |
| `run_retries: false` | Retry sweep belongs in backfill; enabling in daily caused 2h+ archive-scale sweeps |
| Zero-sample check in backfill | `npts=0` raises `NoDataError` — prevents 512-byte stubs being marked `success` |
| One `FDSNClient` per thread | `self._clients` dict is not thread-safe; each worker instantiates its own client |
| `write_stream` merges via `Stream.merge(method=1)` | Existing data preserved; file can only grow |
| Metadata start date = hint | Station epochs in metadata are sometimes wrong |
| Geographic filter at inventory sync | Filter once; don't re-check per waveform request |

---

## Database Schema

Three tables in `archive.db` (instance directory — never in this repo):

**`stations`** — one row per SEED channel ID (net.sta.loc.cha)
- `in_geo_bounds`: set at inventory sync; only in-bounds stations are fetched

**`fetch_requests`** — one row per (channel, day, server)
- `status`: `pending` | `success` | `no_data` | `error` | `rate_limited`
- `retry_after`: next scheduled attempt date (set for `no_data`/`error`)
- `attempt_count`: incremented on each try; backfill skips at ≥ 5 (daily ignores this)

**`server_health`** — adaptive backoff state per server
- `backoff_until`: set after failures; checked by `FDSNClient` before each request
- **Known issue:** `server_health` is shared between backfill and daily. An OZ/AUSPASS backoff from a historical fill can temporarily block the daily job. Backoffs expire automatically (max 2 hours); clear manually: `UPDATE server_health SET backoff_until=NULL WHERE server='AUSPASS'`

---

## sds-verify

Scans the archive for suspect files. Two failure modes:
- **512-byte stubs** — server returned empty MSEED; written before zero-sample check was added
- **Partial files** — server had incomplete data on first fetch

**Algorithm:**
- Pass 1 (fast): file size vs median of ±7 neighbouring days. Flag if < 4096 bytes absolute OR < `relative_threshold` × median. Channel size index built from ALL files so medians are accurate even in windowed scans.
- Pass 2 (`--full`): ObsPy sample count — separates empty from legitimately short files.

**`--fix`:** resets flagged records to `status=error, retry_after=today, attempt_count=1`. No file deletion — `write_stream` merges on re-fetch.

**Thresholds:**
- Standalone (`sds-verify` cron): `--threshold 0.20` (default) — catches near-empty stubs
- Daily pipeline: `verify_threshold: 0.70` in `archive.yaml` — catches materially partial files; configured per-instance

**Windowed scan:** pass `--days N` (CLI) or `since=` (Python API) to limit flagging to a recent window. Full channel size index is still built for accurate medians.

---

## Operational Phases

All cron times in AEDT (server local time).

**Active Archive Building:**
```cron
0 5 */3 * *     sds-verify --instance ~/instances/gippsland --fix
0 6 */3 * *     sds-backfill --instance ~/instances/gippsland --delay 1.0
0 7 * * 0       sds-inventory --instance ~/instances/gippsland
0 6,14,22 * * * sds-daily --instance ~/instances/gippsland --rsync
```

**BAU / Steady State:**
```cron
0 4 1 * *       sds-verify --instance ~/instances/gippsland --fix
0 5 1 * *       sds-inventory --instance ~/instances/gippsland
0 6 1 * *       sds-backfill --instance ~/instances/gippsland --delay 1.0
0 6,14,22 * * * sds-daily --instance ~/instances/gippsland --rsync
```

Switch by replacing `*/3` with `1 *`. `sds-daily` is identical in both phases.

Note: `sds-daily` also runs `sds-verify` internally (windowed, 70% threshold). The standalone cron entry above is a separate looser pass (20%) across the full archive before each backfill.

**Monitoring:** compare `SELECT network, status, count(*) FROM fetch_requests GROUP BY network, status` across successive backfill passes to quantify data recovery. Switch to BAU when error counts plateau.

---

## Concurrency

`run_backfill` uses `ThreadPoolExecutor(max_workers=archive_config.max_concurrent_requests)`. Default 2 workers → ~2 req/s (vs ~1 req/s serial). Each worker creates its own `FDSNClient`. Safe to raise to 4 for AU/AM; watch for 429s from AUSPASS (OZ/S1).

---

## Geographic Buffering

Inventory queries are expanded by `buffer_deg` (default 0.5°) around the target bounds. The tighter `geo_bounds` filter is applied post-fetch via `in_geo_bounds` in the stations table.

---

## Conventions

- All DB timestamps in **UTC**; log/cron times are AEDT on this VM
- SEED channel codes — never assume a channel exists without checking inventory
- SDS path: `{root}/{year}/{net}/{sta}/{cha}.D/{net}.{sta}.{loc}.{cha}.D.{year}.{julday}`
- Config files: YAML only
- No hardcoded network names, server URLs, or date ranges in Python source
- Log with Python `logging`; scripts set up handlers; library code uses `getLogger(__name__)`

---

## Phase 2: ASDF Integration

GA ran a 2-year Victoria deployment stored as ASDF on a supercomputer. `asdf_client.py` will accept a path to an ASDF HDF5 file, iterate waveforms via `pyasdf.ASDFDataSet`, and return `obspy.Stream` — identical interface to `fdsn_client.py`. Logged to `fetch_requests` with `server = "ASDF:<filename>"`.

---

## What NOT to Do

- Do not trust metadata `start_date` / `end_date` as definitive waveform availability
- Do not treat a failed FDSN response as proof that no data exists
- Do not hardcode geographic bounds — read from `archive.yaml`
- Do not use `pip install` alone — always update `environment.yml`
- Do not commit instance configs (`archive.yaml`, `networks/*.yaml`, `archive.db`) — instance directory only
- Do not delete `archive.db` in production — it is the source of truth for all request history
- Do not assume local staging is in use — Gippsland writes directly to SMB
