# SDS Archive Builder

A Python toolkit for building and maintaining a local seismic waveform archive in **SDS (SeisComP Data Structure)** format.

Data is retrieved from **FDSN servers** via [ObsPy](https://docs.obspy.org/) and written directly to SDS, with request tracking, server backoff, and scheduled retries for retrospective data uploads.

---

## Two primary workflows

### 1. Historical archive build (`sds-backfill`)

Walks a date range from `history.start` to the present for all in-bounds stations. Skips days already marked `success` in the DB. Records every attempt; schedules retries for `no_data` and `error` responses at increasing intervals (7 → 30 → 90 days). Re-runs of the same date range are efficient — successful days are skipped immediately.

Run inside `tmux` so it survives SSH disconnects:

```bash
tmux new -s backfill
sds-backfill --instance ~/instances/gippsland --delay 1.0
```

### 2. Daily update (`sds-daily`)

Pulls waveforms for the last N days (`lookback_days: 3`) on each run. Designed to be called from cron multiple times per day.

Key behaviours:
- **Respects success records** — does not re-download files already marked good
- **Ignores server backoff state** — `retry_after` and `attempt_count` from historical fills are irrelevant for yesterday's data; the daily job always attempts recent days
- **Runs `sds-verify` first** — scans the lookback window with a strict threshold (default 70% of median) and resets any partial files so they are re-fetched in the same run
- **Does not sweep historical retries** — that belongs in `sds-backfill`

---

## Package vs. Instance

This repository is a **generic, reusable tool**. A specific deployment (e.g. "Gippsland seismic archive") is an **instance** — a separate directory on your server containing only the config files and the live SQLite database.

```
# Generic tool — this repo (public, version controlled)
~/sds_archive_builder/

# Deployment instance — on your VM, not in this repo
~/instances/gippsland/
    archive.yaml       # Paths, geo bounds, retry policy, daily settings
    networks/
        AM.yaml
        AU.yaml
        OZ.yaml
        S1.yaml
    archive.db         # Live request-tracking DB — never commit this
    logs/
```

All scripts accept `--instance <path>` or read `SDS_ARCHIVE_INSTANCE` from the environment.

---

## Setup

```bash
conda env create -f environment.yml
conda activate sds_archive_builder
pip install -e .

# Scaffold a new instance directory from templates
sds-init ~/instances/myprojject
```

Edit `archive.yaml` (paths, geo bounds) and each `networks/*.yaml` (channels, servers, history start date).

---

## Crontab

### Active archive building phase

Run `sds-backfill` frequently to fill coverage aggressively. Server blocks from one pass are typically resolved within the next 3-day cycle.

```cron
# Integrity check + backfill every 3 days
0 5 */3 * *     sds-verify --instance ~/instances/gippsland --fix
0 6 */3 * *     sds-backfill --instance ~/instances/gippsland --delay 1.0

# Daily — 3x/day throughout all phases
0 6,14,22 * * * sds-daily --instance ~/instances/gippsland --rsync

# Weekly inventory refresh
0 7 * * 0       sds-inventory --instance ~/instances/gippsland
```

Note: `sds-daily` now runs `sds-verify` internally over its lookback window before each fetch. The standalone `sds-verify --fix` in the crontab above is a separate, looser pass (20% threshold) across the full archive before each backfill run.

### Steady state (BAU)

Once the archive is built out, switch to monthly infill:

```cron
0 4 1 * *       sds-verify --instance ~/instances/gippsland --fix
0 5 1 * *       sds-inventory --instance ~/instances/gippsland
0 6 1 * *       sds-backfill --instance ~/instances/gippsland --delay 1.0
0 6,14,22 * * * sds-daily --instance ~/instances/gippsland --rsync
```

The only change from active phase to BAU is frequency (`*/3` → `1 *`). The `sds-daily` line is identical in both.

---

## Commands

| Command | Description |
|---------|-------------|
| `sds-init` | Scaffold a new instance directory from config templates |
| `sds-inventory` | Fetch station metadata; apply geo filter; populate DB |
| `sds-backfill` | Historical fill from `history.start` to present |
| `sds-daily` | Pull recent days; run integrity check; ignore historical backoff |
| `sds-verify` | Scan archive for suspect files; optionally reset DB records |
| `sds-audit` | Read-only coverage and retry-candidate report |

### sds-verify options

```bash
# Full archive scan (default threshold: 20% of median)
sds-verify --instance ~/instances/gippsland --fix

# Strict threshold — flag files below 70% of median
sds-verify --instance ~/instances/gippsland --threshold 0.70 --fix

# Recent window only
sds-verify --instance ~/instances/gippsland --days 7 --threshold 0.70 --fix

# Add ObsPy sample-count check on flagged files
sds-verify --instance ~/instances/gippsland --full
```

---

## Network configuration

Each file in `<instance>/networks/` configures one seismic network:

```yaml
network: AU
description: "Geoscience Australia broadband network"

channels: ["HHZ", "HHN", "HHE", "BHZ", "BHN", "BHE"]
location_codes: ["", "00", "10"]

servers:
  primary: "AUSPASS"
  fallback: "IRIS"

history:
  start: "2000-01-01"    # Earliest date sds-backfill will attempt
  data_lag_days: 2       # Data may appear up to N days after real-time

retry_policy:            # Optional — inherits from archive.yaml if absent
  max_attempts: 5
  retry_after_days: [7, 30, 90]
```

---

## Request tracking

Every request (station × day × server) is logged to `archive.db`. Possible statuses:

| Status | Meaning |
|--------|---------|
| `success` | Data received and written to SDS |
| `no_data` | Server responded: no waveforms for this request |
| `error` | Request failed (timeout, HTTP error, etc.) |
| `rate_limited` | Server returned 429 |

**`no_data` is never treated as permanent.** Some servers (S1/AusArray, OZ/AUSPASS) add historical data retrospectively. Every `no_data` response is scheduled for retry at 7, 30, and 90 day intervals. `sds-backfill` picks these up automatically on each run.

---

## Direct database queries

```bash
SQLITE=~/miniconda3/bin/sqlite3   # sqlite3 may not be on PATH
DB=~/instances/gippsland/archive.db

# Status counts by network
$SQLITE $DB "SELECT network, status, count(*) FROM fetch_requests GROUP BY network, status"

# Clear server backoff (if a server recovered but DB still shows it as backed off)
$SQLITE $DB "UPDATE server_health SET backoff_until=NULL, consecutive_failures=0 WHERE server='AUSPASS'"

# Reset error records for recent dates to retry immediately
$SQLITE $DB "UPDATE fetch_requests SET retry_after=date('now'), attempt_count=1
             WHERE status='error' AND day >= date('now','-7 days')"
```

---

## Write strategy

**Direct to SMB** (recommended when storage is ample):
```yaml
# archive.yaml — set both to the same mount path
sds_root: "/mnt/sds_other_nets/gippsland"
local_staging: "/mnt/sds_other_nets/gippsland"
```

**Two-step via local staging** (when SMB is unreliable or local disk is fast):
```yaml
sds_root: "/mnt/smb/archive"
local_staging: "/scratch/sds_staging"
```
Use `sds-daily --rsync` to push staging → archive after each run.

---

## Phase 2: ASDF support

Geoscience Australia ran a 2-year deployment across Victoria stored in ASDF format. `asdf_client.py` (Phase 2) will read these files via [pyasdf](https://github.com/SeismicData/pyasdf) and ingest waveforms through the same pipeline, logged to `fetch_requests` with `server = "ASDF:<filename>"`.

---

## Dependencies

- [ObsPy](https://docs.obspy.org/) — FDSN client, waveform I/O
- [SQLAlchemy](https://www.sqlalchemy.org/) — request tracking DB
- [PyYAML](https://pyyaml.org/) — configuration
- [Click](https://click.palletsprojects.com/) — CLI
- [pyasdf](https://github.com/SeismicData/pyasdf) — ASDF (Phase 2)

---

## License

MIT
