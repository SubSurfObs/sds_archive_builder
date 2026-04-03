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

### 1. Install

```bash
conda env create -f environment.yml
conda activate sds_archive_builder
pip install -e .
```

### 2. Initialise an instance

An **instance** is a directory on your server holding config and the tracking database for one deployment. It never goes in this repo.

```bash
sds-init ~/instances/victoria
```

This copies the config templates into the instance directory with inline documentation.

### 3. Edit the instance config

`~/instances/victoria/archive.yaml` — set at minimum:

```yaml
sds_root: "/home/user/mnt/SDS/victoria"   # Final archive (SMB mount)
local_staging: "/home/user/scratch/staging" # Local VM disk — fast, temporary

geo_bounds:
  min_lat: -39.0
  max_lat: -36.0
  min_lon: 144.5
  max_lon: 149.0
  buffer_deg: 0.5   # Inventory query is expanded by this — filtered back afterwards
```

Then edit each file in `~/instances/victoria/networks/`. Key fields:

```yaml
channels: ["HHZ", "HHN", "HHE", "BHZ", "BHN", "BHE"]  # High-rate seismic only
servers:
  primary: "EARTHSCOPE"
  fallback: "https://auspass.edu.au"
history:
  start: "2000-01-01"   # Earliest date to attempt backfill
```

Channel naming follows SEED convention — band code first:
`H`/`B`/`E`/`S` = high-rate seismic; `L` = long period (exclude); `N` = accelerometer (exclude).

### 4. Sync station inventory

```bash
sds-inventory --instance ~/instances/victoria
```

Fetches station metadata from all configured servers, applies the geographic filter, and populates the database. **Re-run this whenever you want to pick up new or recently added stations** (see [New stations](#new-stations)).

### 5. Test with a short window

Before committing to a full backfill, run a bounded test. This writes to a temporary directory and never touches the main archive:

```bash
sds-backfill \
  --instance ~/instances/victoria \
  --start 2024-01-01 --end 2024-01-07 \
  --testing -v
```

Then audit what came back:

```bash
sds-audit --instance ~/instances/victoria --start 2024-01-01
```

### 6. Production backfill

Run inside `tmux` or `screen` so it survives SSH disconnects:

```bash
tmux new -s backfill
sds-backfill --instance ~/instances/victoria --start 2015-01-01 --delay 1.0
# Ctrl-B D to detach; tmux attach -t backfill to return
```

**Scale guide** (~90 channels across 4 networks):

| Period | Requests | Wall time (est.) |
|--------|----------|-----------------|
| 1 year | ~33,000 | ~1–2 days |
| 5 years | ~165,000 | ~5–10 days |
| 10 years | ~330,000 | ~10–20 days |

`no_data` responses return immediately (< 1s), so gaps in early years of S1/AM move fast.
The default `newest_first` mode means recent data is usable quickly while older years fill in.

**rsync to the main archive periodically during long backfills** — local staging disk is finite:

```bash
rsync -av --checksum ~/scratch/staging/ ~/mnt/SDS/victoria/
```

### 7. Schedule ongoing updates

Add to crontab (`crontab -e`) on the VM:

```cron
# Daily — pull recent data + process retries + rsync to SMB (08:00 UTC)
0 8 * * * SDS_ARCHIVE_INSTANCE=/home/user/instances/victoria \
  /home/user/miniconda3/envs/sds_archive_builder/bin/sds-daily \
  --rsync >> /home/user/instances/victoria/logs/cron.log 2>&1

# Weekly — refresh station inventory to pick up new stations (Sunday 07:00 UTC)
0 7 * * 0 SDS_ARCHIVE_INSTANCE=/home/user/instances/victoria \
  /home/user/miniconda3/envs/sds_archive_builder/bin/sds-inventory \
  >> /home/user/instances/victoria/logs/cron.log 2>&1

# Monthly — re-sweep full history to catch retrospectively uploaded data
0 6 1 * * SDS_ARCHIVE_INSTANCE=/home/user/instances/victoria \
  /home/user/miniconda3/envs/sds_archive_builder/bin/sds-backfill \
  --start 2015-01-01 --delay 1.0 \
  >> /home/user/instances/victoria/logs/cron.log 2>&1
```

### 8. Check on progress

**While a backfill is running:**

```bash
# Attach to the tmux session to watch live output
tmux attach -t backfill
# Ctrl-B D to detach without stopping it

# Or tail the log file
tail -f ~/instances/gippsland/logs/backfill.log
```

**Quick DB status — how many requests in each state:**

```bash
sqlite3 ~/instances/gippsland/archive.db \
  "SELECT status, count(*) FROM fetch_requests GROUP BY status"
```

**Coverage report for a date range:**

```bash
sds-audit --instance ~/instances/gippsland --start 2025-01-01
```

**Show gaps and retries:**

```bash
sds-audit --instance ~/instances/gippsland --start 2025-01-01 \
  --show-missing --show-retries
```

**Disk usage:**

```bash
df -h /mnt/sds_other_nets/gippsland
du -sh /mnt/sds_other_nets/gippsland
```

**Cron log:**

```bash
tail -f ~/instances/gippsland/logs/cron.log
```

---

## Direct Database Manipulation

The SQLite database is plain SQL — you can inspect and patch state directly when needed.
Use `~/miniconda3/bin/sqlite3` on the VM (sqlite3 may not be on PATH).

```bash
DB=~/instances/gippsland/archive.db
SQLITE=~/miniconda3/bin/sqlite3
```

**Inspect retry state by network:**
```bash
$SQLITE $DB "SELECT network, status, attempt_count, retry_after, count(*)
             FROM fetch_requests WHERE status='no_data'
             GROUP BY network, attempt_count, retry_after ORDER BY network, attempt_count"
```

**Reset inflated retry dates** (e.g. after testing pushed attempt_count up):
```bash
# All no_data records back to 7 days / attempt_count=1
$SQLITE $DB "UPDATE fetch_requests SET attempt_count=1, retry_after=date('now','+7 days')
             WHERE status='no_data'"

# Single network only
$SQLITE $DB "UPDATE fetch_requests SET attempt_count=1, retry_after=date('now','+7 days')
             WHERE status='no_data' AND network='AM'"
```
> Note: resetting `retry_after` without also resetting `attempt_count` is insufficient —
> the next failed attempt will immediately re-push to the 90-day window.

**Clear server backoff** (if a server recovered but the DB still shows it as backed off):
```bash
$SQLITE $DB "SELECT server, consecutive_failures, backoff_until FROM server_health"

# Reset a specific server
$SQLITE $DB "UPDATE server_health SET backoff_until=NULL, consecutive_failures=0
             WHERE server='AUSPASS'"
```

**Force immediate retry of error records for recent dates:**
```bash
$SQLITE $DB "UPDATE fetch_requests SET retry_after=date('now')
             WHERE status='error' AND day >= date('now','-7 days')"
```

**Known state gotcha — shared server backoff:**
The `server_health` table is shared between the backfill and daily jobs. If the backfill
triggers a server backoff (e.g. AUSPASS after repeated failures), the daily sweep inherits
it. Backoffs expire automatically (max 2 hours), but you can clear them manually with the
query above if the daily job is being blocked.

---

## Key Concepts

### The tracking database (`archive.db`)

Every waveform request — success or failure — is recorded in `archive.db` in the instance directory. This is the **source of truth** for what has been attempted and what has been written.

- **Treat it as precious.** Back it up alongside the SDS archive.
- **Never delete it** in production. Without it, the backfill loses all memory of what's been done and will re-request everything.
- Re-running a backfill over an already-completed date range is safe and efficient: successful days are skipped immediately; only `no_data`/`error` records past their retry window generate new requests.

### Retries

`no_data` does not mean no data exists — upstream servers for networks like S1 (AusArray) are actively adding historical data. Every `no_data` response is scheduled for retry at increasing intervals (default: 7 → 30 → 90 days).

Retries are activated by two things:
- **`sds-daily`** — processes all due retries automatically on each run
- **Re-running `sds-backfill`** over the same date range — once a retry window has elapsed, the record is no longer skipped

The monthly cron entry above ensures historical gaps are re-swept as new data comes online.

### New stations

New stations — whether newly deployed or newly added to an FDSN server — are picked up by re-running `sds-inventory`. The weekly cron entry handles this automatically. After a new station appears in the DB:

- The **daily job** will fetch recent days for it automatically
- Historical data for it will be fetched on the next **monthly backfill sweep**
- Or immediately with a manual `sds-backfill --start <earliest date>`

### Write strategy

Two modes are supported depending on available disk space:

**Direct to SMB** (recommended when storage is ample — e.g. 2 TB allocation):

```yaml
# archive.yaml — set both to the same SMB path
sds_root: "/mnt/sds_other_nets/gippsland"
local_staging: "/mnt/sds_other_nets/gippsland"
```

No rsync step needed. Simpler, and fine as long as the SMB mount is reliable.

**Two-step via local staging** (use when local disk is small or SMB is unreliable):

```yaml
sds_root: "/mnt/smb/archive"
local_staging: "/scratch/sds_staging"   # fast local disk
```

```
FDSN Server → local_staging → rsync → sds_root
```

The `--rsync` flag on `sds-daily` triggers the sync step automatically.

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

## Operational Phases

A deployment moves through three phases over its lifetime, each with different cron scheduling:

### Phase 1 — Active Archive Building (weeks to months)

Run `sds-backfill` frequently to fill the archive aggressively. Server blocks from one run are
resolved within a few days by the next. Disable this cron entry once you're satisfied with coverage.

```cron
# Active backfill — every 3 days
0 6 */3 * * SDS_ARCHIVE_INSTANCE=... sds-backfill --delay 1.0 >> .../logs/cron.log 2>&1

# Daily update — 3x/day throughout all phases
0 6,14,22 * * * SDS_ARCHIVE_INSTANCE=... sds-daily --rsync >> .../logs/cron.log 2>&1

# Weekly inventory refresh — keep during active phase to pick up new stations
0 7 * * 0 SDS_ARCHIVE_INSTANCE=... sds-inventory >> .../logs/cron.log 2>&1
```

**Monitoring active building:** run `sds-audit` before and after each backfill pass to track
coverage improvement. Compare DB status counts to watch errors being resolved:

```bash
sqlite3 ~/instances/gippsland/archive.db \
  "SELECT network, status, count(*) FROM fetch_requests GROUP BY network, status"
```

### Phase 2 — Steady State (ongoing)

Once the archive is built out, switch to monthly infill. The daily job handles near-real-time
data; the monthly sweep catches retrospectively uploaded historical data.

```cron
# Daily update — unchanged
0 6,14,22 * * * SDS_ARCHIVE_INSTANCE=... sds-daily --rsync >> .../logs/cron.log 2>&1

# Monthly infill — inventory first, then backfill
0 5 1 * * SDS_ARCHIVE_INSTANCE=... sds-inventory >> .../logs/cron.log 2>&1
0 6 1 * * SDS_ARCHIVE_INSTANCE=... sds-backfill --delay 1.0 >> .../logs/cron.log 2>&1
```

To switch from Phase 1 to Phase 2, replace `0 6 */3 * *` with `0 6 1 * *` in the crontab,
and add the inventory line above it.

### Backfill start date

Set `history.start` in each network YAML — this is the earliest date `sds-backfill` will
attempt when no `--start` flag is given. Update it in the instance `networks/*.yaml` files,
not in the code or cron command.

### CLI Modes

| Mode | Description |
|------|-------------|
| `testing` | Bounded time window, verbose output, writes to temp dir only |
| `backfill` | Fill historical data from `history.start` to present |
| `daily` | Pull recent days; recheck recent successes to merge partial data; process retries |
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
