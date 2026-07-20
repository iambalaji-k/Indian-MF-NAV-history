# Indian Mutual Fund NAV History

## 🔗 Live Website

You can view the latest NAV of all Indian Mutual Funds at: [Indian MF NAV History Dashboard](https://balajik.in/Indian-MF-NAV-history/)

> Self-updating historical NAV archive for Indian mutual funds using AMFI's `NAVAll.txt` feed, SQLite, Cloudflare R2, GZIP compressed daily exports, and GitHub Actions.
> 
> The project is an append-only time-series warehouse with scheme metadata tracking, duplicate protection, inactive-scheme detection, and GZIP storage optimization. SQLite databases are stored durably in Cloudflare R2. Repository-friendly GZIP daily CSV exports (`nav_YYYY-MM-DD.csv.gz`) and metadata mappings (`schemes.json.gz`) are committed to Git.

---

## What This Builds

- **Financial-year SQLite archives in R2**: `db/nav_fy_YYYY_YY.db`
- **Rolling backups for each financial-year DB in R2**: `db/nav_fy_YYYY_YY.db.bak1` and `db/nav_fy_YYYY_YY.db.bak2`
- **Daily GZIP NAV CSV exports in Git**: `data/YYYY/MM/nav_YYYY-MM-DD.csv.gz` (~53 KB per day)
- **Scheme metadata GZIP JSON mapping in Git**: `data/schemes.json.gz` (~133 KB)
- **Modern Dark-Mode Frontend Explorer**: `index.html` (hosted on GitHub Pages)
- **Daily automation through GitHub Actions**
- **Validation checks for suspicious NAV data**
- **Unit test suite for parsing, GZIP compression, duplicates, R2 sync, and validation**

---

## Data Source

AMFI publishes the official source feed here:

```text
https://portal.amfiindia.com/spages/NAVAll.txt
```

The feed is semicolon-separated and follows this shape:

```text
Scheme Code;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date
```

The file includes category headings, blank lines, and non-data rows. This project keeps only rows that start with a numeric scheme code.

---

## Important Data Rule

This archive intentionally ignores all NAV rows dated before:

```text
2026-04-01
```

Rows dated before 1 April 2026 are treated as discontinued or out-of-scope historical data, even if they still appear in the latest AMFI file.

---

## Repository Layout

```text
.
|-- .github/
|   `-- workflows/
|       `-- update.yml
|-- data/
|   |-- YYYY/
|   |   `-- MM/
|   |       `-- nav_YYYY-MM-DD.csv.gz
|   `-- schemes.json.gz
|-- scripts/
|   |-- fetch_and_update.py
|   |-- r2_storage.py
|   |-- schema.sql
|   `-- validator.py
|-- tests/
|   |-- test_fetch_and_update.py
|   |-- test_r2_storage.py
|   `-- test_validator.py
|-- index.html
|-- .env.example
`-- README.md
```

SQLite `.db` files may exist locally while the updater runs, but `data/*.db` is ignored by Git. The durable master DB copy lives in Cloudflare R2.

---

## Database Design

Every SQLite database uses the same schema.

### `schemes`

Dimension table for scheme metadata.

Tracks:
- `scheme_code`
- ISIN fields (`isin_payout_or_growth`, `isin_reinvestment`)
- current `scheme_name`
- `first_seen_date`
- `last_seen_date`
- `is_active`
- audit timestamps

If a scheme name changes, the latest name is updated against the same scheme code.

### `nav_history`

Append-only NAV fact table.

Tracks:
- `scheme_code`
- `nav_date`
- `nav`
- `ingested_at`

NAV values are parsed as Python `Decimal`, rounded to exactly four decimal places, and stored as canonical text such as `12.3456`. Duplicate protection is enforced with `UNIQUE (scheme_code, nav_date)`.

---

## Financial Year Storage

The archive writes incoming data to two outputs:

1. **Matching financial-year SQLite DB**, uploaded to R2 as `db/nav_fy_YYYY_YY.db`.
2. **Run-specific GZIP NAV CSV**, stored in Git as `data/YYYY/MM/nav_YYYY-MM-DD.csv.gz`.

Indian financial years are calculated from 1 April to 31 March.

| NAV Date | R2 DB |
|---|---|
| `2026-04-01` | `db/nav_fy_2026_27.db` |
| `2027-03-31` | `db/nav_fy_2026_27.db` |
| `2027-04-01` | `db/nav_fy_2027_28.db` |

---

## Git Storage & GZIP Optimization (Normalized Option B)

To prevent repository bloat over time, the project uses a **normalized 2-file architecture**:

1. **Daily NAV Snapshots (`data/YYYY/MM/nav_YYYY-MM-DD.csv.gz`)**:
   - Contains 3 compact columns: `scheme_code,nav,nav_date`.
   - Compressed with GZIP down to **~53 KB per day** (**95% smaller** than raw CSVs).

2. **Scheme Metadata Mapping (`data/schemes.json.gz`)**:
   - Maps scheme codes to names and ISINs:
     `{"100033": ["INF209K01165", "-", "Aditya Birla Sun Life Large Cap Fund"]}`
   - Compressed with GZIP down to **~133 KB**.

### Frontend Integration (`index.html`)

The dark-mode web dashboard automatically loads the latest snapshot using **Method 2**:
* Computes current date in Indian Standard Time (IST).
* Attempts to fetch `data/YYYY/MM/nav_YYYY-MM-DD.csv.gz` with date fallback logic (looking back up to 14 days for weekends/holidays).
* Decompresses `schemes.json.gz` and `nav_YYYY-MM-DD.csv.gz` natively in browser JavaScript using `DecompressionStream('gzip')`.
* Merges metadata in memory for instant $O(1)$ search across scheme names, codes, and ISINs.

---

## Cloudflare R2 Setup

Create an R2 bucket and an R2 API token with object read/write permissions for that bucket.

Copy the example environment file:

```powershell
Copy-Item .env.example .env
```

Fill in:

```text
R2_ACCOUNT_ID=...
R2_BUCKET=...
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_PREFIX=indian-mf-nav-history
```

---

## Local Usage

Run from the repository root:

### Run Tests

```powershell
python -m unittest discover -s tests
```

### Fetch Live AMFI Data Without R2 Sync

```powershell
python scripts/fetch_and_update.py
```

### Fetch Live AMFI Data With R2 Sync

```powershell
python scripts/fetch_and_update.py --r2-sync
```

With `--r2-sync`, the updater:
1. Acquires the R2 lock at `lock/nav.lock`
2. Downloads relevant yearly SQLite DBs from R2
3. Applies latest AMFI rows using batched SQLite writes
4. Generates daily GZIP CSV at `data/YYYY/MM/nav_YYYY-MM-DD.csv.gz`
5. Updates `data/schemes.json.gz` from the yearly DB
6. Validates modified SQLite DBs
7. Atomically uploads modified DBs to R2 and rotates backups
8. Releases the R2 lock

### Preview Frontend Locally

```powershell
python -m http.server 8000
```
Open `http://localhost:8000` in your browser.

---

## GitHub Actions Automation

The workflow (`.github/workflows/update.yml`) runs daily at 13:30 UTC (~7 PM IST):

1. Performs shallow checkout (`fetch-depth: 1`)
2. Runs `scripts/fetch_and_update.py --r2-sync`
3. Runs `scripts/validator.py`
4. Stages `data/**/*.csv.gz` and `data/schemes.json.gz`
5. Commits and pushes changes only if dataset files changed

---

## Requirements

Python standard library modules only:
- `sqlite3`
- `urllib`
- `csv`
- `gzip`
- `json`
- `logging`
- `unittest`

No third-party Python package is required.
