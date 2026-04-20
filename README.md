# Indian Mutual Fund NAV History

## 🔗 Live website

You can view the latest NAV of all Indian Mutual Funds at: [Indian MF NAV History Dashboard](https://balajik.work.gd/Indian-MF-NAV-history/)

> Self-updating historical NAV archive for Indian mutual funds using AMFI's `NAVAll.txt` feed, SQLite, Cloudflare R2, CSV exports, and GitHub Actions.
> 
> The project is an append-only time-series warehouse with scheme metadata tracking, duplicate protection, inactive-scheme detection, and schema evolution support. SQLite databases are stored in Cloudflare R2. Repository-friendly CSV exports are committed to Git.

## What This Builds

- Combined SQLite archive in R2: `db/nav.db`
- Two rolling master DB backups in R2: `db/nav.db.bak1` and `db/nav.db.bak2`
- Financial-year SQLite archives in R2: `db/nav_fy_YYYY_YY.db`
- Daily run NAV CSV exports in Git: `data/YYYY/MM/nav_YYYY-MM-DD.csv`
- Scheme metadata dimension CSV in Git: `data/schemes.csv`
- Latest NAV snapshot in Git: `latest_nav.csv`
- Daily automation through GitHub Actions
- Validation checks for suspicious NAV data
- Tests for parsing, duplicates, inactive schemes, R2 configuration, CSV export, and validation

## Data Source

AMFI publishes the source feed here:

```text
https://portal.amfiindia.com/spages/NAVAll.txt
```

The feed is semicolon-separated and usually follows this shape:

```text
Scheme Code;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date
```

The file also includes category headings, blank lines, and other non-data rows. This project keeps only rows that start with a numeric scheme code.

## Important Data Rule

This archive intentionally ignores all NAV rows before:

```text
2026-04-01
```

Rows dated before 1 April 2026 are treated as discontinued or out-of-scope historical data, even if they still appear in the latest AMFI file.

## Repository Layout

```text
.
|-- .github/
|   `-- workflows/
|       `-- update.yml
|-- data/
|   |-- YYYY/
|   |   `-- MM/
|   |       `-- nav_YYYY-MM-DD.csv
|   `-- schemes.csv
|-- scripts/
|   |-- fetch_and_update.py
|   |-- r2_storage.py
|   |-- schema.sql
|   `-- validator.py
|-- tests/
|   |-- test_fetch_and_update.py
|   |-- test_r2_storage.py
|   `-- test_validator.py
|-- .env.example
|-- latest_nav.csv
`-- README.md
```

SQLite DB files may exist locally while the updater runs, but `data/*.db` is ignored by Git. The durable DB copy lives in Cloudflare R2.

## Database Design

Every SQLite database uses the same schema.

### `schemes`

Dimension table for scheme metadata.

Tracks:

- `scheme_code`
- ISIN fields
- current `scheme_name`
- `first_seen_date`
- `last_seen_date`
- `is_active`
- audit timestamps

If a scheme name changes, the latest name is stored against the same scheme code.

### `nav_history`

Append-only NAV fact table.

Tracks:

- `scheme_code`
- `nav_date`
- `nav`
- `ingested_at`

NAV values are parsed as Python `Decimal`, rounded to exactly four decimal places, and stored as canonical text such as `12.3456`. This avoids binary floating-point drift while keeping SQLite queries simple.

Duplicate protection is enforced with:

```sql
UNIQUE (scheme_code, nav_date)
```

This allows safe repeated runs of the updater.

### `schema_metadata`

Small metadata table used for schema versioning and future migrations.

### Indexes

The schema creates indexes for the main access paths:

- `scheme_code + nav_date` for one-scheme NAV history
- `nav_date + scheme_code` for date-wise snapshots and range queries
- `scheme_code + nav_date + nav` for covering NAV history reads
- `is_active + scheme_name` for active scheme listings
- `scheme_name` for scheme search/order operations
- `last_seen_date + is_active` for inactive-scheme detection

## Financial Year Storage

The archive writes each row to two outputs:

1. Combined SQLite DB, uploaded to R2 as `db/nav.db`
2. Matching financial-year SQLite DB, uploaded to R2 as `db/nav_fy_YYYY_YY.db`

Each daily run also generates a separate CSV export:

3. Run-specific NAV CSV, stored in Git as `data/YYYY/MM/nav_YYYY-MM-DD.csv`

Indian financial years are calculated from 1 April to 31 March.

| NAV Date | R2 DB |
|---|---|
| `2026-04-01` | `db/nav_fy_2026_27.db` |
| `2027-03-31` | `db/nav_fy_2026_27.db` |
| `2027-04-01` | `db/nav_fy_2027_28.db` |

## Cloudflare R2 Setup

Create an R2 bucket and an R2 API token with object read/write permissions for that bucket.

For local runs, copy the example environment file:

```powershell
Copy-Item .env.example .env
```

Then fill in:

```text
R2_ACCOUNT_ID=...
R2_BUCKET=...
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_PREFIX=indian-mf-nav-history
```

The updater uses Cloudflare R2's S3-compatible API directly with Python standard library code. No third-party package is required.

Objects are stored under the optional prefix:

```text
<R2_PREFIX>/db/nav.db
<R2_PREFIX>/db/nav.db.bak1
<R2_PREFIX>/db/nav.db.bak2
<R2_PREFIX>/db/nav_fy_2026_27.db
<R2_PREFIX>/lock/nav.lock
```

If `R2_PREFIX` is blank, objects are stored as:

```text
db/nav.db
db/nav.db.bak1
db/nav.db.bak2
db/nav_fy_2026_27.db
lock/nav.lock
```

## Local Usage

Run from the repository root.

### Run Tests

```powershell
venv\Scripts\python.exe -m unittest discover -s tests
```

### Compile Check

```powershell
venv\Scripts\python.exe -m compileall scripts tests
```

### Fetch Live AMFI Data Without R2 Sync

```powershell
venv\Scripts\python.exe scripts\fetch_and_update.py
```

This updates local DB working files and CSV exports only.

### Fetch Live AMFI Data With R2 Sync

```powershell
venv\Scripts\python.exe scripts\fetch_and_update.py --r2-sync
```

With `--r2-sync`, the updater:

1. Loads `.env` if present
2. Acquires the R2 lock at `lock/nav.lock`
3. Downloads the relevant SQLite DBs from R2
4. Records the SHA-256 hash of the current databases
5. Applies the latest AMFI rows using batched SQLite writes
6. Generates a daily run CSV at `data/YYYY/MM/nav_YYYY-MM-DD.csv`
7. Rewrites `data/schemes.csv` from the combined DB
8. Exports `latest_nav.csv`
9. Validates each SQLite DB if it has changed
10. Uploads each modified DB through a temp object and promotes it after verification
11. Rotates `db/nav.db.bak1` and `db/nav.db.bak2` ONLY if the master database changed
12. Releases the R2 lock

Local outputs:

```text
data/nav.db
data/nav_fy_YYYY_YY.db
data/YYYY/MM/nav_YYYY-MM-DD.csv
data/schemes.csv
latest_nav.csv
logs/update.log
```

The `.db` files are local working copies and are ignored by Git.

### Validate the Archive

```powershell
venv\Scripts\python.exe scripts\validator.py
```

Validate a specific FY database:

```powershell
venv\Scripts\python.exe scripts\validator.py --db data\nav_fy_2026_27.db
```

## Updater Behavior

The updater does the following:

1. Fetches AMFI `NAVAll.txt`
2. Retries up to 3 times
3. Uses a 10 second timeout
4. Removes blank lines and category headings
5. Keeps only numeric scheme-code rows
6. Splits rows by semicolon
7. Requires exactly 6 columns
8. Validates NAV as a number
9. Validates NAV date
10. Converts NAV to `Decimal` with exactly four decimal places
11. Ignores rows before `2026-04-01`
12. Downloads matching DBs from R2 when `--r2-sync` is enabled
13. Upserts scheme metadata in batches
14. Inserts NAV facts in batches with `INSERT OR IGNORE`
15. Marks schemes inactive if not seen for more than 30 days
16. Writes the results of the daily run to `data/YYYY/MM/nav_YYYY-MM-DD.csv`
17. Rewrites `data/schemes.csv` from the combined database
18. Exports `latest_nav.csv`
19. Validates changed SQLite databases before upload
20. Uploads changed SQLite databases atomically to R2 when `--r2-sync` is enabled

R2 operations use retry logic for transient network/server failures.

Bad rows are logged and skipped. A single malformed AMFI row should not crash the daily update.

## CSV Outputs

CSV storage is organized by run date to avoid large Git diffs in a single file and provide clear daily snapshots.

### NAV Fact CSVs

`latest_nav.csv` contains the valid NAV rows from the most recent AMFI fetch after filtering and validation.

Daily run CSVs contain the results of each specific update. They are stored in a nested directory structure:

```text
data/YYYY/MM/nav_YYYY-MM-DD.csv
```

NAV fact CSVs use this compact column format:

```text
scheme_code,nav,nav_date
```

### Scheme Dimension CSV

`data/schemes.csv` stores scheme metadata separately:

```text
scheme_code,isin_payout_or_growth,isin_reinvestment,scheme_name,first_seen_date,last_seen_date,is_active
```

Join daily CSVs to `data/schemes.csv` on `scheme_code` when metadata is needed.

CSV files are meant for Git storage, inspection, spreadsheet use, and lightweight downstream jobs. SQLite databases remain the canonical query storage.

## R2 Safety Model

The R2 sync path is designed to avoid partial or redundant updates:

- Concurrency lock: `lock/nav.lock`
- Hash-based upload: only modified databases are uploaded to R2
- Temp upload object: `db/nav.db.tmp` or `db/nav_fy_YYYY_YY.db.tmp`
- Verification: temp object must exist before promotion
- Promotion: temp object is copied over the final DB key
- Cleanup: temp object is deleted after final verification
- Master backups: before replacing `db/nav.db`, the updater rotates backups ONLY if the content has changed:
  - `db/nav.db.bak1` to `db/nav.db.bak2`
  - `db/nav.db` to `db/nav.db.bak1`

SQLite validation runs before any DB upload. If validation fails, upload is blocked.

## GitHub Actions Automation

The workflow runs daily:

```yaml
schedule:
  - cron: "30 13 * * *"
```

This is approximately 7 PM IST.

The workflow:

1. Checks out the repo
2. Sets up Python
3. Runs `scripts/fetch_and_update.py --r2-sync`
4. Runs `scripts/validator.py`
5. Commits only if CSV outputs changed

Required GitHub repository secrets:

```text
R2_ACCOUNT_ID
R2_BUCKET
R2_ACCESS_KEY_ID
R2_SECRET_ACCESS_KEY
```

Optional secret:

```text
R2_PREFIX
```

Commit message:

```text
Daily NAV update
```

Tracked update outputs:

```text
data/*.csv
latest_nav.csv
```

## Query Examples

For local querying, first run the updater with `--r2-sync` so the current R2 database is downloaded.

Open the combined database:

```powershell
sqlite3 data\nav.db
```

Latest NAV date in the archive:

```sql
SELECT MAX(nav_date) FROM nav_history;
```

NAV history for one scheme:

```sql
SELECT nav_date, nav
FROM nav_history
WHERE scheme_code = 100001
ORDER BY nav_date;
```

Currently active schemes:

```sql
SELECT scheme_code, scheme_name, last_seen_date
FROM schemes
WHERE is_active = 1
ORDER BY scheme_name;
```

Schemes not seen recently:

```sql
SELECT scheme_code, scheme_name, last_seen_date
FROM schemes
WHERE is_active = 0
ORDER BY last_seen_date;
```

## Validation Checks

`scripts/validator.py` checks for:

- Required schema tables
- Duplicate NAV facts
- Long NAV gaps
- Sudden NAV jumps above 50%

Warnings such as long gaps or NAV jumps are logged for review. Structural problems fail validation.

## Test Coverage

The test suite covers:

- Empty file
- Corrupt rows
- Duplicate run on the same input
- New scheme appearing
- Scheme disappearing and becoming inactive
- NAV date routed to the correct financial year
- Pre-2026-04-01 data being ignored
- Index creation
- Daily run CSV export with nested folders
- Separate schemes dimension CSV export
- R2 environment and object-key behavior
- R2 retry behavior
- R2 lock object behavior
- Atomic upload and master backup rotation (only on change)
- Validation-before-upload
- Decimal NAV quantization and REAL-to-TEXT migration
- Validator success and failure cases

Run:

```powershell
venv\Scripts\python.exe -m unittest discover -s tests
```

## Design Notes

- The archive is append-only for NAV facts in SQLite.
- Schemes are never deleted.
- NAV rows are not assumed to arrive daily for every scheme.
- Backdated NAV rows are accepted if they are on or after `2026-04-01`.
- Duplicate facts are ignored using the database constraint.
- SQLite writes are batched for update performance.
- Scheme names are refreshed because AMFI can rename schemes under the same scheme code.
- Discontinued schemes are inferred by absence, not by deletion.
- SQLite DBs are durable in R2; CSVs are durable in Git.
- Git storage uses daily partitioned files to keep diffs small and manageable.

## Requirements

The project uses Python standard library modules only:

- `sqlite3`
- `urllib`
- `csv`
- `logging`
- `unittest`

No third-party Python package is required for the core updater, R2 sync, validator, or tests.
