# Indian Mutual Fund NAV History

## 🔗 Live Website

Explore and search the latest NAV data for all Indian Mutual Funds at: **[Indian MF NAV History Dashboard](https://balajik.in/Indian-MF-NAV-history/)**

> **Self-updating historical NAV archive and warehouse for Indian mutual funds** utilizing AMFI's `NAVAll.txt` feed, SQLite, Cloudflare R2, GZIP-compressed daily exports, a self-adapting parser, sanity validation gates, and GitHub Actions.
> 
> The project functions as an append-only time-series data warehouse with scheme metadata tracking, duplicate protection, inactive-scheme detection, and high-efficiency GZIP storage optimization. Financial-year SQLite databases are preserved durably in Cloudflare R2, while compact daily CSV exports (`nav_YYYY-MM-DD.csv.gz`) and metadata mappings (`schemes.json.gz`) are committed directly to Git for public access and client-side web consumption.

---

## Key Features & What This Builds

- **Self-Adapting Feed Parser & Schema Resilience**: Dynamically infers column layouts (6-column legacy, 8-column expanded, or shifted layouts) using heuristic pattern matching across ISINs, dates, scheme codes, and NAV values.
- **Feed Profiling & Drift Detection**: Tracks layout fingerprints and row counts in `data/.feed_profile.json` to monitor upstream structural changes.
- **Sanity Gates & Quality Controls**:
  - **Plausibility Sanity Gate**: Compares incoming row counts against a 7-day rolling historical median (fails if < 80% of median to prevent publishing truncated feeds).
  - **Stale Feed Warning**: Flags feeds where the latest NAV date lags significantly behind the execution date.
  - **Zero-Row Guard**: Aborts execution before mutating archives if zero valid rows are parsed.
  - **Data Integrity Validator**: Scans SQLite DBs for duplicate records, unexpected NAV jumps (> 50%), and long gaps (> 45 days).
- **Financial-Year SQLite Archives in R2**: Durable partition storage (`db/nav_fy_YYYY_YY.db`) with rolling backups (`.bak1`, `.bak2`), SHA-256 checksum verification on every upload/download round-trip, and distributed lock protection (`lock/nav.lock`) with automatic stale-lock takeover after 1 hour.
- **Normalized GZIP Git Storage (~95% Size Reduction)**:
  - Daily NAV snapshots: `data/YYYY/MM/nav_YYYY-MM-DD.csv.gz` (~53 KB/day), named by the **NAV (trading) date** so weekend/holiday reruns are byte-identical no-ops instead of duplicate commits.
  - Deterministic GZIP output (`mtime=0`): identical data always produces identical bytes, keeping Git history clean.
  - Scheme metadata mapping: `data/schemes.json.gz` (~133 KB), merged across all financial-year partitions so retired schemes survive the April rollover.
  - `data/latest.json`: tiny pointer file letting the frontend resolve the newest snapshot in a single request.
- **Dark-Mode Frontend Explorer (`index.html`)**:
  - Direct browser-native decompression via `DecompressionStream('gzip')`.
  - `data/latest.json` pointer resolves the newest snapshot in one request; a 14-day rolling fallback handles market holidays, weekends, and missing pointer files.
  - Instant client-side search (single-pass scan over a precomputed lowercase index, debounced input) across scheme names, scheme codes, and ISINs.
  - XSS-safe rendering: all feed-derived strings are inserted via DOM text nodes — never `innerHTML`.
  - Content-Security-Policy meta tag plus pinned CDN assets with Subresource Integrity hashes; no third-party CSV parser.
- **Automated Daily Pipeline with Failure Alerts**:
  - GitHub Actions runs daily at 13:30 UTC (~7:00 PM IST) with a `concurrency` group (overlapping runs queue instead of racing), a 15-minute job timeout, and a rebase-before-push guard.
  - Automatically creates a GitHub issue with the `bug` label and direct run logs upon workflow or sanity gate failure — deduplicated so a persistent problem opens one issue, not one per day.
- **Zero Third-Party Python Dependencies**: Uses only standard library modules.

---

## Data Source

AMFI publishes the official source feed at:

```text
https://portal.amfiindia.com/spages/NAVAll.txt
```

The feed is semicolon-delimited and historically follows formats such as:

```text
# 6-column format:
Scheme Code;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date

# 8-column format:
Scheme Code;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Scheme Name;Plan;Option;Net Asset Value;Date
```

The updater cleans headers, section banners, category text, and whitespace, dynamically identifying the exact column indices for each record.

---

## Important Data Scope Rule

This archive tracks data starting from:

```text
2026-04-01
```

Rows dated before 1 April 2026 are treated as out-of-scope historical records and are ignored even if present in the raw AMFI feed.

---

## Repository Layout

```text
.
|-- .github/
|   `-- workflows/
|       `-- update.yml          # Daily scheduled GitHub Actions workflow & failure alerting
|-- data/
|   |-- .feed_profile.json      # Fingerprint profile of AMFI feed layout & metrics
|   |-- schemes.json.gz         # GZIP-compressed scheme metadata dictionary (~133 KB)
|   |-- latest.json             # Pointer to the newest daily snapshot (O(1) frontend lookup)
|   `-- YYYY/
|       `-- MM/
|           `-- nav_YYYY-MM-DD.csv.gz   # Daily GZIP snapshot (code, nav, date) (~53 KB)
|-- scripts/
|   |-- fetch_and_update.py     # Main ingestion pipeline, adaptive parser, & R2 sync
|   |-- r2_storage.py           # S3-compatible Cloudflare R2 connector, backups & locking
|   |-- schema.sql              # SQLite DDL for dimension, fact, and metadata tables
|   `-- validator.py            # Integrity verification (duplicates, jumps, gap anomalies)
|-- tests/
|   |-- test_fetch_and_update.py# Ingestion, parsing heuristics, sanity, and drift tests
|   |-- test_r2_storage.py      # Mocked R2 upload, download, and locking unit tests
|-- index.html                  # Responsive dark-mode dashboard (GitHub Pages)
|-- styles.min.css              # Pre-compiled static production stylesheet (~16 KB)
|-- input.css                   # Tailwind source stylesheet
|-- tailwind.config.js          # Tailwind theme & design tokens configuration
|-- .env.example                # Cloudflare R2 environment variable template
`-- README.md
```

> **Note:** SQLite `.db` files generated during execution are kept locally and in Cloudflare R2, but excluded from Git to keep the repository lightweight.

---

## Database Architecture

Every financial-year SQLite database enforces the following relational model:

### `schemes` (Dimension Table)
Tracks scheme master data and active status:
- `scheme_code` (INTEGER PRIMARY KEY)
- `isin_payout_or_growth` (TEXT)
- `isin_reinvestment` (TEXT)
- `scheme_name` (TEXT)
- `first_seen_date` (TEXT)
- `last_seen_date` (TEXT)
- `is_active` (INTEGER DEFAULT 1)
- `created_at`, `updated_at` (TEXT)

If an AMFI scheme name changes over time, the master record is updated in place against the unique `scheme_code`.

### `nav_history` (Fact Table)
Append-only time series table storing historical valuations:
- `scheme_code` (INTEGER)
- `nav_date` (TEXT)
- `nav` (TEXT - canonical 4-decimal representation, e.g., `'105.4520'`)
- `ingested_at` (TEXT)
- Unique constraint on `(scheme_code, nav_date)` to prevent duplicate inserts.

### `schema_metadata`
Stores internal schema version and migration metadata.

---

## Financial-Year Partitioning

Incoming rows are routed into financial-year database partitions based on `nav_date` (1 April to 31 March):

| NAV Date Range | Partition Target in R2 |
|---|---|
| `2026-04-01` to `2027-03-31` | `db/nav_fy_2026_27.db` |
| `2027-04-01` to `2028-03-31` | `db/nav_fy_2027_28.db` |

---

## Git Storage & GZIP Optimization

To prevent Git repository bloat, data is stored using a **normalized 2-file architecture**:

1. **Daily NAV Snapshots (`data/YYYY/MM/nav_YYYY-MM-DD.csv.gz`)**:
   - 3 columns: `scheme_code,nav,nav_date`
   - Compressed with GZIP to **~53 KB/day** (**~95% smaller** than uncompressed CSV).
2. **Scheme Metadata Mapping (`data/schemes.json.gz`)**:
   - Compact key-value mapping:
     ```json
     {"100033": ["INF209K01165", "-", "Aditya Birla Sun Life Large Cap Fund"]}
     ```
   - Compressed with GZIP to **~133 KB**.

---

## Frontend Web Dashboard (`index.html`)

The dashboard is a static web application hosted on GitHub Pages:
- **Zero Build Step at Runtime**: Pure HTML5, pre-compiled CSS3, and modern Vanilla JavaScript.
- **Standalone Production Stylesheet (`styles.min.css`)**: 16 KB pre-compiled CSS provides instantaneous rendering with zero in-browser JIT overhead or console warnings.
- **Automatic CDN Fallback**: In the event `styles.min.css` is missing or fails to load, `index.html` dynamically injects Tailwind Play CDN as a resilient fallback.
- **Client-Side GZIP Decompression**: Leverages the browser `DecompressionStream('gzip')` API to decompress datasets in memory.
- **O(1) Snapshot Resolution**: Reads `data/latest.json` first; if unavailable, probes up to 14 prior days (weekends, market holidays).
- **Hardened Supply Chain**: CSP meta tag restricts script/style/font origins; Font Awesome is version-pinned with Subresource Integrity hashes; the daily CSV is parsed natively (no third-party parser).
- **Fast In-Memory Search**: Instant client-side search across scheme names, codes, and ISINs using a precomputed lowercase index with debounced input and XSS-safe DOM rendering.

---

## Cloudflare R2 Configuration

1. Create a Cloudflare R2 bucket and generate an API Token with read/write permissions.
2. Create your `.env` file from the template:

```powershell
Copy-Item .env.example .env
```

3. Configure your credentials in `.env`:

```ini
R2_ACCOUNT_ID=your_account_id
R2_BUCKET=your_bucket_name
R2_ACCESS_KEY_ID=your_access_key_id
R2_SECRET_ACCESS_KEY=your_secret_access_key
R2_PREFIX=indian-mf-nav-history
```

---

## Local Usage

### 1. Run Unit Tests

```powershell
python -m unittest discover -s tests
```

To run the live canary test against AMFI's live endpoint:

```powershell
$env:LIVE_FEED="1"; python -m unittest tests/test_fetch_and_update.py
```

### 2. Ingest AMFI Data (Local Only)

```powershell
python scripts/fetch_and_update.py
```

### 3. Ingest AMFI Data with Cloudflare R2 Synchronization

```powershell
python scripts/fetch_and_update.py --r2-sync
```

### 4. Advanced CLI Options

| Flag | Description |
|---|---|
| `--r2-sync` | Synchronize SQLite databases with Cloudflare R2 before and after ingestion. |
| `--strict-feed-drift` | Enforce strict validation and fail if feed layout diverges from `.feed_profile.json`. |
| `--allow-feed-drift` | Acknowledge and allow feed schema drift under strict mode. |
| `--skip-sanity` | Skip the 7-day rolling median row count sanity gate. |
| `--seen-on YYYY-MM-DD` | Override the processing date (useful for backfilling or testing). |
| `--input <path>` | Ingest a local AMFI text file instead of fetching live from the web. |

### 5. Validate Database Integrity

```powershell
python scripts/validator.py
```

### 6. Preview Frontend Locally

```powershell
python -m http.server 8000
```

Navigate to `http://localhost:8000` in your web browser.

### 7. Rebuild Production Stylesheet (Optional)

If you modify UI classes in `index.html`, recompile `styles.min.css`:

```powershell
npx tailwindcss -i input.css -o styles.min.css --minify
```

---

## GitHub Actions Automation

The scheduled workflow (`.github/workflows/update.yml`) runs daily at **13:30 UTC (~7:00 PM IST)**:

1. Checks out repository (`fetch-depth: 1`).
2. Configures Python 3.12 environment.
3. Executes `scripts/fetch_and_update.py --r2-sync`.
4. Runs `scripts/validator.py --window-days 90` to audit database integrity (add `--fail-on-anomaly` to turn anomaly thresholds into failures).
5. Commits and pushes changes only when new `.csv.gz`, `schemes.json.gz`, or `latest.json` files change — deterministic snapshots keep no-change days commit-free. A rebase-before-push guards against racing manual edits.
6. **Failure Alerting**: If any step fails (e.g., feed drift, sanity gate check, network timeout), an issue titled `🚨 AMFI Feed Drift / Daily NAV Update Failure` is automatically created with workflow run details. Duplicate suppression ensures only one open failure issue exists at a time.

The job also declares a `concurrency` group (`nav-update`) so overlapping scheduled/manual runs queue instead of racing, and a 15-minute timeout so hung steps cannot burn runner minutes.

---

## Requirements

Python standard library only:
- `sqlite3`, `urllib`, `csv`, `gzip`, `json`, `logging`, `unittest`, `dataclasses`, `decimal`, `datetime`

No third-party packages (pip requirements) needed.
