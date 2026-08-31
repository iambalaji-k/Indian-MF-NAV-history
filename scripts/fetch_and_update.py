from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import logging
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

try:
    from scripts.r2_storage import (
        R2Config,
        atomic_upload_object,
        download_object,
        file_sha256,
        load_dotenv,
        r2_lock,
    )
    from scripts.validator import validate_database
except ModuleNotFoundError:
    from r2_storage import R2Config, atomic_upload_object, download_object, file_sha256, load_dotenv, r2_lock
    from validator import validate_database


AMFI_URL = "https://portal.amfiindia.com/spages/NAVAll.txt"
MIN_NAV_DATE = date(2026, 4, 1)
ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
LOG_DIR = ROOT_DIR / "logs"
SCHEMA_PATH = Path(__file__).with_name("schema.sql")
NAV_QUANT = Decimal("0.0001")
IST = timezone(timedelta(hours=5, minutes=30))

SCHEME_LINE_RE = re.compile(r"^\s*\d+\s*;")


ISIN_RE = re.compile(r"^INF[0-9A-Z]{9}$")
FEED_PROFILE_FILE = ".feed_profile.json"


@dataclass(frozen=True)
class NavRow:
    scheme_code: int
    isin_payout_or_growth: str | None
    isin_reinvestment: str | None
    scheme_name: str
    nav: Decimal
    nav_date: date


@dataclass(frozen=True)
class ColumnMap:
    scheme_code_idx: int
    isin_payout_idx: int | None
    isin_reinvest_idx: int | None
    scheme_name_idx: int
    nav_idx: int
    date_idx: int
    layout_name: str
    confidence: float


def setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def fetch_text(url: str, retries: int = 3, timeout: int = 10) -> str:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "Indian-MF-NAV-history/1.0"})
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read().decode("utf-8-sig", errors="replace")
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = exc
            logging.warning("Fetch attempt %s/%s failed: %s", attempt, retries, exc)
            if attempt < retries:
                time.sleep(attempt)
    raise RuntimeError(f"Failed to fetch AMFI NAV file after {retries} attempts") from last_error


def try_parse_amfi_date(value: str) -> date | None:
    cleaned = value.strip()
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


def parse_amfi_date(value: str) -> date:
    parsed = try_parse_amfi_date(value)
    if parsed is not None:
        return parsed
    raise ValueError(f"invalid date: {value!r}")


def clean_optional(value: str) -> str | None:
    cleaned = value.strip()
    return cleaned or None


def try_normalize_nav(value: str) -> Decimal | None:
    try:
        nav = Decimal(value.strip()).quantize(NAV_QUANT, rounding=ROUND_HALF_UP)
        if nav.is_nan() or nav.is_infinite() or nav <= 0:
            return None
        return nav
    except (InvalidOperation, ValueError):
        return None


def normalize_nav(value: str) -> Decimal:
    nav = Decimal(value).quantize(NAV_QUANT, rounding=ROUND_HALF_UP)
    if nav.is_nan() or nav.is_infinite():
        raise InvalidOperation("NAV is not finite")
    if nav <= 0:
        raise InvalidOperation("NAV must be positive")
    return nav


def format_nav(nav: Decimal) -> str:
    return f"{nav:.4f}"


def resolve_feed_columns(column_count: int) -> tuple[int, int] | None:
    if column_count >= 8:
        return 6, 7
    if column_count == 6:
        return 4, 5
    return None


def detect_feed_layout(sample_rows: list[list[str]], col_count: int) -> ColumnMap:
    # 1. Fast path for known standard layouts
    if col_count == 8:
        return ColumnMap(
            scheme_code_idx=0,
            isin_payout_idx=1,
            isin_reinvest_idx=2,
            scheme_name_idx=3,
            nav_idx=6,
            date_idx=7,
            layout_name="8col_standard",
            confidence=1.0,
        )
    if col_count == 6:
        return ColumnMap(
            scheme_code_idx=0,
            isin_payout_idx=1,
            isin_reinvest_idx=2,
            scheme_name_idx=3,
            nav_idx=4,
            date_idx=5,
            layout_name="6col_legacy",
            confidence=1.0,
        )

    if col_count < 4 or not sample_rows:
        raise ValueError(f"Cannot detect layout for column count {col_count}")

    num_samples = len(sample_rows)

    # 2. Date column scoring: rightmost column with >= 80% valid date strings
    date_idx = None
    best_date_score = 0.0
    for col in reversed(range(col_count)):
        matches = sum(1 for row in sample_rows if col < len(row) and try_parse_amfi_date(row[col]) is not None)
        score = matches / num_samples
        if score >= 0.80 and score > best_date_score:
            best_date_score = score
            date_idx = col

    if date_idx is None:
        raise ValueError(f"Failed to detect date column in {col_count}-col layout")

    # 3. NAV column scoring: rightmost column (excluding date) with >= 80% valid positive decimals
    nav_idx = None
    best_nav_score = 0.0
    for col in reversed(range(col_count)):
        if col == date_idx:
            continue
        matches = sum(1 for row in sample_rows if col < len(row) and try_normalize_nav(row[col]) is not None)
        score = matches / num_samples
        if score >= 0.80 and score > best_nav_score:
            best_nav_score = score
            nav_idx = col

    if nav_idx is None:
        raise ValueError(f"Failed to detect NAV column in {col_count}-col layout")

    # 4. Scheme code column: first integer column (excluding date, nav) where >= 85% parse as positive int
    code_idx = None
    for col in range(col_count):
        if col in (date_idx, nav_idx):
            continue
        int_matches = 0
        for row in sample_rows:
            if col < len(row):
                val = row[col].strip()
                if val.isdigit() and int(val) > 0:
                    int_matches += 1
        if int_matches / num_samples >= 0.85:
            code_idx = col
            break

    if code_idx is None:
        raise ValueError(f"Failed to detect scheme code column in {col_count}-col layout")

    # 5. ISIN columns: columns matching ^INF[0-9A-Z]{9}$ or placeholder
    isin_cols: list[int] = []
    for col in range(col_count):
        if col in (date_idx, nav_idx, code_idx):
            continue
        isin_matches = 0
        non_empty = 0
        for row in sample_rows:
            if col < len(row):
                val = row[col].strip()
                if val and val not in ("-", "N.A.", "NA", "N/A"):
                    non_empty += 1
                    if ISIN_RE.match(val):
                        isin_matches += 1
        if non_empty > 0 and (isin_matches / non_empty) >= 0.80 and isin_matches >= 1:
            isin_cols.append(col)

    isin1_idx = isin_cols[0] if len(isin_cols) > 0 else None
    isin2_idx = isin_cols[1] if len(isin_cols) > 1 else None

    # 6. Scheme Name column: remaining column with highest average text length
    candidate_name_cols = [
        c for c in range(col_count)
        if c not in (date_idx, nav_idx, code_idx) and c not in isin_cols
    ]
    if not candidate_name_cols:
        raise ValueError(f"No available column for scheme name in {col_count}-col layout")

    def name_score(col: int) -> float:
        total_len = sum(len(row[col].strip()) for row in sample_rows if col < len(row))
        return total_len / num_samples

    name_idx = max(candidate_name_cols, key=name_score)
    confidence = min(best_date_score, best_nav_score)

    return ColumnMap(
        scheme_code_idx=code_idx,
        isin_payout_idx=isin1_idx,
        isin_reinvest_idx=isin2_idx,
        scheme_name_idx=name_idx,
        nav_idx=nav_idx,
        date_idx=date_idx,
        layout_name=f"{col_count}col_dynamic",
        confidence=round(confidence, 2),
    )


def parse_nav_feed(text: str) -> tuple[list[NavRow], int, dict[int, ColumnMap]]:
    parsed_lines: list[tuple[int, list[str]]] = []
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line or not SCHEME_LINE_RE.match(line):
            continue
        parts = [part.strip() for part in line.split(";")]
        parsed_lines.append((line_number, parts))

    if not parsed_lines:
        return [], 0, {}

    lines_by_col_count: dict[int, list[tuple[int, list[str]]]] = {}
    for line_number, parts in parsed_lines:
        lines_by_col_count.setdefault(len(parts), []).append((line_number, parts))

    detected_layouts: dict[int, ColumnMap] = {}
    rows: list[NavRow] = []
    invalid_count = 0

    for col_count, line_group in lines_by_col_count.items():
        sample_parts = [parts for _, parts in line_group[:500]]
        try:
            col_map = detect_feed_layout(sample_parts, col_count)
            detected_layouts[col_count] = col_map
        except (ValueError, RuntimeError) as exc:
            invalid_count += len(line_group)
            logging.warning("Skipping %d lines with unsupported/unparseable %d-column layout: %s", len(line_group), col_count, exc)
            continue

        for line_number, parts in line_group:
            try:
                scheme_code = int(parts[col_map.scheme_code_idx])
                nav_decimal = normalize_nav(parts[col_map.nav_idx])
                nav_date = parse_amfi_date(parts[col_map.date_idx])
                if nav_date < MIN_NAV_DATE:
                    logging.info(
                        "Skipping line %s: NAV date %s is before cutoff %s",
                        line_number,
                        nav_date.isoformat(),
                        MIN_NAV_DATE.isoformat(),
                    )
                    continue
                scheme_name = parts[col_map.scheme_name_idx].strip()
                if not scheme_name:
                    raise ValueError("missing scheme name")
                isin1 = clean_optional(parts[col_map.isin_payout_idx]) if col_map.isin_payout_idx is not None else None
                isin2 = clean_optional(parts[col_map.isin_reinvest_idx]) if col_map.isin_reinvest_idx is not None else None
            except (ValueError, InvalidOperation, IndexError) as exc:
                invalid_count += 1
                logging.warning("Skipping line %s: %s", line_number, exc)
                continue

            rows.append(
                NavRow(
                    scheme_code=scheme_code,
                    isin_payout_or_growth=isin1,
                    isin_reinvestment=isin2,
                    scheme_name=scheme_name,
                    nav=nav_decimal,
                    nav_date=nav_date,
                )
            )

    return rows, invalid_count, detected_layouts


def parse_nav_text(text: str) -> tuple[list[NavRow], int]:
    rows, invalid_count, _ = parse_nav_feed(text)
    return rows, invalid_count


def log_feed_summary(rows: list[NavRow], invalid_count: int, layouts: dict[int, ColumnMap]) -> None:
    distinct_dates = len({r.nav_date for r in rows}) if rows else 0
    newest_date = max((r.nav_date for r in rows), default=None)
    layout_desc = "; ".join(
        f"{cols}col:{cm.layout_name}[code={cm.scheme_code_idx},name={cm.scheme_name_idx},nav={cm.nav_idx},date={cm.date_idx},conf={cm.confidence}]"
        for cols, cm in layouts.items()
    )
    logging.info(
        "FEED_SUMMARY: layouts={%s} rows=%d distinct_dates=%d newest_date=%s invalid_rows=%d",
        layout_desc,
        len(rows),
        distinct_dates,
        newest_date.isoformat() if newest_date else "None",
        invalid_count,
    )


def check_stale_feed(rows: list[NavRow], seen_on: date, max_stale_days: int = 4) -> None:
    if not rows:
        return
    newest_date = max(row.nav_date for row in rows)
    age_days = (seen_on - newest_date).days
    if age_days > max_stale_days:
        logging.warning(
            "STALE_FEED_WARNING: Newest NAV date in feed is %s (%d days behind run date %s).",
            newest_date.isoformat(),
            age_days,
            seen_on.isoformat(),
        )


def profile_layout_payload(layouts: dict[int, ColumnMap]) -> dict:
    return {
        str(col_count): {
            "layout_name": cm.layout_name,
            "col_count": col_count,
            "scheme_code_idx": cm.scheme_code_idx,
            "isin_payout_idx": cm.isin_payout_idx,
            "isin_reinvest_idx": cm.isin_reinvest_idx,
            "scheme_name_idx": cm.scheme_name_idx,
            "nav_idx": cm.nav_idx,
            "date_idx": cm.date_idx,
            "confidence": cm.confidence,
        }
        for col_count, cm in layouts.items()
    }


def check_feed_drift(
    layouts: dict[int, ColumnMap],
    data_dir: Path,
    allow_feed_drift: bool = False,
    strict_drift: bool = False,
) -> bool:
    """Compare detected layouts against the saved profile without mutating it.

    Returns True when drift was detected. Raises under strict mode unless the
    drift is explicitly acknowledged. Persistence is deferred to
    save_feed_profile so a failed run cannot erase an unacknowledged alarm.
    """
    profile_path = data_dir / FEED_PROFILE_FILE
    current_profile_layouts = profile_layout_payload(layouts)

    drift_detected = False
    if profile_path.exists():
        try:
            prev_profile = json.loads(profile_path.read_text(encoding="utf-8"))
            prev_layouts = prev_profile.get("layouts", {})
            if prev_layouts != current_profile_layouts:
                drift_detected = True
                logging.warning(
                    "FEED_DRIFT_ALARM: AMFI feed layout changed! Previous: %s, Current: %s",
                    prev_layouts,
                    current_profile_layouts,
                )
                if strict_drift and not allow_feed_drift:
                    raise RuntimeError(
                        "Feed drift detected with --strict-feed-drift. Pass --allow-feed-drift to accept changes."
                    )
        except json.JSONDecodeError:
            logging.warning("Failed to parse existing %s, will overwrite.", profile_path)

    return drift_detected


def save_feed_profile(
    layouts: dict[int, ColumnMap],
    total_rows: int,
    seen_on: date,
    data_dir: Path,
) -> None:
    new_profile = {
        "last_updated": seen_on.isoformat(),
        "total_rows": total_rows,
        "layouts": profile_layout_payload(layouts),
    }
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / FEED_PROFILE_FILE).write_text(json.dumps(new_profile, indent=2), encoding="utf-8")
        logging.info("Saved feed profile to %s", data_dir / FEED_PROFILE_FILE)
    except OSError as exc:
        logging.warning("Could not write feed profile to %s: %s", data_dir / FEED_PROFILE_FILE, exc)


def get_recent_daily_row_counts(data_dir: Path, limit: int = 7) -> list[int]:
    db_files = list(data_dir.glob("nav_fy_*.db"))
    if not db_files:
        return []

    date_counts: dict[str, int] = {}
    for db_path in db_files:
        try:
            with closing(sqlite3.connect(db_path)) as conn:
                tbl_exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='nav_history'"
                ).fetchone()
                if not tbl_exists:
                    continue
                for nav_date, count in conn.execute(
                    "SELECT nav_date, COUNT(*) FROM nav_history GROUP BY nav_date"
                ):
                    date_counts[nav_date] = count
        except sqlite3.Error as exc:
            logging.warning("Error reading historical counts from %s: %s", db_path, exc)
            continue

    if not date_counts:
        return []

    sorted_dates = sorted(date_counts.keys(), reverse=True)[:limit]
    return [date_counts[d] for d in sorted_dates]


def check_row_count_plausibility(
    parsed_count: int,
    data_dir: Path,
    min_ratio: float = 0.80,
    min_history_days: int = 1,
) -> None:
    recent_counts = get_recent_daily_row_counts(data_dir, limit=7)
    if len(recent_counts) < min_history_days:
        logging.info(
            "Row count sanity check: Insufficient history (%d days found); skipping gate.",
            len(recent_counts),
        )
        return

    sorted_counts = sorted(recent_counts)
    n = len(sorted_counts)
    mid = n // 2
    median_count = (sorted_counts[mid] if n % 2 != 0 else (sorted_counts[mid - 1] + sorted_counts[mid]) / 2)

    if median_count < 100:
        logging.info("Row count sanity check: Median count is small (%s); skipping gate.", median_count)
        return

    threshold = median_count * min_ratio
    if parsed_count < threshold:
        raise RuntimeError(
            f"Row count sanity gate failed: parsed {parsed_count} rows, which is below "
            f"{min_ratio:.0%} of 7-day median ({median_count:.0f} rows, threshold: {threshold:.0f})"
        )
    logging.info(
        "Row count sanity gate passed: %d rows (7-day median: %.0f, threshold: %.0f)",
        parsed_count,
        median_count,
        threshold,
    )


def financial_year_label(nav_date: date) -> str:
    start_year = nav_date.year if nav_date.month >= 4 else nav_date.year - 1
    return f"{start_year}_{str(start_year + 1)[-2:]}"


def fy_db_path(nav_date: date, data_dir: Path = DATA_DIR) -> Path:
    return data_dir / f"nav_fy_{financial_year_label(nav_date)}.db"


def r2_key_for_db(db_path: Path, data_dir: Path = DATA_DIR) -> str:
    try:
        relative_path = db_path.relative_to(data_dir)
    except ValueError:
        relative_path = Path(db_path.name)
    return f"db/{relative_path.as_posix()}"


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(db_path)) as conn:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        migrate_nav_to_text(conn)
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()


def migrate_nav_to_text(conn: sqlite3.Connection) -> None:
    nav_columns = conn.execute("PRAGMA table_info(nav_history)").fetchall()
    nav_type = next((column[2].upper() for column in nav_columns if column[1] == "nav"), "")
    if nav_type == "TEXT":
        return

    logging.info("Migrating nav_history.nav from %s to TEXT", nav_type or "unknown")
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("ALTER TABLE nav_history RENAME TO nav_history_old")
    conn.execute(
        """
        CREATE TABLE nav_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scheme_code INTEGER NOT NULL,
            nav_date TEXT NOT NULL,
            nav TEXT NOT NULL,
            ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (scheme_code) REFERENCES schemes (scheme_code),
            UNIQUE (scheme_code, nav_date)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO nav_history (id, scheme_code, nav_date, nav, ingested_at)
        SELECT
            id,
            scheme_code,
            nav_date,
            printf('%.4f', CAST(nav AS REAL)),
            ingested_at
        FROM nav_history_old
        """
    )
    conn.execute("DROP TABLE nav_history_old")
    conn.execute("PRAGMA foreign_keys = ON")


def upsert_rows(db_path: Path, rows: list[NavRow], seen_on: date) -> tuple[int, int]:
    init_db(db_path)
    inserted = 0
    seen_on_text = seen_on.isoformat()
    inactive_before = (seen_on - timedelta(days=30)).isoformat()

    with closing(sqlite3.connect(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executemany(
            """
            INSERT INTO schemes (
                scheme_code,
                isin_payout_or_growth,
                isin_reinvestment,
                scheme_name,
                first_seen_date,
                last_seen_date,
                is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(scheme_code) DO UPDATE SET
                isin_payout_or_growth = excluded.isin_payout_or_growth,
                isin_reinvestment = excluded.isin_reinvestment,
                scheme_name = excluded.scheme_name,
                last_seen_date = excluded.last_seen_date,
                is_active = 1,
                updated_at = CURRENT_TIMESTAMP
            """,
            [
                (
                    row.scheme_code,
                    row.isin_payout_or_growth,
                    row.isin_reinvestment,
                    row.scheme_name,
                    seen_on_text,
                    seen_on_text,
                )
                for row in rows
            ],
        )
        before_nav_insert = conn.total_changes
        conn.executemany(
            """
            INSERT OR IGNORE INTO nav_history (scheme_code, nav_date, nav)
            VALUES (?, ?, ?)
            """,
            [(row.scheme_code, row.nav_date.isoformat(), format_nav(row.nav)) for row in rows],
        )
        inserted = conn.total_changes - before_nav_insert

        conn.execute(
            """
            UPDATE schemes
            SET is_active = 0, updated_at = CURRENT_TIMESTAMP
            WHERE last_seen_date < ?
            """,
            (inactive_before,),
        )

        active_count = conn.execute("SELECT COUNT(*) FROM schemes WHERE is_active = 1").fetchone()[0]
        conn.commit()

    return inserted, active_count


SCHEMES_JSON_GZ = DATA_DIR / "schemes.json.gz"
LATEST_JSON = DATA_DIR / "latest.json"

NAV_CSV_HEADER = [
    "scheme_code",
    "nav",
    "nav_date",
]


def deterministic_gzip_bytes(text: str) -> bytes:
    """GZIP with a fixed mtime so identical content yields identical bytes."""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=9, mtime=0) as gz:
        gz.write(text.encode("utf-8"))
    return buffer.getvalue()


def write_bytes_if_changed(path: Path, payload: bytes) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_bytes() == payload:
        return False
    path.write_bytes(payload)
    return True


def write_daily_run_csv(data_dir: Path, rows: list[NavRow], seen_on: date) -> Path:
    if not rows:
        raise ValueError("No NAV rows available to snapshot")

    # Name the snapshot by the newest NAV date in the feed. Weekend/holiday
    # runs then reuse the last trading day's filename; combined with
    # deterministic GZIP output this makes repeat runs byte-identical no-ops
    # for Git instead of duplicating snapshots.
    snapshot_date = max(row.nav_date for row in rows)
    year = snapshot_date.year
    month = f"{snapshot_date.month:02d}"
    folder = data_dir / str(year) / month
    csv_path = folder / f"nav_{snapshot_date.isoformat()}.csv.gz"

    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(NAV_CSV_HEADER)
    for row in sorted(rows, key=lambda r: (r.scheme_code, r.nav_date)):
        writer.writerow(
            [
                row.scheme_code,
                format_nav(row.nav),
                row.nav_date.isoformat(),
            ]
        )

    payload = deterministic_gzip_bytes(buffer.getvalue())
    changed = write_bytes_if_changed(csv_path, payload)
    logging.info(
        "%s daily run CSV: %s", "Wrote" if changed else "Skipped (unchanged)", csv_path
    )
    return csv_path


def write_latest_json(
    data_dir: Path,
    csv_path: Path,
    snapshot_date: date,
    scheme_count: int,
) -> None:
    relative = csv_path.relative_to(data_dir).as_posix() if csv_path.is_relative_to(data_dir) else csv_path.name
    pointer = {
        "date": snapshot_date.isoformat(),
        "file": relative,
        "schemes": scheme_count,
    }
    payload = json.dumps(pointer, separators=(",", ":")) + "\n"
    write_bytes_if_changed(data_dir / "latest.json", payload.encode("utf-8"))


def write_schemes_json(db_paths: list[Path], target_path: Path = DATA_DIR / "schemes.json.gz") -> None:
    """Merge scheme master data across financial-year partitions.

    Later (newer) databases win on conflicts so renames propagate, while
    schemes retired in an older FY remain visible instead of vanishing at
    the April rollover.
    """
    target_path.parent.mkdir(parents=True, exist_ok=True)
    schemes_dict: dict[str, list[str]] = {}
    if target_path.exists():
        try:
            with gzip.open(target_path, "rt", encoding="utf-8") as gz:
                loaded = json.load(gz)
                if isinstance(loaded, dict):
                    schemes_dict = loaded
        except Exception:
            schemes_dict = {}

    for db_path in db_paths:
        if not db_path.exists():
            continue
        with closing(sqlite3.connect(db_path)) as conn:
            for row in conn.execute(
                """
                SELECT scheme_code, isin_payout_or_growth, isin_reinvestment, scheme_name
                FROM schemes
                ORDER BY scheme_code
                """
            ):
                code, isin1, isin2, name = row
                schemes_dict[str(code)] = [isin1 or "", isin2 or "", name]

    payload = json.dumps(schemes_dict, ensure_ascii=False, separators=(",", ":"))
    changed = write_bytes_if_changed(target_path, deterministic_gzip_bytes(payload))
    logging.info(
        "%s schemes JSON GZIP map: %s (%s schemes)",
        "Wrote" if changed else "Skipped (unchanged)",
        target_path,
        len(schemes_dict),
    )


def fy_start_years_through(seen_on: date) -> list[int]:
    last_start_year = seen_on.year if seen_on.month >= 4 else seen_on.year - 1
    return list(range(MIN_NAV_DATE.year, last_start_year + 1))


def all_fy_db_paths(seen_on: date, data_dir: Path = DATA_DIR) -> list[Path]:
    return [
        data_dir / f"nav_fy_{start_year}_{str(start_year + 1)[-2:]}.db"
        for start_year in fy_start_years_through(seen_on)
    ]


def db_paths_for_rows(rows: list[NavRow], data_dir: Path = DATA_DIR) -> set[Path]:
    db_paths: set[Path] = set()
    for row in rows:
        db_paths.add(fy_db_path(row.nav_date, data_dir))
    return db_paths


def sync_down_databases_from_r2(
    db_paths: set[Path],
    data_dir: Path,
    config: R2Config,
    max_workers: int = 4,
) -> None:
    if not db_paths:
        return
    sorted_paths = sorted(db_paths)
    with ThreadPoolExecutor(max_workers=min(max_workers, len(sorted_paths))) as executor:
        futures = [
            executor.submit(download_object, config, r2_key_for_db(db_path, data_dir), db_path)
            for db_path in sorted_paths
        ]
        for future in futures:
            future.result()


def _upload_single_db(
    db_path: Path,
    old_hash: str,
    data_dir: Path,
    config: R2Config,
) -> None:
    if not db_path.exists():
        return

    new_hash = file_sha256(db_path)
    if new_hash == old_hash:
        logging.info("Database unchanged, skipping upload: %s", db_path)
        return

    if validate_database(db_path) != 0:
        raise RuntimeError(f"Database validation failed before upload: {db_path}")

    logging.info("Database changed (hash %s -> %s), uploading: %s", old_hash[:8], new_hash[:8], db_path)
    atomic_upload_object(config, r2_key_for_db(db_path, data_dir), db_path, rotate_backups=True)


def sync_up_databases_to_r2(
    db_hashes: dict[Path, str],
    data_dir: Path,
    config: R2Config,
    max_workers: int = 4,
) -> None:
    if not db_hashes:
        return
    sorted_items = sorted(db_hashes.items())
    with ThreadPoolExecutor(max_workers=min(max_workers, len(sorted_items))) as executor:
        futures = [
            executor.submit(_upload_single_db, db_path, old_hash, data_dir, config)
            for db_path, old_hash in sorted_items
        ]
        for future in futures:
            future.result()


def update_databases(rows: list[NavRow], seen_on: date, data_dir: Path = DATA_DIR) -> set[Path]:
    rows_by_db: dict[Path, list[NavRow]] = {}
    for row in rows:
        rows_by_db.setdefault(fy_db_path(row.nav_date, data_dir), []).append(row)

    for db_path, db_rows in sorted(rows_by_db.items()):
        inserted, active = upsert_rows(db_path, db_rows, seen_on)
        logging.info("Updated %s: inserted %s NAV rows, %s active schemes", db_path, inserted, active)

    return db_paths_for_rows(rows, data_dir)


def load_input(args: argparse.Namespace) -> str:
    if args.input:
        return Path(args.input).read_text(encoding="utf-8")
    return fetch_text(args.url, retries=args.retries, timeout=args.timeout)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch and update the AMFI NAV SQLite archive.")
    parser.add_argument("--url", default=AMFI_URL)
    parser.add_argument("--input", help="Read AMFI text from a local fixture instead of fetching.")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--log-file", type=Path, default=LOG_DIR / "update.log")
    parser.add_argument("--seen-on", help="Override ingestion date as YYYY-MM-DD, mainly for tests.")
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--r2-sync", action="store_true", help="Download DBs from R2 before update and upload them after.")
    parser.add_argument("--sync-all-fy", action="store_true", help="Download all historical financial-year DB partitions from R2 instead of single-FY.")
    parser.add_argument("--env-file", type=Path, default=ROOT_DIR / ".env")
    parser.add_argument("--allow-feed-drift", action="store_true", help="Allow feed drift under strict mode.")
    parser.add_argument("--strict-feed-drift", action="store_true", help="Fail if feed layout changes vs saved profile.")
    parser.add_argument("--skip-sanity", action="store_true", help="Skip row count plausibility sanity gate.")
    return parser


def publish_artifacts(
    rows: list[NavRow],
    seen_on: date,
    data_dir: Path,
    db_paths: list[Path] | None = None,
) -> None:
    csv_path = write_daily_run_csv(data_dir, rows, seen_on)
    snapshot_date = max(row.nav_date for row in rows)
    write_latest_json(
        data_dir,
        csv_path,
        snapshot_date=snapshot_date,
        scheme_count=len({row.scheme_code for row in rows}),
    )
    paths_to_merge = db_paths if db_paths is not None else all_fy_db_paths(seen_on, data_dir)
    write_schemes_json(paths_to_merge, target_path=data_dir / "schemes.json.gz")


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    setup_logging(args.log_file)
    seen_on = date.fromisoformat(args.seen_on) if args.seen_on else datetime.now(IST).date()

    try:
        load_dotenv(args.env_file)
        text = load_input(args)
        rows, invalid_count, detected_layouts = parse_nav_feed(text)
        log_feed_summary(rows, invalid_count, detected_layouts)
        logging.info("Parsed %s valid NAV rows; skipped %s invalid rows", len(rows), invalid_count)
        if not rows and args.input is None:
            raise RuntimeError(
                "AMFI feed yielded zero valid NAV rows; aborting to avoid publishing an empty dataset"
            )

        check_stale_feed(rows, seen_on)
        # Detect drift up front (strict mode aborts here), but only persist
        # the new profile after the pipeline succeeds so a failed run cannot
        # silently erase an unacknowledged drift alarm.
        check_feed_drift(
            detected_layouts,
            args.data_dir,
            allow_feed_drift=args.allow_feed_drift,
            strict_drift=args.strict_feed_drift,
        )

        db_paths = db_paths_for_rows(rows, args.data_dir)
        r2_config = R2Config.from_env() if args.r2_sync else None
        if r2_config:
            with r2_lock(r2_config):
                # Single-FY sync: by default download only the partitions touched by the incoming feed.
                sync_paths = (
                    set(all_fy_db_paths(seen_on, args.data_dir))
                    if args.sync_all_fy
                    else db_paths
                )
                sync_down_databases_from_r2(sync_paths, args.data_dir, r2_config)
                if not args.skip_sanity and args.input is None:
                    check_row_count_plausibility(len(rows), args.data_dir)
                # Hash every rows-derived partition even when it does not
                # exist yet: file_sha256 returns "" for missing files, so a
                # freshly created financial-year database still differs from
                # its baseline hash and gets uploaded after the update.
                db_hashes = {path: file_sha256(path) for path in db_paths}
                update_databases(rows, seen_on, args.data_dir)
                publish_artifacts(rows, seen_on, args.data_dir, db_paths=sorted(db_paths))
                sync_up_databases_to_r2(db_hashes, args.data_dir, r2_config)
        else:
            if not args.skip_sanity and args.input is None:
                check_row_count_plausibility(len(rows), args.data_dir)
            update_databases(rows, seen_on, args.data_dir)
            publish_artifacts(rows, seen_on, args.data_dir, db_paths=sorted(db_paths))

        save_feed_profile(detected_layouts, len(rows), seen_on, args.data_dir)
    except Exception:
        logging.exception("NAV update failed")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
