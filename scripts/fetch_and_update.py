from __future__ import annotations

import argparse
import csv
import logging
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timedelta
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
LATEST_CSV = ROOT_DIR / "latest_nav.csv"
SCHEMES_CSV = DATA_DIR / "schemes.csv"
NAV_QUANT = Decimal("0.0001")

SCHEME_LINE_RE = re.compile(r"^\s*\d+\s*;")


@dataclass(frozen=True)
class NavRow:
    scheme_code: int
    isin_payout_or_growth: str | None
    isin_reinvestment: str | None
    scheme_name: str
    nav: Decimal
    nav_date: date


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


def parse_amfi_date(value: str) -> date:
    cleaned = value.strip()
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"invalid date: {value!r}")


def clean_optional(value: str) -> str | None:
    cleaned = value.strip()
    return cleaned or None


def normalize_nav(value: str) -> Decimal:
    nav = Decimal(value).quantize(NAV_QUANT, rounding=ROUND_HALF_UP)
    if nav.is_nan() or nav.is_infinite():
        raise InvalidOperation("NAV is not finite")
    return nav


def format_nav(nav: Decimal) -> str:
    return f"{nav:.4f}"


def parse_nav_text(text: str) -> tuple[list[NavRow], int]:
    rows: list[NavRow] = []
    invalid_count = 0

    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line or not SCHEME_LINE_RE.match(line):
            continue

        parts = [part.strip() for part in line.split(";")]
        if len(parts) != 6:
            invalid_count += 1
            logging.warning("Skipping line %s: expected 6 columns, got %s", line_number, len(parts))
            continue

        try:
            scheme_code = int(parts[0])
            nav_decimal = normalize_nav(parts[4])
            nav_date = parse_amfi_date(parts[5])
            if nav_date < MIN_NAV_DATE:
                logging.info(
                    "Skipping line %s: NAV date %s is before cutoff %s",
                    line_number,
                    nav_date.isoformat(),
                    MIN_NAV_DATE.isoformat(),
                )
                continue
            scheme_name = parts[3].strip()
            if not scheme_name:
                raise ValueError("missing scheme name")
        except (ValueError, InvalidOperation) as exc:
            invalid_count += 1
            logging.warning("Skipping line %s: %s", line_number, exc)
            continue

        rows.append(
            NavRow(
                scheme_code=scheme_code,
                isin_payout_or_growth=clean_optional(parts[1]),
                isin_reinvestment=clean_optional(parts[2]),
                scheme_name=scheme_name,
                nav=nav_decimal,
                nav_date=nav_date,
            )
        )

    return rows, invalid_count


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


def write_latest_csv(path: Path, rows: list[NavRow]) -> None:
    sorted_rows = sorted(rows, key=lambda row: (row.scheme_code, row.nav_date))
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(LATEST_NAV_CSV_HEADER)
        for row in sorted_rows:
            writer.writerow(
                [
                    row.scheme_code,
                    row.isin_payout_or_growth or "",
                    row.isin_reinvestment or "",
                    row.scheme_name,
                    format_nav(row.nav),
                    row.nav_date.isoformat(),
                ]
            )


NAV_CSV_HEADER = [
    "scheme_code",
    "nav",
    "nav_date",
]

LATEST_NAV_CSV_HEADER = [
    "scheme_code",
    "isin_payout_or_growth",
    "isin_reinvestment",
    "scheme_name",
    "nav",
    "nav_date",
]

SCHEME_CSV_HEADER = [
    "scheme_code",
    "isin_payout_or_growth",
    "isin_reinvestment",
    "scheme_name",
    "first_seen_date",
    "last_seen_date",
    "is_active",
]


def write_daily_run_csv(data_dir: Path, rows: list[NavRow], seen_on: date) -> Path:
    year = seen_on.year
    month = f"{seen_on.month:02d}"
    folder = data_dir / str(year) / month
    folder.mkdir(parents=True, exist_ok=True)
    csv_path = folder / f"nav_{seen_on.isoformat()}.csv"

    sorted_rows = sorted(rows, key=lambda row: (row.scheme_code, row.nav_date))
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(NAV_CSV_HEADER)
        for row in sorted_rows:
            writer.writerow(
                [
                    row.scheme_code,
                    format_nav(row.nav),
                    row.nav_date.isoformat(),
                ]
            )
    logging.info("Wrote daily run CSV: %s", csv_path)
    return csv_path


def db_paths_for_rows(rows: list[NavRow], data_dir: Path = DATA_DIR) -> set[Path]:
    db_paths: set[Path] = set()
    for row in rows:
        db_paths.add(fy_db_path(row.nav_date, data_dir))
    return db_paths


def sync_down_databases_from_r2(db_paths: set[Path], data_dir: Path, config: R2Config) -> None:
    for db_path in sorted(db_paths):
        download_object(config, r2_key_for_db(db_path, data_dir), db_path)


def sync_up_databases_to_r2(db_hashes: dict[Path, str], data_dir: Path, config: R2Config) -> None:
    for db_path, old_hash in sorted(db_hashes.items()):
        if not db_path.exists():
            continue

        new_hash = file_sha256(db_path)
        if new_hash == old_hash:
            logging.info("Database unchanged, skipping upload: %s", db_path)
            continue

        if validate_database(db_path) != 0:
            raise RuntimeError(f"Database validation failed before upload: {db_path}")

        logging.info("Database changed (hash %s -> %s), uploading: %s", old_hash[:8], new_hash[:8], db_path)
        atomic_upload_object(config, r2_key_for_db(db_path, data_dir), db_path, rotate_backups=True)


def write_schemes_csv(db_path: Path, csv_path: Path = SCHEMES_CSV) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(db_path)) as conn, csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(SCHEME_CSV_HEADER)
        for row in conn.execute(
            """
            SELECT
                scheme_code,
                COALESCE(isin_payout_or_growth, ''),
                COALESCE(isin_reinvestment, ''),
                scheme_name,
                first_seen_date,
                last_seen_date,
                is_active
            FROM schemes
            ORDER BY scheme_code
            """
        ):
            writer.writerow(row)


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
    parser.add_argument("--latest-csv", type=Path, default=LATEST_CSV)
    parser.add_argument("--schemes-csv", type=Path, default=SCHEMES_CSV)
    parser.add_argument("--log-file", type=Path, default=LOG_DIR / "update.log")
    parser.add_argument("--seen-on", help="Override ingestion date as YYYY-MM-DD, mainly for tests.")
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--r2-sync", action="store_true", help="Download DBs from R2 before update and upload them after.")
    parser.add_argument("--env-file", type=Path, default=ROOT_DIR / ".env")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    setup_logging(args.log_file)
    seen_on = date.fromisoformat(args.seen_on) if args.seen_on else date.today()

    try:
        load_dotenv(args.env_file)
        text = load_input(args)
        rows, invalid_count = parse_nav_text(text)
        logging.info("Parsed %s valid NAV rows; skipped %s invalid rows", len(rows), invalid_count)
        db_paths = db_paths_for_rows(rows, args.data_dir)
        r2_config = R2Config.from_env() if args.r2_sync else None
        if r2_config:
            with r2_lock(r2_config):
                sync_down_databases_from_r2(db_paths, args.data_dir, r2_config)
                db_hashes = {path: file_sha256(path) for path in db_paths}
                update_databases(rows, seen_on, args.data_dir)
                write_daily_run_csv(args.data_dir, rows, seen_on)
                
                schemes_source_db = fy_db_path(seen_on, args.data_dir)
                if not schemes_source_db.exists() and db_paths:
                    schemes_source_db = sorted(db_paths)[0]
                if schemes_source_db.exists():
                    write_schemes_csv(schemes_source_db, args.schemes_csv)
                
                write_latest_csv(args.latest_csv, rows)
                sync_up_databases_to_r2(db_hashes, args.data_dir, r2_config)
        else:
            update_databases(rows, seen_on, args.data_dir)
            write_daily_run_csv(args.data_dir, rows, seen_on)
            
            schemes_source_db = fy_db_path(seen_on, args.data_dir)
            if not schemes_source_db.exists() and db_paths:
                schemes_source_db = sorted(db_paths)[0]
            if schemes_source_db.exists():
                write_schemes_csv(schemes_source_db, args.schemes_csv)
                
            write_latest_csv(args.latest_csv, rows)
    except Exception:
        logging.exception("NAV update failed")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
