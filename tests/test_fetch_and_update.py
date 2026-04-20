from __future__ import annotations

import csv
import shutil
import sqlite3
import unittest
import uuid
from contextlib import closing
from datetime import date
from decimal import Decimal
from pathlib import Path

from scripts.fetch_and_update import (
    parse_nav_text,
    sync_up_databases_to_r2,
    update_databases,
    write_daily_run_csv,
    write_latest_csv,
    write_schemes_csv,
)
from scripts.r2_storage import R2Config, file_sha256


TEST_TMP_ROOT = Path(__file__).resolve().parents[1] / ".test-tmp"


class WorkspaceTemporaryDirectory:
    def __enter__(self) -> str:
        TEST_TMP_ROOT.mkdir(exist_ok=True)
        self.path = TEST_TMP_ROOT / f"tmp-{uuid.uuid4().hex}"
        self.path.mkdir()
        return str(self.path)

    def __exit__(self, exc_type, exc_value, traceback) -> bool | None:
        shutil.rmtree(self.path, ignore_errors=True)
        return None


def sample_line(
    scheme_code: int = 100001,
    scheme_name: str = "Example Fund - Growth",
    nav: str = "12.3456",
    nav_date: str = "01-Apr-2026",
) -> str:
    return f"{scheme_code};INF000000001;;{scheme_name};{nav};{nav_date}"


class FetchAndUpdateTests(unittest.TestCase):
    def test_empty_file_creates_empty_csv_and_db(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            rows, invalid = parse_nav_text("")
            update_databases(rows, date(2026, 4, 2), tmp_path / "data")
            write_latest_csv(tmp_path / "latest_nav.csv", rows)

            self.assertEqual(rows, [])
            self.assertEqual(invalid, 0)
            self.assertTrue((tmp_path / "data" / "nav.db").exists())
            with (tmp_path / "latest_nav.csv").open(newline="", encoding="utf-8") as handle:
                self.assertEqual(len(list(csv.reader(handle))), 1)

    def test_corrupt_rows_are_skipped(self) -> None:
        text = "\n".join(
            [
                "Open Ended Schemes (Equity Scheme)",
                sample_line(),
                "100002;INF000000002;;Bad NAV;abc;01-Apr-2026",
                "100003;INF000000003;;Bad Date;11.00;99-Apr-2026",
                "100004;too;few",
            ]
        )

        rows, invalid = parse_nav_text(text)

        self.assertEqual(len(rows), 1)
        self.assertEqual(invalid, 3)

    def test_nav_is_decimal_quantized_to_four_places(self) -> None:
        rows, invalid = parse_nav_text(sample_line(nav="12.34567"))

        self.assertEqual(invalid, 0)
        self.assertEqual(rows[0].nav, Decimal("12.3457"))

    def test_duplicate_run_same_day_is_ignored(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            rows, _ = parse_nav_text(sample_line())

            update_databases(rows, date(2026, 4, 2), data_dir)
            update_databases(rows, date(2026, 4, 2), data_dir)

            with closing(sqlite3.connect(data_dir / "nav.db")) as conn:
                count = conn.execute("SELECT COUNT(*) FROM nav_history").fetchone()[0]
                nav = conn.execute("SELECT nav FROM nav_history").fetchone()[0]

            self.assertEqual(count, 1)
            self.assertEqual(nav, "12.3456")

    def test_new_scheme_appears_and_name_is_updated(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            first_rows, _ = parse_nav_text(sample_line(scheme_name="Old Name"))
            second_rows, _ = parse_nav_text(
                "\n".join(
                    [
                        sample_line(scheme_name="New Name", nav_date="02-Apr-2026"),
                        sample_line(100002, "Second Fund", "20.00", "02-Apr-2026"),
                    ]
                )
            )

            update_databases(first_rows, date(2026, 4, 2), data_dir)
            update_databases(second_rows, date(2026, 4, 3), data_dir)

            with closing(sqlite3.connect(data_dir / "nav.db")) as conn:
                scheme_count = conn.execute("SELECT COUNT(*) FROM schemes").fetchone()[0]
                scheme_name = conn.execute(
                    "SELECT scheme_name FROM schemes WHERE scheme_code = 100001"
                ).fetchone()[0]

            self.assertEqual(scheme_count, 2)
            self.assertEqual(scheme_name, "New Name")

    def test_scheme_disappears_after_30_days(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            old_rows, _ = parse_nav_text(sample_line(100001, "Old Fund", "10.00", "01-Apr-2026"))
            new_rows, _ = parse_nav_text(sample_line(100002, "New Fund", "20.00", "05-May-2026"))

            update_databases(old_rows, date(2026, 4, 1), data_dir)
            update_databases(new_rows, date(2026, 5, 5), data_dir)

            with closing(sqlite3.connect(data_dir / "nav.db")) as conn:
                old_active = conn.execute(
                    "SELECT is_active FROM schemes WHERE scheme_code = 100001"
                ).fetchone()[0]
                new_active = conn.execute(
                    "SELECT is_active FROM schemes WHERE scheme_code = 100002"
                ).fetchone()[0]

            self.assertEqual(old_active, 0)
            self.assertEqual(new_active, 1)

    def test_nav_date_mismatch_uses_nav_date_financial_year(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            rows, _ = parse_nav_text(sample_line(100001, "Backdated Fund", "10.00", "31-Mar-2027"))

            update_databases(rows, date(2027, 4, 2), data_dir)

            self.assertTrue((data_dir / "nav.db").exists())
            self.assertTrue((data_dir / "nav_fy_2026_27.db").exists())
            self.assertFalse((data_dir / "nav_fy_2027_28.db").exists())

    def test_nav_before_april_2026_is_ignored(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            rows, invalid = parse_nav_text(
                "\n".join(
                    [
                        sample_line(100001, "Discontinued Fund", "10.00", "31-Mar-2026"),
                        sample_line(100002, "Current Fund", "20.00", "01-Apr-2026"),
                    ]
                )
            )

            update_databases(rows, date(2026, 4, 2), data_dir)

            with closing(sqlite3.connect(data_dir / "nav.db")) as conn:
                schemes = conn.execute("SELECT scheme_code FROM schemes ORDER BY scheme_code").fetchall()

            self.assertEqual(invalid, 0)
            self.assertEqual(schemes, [(100002,)])
            self.assertFalse((data_dir / "nav_fy_2025_26.db").exists())

    def test_expected_indexes_are_created(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            rows, _ = parse_nav_text(sample_line())

            update_databases(rows, date(2026, 4, 2), data_dir)

            with closing(sqlite3.connect(data_dir / "nav.db")) as conn:
                index_names = {
                    row[0]
                    for row in conn.execute(
                        """
                        SELECT name
                        FROM sqlite_master
                        WHERE type = 'index'
                          AND name NOT LIKE 'sqlite_autoindex_%'
                        """
                    )
                }
                schema_version = conn.execute(
                    "SELECT value FROM schema_metadata WHERE key = 'schema_version'"
                ).fetchone()[0]

            self.assertEqual(schema_version, "3")
            self.assertGreaterEqual(
                index_names,
                {
                    "idx_nav_history_scheme_date",
                    "idx_nav_history_nav_date",
                    "idx_nav_history_nav_date_scheme",
                    "idx_nav_history_scheme_date_nav",
                    "idx_schemes_active",
                    "idx_schemes_last_seen",
                    "idx_schemes_active_name",
                    "idx_schemes_name",
                    "idx_schemes_last_seen_active",
                },
            )

    def test_daily_run_csv_is_created_with_nested_folders(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            rows, _ = parse_nav_text(
                "\n".join(
                    [
                        sample_line(100001, "First Fund", "10.00", "01-Apr-2026"),
                        sample_line(100002, "Second Fund", "20.00", "01-Apr-2026"),
                    ]
                )
            )

            seen_on = date(2026, 4, 2)
            csv_path = write_daily_run_csv(data_dir, rows, seen_on)

            expected_path = data_dir / "2026" / "04" / "nav_2026-04-02.csv"
            self.assertEqual(csv_path, expected_path)
            self.assertTrue(expected_path.exists())

            with csv_path.open(newline="", encoding="utf-8") as handle:
                csv_rows = list(csv.reader(handle))

            self.assertEqual(csv_rows[0], [
                "scheme_code",
                "nav",
                "nav_date",
            ])
            self.assertEqual(len(csv_rows), 3)
            self.assertEqual(csv_rows[1][0], "100001")
            self.assertEqual(csv_rows[1][1], "10.0000")

    def test_latest_csv_contains_only_nav_fact_columns(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            latest_csv = Path(tmp) / "latest_nav.csv"
            rows, _ = parse_nav_text(sample_line())

            write_latest_csv(latest_csv, rows)

            with latest_csv.open(newline="", encoding="utf-8") as handle:
                csv_rows = list(csv.reader(handle))

            self.assertEqual(csv_rows[0], ["scheme_code", "nav", "nav_date"])
            self.assertEqual(csv_rows[1], ["100001", "12.3456", "2026-04-01"])

    def test_schemes_csv_contains_dimension_columns(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            schemes_csv = data_dir / "schemes.csv"
            rows, _ = parse_nav_text(sample_line(100001, "Scheme Dimension Fund", "10.00", "01-Apr-2026"))

            update_databases(rows, date(2026, 4, 2), data_dir)
            write_schemes_csv(data_dir / "nav.db", schemes_csv)

            with schemes_csv.open(newline="", encoding="utf-8") as handle:
                csv_rows = list(csv.reader(handle))

            self.assertEqual(
                csv_rows[0],
                [
                    "scheme_code",
                    "isin_payout_or_growth",
                    "isin_reinvestment",
                    "scheme_name",
                    "first_seen_date",
                    "last_seen_date",
                    "is_active",
                ],
            )
            self.assertEqual(csv_rows[1][0], "100001")
            self.assertEqual(csv_rows[1][3], "Scheme Dimension Fund")

    def test_existing_real_nav_column_is_migrated_to_text(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            db_path = data_dir / "nav.db"
            db_path.parent.mkdir(parents=True)
            with closing(sqlite3.connect(db_path)) as conn:
                conn.executescript(
                    """
                    CREATE TABLE schema_metadata (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                    CREATE TABLE schemes (
                        scheme_code INTEGER PRIMARY KEY,
                        isin_payout_or_growth TEXT,
                        isin_reinvestment TEXT,
                        scheme_name TEXT NOT NULL,
                        first_seen_date TEXT NOT NULL,
                        last_seen_date TEXT NOT NULL,
                        is_active INTEGER NOT NULL DEFAULT 1,
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                    CREATE TABLE nav_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        scheme_code INTEGER NOT NULL,
                        nav_date TEXT NOT NULL,
                        nav REAL NOT NULL,
                        ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (scheme_code, nav_date)
                    );
                    INSERT INTO schemes (scheme_code, scheme_name, first_seen_date, last_seen_date)
                    VALUES (100001, 'Migrated Fund', '2026-04-01', '2026-04-01');
                    INSERT INTO nav_history (scheme_code, nav_date, nav)
                    VALUES (100001, '2026-04-01', 12.3);
                    """
                )
                conn.commit()

            update_databases([], date(2026, 4, 2), data_dir)

            with closing(sqlite3.connect(db_path)) as conn:
                nav_type = next(
                    column[2]
                    for column in conn.execute("PRAGMA table_info(nav_history)").fetchall()
                    if column[1] == "nav"
                )
                nav = conn.execute("SELECT nav FROM nav_history").fetchone()[0]

            self.assertEqual(nav_type.upper(), "TEXT")
            self.assertEqual(nav, "12.3000")

    def test_r2_upload_validates_database_before_upload(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            db_path = data_dir / "nav.db"
            db_path.parent.mkdir(parents=True)
            with closing(sqlite3.connect(db_path)) as conn:
                conn.execute("CREATE TABLE placeholder (id INTEGER)")
                conn.commit()
            config = R2Config(
                account_id="account123",
                bucket="nav-archive",
                access_key_id="access",
                secret_access_key="secret",
                endpoint="https://account123.r2.cloudflarestorage.com",
            )

            db_hashes = {db_path: "old-hash"}
            with self.assertRaises(RuntimeError):
                sync_up_databases_to_r2(db_hashes, data_dir, config)


if __name__ == "__main__":
    unittest.main()
