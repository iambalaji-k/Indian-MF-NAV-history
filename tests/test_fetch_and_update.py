from __future__ import annotations

import csv
import gzip
import json
import os
import shutil
import sqlite3
import unittest
import uuid
from contextlib import closing, contextmanager
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from unittest.mock import patch

from scripts.fetch_and_update import (
    ColumnMap,
    NavRow,
    all_fy_db_paths,
    check_feed_drift,
    check_row_count_plausibility,
    check_stale_feed,
    detect_feed_layout,
    fy_start_years_through,
    normalize_nav,
    parse_nav_feed,
    parse_nav_text,
    publish_artifacts,
    save_feed_profile,
    sync_down_databases_from_r2,
    sync_up_databases_to_r2,
    update_databases,
    write_daily_run_csv,
    write_schemes_json,
    main,
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

            self.assertEqual(rows, [])
            self.assertEqual(invalid, 0)
            self.assertFalse((tmp_path / "data" / "nav_fy_2026_27.db").exists())

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

    def test_non_positive_nav_is_rejected(self) -> None:
        for bad in ("-5.0000", "0", "0.0000", "-0.0001"):
            rows, invalid = parse_nav_text(sample_line(nav=bad))
            self.assertEqual(rows, [], f"NAV {bad} must be rejected")
            self.assertEqual(invalid, 1)

        with self.assertRaises(InvalidOperation):
            normalize_nav("-10.00")
        with self.assertRaises(InvalidOperation):
            normalize_nav("0.00")

    def test_nav_is_decimal_quantized_to_four_places(self) -> None:
        rows, invalid = parse_nav_text(sample_line(nav="12.34567"))

        self.assertEqual(invalid, 0)
        self.assertEqual(rows[0].nav, Decimal("12.3457"))

    def test_parses_new_eight_column_amfi_format(self) -> None:
        text = "\n".join(
            [
                "Open Ended Schemes (Debt Scheme)",
                "119551;INF209KA12Z1;INF209KA13Z9;Aditya Birla Sun Life Banking & PSU Debt Fund;Direct Plan;IDCW-Re-investment;106.9996;20-Aug-2026",
                "119552;INF209K01YM2;-;Aditya Birla Sun Life Banking & PSU Debt Fund;Direct Plan;MONTHLY DCW Payout;117.3095;20-Aug-2026",
            ]
        )

        rows, invalid = parse_nav_text(text)

        self.assertEqual(invalid, 0)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].scheme_code, 119551)
        self.assertEqual(rows[0].isin_payout_or_growth, "INF209KA12Z1")
        self.assertEqual(rows[0].isin_reinvestment, "INF209KA13Z9")
        self.assertEqual(rows[0].scheme_name, "Aditya Birla Sun Life Banking & PSU Debt Fund")
        self.assertEqual(rows[0].nav, Decimal("106.9996"))
        self.assertEqual(rows[0].nav_date, date(2026, 8, 20))
        self.assertEqual(rows[1].isin_reinvestment, "-")
        self.assertEqual(rows[1].nav, Decimal("117.3095"))

    def test_parses_mixed_legacy_and_eight_column_formats(self) -> None:
        text = "\n".join(
            [
                sample_line(100001, "Legacy Fund", "10.00", "01-Apr-2026"),
                "119551;INF209KA12Z1;;New Format Fund;Direct Plan;Growth;106.9996;20-Aug-2026",
            ]
        )

        rows, invalid = parse_nav_text(text)

        self.assertEqual(invalid, 0)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].scheme_code, 100001)
        self.assertEqual(rows[0].nav, Decimal("10.0000"))
        self.assertEqual(rows[1].scheme_code, 119551)
        self.assertEqual(rows[1].nav, Decimal("106.9996"))
        self.assertEqual(rows[1].nav_date, date(2026, 8, 20))

    def test_unsupported_column_count_is_skipped(self) -> None:
        text = "\n".join(
            [
                "100001;INF000000001;;Too Few Columns;12.00",
                "100002;INF000000002;;Wrong Shape;NoDateFound;NoNavFound;StillNoDate",
            ]
        )

        rows, invalid = parse_nav_text(text)

        self.assertEqual(rows, [])
        self.assertEqual(invalid, 2)

    def test_duplicate_run_same_day_is_ignored(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            rows, _ = parse_nav_text(sample_line())
            update_databases(rows, date(2026, 4, 2), data_dir)
            update_databases(rows, date(2026, 4, 2), data_dir)

            with closing(sqlite3.connect(data_dir / "nav_fy_2026_27.db")) as conn:
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

            with closing(sqlite3.connect(data_dir / "nav_fy_2026_27.db")) as conn:
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

            with closing(sqlite3.connect(data_dir / "nav_fy_2026_27.db")) as conn:
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

            with closing(sqlite3.connect(data_dir / "nav_fy_2026_27.db")) as conn:
                schemes = conn.execute("SELECT scheme_code FROM schemes ORDER BY scheme_code").fetchall()

            self.assertEqual(invalid, 0)
            self.assertEqual(schemes, [(100002,)])
            self.assertFalse((data_dir / "nav_fy_2025_26.db").exists())

    def test_expected_indexes_are_created(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            rows, _ = parse_nav_text(sample_line())

            update_databases(rows, date(2026, 4, 2), data_dir)

            with closing(sqlite3.connect(data_dir / "nav_fy_2026_27.db")) as conn:
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

    def test_daily_run_csv_is_named_by_nav_date(self) -> None:
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

            # Snapshot is named by newest NAV date, not the run date.
            expected_path = data_dir / "2026" / "04" / "nav_2026-04-01.csv.gz"
            self.assertEqual(csv_path, expected_path)
            self.assertTrue(expected_path.exists())

            with gzip.open(csv_path, "rt", newline="", encoding="utf-8") as handle:
                csv_rows = list(csv.reader(handle))

            self.assertEqual(csv_rows[0], ["scheme_code", "nav", "nav_date"])
            self.assertEqual(len(csv_rows), 3)
            self.assertEqual(csv_rows[1][0], "100001")
            self.assertEqual(csv_rows[1][1], "10.0000")
            self.assertEqual(csv_rows[1][2], "2026-04-01")

    def test_weekend_rerun_produces_identical_snapshot_bytes(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            friday_rows, _ = parse_nav_text(sample_line(100001, "Fund", "10.00", "03-Apr-2026"))
            saturday_rows, _ = parse_nav_text(sample_line(100001, "Fund", "10.00", "03-Apr-2026"))

            first = write_daily_run_csv(data_dir, friday_rows, date(2026, 4, 3))
            first_bytes = first.read_bytes()
            second = write_daily_run_csv(data_dir, saturday_rows, date(2026, 4, 4))

            self.assertEqual(first, second)
            self.assertEqual(second.read_bytes(), first_bytes)
            siblings = list((data_dir / "2026" / "04").glob("nav_*.csv.gz"))
            self.assertEqual(len(siblings), 1, "weekend rerun must not create a second snapshot")

    def test_publish_artifacts_writes_pointer_and_merged_schemes(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"

            fy26_rows, _ = parse_nav_text(sample_line(100001, "Retired Scheme", "10.00", "01-Apr-2026"))
            update_databases(fy26_rows, date(2026, 4, 1), data_dir)
            # Simulate a rollover: FY27 db only knows the new scheme.
            fy27_rows, _ = parse_nav_text(sample_line(200001, "New FY Scheme", "30.00", "01-Apr-2027"))
            update_databases(fy27_rows, date(2027, 4, 1), data_dir)

            publish_artifacts(fy27_rows, date(2027, 4, 1), data_dir)

            pointer = json.loads((data_dir / "latest.json").read_text(encoding="utf-8"))
            self.assertEqual(pointer["date"], "2027-04-01")
            self.assertEqual(pointer["file"], "2027/04/nav_2027-04-01.csv.gz")
            self.assertEqual(pointer["schemes"], 1)

            with gzip.open(data_dir / "schemes.json.gz", "rt", encoding="utf-8") as handle:
                schemes_map = json.load(handle)

            # Retired scheme from the previous FY survives the rollover.
            self.assertIn("100001", schemes_map)
            self.assertEqual(schemes_map["100001"][2], "Retired Scheme")
            self.assertIn("200001", schemes_map)

    def test_fy_paths_cover_scope_through_seen_on(self) -> None:
        self.assertEqual(fy_start_years_through(date(2026, 8, 21)), [2026])
        self.assertEqual(fy_start_years_through(date(2027, 4, 2)), [2026, 2027])
        self.assertEqual(fy_start_years_through(date(2027, 3, 31)), [2026])

        paths = all_fy_db_paths(date(2027, 4, 2), Path("data"))
        self.assertEqual(
            [p.name for p in paths],
            ["nav_fy_2026_27.db", "nav_fy_2027_28.db"],
        )

    def test_write_schemes_json_merges_partitions_newest_wins(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"

            dir26 = data_dir / "p26"
            dir26.mkdir(parents=True, exist_ok=True)
            rows_a, _ = parse_nav_text(
                "\n".join(
                    [
                        sample_line(100001, "Shared Old", "10.00", "05-Jan-2027"),
                        sample_line(300001, "Retired Only", "11.00", "05-Jan-2027"),
                    ]
                )
            )
            update_databases(rows_a, date(2027, 1, 5), dir26)

            dir27 = data_dir / "p27"
            dir27.mkdir(parents=True, exist_ok=True)
            rows_b, _ = parse_nav_text(sample_line(100001, "Shared New", "12.00", "05-Jun-2027"))
            update_databases(rows_b, date(2027, 6, 5), dir27)

            target = data_dir / "schemes.json.gz"
            write_schemes_json(sorted(dir26.glob("*.db")) + sorted(dir27.glob("*.db")), target)

            with gzip.open(target, "rt", encoding="utf-8") as handle:
                schemes_map = json.load(handle)

            self.assertEqual(schemes_map["100001"][2], "Shared New")
            self.assertIn("300001", schemes_map)

    def test_existing_real_nav_column_is_migrated_to_text(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            db_path = data_dir / "nav_fy_2026_27.db"
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

            rows, _ = parse_nav_text(sample_line())
            update_databases(rows, date(2026, 4, 2), data_dir)

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
            db_path = data_dir / "nav_fy_2026_27.db"
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

    def test_fresh_fy_partition_db_is_uploaded_on_first_sync(self) -> None:
        """Regression: a partition created by this run must reach R2.

        The baseline hash for a not-yet-existing database is "", so it must
        still be present in db_hashes and uploaded after creation.
        """
        with WorkspaceTemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            data_dir = tmp_path / "data"
            fixture = tmp_path / "feed.txt"
            fixture.write_text(
                sample_line(100001, "New FY Fund", "25.00", "01-Apr-2027"),
                encoding="utf-8",
            )

            env = {
                "R2_ACCOUNT_ID": "account123",
                "R2_BUCKET": "nav-archive",
                "R2_ACCESS_KEY_ID": "access",
                "R2_SECRET_ACCESS_KEY": "secret",
            }

            @contextmanager
            def fake_lock(config, key="lock/nav.lock", stale_after_seconds=3600):
                yield

            with (
                patch.dict(os.environ, env),
                patch("scripts.fetch_and_update.r2_lock", fake_lock),
                patch("scripts.fetch_and_update.download_object", return_value=False) as dl,
                patch("scripts.fetch_and_update.atomic_upload_object") as upload,
            ):
                from scripts.fetch_and_update import main

                exit_code = main(
                    [
                        "--r2-sync",
                        "--input", str(fixture),
                        "--data-dir", str(data_dir),
                        "--env-file", str(tmp_path / "missing.env"),
                        "--seen-on", "2027-04-02",
                        "--log-file", str(tmp_path / "log.txt"),
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertTrue((data_dir / "nav_fy_2027_28.db").exists())
            self.assertTrue(dl.called)
            upload.assert_called_once()
            config_arg, key_arg = upload.call_args.args[0], upload.call_args.args[1]
            self.assertEqual(key_arg, "db/nav_fy_2027_28.db")
            self.assertEqual(config_arg.bucket, "nav-archive")

    def test_parses_dynamic_seven_and_nine_column_layouts(self) -> None:
        text_9col = "\n".join(
            [
                "119551;INF209KA12Z1;INF209KA13Z9;Aditya Birla Sun Life Banking Fund;Direct Plan;Growth;EXTRA_CAT;106.9996;20-Aug-2026",
                "119552;INF209K01YM2;-;Aditya Birla Debt Fund;Regular Plan;IDCW;EXTRA_CAT;117.3095;20-Aug-2026",
            ]
        )
        rows, invalid, layouts = parse_nav_feed(text_9col)
        self.assertEqual(invalid, 0)
        self.assertEqual(len(rows), 2)
        self.assertIn(9, layouts)
        self.assertEqual(layouts[9].layout_name, "9col_dynamic")
        self.assertEqual(layouts[9].nav_idx, 7)
        self.assertEqual(layouts[9].date_idx, 8)
        self.assertEqual(rows[0].scheme_code, 119551)
        self.assertEqual(rows[0].nav, Decimal("106.9996"))
        self.assertEqual(rows[0].nav_date, date(2026, 8, 20))

        text_7col = "119553;INF209KA12Z1;;7Col Test Fund;Direct Plan;10.5000;20-Aug-2026"
        rows_7, invalid_7, layouts_7 = parse_nav_feed(text_7col)
        self.assertEqual(invalid_7, 0)
        self.assertEqual(len(rows_7), 1)
        self.assertIn(7, layouts_7)
        self.assertEqual(rows_7[0].scheme_code, 119553)
        self.assertEqual(rows_7[0].nav, Decimal("10.5000"))
        self.assertEqual(rows_7[0].nav_date, date(2026, 8, 20))

    def test_parses_shuffled_column_layout(self) -> None:
        text_shuffled = "119551;INF209KA12Z1;20-Aug-2026;106.9996;Aditya Birla Sun Life Banking Fund"
        rows, invalid, layouts = parse_nav_feed(text_shuffled)
        self.assertEqual(invalid, 0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].scheme_code, 119551)
        self.assertEqual(rows[0].isin_payout_or_growth, "INF209KA12Z1")
        self.assertEqual(rows[0].scheme_name, "Aditya Birla Sun Life Banking Fund")
        self.assertEqual(rows[0].nav, Decimal("106.9996"))
        self.assertEqual(rows[0].nav_date, date(2026, 8, 20))

    def test_unparseable_low_confidence_layout_fails_loudly(self) -> None:
        sample_unparseable = [["foo", "bar", "baz", "qux", "quux"]]
        with self.assertRaises(ValueError):
            detect_feed_layout(sample_unparseable, 5)

    def test_drift_check_does_not_persist_until_saved(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            layout_6col = {6: ColumnMap(0, 1, 2, 3, 4, 5, "6col_legacy", 1.0)}
            layout_8col = {8: ColumnMap(0, 1, 2, 3, 6, 7, "8col_standard", 1.0)}

            profile_file = data_dir / ".feed_profile.json"
            self.assertFalse(profile_file.exists())
            drift = check_feed_drift(layout_6col, data_dir)
            self.assertFalse(drift)
            self.assertFalse(profile_file.exists(), "drift check alone must not write the profile")

            save_feed_profile(layout_6col, 100, date(2026, 4, 1), data_dir)
            self.assertTrue(profile_file.exists())
            profile_data = json.loads(profile_file.read_text(encoding="utf-8"))
            self.assertIn("6", profile_data["layouts"])

            drift_2 = check_feed_drift(layout_8col, data_dir)
            self.assertTrue(drift_2)
            still = json.loads(profile_file.read_text(encoding="utf-8"))
            self.assertIn("6", still["layouts"], "failed run must not overwrite the saved profile")

            with self.assertRaises(RuntimeError):
                check_feed_drift(layout_8col, data_dir, strict_drift=True)

            acknowledged = check_feed_drift(layout_8col, data_dir, allow_feed_drift=True, strict_drift=True)
            self.assertTrue(acknowledged)

    def test_row_count_sanity_gate(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            db_path = data_dir / "nav_fy_2026_27.db"

            from scripts.fetch_and_update import init_db

            init_db(db_path)
            with closing(sqlite3.connect(db_path)) as conn:
                for d in range(1, 8):
                    dt_str = f"2026-04-0{d}"
                    for s in range(1001, 2001):
                        conn.execute(
                            "INSERT OR IGNORE INTO schemes (scheme_code, scheme_name, first_seen_date, last_seen_date) VALUES (?, 'Test', ?, ?)",
                            (s, dt_str, dt_str),
                        )
                        conn.execute(
                            "INSERT OR IGNORE INTO nav_history (scheme_code, nav_date, nav) VALUES (?, ?, '10.0000')",
                            (s, dt_str),
                        )
                conn.commit()

            check_row_count_plausibility(900, data_dir, min_ratio=0.80)

            with self.assertRaises(RuntimeError) as ctx:
                check_row_count_plausibility(700, data_dir, min_ratio=0.80)
            self.assertIn("Row count sanity gate failed", str(ctx.exception))

    def test_stale_feed_warning(self) -> None:
        rows = [NavRow(100001, None, None, "Test", Decimal("10.00"), date(2026, 8, 1))]
        check_stale_feed(rows, date(2026, 8, 11), max_stale_days=4)

    def test_sync_down_databases_from_r2_parallel(self) -> None:
        config = R2Config(
            account_id="account123",
            bucket="nav-archive",
            access_key_id="access",
            secret_access_key="secret",
            endpoint="https://account123.r2.cloudflarestorage.com",
        )
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            p1 = data_dir / "nav_fy_2026_27.db"
            p2 = data_dir / "nav_fy_2027_28.db"
            with patch("scripts.fetch_and_update.download_object") as mock_download:
                sync_down_databases_from_r2({p1, p2}, data_dir, config, max_workers=2)
                self.assertEqual(mock_download.call_count, 2)

    def test_sync_up_databases_to_r2_parallel(self) -> None:
        config = R2Config(
            account_id="account123",
            bucket="nav-archive",
            access_key_id="access",
            secret_access_key="secret",
            endpoint="https://account123.r2.cloudflarestorage.com",
        )
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            p1 = data_dir / "nav_fy_2026_27.db"
            p1.write_bytes(b"data1")
            with (
                patch("scripts.fetch_and_update.validate_database", return_value=0),
                patch("scripts.fetch_and_update.atomic_upload_object") as mock_upload,
            ):
                sync_up_databases_to_r2({p1: "old_hash"}, data_dir, config, max_workers=2)
                mock_upload.assert_called_once()

    def test_write_schemes_json_preserves_existing_schemes(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            target = data_dir / "schemes.json.gz"

            # Create existing schemes.json.gz with old scheme 100001
            initial_data = {"100001": ["INF001", "", "Old Scheme 1"]}
            with gzip.open(target, "wt", encoding="utf-8") as gz:
                json.dump(initial_data, gz)

            # Update with partition that only contains scheme 200002
            p2 = data_dir / "nav_fy_2027_28.db"
            rows, _ = parse_nav_text(sample_line(200002, "New Scheme 2", "15.00", "05-Jun-2027"))
            update_databases(rows, date(2027, 6, 5), data_dir)

            write_schemes_json([p2], target_path=target)

            with gzip.open(target, "rt", encoding="utf-8") as gz:
                merged = json.load(gz)

            self.assertIn("100001", merged)
            self.assertEqual(merged["100001"][2], "Old Scheme 1")
            self.assertIn("200002", merged)
            self.assertEqual(merged["200002"][2], "New Scheme 2")

    @unittest.skipUnless(os.environ.get("LIVE_FEED") == "1", "Requires LIVE_FEED=1")
    def test_live_amfi_canary(self) -> None:
        from scripts.fetch_and_update import fetch_text

        text = fetch_text("https://portal.amfiindia.com/spages/NAVAll.txt")
        rows, invalid, layouts = parse_nav_feed(text)
        self.assertGreater(len(rows), 5000, "Expected > 5,000 schemes in real AMFI feed")
        self.assertLess(invalid, len(rows) * 0.05, "Invalid rows should be < 5% of feed")
        self.assertGreater(len(layouts), 0, "Expected at least 1 detected layout")


if __name__ == "__main__":
    unittest.main()
