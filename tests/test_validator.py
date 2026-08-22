from __future__ import annotations

import sqlite3
import unittest
from contextlib import closing
from datetime import date, timedelta
from pathlib import Path

from scripts.fetch_and_update import init_db, parse_nav_text, update_databases
from scripts.validator import validate_database
from tests.test_fetch_and_update import WorkspaceTemporaryDirectory, sample_line


def _build_anomalous_database(data_dir: Path) -> Path:
    """Rows: clean day, huge jump, 60-day gap, then a contiguous daily tail."""
    db_path = data_dir / "nav_fy_2026_27.db"
    init_db(db_path)

    dates_and_navs = [
        ("2026-04-01", "10.0000"),
        ("2026-04-02", "30.0000"),  # 200% jump -> anomaly
        ("2026-06-01", "31.0000"),  # 60-day gap -> anomaly
    ]
    current = date(2026, 6, 2)
    while current <= date(2026, 6, 25):  # clean contiguous tail, newest = 2026-06-25
        dates_and_navs.append((current.isoformat(), "31.0000"))
        current += timedelta(days=1)

    with closing(sqlite3.connect(db_path)) as conn:
        conn.execute(
            "INSERT INTO schemes (scheme_code, scheme_name, first_seen_date, last_seen_date)"
            " VALUES (100001, 'Anomaly Fund', '2026-04-01', '2026-06-25')"
        )
        conn.executemany(
            "INSERT INTO nav_history (scheme_code, nav_date, nav) VALUES (100001, ?, ?)",
            dates_and_navs,
        )
        conn.commit()
    return db_path


class ValidatorTests(unittest.TestCase):
    def test_validator_accepts_generated_database(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            rows, _ = parse_nav_text(sample_line())
            update_databases(rows, date(2026, 4, 2), data_dir)

            self.assertEqual(validate_database(data_dir / "nav_fy_2026_27.db"), 0)

    def test_validator_rejects_missing_schema(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bad.db"
            with closing(sqlite3.connect(db_path)) as conn:
                conn.execute("CREATE TABLE placeholder (id INTEGER)")
                conn.commit()

            self.assertEqual(validate_database(db_path), 1)

    def test_full_history_counts_all_anomalies(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            db_path = _build_anomalous_database(Path(tmp) / "data")

            # Warnings alone never fail validation.
            self.assertEqual(
                validate_database(db_path, window_days=0),
                0,
            )
            # ...but the anomaly gate does when thresholds are breached.
            self.assertEqual(
                validate_database(db_path, window_days=0, fail_on_anomaly=True, max_gaps=0, max_jumps=0),
                1,
            )

    def test_recent_window_excludes_old_anomalies(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            db_path = _build_anomalous_database(Path(tmp) / "data")

            # Newest NAV is 2026-06-25; a 10-day window starts 2026-06-15,
            # which excludes both the April jump and the June 1 gap end.
            self.assertEqual(
                validate_database(
                    db_path,
                    window_days=10,
                    fail_on_anomaly=True,
                    max_gaps=0,
                    max_jumps=0,
                ),
                0,
            )

    def test_window_catches_fresh_gap_inside_tail(self) -> None:
        with WorkspaceTemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            db_path = _build_anomalous_database(data_dir)

            # Drop the last two days so the newest row is 2026-06-23 and the
            # next run would see a growing fresh gap inside any small window.
            with closing(sqlite3.connect(db_path)) as conn:
                conn.execute("DELETE FROM nav_history WHERE nav_date >= '2026-06-22'")
                conn.commit()

            self.assertEqual(
                validate_database(db_path, window_days=10, fail_on_anomaly=True, max_gaps=0, max_jumps=0),
                0,
                "tail remains contiguous; nothing new to flag",
            )


if __name__ == "__main__":
    unittest.main()
