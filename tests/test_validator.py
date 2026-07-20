from __future__ import annotations

import sqlite3
import unittest
from contextlib import closing
from datetime import date
from pathlib import Path

from scripts.fetch_and_update import parse_nav_text, update_databases
from scripts.validator import validate_database
from tests.test_fetch_and_update import WorkspaceTemporaryDirectory, sample_line


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


if __name__ == "__main__":
    unittest.main()
