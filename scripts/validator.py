from __future__ import annotations

import argparse
import logging
import sqlite3
from contextlib import closing
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT_DIR / "data" / "nav.db"


def validate_database(db_path: Path, gap_days: int = 45, jump_threshold: float = 0.50) -> int:
    if not db_path.exists():
        logging.error("Database does not exist: %s", db_path)
        return 1

    warnings = 0
    with closing(sqlite3.connect(db_path)) as conn:
        table_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM sqlite_master
            WHERE type = 'table' AND name IN ('schemes', 'nav_history', 'schema_metadata')
            """
        ).fetchone()[0]
        if table_count != 3:
            logging.error("Required tables are missing")
            return 1

        duplicate_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT scheme_code, nav_date
                FROM nav_history
                GROUP BY scheme_code, nav_date
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
        if duplicate_count:
            logging.error("Duplicate NAV facts found: %s", duplicate_count)
            return 1

        gaps = conn.execute(
            """
            WITH ordered AS (
                SELECT
                    scheme_code,
                    nav_date,
                    LAG(nav_date) OVER (PARTITION BY scheme_code ORDER BY nav_date) AS previous_nav_date
                FROM nav_history
            )
            SELECT scheme_code, previous_nav_date, nav_date
            FROM ordered
            WHERE previous_nav_date IS NOT NULL
              AND julianday(nav_date) - julianday(previous_nav_date) > ?
            LIMIT 20
            """,
            (gap_days,),
        ).fetchall()
        for scheme_code, previous_date, nav_date in gaps:
            warnings += 1
            logging.warning(
                "Long NAV gap for scheme %s: %s to %s",
                scheme_code,
                previous_date,
                nav_date,
            )

        jumps = conn.execute(
            """
            WITH ordered AS (
                SELECT
                    scheme_code,
                    nav_date,
                    nav,
                    LAG(nav) OVER (PARTITION BY scheme_code ORDER BY nav_date) AS previous_nav
                FROM nav_history
            )
            SELECT scheme_code, nav_date, previous_nav, nav
            FROM ordered
            WHERE previous_nav IS NOT NULL
              AND previous_nav != 0
              AND ABS(nav - previous_nav) / ABS(previous_nav) > ?
            LIMIT 20
            """,
            (jump_threshold,),
        ).fetchall()
        for scheme_code, nav_date, previous_nav, nav in jumps:
            warnings += 1
            logging.warning(
                "Large NAV jump for scheme %s on %s: %s -> %s",
                scheme_code,
                nav_date,
                previous_nav,
                nav,
            )

    logging.info("Validation completed for %s with %s warnings", db_path, warnings)
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate the AMFI NAV SQLite archive.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--gap-days", type=int, default=45)
    parser.add_argument("--jump-threshold", type=float, default=0.50)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_arg_parser().parse_args(argv)
    return validate_database(args.db, args.gap_days, args.jump_threshold)


if __name__ == "__main__":
    raise SystemExit(main())
