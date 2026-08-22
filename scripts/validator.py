from __future__ import annotations

import argparse
import logging
import sqlite3
from contextlib import closing
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"

SAMPLE_LOG_LIMIT = 5


def _newest_julianday(conn: sqlite3.Connection) -> float | None:
    row = conn.execute("SELECT MAX(julianday(nav_date)) FROM nav_history").fetchone()
    return row[0] if row and row[0] is not None else None


def validate_database(
    db_path: Path,
    gap_days: int = 45,
    jump_threshold: float = 0.50,
    window_days: int = 90,
    fail_on_anomaly: bool = False,
    max_gaps: int = 10,
    max_jumps: int = 10,
) -> int:
    """Validate one archive partition.

    Structural problems (missing tables, duplicate facts) always return 1.
    Gap/jump anomalies inside the most recent ``window_days`` relative to the
    newest NAV date in the database are counted in full and sampled in logs;
    pass ``fail_on_anomaly=True`` to turn threshold breaches into exit code 1.
    ``window_days=0`` scans full history.
    """
    if not db_path.exists():
        logging.error("Database does not exist: %s", db_path)
        return 1

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

        window_sql = ""
        if window_days > 0:
            newest = _newest_julianday(conn)
            if newest is None:
                logging.info("Validation completed for %s: empty fact table", db_path)
                return 0
            window_sql = f"AND julianday(nav_date) >= {newest - float(window_days)}"

        gaps = conn.execute(
            f"""
            WITH ordered AS (
                SELECT
                    scheme_code,
                    nav_date,
                    LAG(nav_date) OVER (PARTITION BY scheme_code ORDER BY nav_date) AS previous_nav_date
                FROM nav_history
            )
            SELECT scheme_code, previous_nav_date, nav_date
            FROM ordered AS o
            WHERE o.previous_nav_date IS NOT NULL
              AND julianday(o.nav_date) - julianday(o.previous_nav_date) > ?
              {window_sql.replace("julianday(nav_date)", "julianday(o.nav_date)")}
            """,
            (gap_days,),
        ).fetchall()

        jumps = conn.execute(
            f"""
            WITH ordered AS (
                SELECT
                    scheme_code,
                    nav_date,
                    CAST(nav AS REAL) AS nav,
                    LAG(CAST(nav AS REAL)) OVER (PARTITION BY scheme_code ORDER BY nav_date) AS previous_nav
                FROM nav_history
            )
            SELECT scheme_code, nav_date, previous_nav, nav
            FROM ordered
            WHERE previous_nav IS NOT NULL
              AND previous_nav != 0
              AND ABS(nav - previous_nav) / ABS(previous_nav) > ?
              {window_sql}
            """,
            (jump_threshold,),
        ).fetchall()

    for scheme_code, previous_date, nav_date in gaps[:SAMPLE_LOG_LIMIT]:
        logging.warning("Long NAV gap for scheme %s: %s to %s", scheme_code, previous_date, nav_date)
    for scheme_code, nav_date, previous_nav, nav in jumps[:SAMPLE_LOG_LIMIT]:
        logging.warning(
            "Large NAV jump for scheme %s on %s: %s -> %s",
            scheme_code,
            nav_date,
            previous_nav,
            nav,
        )

    gap_count = len(gaps)
    jump_count = len(jumps)
    logging.info(
        "Validation completed for %s: %s long gaps, %s large jumps (window: %s)",
        db_path,
        gap_count,
        jump_count,
        f"{window_days}d" if window_days > 0 else "full",
    )

    if fail_on_anomaly and (gap_count > max_gaps or jump_count > max_jumps):
        logging.error(
            "Anomaly gate failed: gaps=%s (max %s), jumps=%s (max %s)",
            gap_count,
            max_gaps,
            jump_count,
            max_jumps,
        )
        return 1

    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate the AMFI NAV SQLite archive.")
    parser.add_argument("--db", type=Path, help="Specific database to validate. If omitted, all .db files in data/ are validated.")
    parser.add_argument("--gap-days", type=int, default=45)
    parser.add_argument("--jump-threshold", type=float, default=0.50)
    parser.add_argument(
        "--window-days",
        type=int,
        default=90,
        help="Only count anomalies within this many days of the newest NAV date (0 = full history).",
    )
    parser.add_argument(
        "--fail-on-anomaly",
        action="store_true",
        help="Exit non-zero when anomaly counts breach the configured maximums.",
    )
    parser.add_argument("--max-gaps", type=int, default=10, help="Maximum tolerated long gaps under --fail-on-anomaly.")
    parser.add_argument("--max-jumps", type=int, default=10, help="Maximum tolerated large jumps under --fail-on-anomaly.")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_arg_parser().parse_args(argv)

    def run(db_path: Path) -> int:
        return validate_database(
            db_path,
            args.gap_days,
            args.jump_threshold,
            window_days=args.window_days,
            fail_on_anomaly=args.fail_on_anomaly,
            max_gaps=args.max_gaps,
            max_jumps=args.max_jumps,
        )

    if args.db:
        return run(args.db)

    db_files = list(DATA_DIR.glob("*.db"))
    if not db_files:
        logging.info("No databases found in %s", DATA_DIR)
        return 0

    exit_code = 0
    for db_path in sorted(db_files):
        if run(db_path) != 0:
            exit_code = 1

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
