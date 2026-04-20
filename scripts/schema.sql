PRAGMA foreign_keys = ON;
PRAGMA user_version = 3;

CREATE TABLE IF NOT EXISTS schema_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO schema_metadata (key, value, updated_at)
VALUES ('schema_version', '3', CURRENT_TIMESTAMP)
ON CONFLICT(key) DO UPDATE SET
    value = excluded.value,
    updated_at = excluded.updated_at;

CREATE TABLE IF NOT EXISTS schemes (
    scheme_code INTEGER PRIMARY KEY,
    isin_payout_or_growth TEXT,
    isin_reinvestment TEXT,
    scheme_name TEXT NOT NULL,
    first_seen_date TEXT NOT NULL,
    last_seen_date TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS nav_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scheme_code INTEGER NOT NULL,
    nav_date TEXT NOT NULL,
    nav TEXT NOT NULL,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (scheme_code) REFERENCES schemes (scheme_code),
    UNIQUE (scheme_code, nav_date)
);

CREATE INDEX IF NOT EXISTS idx_nav_history_scheme_date
    ON nav_history (scheme_code, nav_date);

CREATE INDEX IF NOT EXISTS idx_nav_history_nav_date
    ON nav_history (nav_date);

CREATE INDEX IF NOT EXISTS idx_nav_history_nav_date_scheme
    ON nav_history (nav_date, scheme_code);

CREATE INDEX IF NOT EXISTS idx_nav_history_scheme_date_nav
    ON nav_history (scheme_code, nav_date, nav);

CREATE INDEX IF NOT EXISTS idx_schemes_active
    ON schemes (is_active);

CREATE INDEX IF NOT EXISTS idx_schemes_last_seen
    ON schemes (last_seen_date);

CREATE INDEX IF NOT EXISTS idx_schemes_active_name
    ON schemes (is_active, scheme_name);

CREATE INDEX IF NOT EXISTS idx_schemes_name
    ON schemes (scheme_name);

CREATE INDEX IF NOT EXISTS idx_schemes_last_seen_active
    ON schemes (last_seen_date, is_active);
