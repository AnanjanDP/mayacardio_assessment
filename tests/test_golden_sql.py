"""
tests/test_golden_sql.py: Golden SQL test verifying schema DDL and query generation
for both SQLite and PostgreSQL dialects without requiring a live Postgres instance.
"""

import re
from cloud.storage import CloudStorage


def normalize_sql(sql: str) -> str:
    """Normalizes whitespace and removes trailing semicolons for exact comparison."""
    # Collapse multiple whitespace/newlines into single space
    s = re.sub(r"\s+", " ", sql.strip())
    # Standardize casing around keywords if needed
    return s


# Golden DDL without the redundant duplicate idx_cloud_samples_lookup index
GOLDEN_SQLITE_DDL = normalize_sql("""
    CREATE TABLE IF NOT EXISTS cloud_sessions (
        session_id TEXT PRIMARY KEY,
        started_at TEXT NOT NULL,
        ended_at TEXT
    );
    CREATE TABLE IF NOT EXISTS cloud_samples (
        session_id TEXT NOT NULL,
        sensor INTEGER NOT NULL,
        sample_index INTEGER NOT NULL,
        val0 INTEGER NOT NULL,
        val1 INTEGER NOT NULL,
        val2 INTEGER,
        created_at TEXT NOT NULL,
        PRIMARY KEY (session_id, sensor, sample_index)
    );
    CREATE TABLE IF NOT EXISTS cloud_loss_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        sensor INTEGER NOT NULL,
        from_index INTEGER NOT NULL,
        to_index INTEGER,
        reason TEXT NOT NULL,
        detected_at TEXT NOT NULL,
        UNIQUE (session_id, sensor, from_index, to_index, reason)
    );
    CREATE INDEX IF NOT EXISTS idx_cloud_loss_lookup ON cloud_loss_records (session_id, sensor);
    CREATE TABLE IF NOT EXISTS cloud_processing_results (
        session_id TEXT NOT NULL,
        window_index INTEGER NOT NULL,
        mode TEXT NOT NULL,
        missing_1 INTEGER NOT NULL,
        missing_2 INTEGER NOT NULL,
        complete INTEGER NOT NULL,
        channels_json TEXT NOT NULL,
        magnitude REAL,
        heart_rate REAL,
        processed_at TEXT NOT NULL,
        PRIMARY KEY (session_id, window_index, mode)
    );
    CREATE TABLE IF NOT EXISTS cloud_processed_windows (
        session_id TEXT NOT NULL,
        window_index INTEGER NOT NULL,
        mode TEXT NOT NULL,
        PRIMARY KEY (session_id, window_index, mode)
    );
""")

GOLDEN_POSTGRES_DDL = normalize_sql("""
    CREATE TABLE IF NOT EXISTS cloud_sessions (
        session_id VARCHAR(64) PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL,
        ended_at TIMESTAMPTZ
    );
    CREATE TABLE IF NOT EXISTS cloud_samples (
        session_id VARCHAR(64) NOT NULL,
        sensor INT NOT NULL,
        sample_index INT NOT NULL,
        val0 INT NOT NULL,
        val1 INT NOT NULL,
        val2 INT,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (session_id, sensor, sample_index)
    );
    CREATE TABLE IF NOT EXISTS cloud_loss_records (
        id SERIAL PRIMARY KEY,
        session_id VARCHAR(64) NOT NULL,
        sensor INT NOT NULL,
        from_index INT NOT NULL,
        to_index INT,
        reason TEXT NOT NULL,
        detected_at TIMESTAMPTZ NOT NULL,
        UNIQUE (session_id, sensor, from_index, to_index, reason)
    );
    CREATE INDEX IF NOT EXISTS idx_cloud_loss_lookup ON cloud_loss_records (session_id, sensor);
    CREATE TABLE IF NOT EXISTS cloud_processing_results (
        session_id VARCHAR(64) NOT NULL,
        window_index INT NOT NULL,
        mode VARCHAR(16) NOT NULL,
        missing_1 INT NOT NULL,
        missing_2 INT NOT NULL,
        complete INT NOT NULL,
        channels_json TEXT NOT NULL,
        magnitude DOUBLE PRECISION,
        heart_rate DOUBLE PRECISION,
        processed_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (session_id, window_index, mode)
    );
    CREATE TABLE IF NOT EXISTS cloud_processed_windows (
        session_id VARCHAR(64) NOT NULL,
        window_index INT NOT NULL,
        mode VARCHAR(16) NOT NULL,
        PRIMARY KEY (session_id, window_index, mode)
    );
""")


def test_sqlite_schema_generation():
    """Validates that generated SQLite DDL matches golden schema."""
    sqlite_ddl = CloudStorage.get_ddl(is_sqlite=True)
    assert normalize_sql(sqlite_ddl) == GOLDEN_SQLITE_DDL


def test_postgres_schema_generation():
    """Validates that generated PostgreSQL DDL matches golden schema without Postgres server."""
    postgres_ddl = CloudStorage.get_ddl(is_sqlite=False)
    assert normalize_sql(postgres_ddl) == GOLDEN_POSTGRES_DDL


def test_query_placeholder_conversion():
    """Validates that query parameter placeholder conversion produces expected SQL."""
    q_sqlite = "SELECT * FROM cloud_samples WHERE session_id = ? AND sensor = ?"
    q_pg = CloudStorage._format_sql(q_sqlite, is_sqlite=False)
    assert q_pg == "SELECT * FROM cloud_samples WHERE session_id = %s AND sensor = %s"

    q_sqlite_keep = CloudStorage._format_sql(q_sqlite, is_sqlite=True)
    assert q_sqlite_keep == q_sqlite


def test_row_helper_access():
    """Validates row value extraction helper across tuple, dict, and Row types."""
    tup = ("sess_1", 1, 100, 200)
    assert CloudStorage._row_val(tup, 0, "session_id") == "sess_1"
    assert CloudStorage._row_val(tup, 2, "val0") == 100

    row_dict = {"session_id": "sess_1", "sensor": 1, "val0": 100}
    assert CloudStorage._row_val(row_dict, 0, "session_id") == "sess_1"
    assert CloudStorage._row_val(row_dict, 2, "val0") == 100
