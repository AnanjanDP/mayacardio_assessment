"""
cloud/storage.py: Storage engine supporting SQLite and PostgreSQL with idempotent batch inserts and streaming exports.
"""

import datetime
import json
import logging
import os
import sqlite3
from typing import Generator, List, Tuple

logger = logging.getLogger(__name__)


class CloudStorage:
    def __init__(self, db_url: str = "sqlite:///data/cloud.db"):
        if db_url.startswith("postgres://"):
            db_url = "postgresql://" + db_url[len("postgres://"):]

        self.db_url = db_url
        self.is_sqlite = db_url.startswith("sqlite")

        if self.is_sqlite:
            self.db_path = db_url.replace("sqlite:///", "")
            dir_name = os.path.dirname(self.db_path)
            if dir_name:
                os.makedirs(dir_name, exist_ok=True)

        self._init_db()

    def get_connection(self):
        if self.is_sqlite:
            conn = sqlite3.connect(self.db_path, timeout=15.0, isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA busy_timeout = 5000;")
            return conn
        else:
            import psycopg2
            url = self.db_url
            if "sslmode=" not in url and ("supabase" in url or "neon.tech" in url):
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}sslmode=require"
            return psycopg2.connect(url)

    def _init_db(self):
        conn = self.get_connection()
        try:
            if self.is_sqlite:
                with conn:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.executescript("""
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
            else:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute("""
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
        finally:
            conn.close()

    def ingest_batch(self, session_id: str, samples: List[dict], loss_records: List[dict]) -> Tuple[int, int]:
        now = datetime.datetime.utcnow().isoformat()
        sample_rows = []
        for s in samples:
            sensor = s["sensor"]
            idx = s["index"]
            vals = s.get("values", [])
            val0 = vals[0] if len(vals) > 0 else 0
            val1 = vals[1] if len(vals) > 1 else 0
            val2 = vals[2] if len(vals) > 2 else None
            sample_rows.append((session_id, sensor, idx, val0, val1, val2, now))

        loss_rows = []
        for lr in loss_records:
            loss_rows.append((
                session_id,
                lr["sensor"],
                lr["from_index"],
                lr.get("to_index"),
                lr.get("reason", "unknown"),
                now,
            ))

        conn = self.get_connection()
        try:
            if self.is_sqlite:
                with conn:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute("INSERT OR IGNORE INTO cloud_sessions (session_id, started_at) VALUES (?, ?)", (session_id, now))
                    if loss_rows:
                        conn.executemany(
                            "INSERT OR IGNORE INTO cloud_loss_records (session_id, sensor, from_index, to_index, reason, detected_at) VALUES (?, ?, ?, ?, ?, ?)",
                            loss_rows,
                        )
                    if sample_rows:
                        conn.executemany(
                            "INSERT OR IGNORE INTO cloud_samples (session_id, sensor, sample_index, val0, val1, val2, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            sample_rows,
                        )
            else:
                from psycopg2.extras import execute_values
                with conn:
                    with conn.cursor() as cur:
                        cur.execute("INSERT INTO cloud_sessions (session_id, started_at) VALUES (%s, %s) ON CONFLICT (session_id) DO NOTHING", (session_id, now))
                        if loss_rows:
                            execute_values(
                                cur,
                                "INSERT INTO cloud_loss_records (session_id, sensor, from_index, to_index, reason, detected_at) VALUES %s ON CONFLICT (session_id, sensor, from_index, to_index, reason) DO NOTHING",
                                loss_rows,
                                page_size=1000,
                            )
                        if sample_rows:
                            execute_values(
                                cur,
                                "INSERT INTO cloud_samples (session_id, sensor, sample_index, val0, val1, val2, created_at) VALUES %s ON CONFLICT (session_id, sensor, sample_index) DO NOTHING",
                                sample_rows,
                                page_size=2000,
                            )
            return len(sample_rows), len(loss_rows)
        finally:
            conn.close()

    def list_session_ids(self) -> List[str]:
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT session_id FROM cloud_sessions ORDER BY started_at DESC")
            return [r[0] if isinstance(r, tuple) else r["session_id"] for r in cur.fetchall()]
        finally:
            conn.close()

    def get_status(self) -> dict:
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM cloud_samples")
            total_samples = cur.fetchone()[0]

            cur.execute("""
                SELECT session_id, sensor, MAX(sample_index) as max_idx, COUNT(*) as count
                FROM cloud_samples
                GROUP BY session_id, sensor
                ORDER BY session_id, sensor
            """)
            sessions = []
            for r in cur.fetchall():
                sessions.append({
                    "session_id": r[0] if isinstance(r, tuple) else r["session_id"],
                    "sensor": r[1] if isinstance(r, tuple) else r["sensor"],
                    "max_index": r[2] if isinstance(r, tuple) else r["max_idx"],
                    "count": r[3] if isinstance(r, tuple) else r["count"],
                })
            return {"total_samples": total_samples, "sessions": sessions}
        finally:
            conn.close()

    def export_session_samples(self, session_id: str, chunk_size: int = 1000) -> Generator[str, None, None]:
        conn = self.get_connection()
        try:
            sql = (
                "SELECT sensor, sample_index, val0, val1, val2 FROM cloud_samples WHERE session_id = ? ORDER BY sample_index ASC, sensor ASC"
                if self.is_sqlite
                else "SELECT sensor, sample_index, val0, val1, val2 FROM cloud_samples WHERE session_id = %s ORDER BY sample_index ASC, sensor ASC"
            )
            cur = conn.cursor()
            cur.execute(sql, (session_id,))
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                for row in rows:
                    sensor = row[0] if isinstance(row, tuple) else row["sensor"]
                    idx = row[1] if isinstance(row, tuple) else row["sample_index"]
                    val0 = row[2] if isinstance(row, tuple) else row["val0"]
                    val1 = row[3] if isinstance(row, tuple) else row["val1"]
                    val2 = row[4] if isinstance(row, tuple) else row["val2"]
                    vals = [val0, val1]
                    if val2 is not None:
                        vals.append(val2)
                    yield json.dumps({"sensor": sensor, "index": idx, "values": vals})
        finally:
            conn.close()

    def export_session_losses(self, session_id: str, chunk_size: int = 1000) -> Generator[str, None, None]:
        conn = self.get_connection()
        try:
            sql = (
                "SELECT sensor, from_index, to_index, reason FROM cloud_loss_records WHERE session_id = ? ORDER BY from_index ASC, sensor ASC"
                if self.is_sqlite
                else "SELECT sensor, from_index, to_index, reason FROM cloud_loss_records WHERE session_id = %s ORDER BY from_index ASC, sensor ASC"
            )
            cur = conn.cursor()
            cur.execute(sql, (session_id,))
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                for row in rows:
                    sensor = row[0] if isinstance(row, tuple) else row["sensor"]
                    f_idx = row[1] if isinstance(row, tuple) else row["from_index"]
                    t_idx = row[2] if isinstance(row, tuple) else row["to_index"]
                    reason = row[3] if isinstance(row, tuple) else row["reason"]
                    yield json.dumps({"sensor": sensor, "from_index": f_idx, "to_index": t_idx, "reason": reason})
        finally:
            conn.close()

    def export_results(self, session_id: str, chunk_size: int = 500) -> Generator[str, None, None]:
        conn = self.get_connection()
        try:
            sql = (
                "SELECT window_index, mode, missing_1, missing_2, complete, channels_json, magnitude, heart_rate FROM cloud_processing_results WHERE session_id = ? ORDER BY window_index ASC, mode ASC"
                if self.is_sqlite
                else "SELECT window_index, mode, missing_1, missing_2, complete, channels_json, magnitude, heart_rate FROM cloud_processing_results WHERE session_id = %s ORDER BY window_index ASC, mode ASC"
            )
            cur = conn.cursor()
            cur.execute(sql, (session_id,))
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                for row in rows:
                    w_idx = row[0] if isinstance(row, tuple) else row["window_index"]
                    mode = row[1] if isinstance(row, tuple) else row["mode"]
                    m1 = row[2] if isinstance(row, tuple) else row["missing_1"]
                    m2 = row[3] if isinstance(row, tuple) else row["missing_2"]
                    complete = row[4] if isinstance(row, tuple) else row["complete"]
                    ch_json = row[5] if isinstance(row, tuple) else row["channels_json"]
                    mag = row[6] if isinstance(row, tuple) else row["magnitude"]
                    hr = row[7] if isinstance(row, tuple) else row["heart_rate"]
                    yield json.dumps({
                        "window": w_idx,
                        "mode": mode,
                        "missing_1": m1,
                        "missing_2": m2,
                        "complete": bool(complete),
                        "channels": json.loads(ch_json),
                        "magnitude": mag,
                        "heart_rate": hr,
                    })
        finally:
            conn.close()
