"""
cloud/storage.py: Storage engine supporting SQLite and PostgreSQL with unified DDL templates,
query parameter translation, and memory-bounded streaming exports.
"""

import datetime
import json
import logging
import os
import sqlite3
from typing import Callable, Generator, List, Tuple

logger = logging.getLogger(__name__)


class CloudStorage:
    _DDL_TEMPLATE = """
        CREATE TABLE IF NOT EXISTS cloud_sessions (
            session_id {t_sess_id} PRIMARY KEY,
            started_at {t_time} NOT NULL,
            ended_at {t_time}
        );
        CREATE TABLE IF NOT EXISTS cloud_samples (
            session_id {t_sess_id} NOT NULL,
            sensor {t_int} NOT NULL,
            sample_index {t_int} NOT NULL,
            val0 {t_int} NOT NULL,
            val1 {t_int} NOT NULL,
            val2 {t_int},
            created_at {t_time} NOT NULL,
            PRIMARY KEY (session_id, sensor, sample_index)
        );
        CREATE TABLE IF NOT EXISTS cloud_loss_records (
            id {t_pk_auto},
            session_id {t_sess_id} NOT NULL,
            sensor {t_int} NOT NULL,
            from_index {t_int} NOT NULL,
            to_index {t_int},
            reason TEXT NOT NULL,
            detected_at {t_time} NOT NULL,
            UNIQUE (session_id, sensor, from_index, to_index, reason)
        );
        CREATE INDEX IF NOT EXISTS idx_cloud_loss_lookup ON cloud_loss_records (session_id, sensor);
        CREATE TABLE IF NOT EXISTS cloud_processing_results (
            session_id {t_sess_id} NOT NULL,
            window_index {t_int} NOT NULL,
            mode {t_mode} NOT NULL,
            missing_1 {t_int} NOT NULL,
            missing_2 {t_int} NOT NULL,
            complete {t_int} NOT NULL,
            channels_json TEXT NOT NULL,
            magnitude {t_real},
            heart_rate {t_real},
            processed_at {t_time} NOT NULL,
            PRIMARY KEY (session_id, window_index, mode)
        );
        CREATE TABLE IF NOT EXISTS cloud_processed_windows (
            session_id {t_sess_id} NOT NULL,
            window_index {t_int} NOT NULL,
            mode {t_mode} NOT NULL,
            PRIMARY KEY (session_id, window_index, mode)
        );
    """

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

    @staticmethod
    def _row_val(row, idx: int, key: str = ""):
        """Safely extracts a column value by index or column name across tuple, dict, and Row types."""
        if isinstance(row, tuple):
            return row[idx]
        if key and hasattr(row, "__getitem__"):
            try:
                return row[key]
            except (KeyError, IndexError, TypeError):
                pass
        return row[idx]

    @staticmethod
    def _format_sql(sql: str, is_sqlite: bool) -> str:
        """Converts standard ? parameter placeholders to %s for PostgreSQL."""
        return sql if is_sqlite else sql.replace("?", "%s")

    @classmethod
    def get_ddl(cls, is_sqlite: bool) -> str:
        """Generates dialect-accurate DDL from a single unified schema template."""
        types = {
            "t_sess_id": "TEXT" if is_sqlite else "VARCHAR(64)",
            "t_time": "TEXT" if is_sqlite else "TIMESTAMPTZ",
            "t_int": "INTEGER" if is_sqlite else "INT",
            "t_pk_auto": "INTEGER PRIMARY KEY AUTOINCREMENT" if is_sqlite else "SERIAL PRIMARY KEY",
            "t_real": "REAL" if is_sqlite else "DOUBLE PRECISION",
            "t_mode": "TEXT" if is_sqlite else "VARCHAR(16)",
        }
        return cls._DDL_TEMPLATE.format(**types)

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
            ddl = self.get_ddl(self.is_sqlite)
            if self.is_sqlite:
                with conn:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.executescript(ddl)
            else:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(ddl)
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
            return [self._row_val(r, 0, "session_id") for r in cur.fetchall()]
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
                    "session_id": self._row_val(r, 0, "session_id"),
                    "sensor": self._row_val(r, 1, "sensor"),
                    "max_index": self._row_val(r, 2, "max_idx"),
                    "count": self._row_val(r, 3, "count"),
                })
            return {"total_samples": total_samples, "sessions": sessions}
        finally:
            conn.close()

    def _stream_query(
        self,
        sql: str,
        params: tuple,
        row_mapper: Callable,
        chunk_size: int = 1000,
    ) -> Generator[str, None, None]:
        """Generic memory-bounded streaming helper across database dialects."""
        formatted_sql = self._format_sql(sql, self.is_sqlite)
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(formatted_sql, params)
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                for row in rows:
                    yield row_mapper(row)
        finally:
            conn.close()

    def export_session_samples(self, session_id: str, chunk_size: int = 1000) -> Generator[str, None, None]:
        sql = "SELECT sensor, sample_index, val0, val1, val2 FROM cloud_samples WHERE session_id = ? ORDER BY sample_index ASC, sensor ASC"

        def mapper(row):
            sensor = self._row_val(row, 0, "sensor")
            idx = self._row_val(row, 1, "sample_index")
            val0 = self._row_val(row, 2, "val0")
            val1 = self._row_val(row, 3, "val1")
            val2 = self._row_val(row, 4, "val2")
            vals = [val0, val1] if val2 is None else [val0, val1, val2]
            return json.dumps({"sensor": sensor, "index": idx, "values": vals})

        return self._stream_query(sql, (session_id,), mapper, chunk_size)

    def export_session_losses(self, session_id: str, chunk_size: int = 1000) -> Generator[str, None, None]:
        sql = "SELECT sensor, from_index, to_index, reason FROM cloud_loss_records WHERE session_id = ? ORDER BY from_index ASC, sensor ASC"

        def mapper(row):
            sensor = self._row_val(row, 0, "sensor")
            f_idx = self._row_val(row, 1, "from_index")
            t_idx = self._row_val(row, 2, "to_index")
            reason = self._row_val(row, 3, "reason")
            return json.dumps({"sensor": sensor, "from_index": f_idx, "to_index": t_idx, "reason": reason})

        return self._stream_query(sql, (session_id,), mapper, chunk_size)

    def export_results(self, session_id: str, chunk_size: int = 500) -> Generator[str, None, None]:
        sql = "SELECT window_index, mode, missing_1, missing_2, complete, channels_json, magnitude, heart_rate FROM cloud_processing_results WHERE session_id = ? ORDER BY window_index ASC, mode ASC"

        def mapper(row):
            w_idx = self._row_val(row, 0, "window_index")
            mode = self._row_val(row, 1, "mode")
            m1 = self._row_val(row, 2, "missing_1")
            m2 = self._row_val(row, 3, "missing_2")
            complete = self._row_val(row, 4, "complete")
            ch_json = self._row_val(row, 5, "channels_json")
            mag = self._row_val(row, 6, "magnitude")
            hr = self._row_val(row, 7, "heart_rate")
            return json.dumps({
                "window": w_idx,
                "mode": mode,
                "missing_1": m1,
                "missing_2": m2,
                "complete": bool(complete),
                "channels": json.loads(ch_json),
                "magnitude": mag,
                "heart_rate": hr,
            })

        return self._stream_query(sql, (session_id,), mapper, chunk_size)
