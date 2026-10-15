"""
Durable SQLite storage engine for Never Lose a Heartbeat.
Enforces WAL mode, synchronous=FULL durability, transactions, and idempotent inserts.
"""

import datetime
import json
import logging
import os
import sqlite3
from typing import Generator, List, Optional, Tuple

from heartbeat.protocol import LossItem

logger = logging.getLogger(__name__)


class DeviceStorage:
    """Manages edge SQLite database with strict durability and transaction semantics."""

    def __init__(self, db_path: str = "data/device.db", synchronous: str = "FULL", journal_mode: str = "WAL"):
        self.db_path = db_path
        self.synchronous = synchronous
        self.journal_mode = journal_mode
        self._ensure_dir()
        self._init_db()

    def _ensure_dir(self):
        dir_name = os.path.dirname(self.db_path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

    def get_connection(self) -> sqlite3.Connection:
        """Opens a SQLite connection configured for durability and concurrency."""
        conn = sqlite3.connect(self.db_path, timeout=10.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(f"PRAGMA journal_mode = {self.journal_mode};")
        cur.execute(f"PRAGMA synchronous = {self.synchronous};")
        cur.execute("PRAGMA busy_timeout = 5000;")
        cur.execute("PRAGMA foreign_keys = ON;")
        cur.close()
        return conn

    def _init_db(self):
        """Initializes tables, indices, and constraints."""
        conn = self.get_connection()
        try:
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.executescript("""
                    CREATE TABLE IF NOT EXISTS sessions (
                        session_id TEXT PRIMARY KEY,
                        started_at TEXT NOT NULL,
                        ended_at TEXT
                    );

                    CREATE TABLE IF NOT EXISTS samples (
                        session_id TEXT NOT NULL,
                        sensor INTEGER NOT NULL,
                        sample_index INTEGER NOT NULL,
                        val0 INTEGER NOT NULL,
                        val1 INTEGER NOT NULL,
                        val2 INTEGER,
                        created_at TEXT NOT NULL,
                        PRIMARY KEY (session_id, sensor, sample_index)
                    );

                    CREATE TABLE IF NOT EXISTS loss_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id TEXT NOT NULL,
                        sensor INTEGER NOT NULL,
                        from_index INTEGER NOT NULL,
                        to_index INTEGER,
                        reason TEXT NOT NULL,
                        detected_at TEXT NOT NULL
                    );

                    CREATE INDEX IF NOT EXISTS idx_loss_records_session 
                    ON loss_records (session_id, sensor);

                    CREATE TABLE IF NOT EXISTS collection_cursor (
                        session_id TEXT NOT NULL,
                        sensor INTEGER NOT NULL,
                        last_persisted_index INTEGER NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (session_id, sensor)
                    );

                    CREATE TABLE IF NOT EXISTS sync_cursor (
                        session_id TEXT NOT NULL,
                        sensor INTEGER NOT NULL,
                        last_uploaded_index INTEGER NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (session_id, sensor)
                    );

                    CREATE TABLE IF NOT EXISTS synced_loss_records (
                        loss_record_id INTEGER PRIMARY KEY,
                        synced_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS processing_results (
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

                    CREATE TABLE IF NOT EXISTS processed_windows (
                        session_id TEXT NOT NULL,
                        window_index INTEGER NOT NULL,
                        mode TEXT NOT NULL,
                        PRIMARY KEY (session_id, window_index, mode)
                    );
                """)
        finally:
            conn.close()

    def record_session(self, session_id: str):
        """Idempotently records session start."""
        now = datetime.datetime.utcnow().isoformat()
        conn = self.get_connection()
        try:
            with conn:
                conn.execute(
                    "INSERT OR IGNORE INTO sessions (session_id, started_at) VALUES (?, ?)",
                    (session_id, now),
                )
        finally:
            conn.close()

    def end_session(self, session_id: str):
        """Marks session as ended."""
        now = datetime.datetime.utcnow().isoformat()
        conn = self.get_connection()
        try:
            with conn:
                conn.execute(
                    "UPDATE sessions SET ended_at = ? WHERE session_id = ?",
                    (now, session_id),
                )
        finally:
            conn.close()

    def get_latest_session_id(self) -> Optional[str]:
        """Returns the most recently recorded session ID, if any."""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT session_id FROM sessions ORDER BY rowid DESC LIMIT 1")
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def list_sessions(self) -> List[dict]:
        """Returns all recorded sessions with metadata and sample counts."""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT s.session_id, s.started_at, s.ended_at, COUNT(samp.sample_index) as sample_count
                FROM sessions s
                LEFT JOIN samples samp ON s.session_id = samp.session_id
                GROUP BY s.session_id
                ORDER BY s.rowid DESC
            """)
            sessions = []
            for r in cur.fetchall():
                sessions.append({
                    "session_id": r["session_id"],
                    "started_at": r["started_at"],
                    "ended_at": r["ended_at"],
                    "samples": r["sample_count"],
                })
            return sessions
        finally:
            conn.close()

    def get_last_persisted_index(self, session_id: str, sensor: int) -> Optional[int]:
        """Returns the highest persisted index for this session and sensor."""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT last_persisted_index FROM collection_cursor WHERE session_id = ? AND sensor = ?",
                (session_id, sensor),
            )
            row = cur.fetchone()
            if row is not None:
                return row[0]
            # Fallback to direct max in samples
            cur.execute(
                "SELECT MAX(sample_index) FROM samples WHERE session_id = ? AND sensor = ?",
                (session_id, sensor),
            )
            max_row = cur.fetchone()
            return max_row[0] if (max_row and max_row[0] is not None) else None
        finally:
            conn.close()

    def persist_batch(
        self,
        session_id: str,
        sensor: int,
        first_index: int,
        data: List[List[int]],
        loss_items: List[LossItem],
        session_end: bool = False,
    ) -> int:
        """
        Transactionally persists samples and loss items, then advances collection_cursor.
        Returns number of samples persisted.
        """
        now = datetime.datetime.utcnow().isoformat()
        conn = self.get_connection()
        try:
            with conn:
                conn.execute("BEGIN IMMEDIATE")

                # Ensure session
                conn.execute(
                    "INSERT OR IGNORE INTO sessions (session_id, started_at) VALUES (?, ?)",
                    (session_id, now),
                )

                # Insert loss records
                for loss in loss_items:
                    conn.execute(
                        """INSERT INTO loss_records (session_id, sensor, from_index, to_index, reason, detected_at)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (session_id, sensor, loss.from_index, loss.to_index, loss.reason, now),
                    )

                # Insert samples
                inserted_count = 0
                sample_rows = []
                for offset, sample in enumerate(data):
                    idx = first_index + offset
                    val0 = sample[0]
                    val1 = sample[1]
                    val2 = sample[2] if len(sample) > 2 else None
                    sample_rows.append((session_id, sensor, idx, val0, val1, val2, now))

                if sample_rows:
                    cur = conn.executemany(
                        """INSERT OR IGNORE INTO samples (session_id, sensor, sample_index, val0, val1, val2, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        sample_rows,
                    )
                    inserted_count = len(sample_rows)

                # Update collection cursor
                if data:
                    max_idx = first_index + len(data) - 1
                    conn.execute(
                        """INSERT INTO collection_cursor (session_id, sensor, last_persisted_index, updated_at)
                           VALUES (?, ?, ?, ?)
                           ON CONFLICT(session_id, sensor) DO UPDATE SET
                           last_persisted_index = MAX(last_persisted_index, excluded.last_persisted_index),
                           updated_at = excluded.updated_at""",
                        (session_id, sensor, max_idx, now),
                    )

                if session_end:
                    conn.execute(
                        "UPDATE sessions SET ended_at = ? WHERE session_id = ?",
                        (now, session_id),
                    )

                return inserted_count
        except sqlite3.OperationalError as e:
            logger.error("SQLite OperationalError during persist_batch: %s", e)
            raise
        finally:
            conn.close()

    def record_loss(
        self,
        session_id: str,
        sensor: int,
        from_index: int,
        to_index: Optional[int],
        reason: str,
    ):
        """Records an explicit loss event."""
        now = datetime.datetime.utcnow().isoformat()
        conn = self.get_connection()
        try:
            with conn:
                conn.execute(
                    """INSERT INTO loss_records (session_id, sensor, from_index, to_index, reason, detected_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (session_id, sensor, from_index, to_index, reason, now),
                )
        finally:
            conn.close()

    def get_sync_status(self) -> Tuple[int, int, int]:
        """
        Returns (samples_saved, samples_in_cloud, backlog).
        Calculated from actual persistent SQLite state.
        """
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM samples")
            samples_saved = cur.fetchone()[0]

            # Cloud count tracked across sessions from sync_cursor
            # Calculate backlog: samples with index > last_uploaded_index for each (session, sensor)
            cur.execute("""
                SELECT s.session_id, s.sensor, COALESCE(sc.last_uploaded_index, -1) as last_uploaded
                FROM (SELECT DISTINCT session_id, sensor FROM samples) s
                LEFT JOIN sync_cursor sc ON s.session_id = sc.session_id AND s.sensor = sc.sensor
            """)
            sync_rows = cur.fetchall()

            backlog = 0
            for row in sync_rows:
                sess_id = row["session_id"]
                sens = row["sensor"]
                last_up = row["last_uploaded"]
                cur.execute(
                    "SELECT COUNT(*) FROM samples WHERE session_id = ? AND sensor = ? AND sample_index > ?",
                    (sess_id, sens, last_up),
                )
                backlog += cur.fetchone()[0]

            samples_in_cloud = max(0, samples_saved - backlog)
            return samples_saved, samples_in_cloud, backlog
        finally:
            conn.close()

    def get_unuploaded_samples(
        self, session_id: str, sensor: int, limit: int = 2000
    ) -> List[Tuple[int, List[int]]]:
        """
        Fetches next batch of unuploaded samples for given session and sensor.
        Returns list of (sample_index, values).
        """
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT last_uploaded_index FROM sync_cursor WHERE session_id = ? AND sensor = ?",
                (session_id, sensor),
            )
            row = cur.fetchone()
            last_idx = row[0] if row else -1

            cur.execute(
                """SELECT sample_index, val0, val1, val2
                   FROM samples
                   WHERE session_id = ? AND sensor = ? AND sample_index > ?
                   ORDER BY sample_index ASC
                   LIMIT ?""",
                (session_id, sensor, last_idx, limit),
            )
            samples = []
            for r in cur.fetchall():
                vals = [r["val0"], r["val1"]]
                if r["val2"] is not None:
                    vals.append(r["val2"])
                samples.append((r["sample_index"], vals))
            return samples
        finally:
            conn.close()

    def get_unuploaded_loss_records(self, session_id: str, limit: int = 500) -> List[dict]:
        """Fetches loss records that have not yet been confirmed synced to the cloud."""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """SELECT lr.id, lr.session_id, lr.sensor, lr.from_index, lr.to_index, lr.reason
                   FROM loss_records lr
                   LEFT JOIN synced_loss_records slr ON lr.id = slr.loss_record_id
                   WHERE lr.session_id = ? AND slr.loss_record_id IS NULL
                   ORDER BY lr.id ASC
                   LIMIT ?""",
                (session_id, limit),
            )
            results = []
            for r in cur.fetchall():
                results.append({
                    "id": r["id"],
                    "session_id": r["session_id"],
                    "sensor": r["sensor"],
                    "from_index": r["from_index"],
                    "to_index": r["to_index"],
                    "reason": r["reason"],
                })
            return results
        finally:
            conn.close()

    def advance_sync_cursor(self, session_id: str, sensor: int, confirmed_index: int):
        """Advances the sync cursor after cloud transaction commits."""
        now = datetime.datetime.utcnow().isoformat()
        conn = self.get_connection()
        try:
            with conn:
                conn.execute(
                    """INSERT INTO sync_cursor (session_id, sensor, last_uploaded_index, updated_at)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT(session_id, sensor) DO UPDATE SET
                       last_uploaded_index = MAX(last_uploaded_index, excluded.last_uploaded_index),
                       updated_at = excluded.updated_at""",
                    (session_id, sensor, confirmed_index, now),
                )
        finally:
            conn.close()

    def mark_loss_records_synced(self, loss_record_ids: List[int]):
        """Marks loss record IDs as confirmed uploaded to the cloud."""
        if not loss_record_ids:
            return
        now = datetime.datetime.utcnow().isoformat()
        conn = self.get_connection()
        try:
            with conn:
                conn.executemany(
                    "INSERT OR IGNORE INTO synced_loss_records (loss_record_id, synced_at) VALUES (?, ?)",
                    [(lid, now) for lid in loss_record_ids],
                )
        finally:
            conn.close()

    def export_session_samples(self, session_id: str) -> Generator[str, None, None]:
        """
        Exports samples for session as formatted JSON Lines.
        Schema: {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}
        """
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """SELECT sensor, sample_index, val0, val1, val2
                   FROM samples
                   WHERE session_id = ?
                   ORDER BY sample_index ASC, sensor ASC""",
                (session_id,),
            )
            for row in cur:
                vals = [row["val0"], row["val1"]]
                if row["val2"] is not None:
                    vals.append(row["val2"])
                rec = {
                    "sensor": row["sensor"],
                    "index": row["sample_index"],
                    "values": vals,
                }
                yield json.dumps(rec)
        finally:
            conn.close()

    def export_session_losses(self, session_id: str) -> Generator[str, None, None]:
        """
        Exports loss records for session as formatted JSON Lines.
        Schema: {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}
        """
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """SELECT sensor, from_index, to_index, reason
                   FROM loss_records
                   WHERE session_id = ?
                   ORDER BY from_index ASC, sensor ASC""",
                (session_id,),
            )
            for row in cur:
                rec = {
                    "sensor": row["sensor"],
                    "from_index": row["from_index"],
                    "to_index": row["to_index"],
                    "reason": row["reason"],
                }
                yield json.dumps(rec)
        finally:
            conn.close()

    def persist_result(
        self,
        session_id: str,
        window_index: int,
        mode: str,
        missing_1: int,
        missing_2: int,
        complete: bool,
        channels: dict,
        magnitude: Optional[float],
        heart_rate: Optional[float],
    ):
        """Persists a window processing result."""
        now = datetime.datetime.utcnow().isoformat()
        conn = self.get_connection()
        try:
            with conn:
                conn.execute(
                    """INSERT OR REPLACE INTO processing_results 
                       (session_id, window_index, mode, missing_1, missing_2, complete, channels_json, magnitude, heart_rate, processed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        session_id,
                        window_index,
                        mode,
                        missing_1,
                        missing_2,
                        1 if complete else 0,
                        json.dumps(channels),
                        magnitude,
                        heart_rate,
                        now,
                    ),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO processed_windows (session_id, window_index, mode) VALUES (?, ?, ?)",
                    (session_id, window_index, mode),
                )
        finally:
            conn.close()

    def export_results(self, session_id: str) -> Generator[str, None, None]:
        """
        Exports processing results for session as formatted JSON Lines.
        """
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """SELECT window_index, mode, missing_1, missing_2, complete, channels_json, magnitude, heart_rate
                   FROM processing_results
                   WHERE session_id = ?
                   ORDER BY window_index ASC, mode ASC""",
                (session_id,),
            )
            for row in cur:
                rec = {
                    "window": row["window_index"],
                    "mode": row["mode"],
                    "missing_1": row["missing_1"],
                    "missing_2": row["missing_2"],
                    "complete": bool(row["complete"]),
                    "channels": json.loads(row["channels_json"]),
                    "magnitude": row["magnitude"],
                    "heart_rate": row["heart_rate"],
                }
                yield json.dumps(rec)
        finally:
            conn.close()
