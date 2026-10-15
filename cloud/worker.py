"""
cloud/worker.py: Production cloud background worker for online biosignal window processing.
Handles concurrent batch triggers safely with session-level debounce and serialization.
"""

import datetime
import json
import logging
import threading
from typing import Dict, Optional, Set

from cloud.storage import CloudStorage
from heartbeat.processor import WINDOW_SIZE, process_window

logger = logging.getLogger(__name__)


class CloudWorker:
    """
    Processes streaming biosignal windows in the cloud with mode='online'.
    Guarantees thread-safety and avoids database locking during concurrent batch ingestion.
    """

    def __init__(self, storage: CloudStorage):
        self.storage = storage
        self._session_locks: Dict[str, threading.Lock] = {}
        self._pending_reruns: Dict[str, bool] = {}
        self._meta_lock = threading.Lock()

    def _get_session_lock(self, session_id: str) -> threading.Lock:
        with self._meta_lock:
            if session_id not in self._session_locks:
                self._session_locks[session_id] = threading.Lock()
            return self._session_locks[session_id]

    def process_pending_windows(self, session_id: Optional[str] = None) -> int:
        """
        Thread-safe entrypoint for online window processing.
        If a processing run is already active for this session, marks a rerun flag
        and returns immediately, coalescing rapid batch arrivals into a single worker pass.
        """
        if not session_id:
            sessions = self.storage.list_session_ids()
            total = 0
            for s in sessions:
                total += self.process_pending_windows(s)
            return total

        lock = self._get_session_lock(session_id)
        # Attempt non-blocking lock acquisition to prevent worker thread pileups
        if not lock.acquire(blocking=False):
            with self._meta_lock:
                self._pending_reruns[session_id] = True
            logger.debug("Worker already in-flight for session %s; queued rerun pass", session_id)
            return 0

        try:
            total_processed = 0
            while True:
                with self._meta_lock:
                    self._pending_reruns[session_id] = False

                processed_in_pass = self._process_session_windows(session_id)
                total_processed += processed_in_pass

                with self._meta_lock:
                    rerun_needed = self._pending_reruns.get(session_id, False)

                # If no new triggers arrived or no more complete windows exist, terminate pass
                if not rerun_needed or processed_in_pass == 0:
                    break

            return total_processed
        finally:
            lock.release()

    def _process_session_windows(self, session_id: str) -> int:
        """Internal processor executing a single sweep over unprocessed windows for a session."""
        conn = self.storage.get_connection()
        try:
            cur = conn.cursor()

            # Determine maximum sample index available
            if self.storage.is_sqlite:
                cur.execute("SELECT MAX(sample_index) FROM cloud_samples WHERE session_id = ?", (session_id,))
            else:
                cur.execute("SELECT MAX(sample_index) FROM cloud_samples WHERE session_id = %s", (session_id,))

            max_row = cur.fetchone()
            if not max_row or max_row[0] is None:
                return 0

            max_idx = max_row[0]
            max_window = (max_idx + 1) // WINDOW_SIZE

            # Fetch set of already processed online windows
            if self.storage.is_sqlite:
                cur.execute(
                    "SELECT window_index FROM cloud_processed_windows WHERE session_id = ? AND mode = 'online'",
                    (session_id,),
                )
            else:
                cur.execute(
                    "SELECT window_index FROM cloud_processed_windows WHERE session_id = %s AND mode = 'online'",
                    (session_id,),
                )

            processed: Set[int] = {r[0] if isinstance(r, tuple) else r["window_index"] for r in cur.fetchall()}

            windows_to_process = [w for w in range(max_window) if w not in processed]
            if not windows_to_process:
                return 0

            processed_count = 0
            for w in windows_to_process:
                w_start = 400 * w
                w_end = 400 * w + 399

                # 1. Fetch sensor 1 samples in window
                if self.storage.is_sqlite:
                    cur.execute(
                        """SELECT sample_index, val0, val1, val2 
                           FROM cloud_samples 
                           WHERE session_id = ? AND sensor = 1 AND sample_index BETWEEN ? AND ?""",
                        (session_id, w_start, w_end),
                    )
                else:
                    cur.execute(
                        """SELECT sample_index, val0, val1, val2 
                           FROM cloud_samples 
                           WHERE session_id = %s AND sensor = 1 AND sample_index BETWEEN %s AND %s""",
                        (session_id, w_start, w_end),
                    )
                s1_samples = {}
                for r in cur.fetchall():
                    idx = r[0] if isinstance(r, tuple) else r["sample_index"]
                    v0 = r[1] if isinstance(r, tuple) else r["val0"]
                    v1 = r[2] if isinstance(r, tuple) else r["val1"]
                    v2 = r[3] if isinstance(r, tuple) else r["val2"]
                    s1_samples[idx] = [v0, v1, v2]

                # 2. Fetch sensor 2 samples in window
                if self.storage.is_sqlite:
                    cur.execute(
                        """SELECT sample_index, val0, val1 
                           FROM cloud_samples 
                           WHERE session_id = ? AND sensor = 2 AND sample_index BETWEEN ? AND ?""",
                        (session_id, w_start, w_end),
                    )
                else:
                    cur.execute(
                        """SELECT sample_index, val0, val1 
                           FROM cloud_samples 
                           WHERE session_id = %s AND sensor = 2 AND sample_index BETWEEN %s AND %s""",
                        (session_id, w_start, w_end),
                    )
                s2_samples = {}
                for r in cur.fetchall():
                    idx = r[0] if isinstance(r, tuple) else r["sample_index"]
                    v0 = r[1] if isinstance(r, tuple) else r["val0"]
                    v1 = r[2] if isinstance(r, tuple) else r["val1"]
                    s2_samples[idx] = [v0, v1]

                # 3. Fetch preceding context for ECG heart rate calculation if w >= 4
                s1_ch2_history = {}
                if w >= 4:
                    hr_start = 400 * (w - 4) - 800
                    hr_end = 400 * (w + 1)
                    if self.storage.is_sqlite:
                        cur.execute(
                            """SELECT sample_index, val1 
                               FROM cloud_samples 
                               WHERE session_id = ? AND sensor = 1 AND sample_index BETWEEN ? AND ?""",
                            (session_id, hr_start, hr_end),
                        )
                    else:
                        cur.execute(
                            """SELECT sample_index, val1 
                               FROM cloud_samples 
                               WHERE session_id = %s AND sensor = 1 AND sample_index BETWEEN %s AND %s""",
                            (session_id, hr_start, hr_end),
                        )
                    for r in cur.fetchall():
                        idx = r[0] if isinstance(r, tuple) else r["sample_index"]
                        v1 = r[1] if isinstance(r, tuple) else r["val1"]
                        s1_ch2_history[idx] = v1

                # 4. Process using shared deterministic pure mathematical engine
                try:
                    res = process_window(
                        window_idx=w,
                        sensor1_samples=s1_samples,
                        sensor2_samples=s2_samples,
                        sensor1_ch2_history=s1_ch2_history,
                        mode="online",
                    )
                except Exception as e:
                    logger.error("Error computing window %d for session %s: %s", w, session_id, e)
                    continue

                # 5. Persist online processing result transactionally
                now_str = datetime.datetime.utcnow().isoformat()
                if self.storage.is_sqlite:
                    with conn:
                        conn.execute(
                            """INSERT OR REPLACE INTO cloud_processing_results 
                               (session_id, window_index, mode, missing_1, missing_2, complete, channels_json, magnitude, heart_rate, processed_at)
                               VALUES (?, ?, 'online', ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                session_id,
                                w,
                                res["missing_1"],
                                res["missing_2"],
                                1 if res["complete"] else 0,
                                json.dumps(res["channels"]),
                                res["magnitude"],
                                res["heart_rate"],
                                now_str,
                            ),
                        )
                        conn.execute(
                            """INSERT OR IGNORE INTO cloud_processed_windows (session_id, window_index, mode)
                               VALUES (?, ?, 'online')""",
                            (session_id, w),
                        )
                else:
                    with conn:
                        with conn.cursor() as ins_cur:
                            ins_cur.execute(
                                """INSERT INTO cloud_processing_results 
                                   (session_id, window_index, mode, missing_1, missing_2, complete, channels_json, magnitude, heart_rate, processed_at)
                                   VALUES (%s, %s, 'online', %s, %s, %s, %s, %s, %s, %s)
                                   ON CONFLICT (session_id, window_index, mode) DO UPDATE SET
                                   missing_1 = EXCLUDED.missing_1,
                                   missing_2 = EXCLUDED.missing_2,
                                   complete = EXCLUDED.complete,
                                   channels_json = EXCLUDED.channels_json,
                                   magnitude = EXCLUDED.magnitude,
                                   heart_rate = EXCLUDED.heart_rate,
                                   processed_at = EXCLUDED.processed_at""",
                                (
                                    session_id,
                                    w,
                                    res["missing_1"],
                                    res["missing_2"],
                                    1 if res["complete"] else 0,
                                    json.dumps(res["channels"]),
                                    res["magnitude"],
                                    res["heart_rate"],
                                    now_str,
                                ),
                            )
                            ins_cur.execute(
                                """INSERT INTO cloud_processed_windows (session_id, window_index, mode)
                                   VALUES (%s, %s, 'online')
                                   ON CONFLICT DO NOTHING""",
                                (session_id, w),
                            )

                processed_count += 1

            return processed_count
        finally:
            conn.close()
