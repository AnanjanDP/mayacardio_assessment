"""
cloud/worker.py: Production cloud background worker for online biosignal window processing.
Handles concurrent batch triggers safely with session-level debounce and serialization.
"""

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
            q_max = self.storage._format_sql("SELECT MAX(sample_index) FROM cloud_samples WHERE session_id = ?", self.storage.is_sqlite)
            cur.execute(q_max, (session_id,))
            max_row = cur.fetchone()
            if not max_row or max_row[0] is None:
                return 0

            max_idx = max_row[0]
            max_window = (max_idx + 1) // WINDOW_SIZE

            # Fetch set of already processed online windows
            q_proc = self.storage._format_sql("SELECT window_index FROM cloud_processed_windows WHERE session_id = ? AND mode = 'online'", self.storage.is_sqlite)
            cur.execute(q_proc, (session_id,))
            processed: Set[int] = {self.storage._row_val(r, 0, "window_index") for r in cur.fetchall()}

            windows_to_process = [w for w in range(max_window) if w not in processed]
            if not windows_to_process:
                return 0

            q_s1 = self.storage._format_sql(
                "SELECT sample_index, val0, val1, val2 FROM cloud_samples WHERE session_id = ? AND sensor = 1 AND sample_index BETWEEN ? AND ?",
                self.storage.is_sqlite,
            )
            q_s2 = self.storage._format_sql(
                "SELECT sample_index, val0, val1 FROM cloud_samples WHERE session_id = ? AND sensor = 2 AND sample_index BETWEEN ? AND ?",
                self.storage.is_sqlite,
            )
            q_hr = self.storage._format_sql(
                "SELECT sample_index, val1 FROM cloud_samples WHERE session_id = ? AND sensor = 1 AND sample_index BETWEEN ? AND ?",
                self.storage.is_sqlite,
            )

            processed_count = 0
            for w in windows_to_process:
                w_start = 400 * w
                w_end = 400 * w + 399

                # 1. Fetch sensor 1 samples in window
                cur.execute(q_s1, (session_id, w_start, w_end))
                s1_samples = {
                    self.storage._row_val(r, 0, "sample_index"): [
                        self.storage._row_val(r, 1, "val0"),
                        self.storage._row_val(r, 2, "val1"),
                        self.storage._row_val(r, 3, "val2"),
                    ]
                    for r in cur.fetchall()
                }

                # 2. Fetch sensor 2 samples in window
                cur.execute(q_s2, (session_id, w_start, w_end))
                s2_samples = {
                    self.storage._row_val(r, 0, "sample_index"): [
                        self.storage._row_val(r, 1, "val0"),
                        self.storage._row_val(r, 2, "val1"),
                    ]
                    for r in cur.fetchall()
                }

                # 3. Fetch preceding context for ECG heart rate calculation if w >= 4
                s1_ch2_history = {}
                if w >= 4:
                    hr_start = 400 * (w - 4) - 800
                    hr_end = 400 * (w + 1)
                    cur.execute(q_hr, (session_id, hr_start, hr_end))
                    s1_ch2_history = {
                        self.storage._row_val(r, 0, "sample_index"): self.storage._row_val(r, 1, "val1")
                        for r in cur.fetchall()
                    }

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

                # 5. Persist online processing result transactionally via storage engine
                self.storage.persist_result(
                    session_id=session_id,
                    window_index=w,
                    mode="online",
                    result=res,
                    conn=conn,
                )
                processed_count += 1

            return processed_count
        finally:
            conn.close()

