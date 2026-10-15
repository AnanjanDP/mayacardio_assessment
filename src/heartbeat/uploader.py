"""
Backlog uploader and cloud synchronization worker for Never Lose a Heartbeat.
Transfers unuploaded samples and loss records in bounded batches with idempotent retry.
"""

import logging
import threading
import time
from typing import Optional

from heartbeat.cloud_client import CloudClient
from heartbeat.connectivity import ModeManager
from heartbeat.storage import DeviceStorage

logger = logging.getLogger(__name__)


class BacklogUploader:
    """Manages continuous synchronization and backlog catch-up to the cloud."""

    def __init__(
        self,
        storage: DeviceStorage,
        cloud_client: CloudClient,
        mode_manager: ModeManager,
        batch_size: int = 2000,
        stop_event: Optional[threading.Event] = None,
    ):
        self.storage = storage
        self.cloud_client = cloud_client
        self.mode_manager = mode_manager
        self.batch_size = batch_size
        self.stop_event = stop_event or threading.Event()

    def sync_session_step(self, session_id: str) -> int:
        """
        Executes one synchronization step for a session:
        1. Uploads unuploaded loss records.
        2. Uploads unuploaded samples for sensor 1 and sensor 2.
        Returns number of samples synced in this step.
        """
        if self.mode_manager.get_mode() != "online":
            return 0

        # 1. Sync loss records first
        unuploaded_losses = self.storage.get_unuploaded_loss_records(session_id, limit=200)
        if unuploaded_losses:
            loss_ids = [lr["id"] for lr in unuploaded_losses]
            ok = self.cloud_client.ingest_batch(
                session_id=session_id,
                samples=[],
                loss_records=unuploaded_losses,
            )
            if ok:
                self.storage.mark_loss_records_synced(loss_ids)
            else:
                return 0

        total_synced = 0
        # 2. Sync samples for sensor 1 and sensor 2
        for sensor in (1, 2):
            samples_data = self.storage.get_unuploaded_samples(
                session_id=session_id, sensor=sensor, limit=self.batch_size
            )
            if not samples_data:
                continue

            sample_payload = [
                {"sensor": sensor, "index": idx, "values": vals}
                for idx, vals in samples_data
            ]
            highest_idx = samples_data[-1][0]

            ok = self.cloud_client.ingest_batch(
                session_id=session_id,
                samples=sample_payload,
                loss_records=[],
            )
            if ok:
                self.storage.advance_sync_cursor(session_id, sensor, highest_idx)
                total_synced += len(sample_payload)
            else:
                logger.warning("Upload failed for session %s sensor %d up to index %d", session_id, sensor, highest_idx)
                break

        return total_synced

    def sync_all_sessions(self, drain_backlog: bool = False) -> int:
        """
        Finds all sessions in device storage and syncs their unuploaded data.
        If drain_backlog is True, continues syncing in batches until complete.
        """
        conn = self.storage.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT session_id FROM samples")
            sessions = [r[0] for r in cur.fetchall()]
        finally:
            conn.close()

        total_synced = 0
        for sess in sessions:
            if drain_backlog:
                while not self.stop_event.is_set():
                    batch_synced = self.sync_session_step(sess)
                    if batch_synced == 0:
                        break
                    total_synced += batch_synced
            else:
                synced = self.sync_session_step(sess)
                total_synced += synced
        return total_synced

    def run_loop(self):
        """Continuous synchronization loop active during online mode."""
        logger.info("Starting backlog uploader loop...")
        while not self.stop_event.is_set():
            if self.mode_manager.get_mode() == "online":
                synced = self.sync_all_sessions(drain_backlog=True)
                if synced > 0:
                    time.sleep(0.01)  # fast catchup
                else:
                    time.sleep(0.2)
            else:
                time.sleep(0.5)
        logger.info("Backlog uploader loop stopped.")
