"""
Loss accounting engine for Never Lose a Heartbeat.
Distinguishes between mock-reported losses, detected gaps, session restarts, and write failures.
"""

import logging
from typing import Dict, List, Optional, Set, Tuple

from heartbeat.protocol import LossItem
from heartbeat.storage import DeviceStorage

logger = logging.getLogger(__name__)


class LossTracker:
    """Manages loss detection, classification, and persistence."""

    def __init__(self, storage: DeviceStorage):
        self.storage = storage
        # Cache of known lost ranges per session and sensor to avoid duplicate gap records
        self._known_loss_ranges: Dict[Tuple[str, int], List[Tuple[int, Optional[int]]]] = {}
        # In-memory buffer for disk-full losses when local storage is temporarily un-writable
        self._pending_disk_losses: List[Dict] = []

    def track_batch_losses(
        self,
        session_id: str,
        sensor: int,
        first_index: int,
        count: int,
        explicit_losses: List[LossItem],
        last_persisted_index: Optional[int],
    ) -> List[LossItem]:
        """
        Inspects incoming batch for gaps and reconciles with explicit losses.
        Returns combined list of loss items to persist.
        """
        all_losses: List[LossItem] = list(explicit_losses)

        # 1. Track explicit losses in cache
        key = (session_id, sensor)
        if key not in self._known_loss_ranges:
            self._known_loss_ranges[key] = []

        for item in explicit_losses:
            self._known_loss_ranges[key].append((item.from_index, item.to_index))

        # 2. Check for unaccounted index gap
        if last_persisted_index is not None and first_index > last_persisted_index + 1:
            gap_start = last_persisted_index + 1
            gap_end = first_index - 1

            # Check if this range is already covered by explicit_losses
            covered = False
            for f_idx, t_idx in self._known_loss_ranges[key]:
                if t_idx is not None and f_idx <= gap_start and t_idx >= gap_end:
                    covered = True
                    break

            if not covered:
                logger.warning(
                    "Detected unaccounted index gap: session %s, sensor %d, range [%d, %d]",
                    session_id,
                    sensor,
                    gap_start,
                    gap_end,
                )
                gap_loss = LossItem(
                    from_index=gap_start,
                    to_index=gap_end,
                    reason="gap_detected",
                )
                all_losses.append(gap_loss)
                self._known_loss_ranges[key].append((gap_start, gap_end))

        return all_losses

    def handle_session_restart(self, old_session_id: str, new_session_id: str):
        """
        Handles transition when mock restarts and generates a new session.
        Records unknown end loss (to_index=None) for sensors in the prior session.
        """
        logger.info("Handling mock restart: old_session=%s -> new_session=%s", old_session_id, new_session_id)
        for sensor in (1, 2):
            last_idx = self.storage.get_last_persisted_index(old_session_id, sensor)
            from_idx = (last_idx + 1) if last_idx is not None else 0
            self.storage.record_loss(
                session_id=old_session_id,
                sensor=sensor,
                from_index=from_idx,
                to_index=None,
                reason="mock_restarted",
            )
        self.storage.end_session(old_session_id)
        self.storage.record_session(new_session_id)

    def handle_disk_full_loss(self, session_id: str, sensor: int, first_index: int, count: int):
        """
        Records unrecoverable loss when persistence fails due to disk-full conditions.
        If disk write fails, buffers in memory until disk space becomes available.
        """
        end_idx = first_index + count - 1
        logger.critical(
            "Disk full! Recording persistence loss: session %s, sensor %d, range [%d, %d]",
            session_id,
            sensor,
            first_index,
            end_idx,
        )
        loss_dict = {
            "session_id": session_id,
            "sensor": sensor,
            "from_index": first_index,
            "to_index": end_idx,
            "reason": "disk_full",
        }
        try:
            self.storage.record_loss(
                session_id=session_id,
                sensor=sensor,
                from_index=first_index,
                to_index=end_idx,
                reason="disk_full",
            )
        except Exception as e:
            logger.warning("Could not persist disk_full record immediately, buffering in memory: %s", e)
            self._pending_disk_losses.append(loss_dict)

    def flush_pending_losses(self):
        """Attempts to flush buffered disk-full losses once disk pressure subsides."""
        if not self._pending_disk_losses:
            return
        remaining = []
        for item in self._pending_disk_losses:
            try:
                self.storage.record_loss(
                    session_id=item["session_id"],
                    sensor=item["sensor"],
                    from_index=item["from_index"],
                    to_index=item["to_index"],
                    reason=item["reason"],
                )
            except Exception:
                remaining.append(item)
        self._pending_disk_losses = remaining
