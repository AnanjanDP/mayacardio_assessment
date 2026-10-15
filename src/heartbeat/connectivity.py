"""
Connectivity and Mode Manager for Never Lose a Heartbeat.
Monitors cloud availability and enforces mode switching within 10 seconds.
"""

import logging
import threading
import time
from typing import Callable, List, Optional

from heartbeat.cloud_client import CloudClient

logger = logging.getLogger(__name__)


class ModeManager:
    """Manages operating mode (online vs offline) based on true cloud reachability."""

    def __init__(
        self,
        cloud_client: CloudClient,
        poll_interval: float = 1.0,
        failure_threshold_seconds: float = 4.0,
        stop_event: Optional[threading.Event] = None,
    ):
        self.cloud_client = cloud_client
        self.poll_interval = poll_interval
        self.failure_threshold_seconds = failure_threshold_seconds
        self.stop_event = stop_event or threading.Event()

        self._mode = "offline"
        self._lock = threading.Lock()
        self._last_success_time: Optional[float] = None
        self._listeners: List[Callable[[str], None]] = []

    def get_mode(self) -> str:
        """Thread-safe accessor for current operating mode."""
        with self._lock:
            return self._mode

    def add_listener(self, callback: Callable[[str], None]):
        """Registers a callback for mode changes."""
        self._listeners.append(callback)

    def _set_mode(self, new_mode: str):
        with self._lock:
            if self._mode != new_mode:
                old_mode = self._mode
                self._mode = new_mode
                logger.info("Operating mode switched: %s -> %s", old_mode, new_mode)
                for cb in self._listeners:
                    try:
                        cb(new_mode)
                    except Exception as e:
                        logger.error("Error in mode change listener: %s", e)

    def check_now(self) -> str:
        """Performs immediate health check probe and updates mode."""
        healthy = self.cloud_client.check_health()
        now = time.time()
        if healthy:
            self._last_success_time = now
            self._set_mode("online")
        else:
            if self._last_success_time is None or (now - self._last_success_time >= self.failure_threshold_seconds):
                self._set_mode("offline")
        return self.get_mode()

    def run_loop(self):
        """Continuous background loop monitoring connectivity."""
        logger.info("Starting connectivity monitor loop...")
        while not self.stop_event.is_set():
            self.check_now()
            time.sleep(self.poll_interval)
        logger.info("Connectivity monitor loop stopped.")
