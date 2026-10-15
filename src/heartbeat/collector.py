import json
import logging
import socket
import threading
import time
from typing import Optional, Tuple

from heartbeat.config import PipelineConfig
from heartbeat.loss_tracker import LossTracker
from heartbeat.protocol import SensorResponse, parse_response
from heartbeat.storage import DeviceStorage

logger = logging.getLogger(__name__)


class SensorCollector:

    def __init__(
        self,
        config: PipelineConfig,
        storage: DeviceStorage,
        loss_tracker: LossTracker,
        stop_event: Optional[threading.Event] = None,
    ):
        self.config = config
        self.storage = storage
        self.loss_tracker = loss_tracker
        self.stop_event = stop_event or threading.Event()

        self.current_session_id: Optional[str] = self.storage.get_latest_session_id()
        self._sock: Optional[socket.socket] = None
        self._recv_buffer: str = ""

    def connect(self) -> bool:
        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

        self._recv_buffer = ""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3.0)
            sock.connect((self.config.mock_host, self.config.mock_port))
            self._sock = sock
            logger.info("Connected to biosignal mock on %s:%d", self.config.mock_host, self.config.mock_port)
            return True
        except Exception as e:
            logger.debug("Failed to connect to mock: %s", e)
            return False

    def close(self):
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    def _send_command(self, cmd_obj: dict) -> bool:
        if not self._sock:
            return False
        try:
            line = json.dumps(cmd_obj) + "\n"
            self._sock.sendall(line.encode("utf-8"))
            return True
        except Exception as e:
            logger.warning("Error sending command: %s", e)
            self.close()
            return False

    def _read_line(self, timeout: float = 3.0) -> Optional[str]:
        if not self._sock:
            return None

        deadline = time.time() + timeout
        while "\n" not in self._recv_buffer:
            rem = deadline - time.time()
            if rem <= 0:
                logger.debug("Socket read timeout")
                return None
            try:
                self._sock.settimeout(rem)
                chunk = self._sock.recv(4096)
                if not chunk:
                    logger.warning("Socket closed by remote peer")
                    self.close()
                    return None
                self._recv_buffer += chunk.decode("utf-8")
            except socket.timeout:
                return None
            except Exception as e:
                logger.warning("Socket read error: %s", e)
                self.close()
                return None

        line, self._recv_buffer = self._recv_buffer.split("\n", 1)
        return line

    def read_sensor_batch(
        self, sensor: int, retry_from: Optional[int] = None
    ) -> Tuple[Optional[SensorResponse], Optional[str]]:
        req: dict = {"cmd": "read", "sensor": sensor}
        if retry_from is not None:
            req["from_index"] = retry_from

        if not self._send_command(req):
            return None, "send_failed"

        line = self._read_line()
        if not line:
            return None, "no_data_received"

        resp, err = parse_response(line)
        return resp, err

    def collect_step(self, sensor: int) -> bool:
        if not self._sock:
            if not self.connect():
                return False

        last_persisted = None
        if self.current_session_id is not None:
            last_persisted = self.storage.get_last_persisted_index(self.current_session_id, sensor)

        req_from = (last_persisted + 1) if last_persisted is not None else None
        resp, err = self.read_sensor_batch(sensor, retry_from=req_from)

        if err == "corrupt_crc32_mismatch":
            logger.warning("Corrupt CRC32 on sensor %d! Retrying from last persisted index...", sensor)
            retry_idx = (last_persisted + 1) if last_persisted is not None else 0
            resp, err = self.read_sensor_batch(sensor, retry_from=retry_idx)
            if err:
                logger.error("Retry failed for sensor %d: %s", sensor, err)
                return False

        if err or not resp:
            logger.debug("Failed reading sensor %d: %s", sensor, err)
            return False

        if self.current_session_id is None:
            self.current_session_id = resp.session_id
            self.storage.record_session(resp.session_id)
        elif self.current_session_id != resp.session_id:
            logger.warning("Mock session changed: %s -> %s", self.current_session_id, resp.session_id)
            self.loss_tracker.handle_session_restart(self.current_session_id, resp.session_id)
            self.current_session_id = resp.session_id
            last_persisted = None

        all_losses = self.loss_tracker.track_batch_losses(
            session_id=resp.session_id,
            sensor=sensor,
            first_index=resp.first_index,
            count=resp.count,
            explicit_losses=resp.lost,
            last_persisted_index=last_persisted,
        )

        try:
            self.storage.persist_batch(
                session_id=resp.session_id,
                sensor=sensor,
                first_index=resp.first_index,
                data=resp.data,
                loss_items=all_losses,
                session_end=resp.session_end,
            )
            self.loss_tracker.flush_pending_losses()
            return True
        except Exception as e:
            logger.error("Failed to persist batch: %s", e)
            self.loss_tracker.handle_disk_full_loss(
                session_id=resp.session_id,
                sensor=sensor,
                first_index=resp.first_index,
                count=resp.count,
            )
            return False

    def run_loop(self):
        logger.info("Starting sensor collector loop...")
        backoff = 0.1
        while not self.stop_event.is_set():
            s1_ok = self.collect_step(1)
            s2_ok = self.collect_step(2)

            if s1_ok or s2_ok:
                backoff = 0.05
                time.sleep(self.config.poll_interval)
            else:
                backoff = min(backoff * 1.5, 2.0)
                time.sleep(backoff)

        self.close()
        logger.info("Sensor collector loop stopped.")
