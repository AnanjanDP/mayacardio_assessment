"""
biosignal-mock: Synthetic biosignal sensor TCP mock service.
Matches the official assessment specification.
"""

import argparse
import json
import math
import random
import socket
import sys
import threading
import time
import uuid
import zlib
from typing import Dict, List, Optional, Tuple

SAMPLE_RATE = 400  # samples per second
BUFFER_SECONDS = 10  # retains latest 10 seconds per sensor
BUFFER_SAMPLES = BUFFER_SECONDS * SAMPLE_RATE  # 4000 samples
MAX_REPLY_SAMPLES = 400  # at most 1 second of data per reply


class BiosignalGenerator:
    """Generates synthetic biosignal data for Sensor 1 and Sensor 2."""

    def __init__(self, mode: str, seed: int):
        self.mode = mode
        self.seed = seed
        self.rng = random.Random(seed)

    def sample_sensor1(self, i: int) -> List[int]:
        """Sensor 1: 3 channels, signed 24-bit integers."""
        if self.mode == "ramp":
            return [(3 * i + c) % 8388608 for c in range(3)]
        else:
            # Signal mode:
            # Minute-based heart rate cycle:
            # Min 0-2 (0-119s): 60 bpm -> interval = 400 samples
            # Min 2-4 (120-239s): 90 bpm -> interval = 400 * 60 / 90 = 266.67 samples
            # Min 4-6 (240-359s): 72 bpm -> interval = 400 * 60 / 72 = 333.33 samples
            # Cycle repeats every 6 minutes (360 seconds = 144,000 samples)
            cycle_second = (i / SAMPLE_RATE) % 360.0
            if cycle_second < 120.0:
                bpm = 60.0
            elif cycle_second < 240.0:
                bpm = 90.0
            else:
                bpm = 72.0

            beat_interval = (SAMPLE_RATE * 60.0) / bpm
            phase = (i % beat_interval)

            # ECG synthetic shape: baseline 2000, sharp QRS peak at phase near 0
            # Peak condition requires:
            # 1. Strictly larger than both neighbours
            # 2. Larger than half of max of preceding 800 samples (max ~45000, threshold ~22500)
            # 3. >= 100 samples after previous peak
            baseline = 2000 + int(100 * math.sin(2 * math.pi * i / (SAMPLE_RATE * 5.0)))
            if phase < 1.0:
                # Sharp peak
                ch2 = 45000
            elif phase < 2.0:
                ch2 = 12000
            elif phase > beat_interval - 1.0:
                ch2 = 12000
            elif phase < 8.0:
                ch2 = baseline + int(4000 * math.exp(-phase / 2.0))
            else:
                # Slight noise
                ch2 = baseline + int(50 * math.sin(i * 0.1))

            # Channel 1 = ch2 * 0.6, Channel 3 = ch2 * 1.3
            ch1 = int(round(ch2 * 0.6))
            ch3 = int(round(ch2 * 1.3))
            return [ch1, ch2, ch3]

    def sample_sensor2(self, i: int) -> List[int]:
        """Sensor 2: 2 channels, signed 24-bit integers."""
        if self.mode == "ramp":
            return [(2 * i + c) % 8388608 for c in range(2)]
        else:
            # Channel 1 near 400,000, Channel 2 near 150,000, varying slowly
            t = i / SAMPLE_RATE
            ch1 = int(round(400000 + 5000 * math.sin(2 * math.pi * t / 30.0) + 1000 * math.cos(2 * math.pi * t / 7.0)))
            ch2 = int(round(150000 + 3000 * math.cos(2 * math.pi * t / 25.0) + 800 * math.sin(2 * math.pi * t / 11.0)))
            return [ch1, ch2]


class MockSession:
    """Manages an active mock session and sensor buffers."""

    def __init__(self, mode: str, seed: int, duration: int, fast_clock: float = 1.0):
        self.session_id = uuid.uuid4().hex[:8]
        self.generator = BiosignalGenerator(mode, seed)
        self.duration = duration
        self.fast_clock = fast_clock
        self.start_wall_time = time.time()
        self.total_samples = duration * SAMPLE_RATE

        # Per sensor state:
        # Buffer tracks available samples: {index: sample}
        # Last read index per sensor by client
        self.read_cursors = {1: 0, 2: 0}
        self.last_buffer_min = {1: 0, 2: 0}
        self.reported_losses: Dict[int, List[dict]] = {1: [], 2: []}

    def current_sample_index(self) -> int:
        elapsed = (time.time() - self.start_wall_time) * self.fast_clock
        idx = int(elapsed * SAMPLE_RATE)
        return min(idx, self.total_samples)

    def is_session_end(self, sensor: int) -> bool:
        return self.current_sample_index() >= self.total_samples

    def read(self, sensor: int, from_index: Optional[int] = None) -> Tuple[dict, str]:
        """
        Reads next available batch for sensor.
        Returns (response_dict, serialized_data_text).
        """
        curr_idx = self.current_sample_index()
        buf_min = max(0, curr_idx - BUFFER_SAMPLES)

        # Track buffer overflow losses
        lost_reports = []
        if from_index is not None:
            req_idx = from_index
        else:
            req_idx = self.read_cursors[sensor]

        if req_idx < buf_min:
            # Buffer overflow: unread data fell off the 10-second buffer
            loss_record = {
                "from_index": req_idx,
                "to_index": buf_min - 1,
                "reason": "buffer_overflow"
            }
            lost_reports.append(loss_record)
            req_idx = buf_min

        # Batch size up to 1 second (400 samples)
        count = min(MAX_REPLY_SAMPLES, curr_idx - req_idx)
        count = max(0, count)

        data = []
        for i in range(req_idx, req_idx + count):
            if sensor == 1:
                data.append(self.generator.sample_sensor1(i))
            else:
                data.append(self.generator.sample_sensor2(i))

        # Advance cursor
        self.read_cursors[sensor] = req_idx + count

        session_end = (req_idx + count >= self.total_samples) and (curr_idx >= self.total_samples)

        # Exact serialized data text
        serialized_data = json.dumps(data, separators=(",", ":"))
        crc_val = zlib.crc32(serialized_data.encode("utf-8")) & 0xFFFFFFFF
        crc32_str = f"{crc_val:08x}"

        resp = {
            "session_id": self.session_id,
            "sensor": sensor,
            "first_index": req_idx,
            "count": count,
            "lost": lost_reports,
            "session_end": session_end,
            "crc32": crc32_str,
            "data": data,
        }
        return resp, serialized_data


def run_mock_server(
    mode: str = "signal",
    seed: int = 7,
    duration: int = 3600,
    port: int = 7100,
    host: str = "127.0.0.1",
    fast_clock: float = 1.0,
    corrupt_crc_prob: float = 0.0,
    duplicate_prob: float = 0.0,
    stop_event: Optional[threading.Event] = None,
):
    """Runs the TCP mock server accepting one client at a time."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(1)
    server.settimeout(1.0)

    session = MockSession(mode=mode, seed=seed, duration=duration, fast_clock=fast_clock)
    last_response: Optional[dict] = None

    try:
        while stop_event is None or not stop_event.is_set():
            try:
                conn, addr = server.accept()
            except socket.timeout:
                continue

            # Handle single client
            with conn:
                conn.settimeout(2.0)
                buffer = ""
                while stop_event is None or not stop_event.is_set():
                    try:
                        data = conn.recv(4096)
                        if not data:
                            break
                        buffer += data.decode("utf-8")
                        while "\n" in buffer:
                            line, buffer = buffer.split("\n", 1)
                            line = line.strip()
                            if not line:
                                continue

                            try:
                                req = json.loads(line)
                            except Exception:
                                continue

                            cmd = req.get("cmd")
                            sensor = req.get("sensor")
                            from_index = req.get("from_index")

                            if cmd == "restart":
                                # Fault injection command: restart session
                                session = MockSession(mode=mode, seed=seed, duration=duration, fast_clock=fast_clock)
                                resp = {"status": "restarted", "session_id": session.session_id}
                                conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))
                                continue

                            if cmd != "read" or sensor not in (1, 2):
                                err_resp = {"error": "invalid command"}
                                conn.sendall((json.dumps(err_resp) + "\n").encode("utf-8"))
                                continue

                            # Duplicate fault injection
                            if duplicate_prob > 0 and last_response is not None and random.random() < duplicate_prob:
                                reply_text = json.dumps(last_response, separators=(",", ":")) + "\n"
                                conn.sendall(reply_text.encode("utf-8"))
                                continue

                            resp_dict, serialized_data = session.read(sensor, from_index)

                            # Corrupt CRC fault injection
                            if corrupt_crc_prob > 0 and random.random() < corrupt_crc_prob:
                                resp_dict["crc32"] = "deadbeef"

                            last_response = resp_dict
                            reply_text = json.dumps(resp_dict, separators=(",", ":")) + "\n"
                            conn.sendall(reply_text.encode("utf-8"))
                    except (socket.timeout, ConnectionResetError, BrokenPipeError):
                        break
    finally:
        server.close()


def main():
    parser = argparse.ArgumentParser(description="Biosignal Mock Service")
    parser.add_argument("--mode", choices=["signal", "ramp"], default="signal", help="Mode: signal or ramp")
    parser.add_argument("--seed", type=int, default=7, help="Random seed")
    parser.add_argument("--duration", type=int, default=3600, help="Duration in seconds")
    parser.add_argument("--port", type=int, default=7100, help="TCP port")
    parser.add_argument("--host", default="127.0.0.1", help="TCP host")
    parser.add_argument("--fast-clock", type=float, default=1.0, help="Clock speedup multiplier")
    parser.add_argument("--corrupt-crc", type=float, default=0.0, help="Probability of corrupt CRC")
    parser.add_argument("--duplicate-reply", type=float, default=0.0, help="Probability of duplicate reply")

    args = parser.parse_args()
    print(f"Starting biosignal-mock on {args.host}:{args.port} (mode={args.mode}, seed={args.seed}, fast_clock={args.fast_clock})")
    run_mock_server(
        mode=args.mode,
        seed=args.seed,
        duration=args.duration,
        port=args.port,
        host=args.host,
        fast_clock=args.fast_clock,
        corrupt_crc_prob=args.corrupt_crc,
        duplicate_prob=args.duplicate_reply,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[biosignal-mock] Service stopped cleanly.")
        sys.exit(0)
