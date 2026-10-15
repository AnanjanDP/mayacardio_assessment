"""
Shared processing engine for Never Lose a Heartbeat.
Computes window statistics, completeness, sensor 2 magnitude, and heart rate.
Pure deterministic logic shared between edge offline processor and cloud online processor.
"""

import logging
import math
from typing import Dict, List, Optional

from heartbeat.peaks import calculate_heart_rate
from heartbeat.storage import DeviceStorage

logger = logging.getLogger(__name__)

SAMPLE_RATE = 400
WINDOW_SIZE = 400


def compute_channel_stats(values: List[int]) -> Dict[str, Optional[float]]:
    """Calculates mean (rounded to 1 dec), min, and max for a list of channel values."""
    if not values:
        return {"mean": None, "min": None, "max": None}
    mean_val = round(sum(values) / len(values), 1)
    return {
        "mean": mean_val,
        "min": min(values),
        "max": max(values),
    }


def compute_sensor2_magnitude(s2_samples: Dict[int, List[int]]) -> Optional[float]:
    """
    Computes mean of sqrt(ch1^2 + ch2^2) across available sensor 2 samples.
    Returns float rounded to 1 decimal place, or None if no samples.
    """
    if not s2_samples:
        return None
    magnitudes = []
    for idx, vals in s2_samples.items():
        if len(vals) >= 2:
            mag = math.sqrt(vals[0] ** 2 + vals[1] ** 2)
            magnitudes.append(mag)
    if not magnitudes:
        return None
    return round(sum(magnitudes) / len(magnitudes), 1)


def process_window(
    window_idx: int,
    sensor1_samples: Dict[int, List[int]],  # idx -> [ch1, ch2, ch3]
    sensor2_samples: Dict[int, List[int]],  # idx -> [ch1, ch2]
    sensor1_ch2_history: Dict[int, int],   # idx -> ch2 for heart rate [400(w-4)-800 ... 400(w+1)]
    mode: str,
) -> dict:
    """
    Processes a single 1-second window according to Section 3 and 5 specifications.
    Returns result dictionary matching the official output contract.
    """
    w_start = 400 * window_idx
    w_end = 400 * window_idx + 399

    # Filter window samples
    w_s1 = {k: v for k, v in sensor1_samples.items() if w_start <= k <= w_end}
    w_s2 = {k: v for k, v in sensor2_samples.items() if w_start <= k <= w_end}

    missing_1 = WINDOW_SIZE - len(w_s1)
    missing_2 = WINDOW_SIZE - len(w_s2)
    complete = (missing_1 == 0 and missing_2 == 0)

    # Per-channel statistics
    s1_c1 = [vals[0] for vals in w_s1.values() if len(vals) > 0]
    s1_c2 = [vals[1] for vals in w_s1.values() if len(vals) > 1]
    s1_c3 = [vals[2] for vals in w_s1.values() if len(vals) > 2]

    s2_c1 = [vals[0] for vals in w_s2.values() if len(vals) > 0]
    s2_c2 = [vals[1] for vals in w_s2.values() if len(vals) > 1]

    channels = {
        "sensor1_ch1": compute_channel_stats(s1_c1),
        "sensor1_ch2": compute_channel_stats(s1_c2),
        "sensor1_ch3": compute_channel_stats(s1_c3),
        "sensor2_ch1": compute_channel_stats(s2_c1),
        "sensor2_ch2": compute_channel_stats(s2_c2),
    }

    magnitude = compute_sensor2_magnitude(w_s2)

    # Heart rate calculation
    heart_rate = calculate_heart_rate(sensor1_ch2_history, window_idx)

    return {
        "window": window_idx,
        "mode": mode,
        "missing_1": missing_1,
        "missing_2": missing_2,
        "complete": complete,
        "channels": channels,
        "magnitude": magnitude,
        "heart_rate": heart_rate,
    }


class LocalProcessor:
    """Executes offline window processing on the edge device."""

    def __init__(self, storage: DeviceStorage):
        self.storage = storage

    def process_pending_windows(self, session_id: str, mode: str = "offline") -> int:
        """Processes all completed windows for the session that haven't been processed yet."""
        conn = self.storage.get_connection()
        try:
            cur = conn.cursor()
            # Determine maximum available index for session
            cur.execute("SELECT MAX(sample_index) FROM samples WHERE session_id = ?", (session_id,))
            max_row = cur.fetchone()
            if not max_row or max_row[0] is None:
                return 0

            max_idx = max_row[0]
            max_window = (max_idx + 1) // WINDOW_SIZE

            # Find processed windows
            cur.execute(
                "SELECT window_index FROM processed_windows WHERE session_id = ? AND mode = ?",
                (session_id, mode),
            )
            processed = {r[0] for r in cur.fetchall()}

            processed_count = 0
            for w in range(max_window):
                if w in processed:
                    continue

                w_start = 400 * w
                w_end = 400 * w + 399

                # Query sensor 1 samples in window
                cur.execute(
                    "SELECT sample_index, val0, val1, val2 FROM samples WHERE session_id = ? AND sensor = 1 AND sample_index BETWEEN ? AND ?",
                    (session_id, w_start, w_end),
                )
                s1_samples = {r["sample_index"]: [r["val0"], r["val1"], r["val2"]] for r in cur.fetchall()}

                # Query sensor 2 samples in window
                cur.execute(
                    "SELECT sample_index, val0, val1 FROM samples WHERE session_id = ? AND sensor = 2 AND sample_index BETWEEN ? AND ?",
                    (session_id, w_start, w_end),
                )
                s2_samples = {r["sample_index"]: [r["val0"], r["val1"]] for r in cur.fetchall()}

                # Query history for heart rate if w >= 4
                s1_ch2_history = {}
                if w >= 4:
                    hr_start = 400 * (w - 4) - 800
                    hr_end = 400 * (w + 1)
                    cur.execute(
                        "SELECT sample_index, val1 FROM samples WHERE session_id = ? AND sensor = 1 AND sample_index BETWEEN ? AND ?",
                        (session_id, hr_start, hr_end),
                    )
                    s1_ch2_history = {r["sample_index"]: r["val1"] for r in cur.fetchall()}

                res = process_window(
                    window_idx=w,
                    sensor1_samples=s1_samples,
                    sensor2_samples=s2_samples,
                    sensor1_ch2_history=s1_ch2_history,
                    mode=mode,
                )

                self.storage.persist_result(
                    session_id=session_id,
                    window_index=w,
                    mode=mode,
                    missing_1=res["missing_1"],
                    missing_2=res["missing_2"],
                    complete=res["complete"],
                    channels=res["channels"],
                    magnitude=res["magnitude"],
                    heart_rate=res["heart_rate"],
                )
                processed_count += 1

            return processed_count
        finally:
            conn.close()
