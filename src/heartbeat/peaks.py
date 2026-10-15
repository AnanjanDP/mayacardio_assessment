"""
Peak detection and heart rate calculations for Never Lose a Heartbeat.
Strictly implements the 4-rule peak detection and 5-second lookback specification.
"""

from typing import Dict, List, Optional

SAMPLE_RATE = 400


def detect_peaks_in_interval(
    samples_ch2: Dict[int, int],
    start_index: int,
    end_index: int,
    history_start: int,
) -> List[int]:
    """
    Detects qualifying ECG peaks in channel 2 over [start_index, end_index].
    Conditions for sample i to qualify as a peak:
    1. Preceding 800 samples [i-800, i-1] and following sample i+1 are available.
    2. Strictly larger than both immediate neighbours: v[i] > v[i-1] and v[i] > v[i+1].
    3. Larger than half the maximum value of the preceding 800 samples: v[i] > 0.5 * max(v[i-800 ... i-1]).
    4. At least 100 samples after the previously accepted peak: (i - last_peak >= 100).
    """
    accepted_peaks: List[int] = []
    last_peak_idx: Optional[int] = None

    # Scan sequentially from history_start to end_index to establish stable peak state
    # Candidate peak must have at least 800 preceding samples in samples_ch2
    scan_start = max(start_index, history_start + 800)

    for i in range(scan_start, end_index + 1):
        if i not in samples_ch2:
            continue

        val_i = samples_ch2[i]

        # Condition 1: i+1 must be available
        if (i + 1) not in samples_ch2:
            continue

        # Condition 2: Strictly larger than both immediate neighbours
        if (i - 1) not in samples_ch2:
            continue
        if not (val_i > samples_ch2[i - 1] and val_i > samples_ch2[i + 1]):
            continue

        # Condition 1 & 3: Check preceding 800 samples [i-800, i-1]
        all_800_available = True
        max_preceding = -float("inf")
        for k in range(i - 800, i):
            if k not in samples_ch2:
                all_800_available = False
                break
            v = samples_ch2[k]
            if v > max_preceding:
                max_preceding = v

        if not all_800_available:
            continue

        if not (val_i > 0.5 * max_preceding):
            continue

        # Condition 4: At least 100 samples after the previously accepted peak
        if last_peak_idx is not None and (i - last_peak_idx < 100):
            continue

        # Qualified!
        accepted_peaks.append(i)
        last_peak_idx = i

    # Filter to only peaks falling within [start_index, end_index]
    return [p for p in accepted_peaks if start_index <= p <= end_index]


def calculate_heart_rate(
    samples_ch2: Dict[int, int],
    window_idx: int,
) -> Optional[float]:
    """
    Calculates heart rate for window w according to Section 5.5 rules:
    - Interval: [400*(w-4), 400*(w+1)-1] (5 seconds, 2000 samples).
    - Null if:
      - w < 4
      - Any sample in the 5-second interval is missing
      - Fewer than 2 qualifying peaks exist
    - Otherwise: 60 / mean_gap_seconds, rounded to 1 decimal place.
    """
    if window_idx < 4:
        return None

    interval_start = 400 * (window_idx - 4)
    interval_end = 400 * (window_idx + 1) - 1

    # Check completeness of the 5-second interval
    for idx in range(interval_start, interval_end + 1):
        if idx not in samples_ch2:
            return None

    # Detect peaks in [interval_start, interval_end] with historical context starting at interval_start - 800
    peaks = detect_peaks_in_interval(
        samples_ch2=samples_ch2,
        start_index=interval_start,
        end_index=interval_end,
        history_start=interval_start - 800,
    )

    if len(peaks) < 2:
        return None

    # Calculate gaps between consecutive peaks
    gaps_samples = [peaks[k + 1] - peaks[k] for k in range(len(peaks) - 1)]
    mean_gap_seconds = (sum(gaps_samples) / len(gaps_samples)) / float(SAMPLE_RATE)

    if mean_gap_seconds <= 0:
        return None

    hr = 60.0 / mean_gap_seconds
    return round(hr, 1)
