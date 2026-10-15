from typing import Dict, List, Optional

SAMPLE_RATE = 400


def detect_peaks_in_interval(
    samples_ch2: Dict[int, int],
    start_index: int,
    end_index: int,
    history_start: int,
) -> List[int]:
    accepted_peaks: List[int] = []
    last_peak_idx: Optional[int] = None

    scan_start = max(start_index, history_start + 800)

    for i in range(scan_start, end_index + 1):
        if i not in samples_ch2:
            continue

        val_i = samples_ch2[i]

        if (i + 1) not in samples_ch2:
            continue

        if (i - 1) not in samples_ch2:
            continue
        if not (val_i > samples_ch2[i - 1] and val_i > samples_ch2[i + 1]):
            continue

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

        if last_peak_idx is not None and (i - last_peak_idx < 100):
            continue

        accepted_peaks.append(i)
        last_peak_idx = i

    return [p for p in accepted_peaks if start_index <= p <= end_index]


def calculate_heart_rate(
    samples_ch2: Dict[int, int],
    window_idx: int,
) -> Optional[float]:
    if window_idx < 4:
        return None

    interval_start = 400 * (window_idx - 4)
    interval_end = 400 * (window_idx + 1) - 1

    for idx in range(interval_start, interval_end + 1):
        if idx not in samples_ch2:
            return None

    peaks = detect_peaks_in_interval(
        samples_ch2=samples_ch2,
        start_index=interval_start,
        end_index=interval_end,
        history_start=interval_start - 800,
    )

    if len(peaks) < 2:
        return None

    gaps_samples = [peaks[k + 1] - peaks[k] for k in range(len(peaks) - 1)]
    mean_gap_seconds = (sum(gaps_samples) / len(gaps_samples)) / float(SAMPLE_RATE)

    if mean_gap_seconds <= 0:
        return None

    hr = 60.0 / mean_gap_seconds
    return round(hr, 1)
