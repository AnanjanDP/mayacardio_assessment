from heartbeat.peaks import calculate_heart_rate, detect_peaks_in_interval


def test_peak_requires_800_preceding_context():
    samples = {i: 100 for i in range(500)}
    samples[500] = 5000
    samples[501] = 100
    peaks = detect_peaks_in_interval(samples, start_index=0, end_index=501, history_start=0)
    assert len(peaks) == 0


def test_peak_qualifies_with_full_context():
    samples = {i: 1000 for i in range(1200)}
    peak_idx = 900
    samples[peak_idx] = 10000
    samples[peak_idx - 1] = 2000
    samples[peak_idx + 1] = 2000

    peaks = detect_peaks_in_interval(samples, start_index=850, end_index=1000, history_start=0)
    assert peaks == [peak_idx]


def test_peak_strictly_larger_than_neighbours():
    samples = {i: 1000 for i in range(1200)}
    peak_idx = 900
    samples[peak_idx] = 10000
    samples[peak_idx - 1] = 10000
    samples[peak_idx + 1] = 2000

    peaks = detect_peaks_in_interval(samples, start_index=850, end_index=1000, history_start=0)
    assert len(peaks) == 0


def test_peak_must_exceed_half_max_of_preceding_800():
    samples = {i: 1000 for i in range(1200)}
    samples[500] = 20000
    peak_idx = 900
    samples[peak_idx] = 9000
    samples[peak_idx - 1] = 1000
    samples[peak_idx + 1] = 1000

    peaks = detect_peaks_in_interval(samples, start_index=850, end_index=1000, history_start=0)
    assert len(peaks) == 0


def test_peak_separation_at_least_100_samples():
    samples = {i: 1000 for i in range(1500)}
    peak1 = 900
    samples[peak1] = 10000
    samples[peak1 - 1] = 2000
    samples[peak1 + 1] = 2000

    peak2_too_close = 950
    samples[peak2_too_close] = 10000
    samples[peak2_too_close - 1] = 2000
    samples[peak2_too_close + 1] = 2000

    peak3_ok = 1050
    samples[peak3_ok] = 10000
    samples[peak3_ok - 1] = 2000
    samples[peak3_ok + 1] = 2000

    peaks = detect_peaks_in_interval(samples, start_index=850, end_index=1100, history_start=0)
    assert peaks == [peak1, peak3_ok]


def test_heart_rate_null_for_w_less_than_4():
    samples = {i: 1000 for i in range(2500)}
    assert calculate_heart_rate(samples, window_idx=3) is None
    assert calculate_heart_rate(samples, window_idx=0) is None


def test_heart_rate_null_when_missing_sample():
    samples = {i: 1000 for i in range(2000)}
    del samples[500]
    assert calculate_heart_rate(samples, window_idx=4) is None


def test_heart_rate_calculation_60_bpm():
    samples = {i: 1000 for i in range(2500)}
    for p in (850, 1250, 1650):
        samples[p] = 10000
        samples[p - 1] = 2000
        samples[p + 1] = 2000

    hr = calculate_heart_rate(samples, window_idx=4)
    assert hr == 60.0
