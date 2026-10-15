"""
Unit tests for shared processing engine: window boundaries, stats, magnitude, completeness.
"""

from heartbeat.processor import (
    compute_channel_stats,
    compute_sensor2_magnitude,
    process_window,
)


def test_channel_stats_calculation():
    vals = [10, 20, 30, 40]
    stats = compute_channel_stats(vals)
    assert stats["mean"] == 25.0
    assert stats["min"] == 10
    assert stats["max"] == 40


def test_channel_stats_empty():
    stats = compute_channel_stats([])
    assert stats["mean"] is None
    assert stats["min"] is None
    assert stats["max"] is None


def test_sensor2_magnitude():
    # Samples with ch1=3, ch2=4 -> sqrt(3^2 + 4^2) = 5.0
    s2_samples = {
        0: [3, 4],
        1: [3, 4],
    }
    mag = compute_sensor2_magnitude(s2_samples)
    assert mag == 5.0


def test_sensor2_magnitude_empty():
    assert compute_sensor2_magnitude({}) is None


def test_process_window_completeness():
    w = 0
    # Provide complete 400 samples for sensor 1 and sensor 2
    s1 = {i: [100, 200, 300] for i in range(400)}
    s2 = {i: [400000, 150000] for i in range(400)}

    res = process_window(
        window_idx=w,
        sensor1_samples=s1,
        sensor2_samples=s2,
        sensor1_ch2_history={},
        mode="offline",
    )

    assert res["window"] == 0
    assert res["mode"] == "offline"
    assert res["missing_1"] == 0
    assert res["missing_2"] == 0
    assert res["complete"] is True
    assert res["channels"]["sensor1_ch1"]["mean"] == 100.0
    assert res["magnitude"] is not None
    assert res["heart_rate"] is None  # w=0 < 4


def test_process_window_partial():
    w = 1
    # Only 300 samples for s1 (100 missing), 400 for s2
    s1 = {i: [100, 200, 300] for i in range(400, 700)}
    s2 = {i: [400000, 150000] for i in range(400, 800)}

    res = process_window(
        window_idx=w,
        sensor1_samples=s1,
        sensor2_samples=s2,
        sensor1_ch2_history={},
        mode="online",
    )

    assert res["window"] == 1
    assert res["mode"] == "online"
    assert res["missing_1"] == 100
    assert res["missing_2"] == 0
    assert res["complete"] is False
