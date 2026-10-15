"""
Integration tests for ground truth verification with ramp mode and check_ground_truth tool.
"""

import os
import threading
import time
import pytest

from heartbeat.collector import SensorCollector
from heartbeat.config import PipelineConfig
from heartbeat.loss_tracker import LossTracker
from heartbeat.processor import LocalProcessor
from heartbeat.storage import DeviceStorage
from mock.biosignal_mock import run_mock_server
from mock.check_ground_truth import check_export, check_results


def test_ramp_mode_ground_truth_integration(temp_dir):
    mock_port = 7188
    stop_mock = threading.Event()

    # Start mock in ramp mode with fast clock
    t_mock = threading.Thread(
        target=run_mock_server,
        kwargs={
            "mode": "ramp",
            "seed": 7,
            "duration": 60,
            "port": mock_port,
            "host": "127.0.0.1",
            "fast_clock": 20.0,  # 20x speed for testing
            "stop_event": stop_mock,
        },
        daemon=True,
    )
    t_mock.start()
    time.sleep(0.3)

    db_path = os.path.join(temp_dir, "gt_device.db")
    config = PipelineConfig(
        db_path=db_path,
        mock_port=mock_port,
        poll_interval=0.01,
    )
    storage = DeviceStorage(db_path=db_path, synchronous="NORMAL")
    loss_tracker = LossTracker(storage)
    collector = SensorCollector(config, storage, loss_tracker)

    # Collect ~5 batches of both sensors
    try:
        collector.connect()
        for _ in range(10):
            collector.collect_step(1)
            collector.collect_step(2)
            time.sleep(0.05)
    finally:
        collector.close()
        stop_mock.set()

    session_id = collector.current_session_id
    assert session_id is not None

    # Export to file
    export_file = os.path.join(temp_dir, "export.ndjson")
    with open(export_file, "w", encoding="utf-8") as f:
        for line in storage.export_session_samples(session_id):
            f.write(line + "\n")
        for line in storage.export_session_losses(session_id):
            f.write(line + "\n")

    # Run check_ground_truth
    passed, summary = check_export(export_file, mode="ramp")
    assert passed is True
    assert summary["duplicates"] == 0
    assert summary["value_mismatches"] == 0
    assert summary["loss_overlaps"] == 0
    assert summary["unaccounted_gaps_s1"] == 0
    assert summary["unaccounted_gaps_s2"] == 0
    assert summary["samples_sensor1"] > 0
    assert summary["samples_sensor2"] > 0

    # Test processing results
    processor = LocalProcessor(storage)
    n_processed = processor.process_pending_windows(session_id, mode="offline")

    results_file = os.path.join(temp_dir, "results.ndjson")
    with open(results_file, "w", encoding="utf-8") as f:
        for line in storage.export_results(session_id):
            f.write(line + "\n")

    res_passed, res_summary = check_results(results_file)
    assert res_passed is True
    assert res_summary["invalid"] == 0
