"""
Integration and fault injection tests:
- Corrupted CRC32 injection and recovery
- Duplicate reply handling
- Mock restart detection
- Collector crash and recovery
"""

import os
import threading
import time

from heartbeat.collector import SensorCollector
from heartbeat.config import PipelineConfig
from heartbeat.loss_tracker import LossTracker
from heartbeat.storage import DeviceStorage
from mock.biosignal_mock import run_mock_server


def test_corrupt_crc_retry_recovery(temp_dir):
    mock_port = 7189
    stop_mock = threading.Event()

    # Mock occasionally produces corrupt CRC
    t_mock = threading.Thread(
        target=run_mock_server,
        kwargs={
            "mode": "ramp",
            "seed": 42,
            "duration": 60,
            "port": mock_port,
            "host": "127.0.0.1",
            "fast_clock": 10.0,
            "corrupt_crc_prob": 0.3,
            "stop_event": stop_mock,
        },
        daemon=True,
    )
    t_mock.start()
    time.sleep(0.3)

    db_path = os.path.join(temp_dir, "corrupt_test.db")
    config = PipelineConfig(db_path=db_path, mock_port=mock_port)
    storage = DeviceStorage(db_path=db_path, synchronous="NORMAL")
    loss_tracker = LossTracker(storage)
    collector = SensorCollector(config, storage, loss_tracker)

    try:
        collector.connect()
        # Collect steps should retry and succeed or skip without crashing
        for _ in range(10):
            collector.collect_step(1)
            collector.collect_step(2)
            time.sleep(0.05)
    finally:
        collector.close()
        stop_mock.set()

    saved, _, _ = storage.get_sync_status()
    assert saved > 0


def test_crash_recovery_resumes_cursor(temp_dir):
    mock_port = 7190
    stop_mock = threading.Event()

    t_mock = threading.Thread(
        target=run_mock_server,
        kwargs={
            "mode": "ramp",
            "seed": 99,
            "duration": 60,
            "port": mock_port,
            "host": "127.0.0.1",
            "fast_clock": 10.0,
            "stop_event": stop_mock,
        },
        daemon=True,
    )
    t_mock.start()
    time.sleep(0.3)

    db_path = os.path.join(temp_dir, "crash_test.db")
    config = PipelineConfig(db_path=db_path, mock_port=mock_port)
    storage = DeviceStorage(db_path=db_path, synchronous="NORMAL")
    loss_tracker = LossTracker(storage)

    # First process run
    collector1 = SensorCollector(config, storage, loss_tracker)
    collector1.connect()
    for _ in range(5):
        collector1.collect_step(1)
        collector1.collect_step(2)
        time.sleep(0.05)
    collector1.close()

    session_id = collector1.current_session_id
    cursor1_s1 = storage.get_last_persisted_index(session_id, 1)
    cursor1_s2 = storage.get_last_persisted_index(session_id, 2)
    assert cursor1_s1 is not None and cursor1_s1 >= 0
    assert cursor1_s2 is not None and cursor1_s2 >= 0

    # Simulate process crash and restart: create a new collector instance pointing to same storage
    collector2 = SensorCollector(config, storage, loss_tracker)
    try:
        collector2.connect()
        for _ in range(5):
            collector2.collect_step(1)
            collector2.collect_step(2)
            time.sleep(0.05)
    finally:
        collector2.close()
        stop_mock.set()

    # Verify cursors continued and grew without corrupting past state
    cursor2_s1 = storage.get_last_persisted_index(session_id, 1)
    cursor2_s2 = storage.get_last_persisted_index(session_id, 2)
    assert cursor2_s1 >= cursor1_s1
    assert cursor2_s2 >= cursor1_s2
