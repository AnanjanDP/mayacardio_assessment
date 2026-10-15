"""
Unit tests for DeviceStorage durability, transactions, idempotency, and cursors.
"""

import json
import pytest
from heartbeat.protocol import LossItem
from heartbeat.storage import DeviceStorage


def test_persist_batch_and_cursor(device_storage):
    session_id = "sess_001"
    data_s1 = [[10, 20, 30], [11, 21, 31]]
    losses = [LossItem(from_index=100, to_index=105, reason="buffer_overflow")]

    count = device_storage.persist_batch(
        session_id=session_id,
        sensor=1,
        first_index=0,
        data=data_s1,
        loss_items=losses,
        session_end=False,
    )
    assert count == 2

    # Check cursor
    last_idx = device_storage.get_last_persisted_index(session_id, 1)
    assert last_idx == 1

    # Check sync status
    saved, in_cloud, backlog = device_storage.get_sync_status()
    assert saved == 2
    assert in_cloud == 0
    assert backlog == 2


def test_idempotent_duplicate_batch(device_storage):
    session_id = "sess_002"
    data = [[100, 200, 300], [101, 201, 301]]

    # Insert batch first time
    device_storage.persist_batch(session_id, 1, 0, data, [])
    # Insert same batch second time
    device_storage.persist_batch(session_id, 1, 0, data, [])

    # Total unique samples must remain 2
    saved, _, _ = device_storage.get_sync_status()
    assert saved == 2
    assert device_storage.get_last_persisted_index(session_id, 1) == 1


def test_sync_cursor_advancement(device_storage):
    session_id = "sess_003"
    data = [[i, i * 2, i * 3] for i in range(10)]
    device_storage.persist_batch(session_id, 1, 0, data, [])

    unuploaded = device_storage.get_unuploaded_samples(session_id, 1, limit=5)
    assert len(unuploaded) == 5
    assert unuploaded[0][0] == 0
    assert unuploaded[-1][0] == 4

    # Advance cursor
    device_storage.advance_sync_cursor(session_id, 1, 4)

    # Next unuploaded batch starts at index 5
    next_unuploaded = device_storage.get_unuploaded_samples(session_id, 1, limit=5)
    assert len(next_unuploaded) == 5
    assert next_unuploaded[0][0] == 5
    assert next_unuploaded[-1][0] == 9

    # Advance cursor to end
    device_storage.advance_sync_cursor(session_id, 1, 9)

    saved, in_cloud, backlog = device_storage.get_sync_status()
    assert saved == 10
    assert in_cloud == 10
    assert backlog == 0


def test_export_formatting(device_storage):
    session_id = "sess_export"
    device_storage.persist_batch(
        session_id=session_id,
        sensor=1,
        first_index=120000,
        data=[[720, 1200, 1560]],
        loss_items=[LossItem(from_index=4000, to_index=4399, reason="buffer_overflow")],
    )

    sample_lines = list(device_storage.export_session_samples(session_id))
    assert len(sample_lines) == 1
    sample_obj = json.loads(sample_lines[0])
    assert sample_obj == {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}

    loss_lines = list(device_storage.export_session_losses(session_id))
    assert len(loss_lines) == 1
    loss_obj = json.loads(loss_lines[0])
    assert loss_obj == {"sensor": 1, "from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}
