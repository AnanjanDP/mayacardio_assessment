"""
Unit tests for LossTracker: gaps, explicit losses, and mock restarts.
"""

from heartbeat.loss_tracker import LossTracker
from heartbeat.protocol import LossItem


def test_gap_detection(device_storage):
    tracker = LossTracker(device_storage)
    session_id = "sess_gap"

    # Suppose last persisted index was 100
    # Next batch starts at 150 (gap of 101..149)
    losses = tracker.track_batch_losses(
        session_id=session_id,
        sensor=1,
        first_index=150,
        count=10,
        explicit_losses=[],
        last_persisted_index=100,
    )

    assert len(losses) == 1
    assert losses[0].from_index == 101
    assert losses[0].to_index == 149
    assert losses[0].reason == "gap_detected"


def test_explicit_loss_not_duplicated_by_gap_detector(device_storage):
    tracker = LossTracker(device_storage)
    session_id = "sess_explicit"

    explicit = [LossItem(from_index=50, to_index=99, reason="buffer_overflow")]
    losses = tracker.track_batch_losses(
        session_id=session_id,
        sensor=1,
        first_index=100,
        count=10,
        explicit_losses=explicit,
        last_persisted_index=49,
    )

    # Since the gap 50..99 is completely covered by the explicit loss,
    # the gap detector should not add an extra duplicate gap record!
    assert len(losses) == 1
    assert losses[0].reason == "buffer_overflow"


def test_mock_restart_handling(device_storage):
    tracker = LossTracker(device_storage)
    old_session = "old_sess"
    new_session = "new_sess"

    # Persist some samples for old session
    device_storage.persist_batch(old_session, 1, 0, [[1, 2, 3] for _ in range(50)], [])
    device_storage.persist_batch(old_session, 2, 0, [[4, 5] for _ in range(50)], [])

    tracker.handle_session_restart(old_session, new_session)

    # Check that old session has to_index=None loss records for sensor 1 and sensor 2
    loss_lines = list(device_storage.export_session_losses(old_session))
    assert len(loss_lines) == 2
    for line in loss_lines:
        assert '"to_index": null' in line
        assert '"reason": "mock_restarted"' in line


def test_disk_full_loss_buffering_and_flush(device_storage):
    tracker = LossTracker(device_storage)
    session_id = "sess_disk"

    # Simulate disk failure by monkeypatching record_loss to fail once
    original_record = device_storage.record_loss
    call_count = 0

    def failing_record(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        raise OSError("Simulated disk full")

    device_storage.record_loss = failing_record

    # This should buffer in memory
    tracker.handle_disk_full_loss(session_id, sensor=1, first_index=100, count=50)
    assert len(tracker._pending_disk_losses) == 1
    assert tracker._pending_disk_losses[0]["reason"] == "disk_full"

    # Restore storage and flush
    device_storage.record_loss = original_record
    tracker.flush_pending_losses()
    assert len(tracker._pending_disk_losses) == 0

    # Verify loss record was written to DB
    losses = list(device_storage.export_session_losses(session_id))
    assert len(losses) == 1
    assert '"reason": "disk_full"' in losses[0]
    assert '"from_index": 100' in losses[0]
    assert '"to_index": 149' in losses[0]

