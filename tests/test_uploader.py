"""
Unit tests for BacklogUploader synchronization and cursor progression.
"""

from heartbeat.uploader import BacklogUploader


class FakeCloudClient:
    def __init__(self, should_succeed: bool = True):
        self.should_succeed = should_succeed
        self.ingested_batches = []

    def ingest_batch(self, session_id, samples, loss_records):
        if not self.should_succeed:
            return False
        self.ingested_batches.append({
            "session_id": session_id,
            "samples": samples,
            "loss_records": loss_records,
        })
        return True


class FakeModeManager:
    def __init__(self, mode="online"):
        self.mode = mode

    def get_mode(self):
        return self.mode


def test_uploader_advances_cursor_on_success(device_storage):
    session_id = "uploader_sess_1"
    # Persist 10 samples
    device_storage.persist_batch(session_id, 1, 0, [[i, i, i] for i in range(10)], [])

    fake_client = FakeCloudClient(should_succeed=True)
    fake_mode = FakeModeManager("online")

    uploader = BacklogUploader(
        storage=device_storage,
        cloud_client=fake_client,
        mode_manager=fake_mode,
        batch_size=5,
    )

    synced = uploader.sync_session_step(session_id)
    assert synced == 5  # first batch of 5
    assert len(fake_client.ingested_batches) == 1

    # Second step
    synced2 = uploader.sync_session_step(session_id)
    assert synced2 == 5
    assert len(fake_client.ingested_batches) == 2

    # All synced
    saved, in_cloud, backlog = device_storage.get_sync_status()
    assert backlog == 0


def test_uploader_does_not_advance_cursor_on_failure(device_storage):
    session_id = "uploader_sess_2"
    device_storage.persist_batch(session_id, 1, 0, [[i, i, i] for i in range(10)], [])

    fake_client = FakeCloudClient(should_succeed=False)  # cloud failure
    fake_mode = FakeModeManager("online")

    uploader = BacklogUploader(
        storage=device_storage,
        cloud_client=fake_client,
        mode_manager=fake_mode,
        batch_size=5,
    )

    synced = uploader.sync_session_step(session_id)
    assert synced == 0

    # Backlog must remain 10!
    saved, in_cloud, backlog = device_storage.get_sync_status()
    assert backlog == 10
