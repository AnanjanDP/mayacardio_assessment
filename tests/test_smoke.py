import json
from fastapi.testclient import TestClient
import pytest

import cloud.api
from cloud.storage import CloudStorage
from cloud.worker import CloudWorker
from heartbeat.connectivity import ModeManager
from heartbeat.processor import LocalProcessor
from heartbeat.protocol import LossItem
from heartbeat.storage import DeviceStorage
from heartbeat.uploader import BacklogUploader


@pytest.fixture
def smoke_cloud(tmp_path, monkeypatch):
    test_db = f"sqlite:///{tmp_path / 'smoke_cloud.db'}"
    c_storage = CloudStorage(test_db)
    monkeypatch.setattr(cloud.api, "storage", c_storage)
    monkeypatch.setattr(cloud.api, "worker", CloudWorker(c_storage))
    return c_storage, TestClient(cloud.api.app)


def test_smoke_edge_device_persistence_and_export(tmp_path):
    db_file = str(tmp_path / "smoke_dev.db")
    storage = DeviceStorage(db_path=db_file, synchronous="NORMAL")

    session_id = "smoke_sess_01"
    samples = [[100, 200, 300], [101, 201, 301], [102, 202, 302]]
    losses = [LossItem(from_index=10, to_index=12, reason="packet_drop")]

    persisted = storage.persist_batch(
        session_id=session_id,
        sensor=1,
        first_index=0,
        data=samples,
        loss_items=losses,
    )
    assert persisted == 3
    assert storage.get_last_persisted_index(session_id, 1) == 2

    exp_samples = [json.loads(line) for line in storage.export_session_samples(session_id)]
    assert len(exp_samples) == 3
    assert exp_samples[0]["sensor"] == 1
    assert exp_samples[0]["index"] == 0
    assert exp_samples[0]["values"] == [100, 200, 300]

    exp_losses = [json.loads(line) for line in storage.export_session_losses(session_id)]
    assert len(exp_losses) == 1
    assert exp_losses[0]["reason"] == "packet_drop"
    assert exp_losses[0]["from_index"] == 10
    assert exp_losses[0]["to_index"] == 12


def test_smoke_cloud_ingest_and_status(smoke_cloud):
    _, client = smoke_cloud

    h_resp = client.get("/health")
    assert h_resp.status_code == 200
    assert h_resp.json()["status"] == "ok"

    session_id = "smoke_cloud_01"
    payload = {
        "session_id": session_id,
        "samples": [
            {"sensor": 1, "index": 0, "values": [500, 600, 700]},
            {"sensor": 1, "index": 1, "values": [505, 605, 705]},
            {"sensor": 2, "index": 0, "values": [1200, 3400]},
        ],
        "loss_records": [
            {"sensor": 2, "from_index": 5, "to_index": 9, "reason": "crc_mismatch"}
        ],
    }
    i_resp = client.post("/api/ingest", json=payload)
    assert i_resp.status_code == 200
    res = i_resp.json()
    assert res["status"] == "ok"
    assert res["inserted_samples"] == 3
    assert res["inserted_losses"] == 1

    s_resp = client.get("/api/status")
    assert s_resp.status_code == 200
    stat = s_resp.json()
    assert stat["total_samples"] == 3


def test_smoke_cloud_export_ndjson_streaming(smoke_cloud):
    _, client = smoke_cloud
    session_id = "smoke_export_sess"

    client.post("/api/ingest", json={
        "session_id": session_id,
        "samples": [{"sensor": 1, "index": 100, "values": [10, 20, 30]}],
        "loss_records": [{"sensor": 1, "from_index": 50, "to_index": 55, "reason": "buffer_overflow"}],
    })

    resp = client.get(f"/api/export?session={session_id}")
    assert resp.status_code == 200
    assert "application/x-ndjson" in resp.headers.get("content-type", "")

    lines = [json.loads(l) for l in resp.text.strip().split("\n") if l.strip()]
    assert len(lines) == 2

    sample_line = next(l for l in lines if "values" in l)
    loss_line = next(l for l in lines if "reason" in l)
    assert sample_line["sensor"] == 1
    assert sample_line["index"] == 100
    assert sample_line["values"] == [10, 20, 30]
    assert loss_line["reason"] == "buffer_overflow"


def test_smoke_backlog_uploader_sync_drain(tmp_path, smoke_cloud):
    _, client = smoke_cloud
    dev_db = str(tmp_path / "smoke_sync_dev.db")
    dev_storage = DeviceStorage(db_path=dev_db, synchronous="NORMAL")

    session_id = "smoke_sync_sess"
    dev_storage.persist_batch(
        session_id=session_id,
        sensor=1,
        first_index=0,
        data=[[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        loss_items=[LossItem(from_index=5, to_index=7, reason="timeout")],
    )

    saved, in_cloud, backlog = dev_storage.get_sync_status()
    assert saved == 3
    assert backlog == 3

    class TestClientAdapter:
        def check_health(self):
            return True

        def ingest_batch(self, session_id, samples, loss_records):
            resp = client.post("/api/ingest", json={
                "session_id": session_id,
                "samples": samples,
                "loss_records": loss_records,
            })
            return resp.status_code == 200 and resp.json().get("status") == "ok"

    adapter = TestClientAdapter()
    mode_mgr = ModeManager(cloud_client=adapter)
    mode_mgr._mode = "online"

    uploader = BacklogUploader(
        storage=dev_storage,
        cloud_client=adapter,
        mode_manager=mode_mgr,
        batch_size=10,
    )

    synced = uploader.sync_all_sessions(drain_backlog=True)
    assert synced == 3

    saved2, in_cloud2, backlog2 = dev_storage.get_sync_status()
    assert saved2 == 3
    assert backlog2 == 0


def test_smoke_local_processor_window_math(tmp_path):
    dev_db = str(tmp_path / "smoke_proc.db")
    dev_storage = DeviceStorage(db_path=dev_db, synchronous="NORMAL")

    session_id = "smoke_proc_sess"
    s1_data = [[100, 200, 300] for _ in range(400)]
    s2_data = [[4000, 3000] for _ in range(400)]

    dev_storage.persist_batch(session_id, 1, 0, s1_data, [])
    dev_storage.persist_batch(session_id, 2, 0, s2_data, [])

    processor = LocalProcessor(dev_storage)
    n_processed = processor.process_pending_windows(session_id, mode="offline")
    assert n_processed == 1

    results = [json.loads(line) for line in dev_storage.export_results(session_id)]
    assert len(results) == 1
    w0 = results[0]
    assert w0["window"] == 0
    assert w0["mode"] == "offline"
    assert w0["complete"] is True
    assert w0["missing_1"] == 0
    assert w0["missing_2"] == 0
    assert w0["channels"]["sensor1_ch1"]["mean"] == 100.0
    assert w0["channels"]["sensor2_ch1"]["mean"] == 4000.0
    assert w0["magnitude"] == 5000.0
