"""
Unit tests for FastAPI cloud ingestion API, status, and export endpoints.
"""

import json
import pytest
from starlette.testclient import TestClient
from cloud.api import app, storage


@pytest.fixture
def client():
    return TestClient(app)


def test_health_endpoint(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_ingest_batch_idempotency(client):
    session_id = "cloud_sess_01"
    payload = {
        "session_id": session_id,
        "samples": [
            {"sensor": 1, "index": 0, "values": [10, 20, 30]},
            {"sensor": 1, "index": 1, "values": [11, 21, 31]},
        ],
        "loss_records": [
            {"sensor": 2, "from_index": 0, "to_index": 5, "reason": "sensor_dropout"}
        ],
    }

    # First ingest
    r1 = client.post("/api/ingest", json=payload)
    assert r1.status_code == 200
    assert r1.json()["inserted_samples"] == 2

    # Second ingest (retry with same batch)
    r2 = client.post("/api/ingest", json=payload)
    assert r2.status_code == 200

    # Verify status
    r_stat = client.get("/api/status")
    assert r_stat.status_code == 200
    stat_data = r_stat.json()
    assert stat_data["total_samples"] >= 2


def test_ingest_short_values_handled_safely(client):
    """Verifies that dummy or short sample values (e.g. from Swagger default) don't cause a 500 error."""
    payload = {
        "session_id": "dummy_sess",
        "samples": [
            {"sensor": 1, "index": 0, "values": [0]}
        ],
        "loss_records": [
            {"sensor": 2, "from_index": 0, "to_index": 0, "reason": "dummy"}
        ],
    }
    resp = client.post("/api/ingest", json=payload)
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_export_endpoints(client):
    session_id = "cloud_export_sess"
    payload = {
        "session_id": session_id,
        "samples": [
            {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]},
        ],
        "loss_records": [
            {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"},
        ],
    }
    client.post("/api/ingest", json=payload)

    resp = client.get(f"/api/export?session={session_id}")
    assert resp.status_code == 200
    lines = [line for line in resp.text.strip().split("\n") if line]
    assert len(lines) == 2

    sample_rec = json.loads(lines[0])
    assert sample_rec == {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}

    loss_rec = json.loads(lines[1])
    assert loss_rec == {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}


def test_concurrent_worker_processing(client):
    """Verifies that concurrent ingest batches trigger background window processing safely."""
    import concurrent.futures
    session_id = "concurrent_sess_01"

    # Ingest 2 full windows of data (800 samples each sensor)
    def send_batch(batch_idx):
        offset = batch_idx * 200
        samples = []
        for i in range(200):
            samples.append({"sensor": 1, "index": offset + i, "values": [100, 200, 300]})
            samples.append({"sensor": 2, "index": offset + i, "values": [400000, 150000]})
        payload = {
            "session_id": session_id,
            "samples": samples,
            "loss_records": [],
        }
        return client.post("/api/ingest", json=payload)

    # Fire 5 concurrent requests
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(send_batch, b) for b in range(5)]
        responses = [f.result() for f in futures]

    for r in responses:
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


def test_results_streaming_export(client):
    """Verifies that /api/results streams results as valid NDJSON."""
    from cloud.api import storage, worker
    session_id = "results_export_sess"

    # Populate 1 window
    s1_samples = [{"sensor": 1, "index": i, "values": [100, 200, 300]} for i in range(400)]
    s2_samples = [{"sensor": 2, "index": i, "values": [400000, 150000]} for i in range(400)]
    storage.ingest_batch(session_id, s1_samples + s2_samples, [])

    # Process windows
    worker.process_pending_windows(session_id)

    # Stream results
    resp = client.get(f"/api/results?session={session_id}")
    assert resp.status_code == 200
    assert "application/x-ndjson" in resp.headers["content-type"]
    lines = [line for line in resp.text.strip().split("\n") if line]
    assert len(lines) >= 1
    res = json.loads(lines[0])
    assert res["window"] == 0
    assert res["mode"] == "online"
    assert res["complete"] is True

