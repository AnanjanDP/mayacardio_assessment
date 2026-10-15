from typing import List, Optional

import uvicorn
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from cloud.config import CloudConfig, setup_cloud_logging
from cloud.storage import CloudStorage
from cloud.worker import CloudWorker

config = CloudConfig()
logger = setup_cloud_logging(config)

app = FastAPI(
    title="Never Lose a Heartbeat Cloud API",
    description="Fault-tolerant biosignal cloud ingestion, storage, and online processing backend.",
    version="1.0.0",
)

storage = CloudStorage(config.db_url)
worker = CloudWorker(storage)


class SampleItem(BaseModel):
    sensor: int = Field(..., description="Sensor ID (1 or 2)", example=1)
    index: int = Field(..., description="Sample index starting from 0", example=120000)
    values: List[int] = Field(
        ...,
        description="Channel values (3 for sensor 1, 2 for sensor 2)",
        example=[720, 1200, 1560],
    )


class LossRecordItem(BaseModel):
    sensor: int = Field(..., description="Sensor ID (1 or 2)", example=2)
    from_index: int = Field(..., description="Start index of missing range", example=4000)
    to_index: Optional[int] = Field(None, description="End index (or null if unbounded)", example=4399)
    reason: str = Field(..., description="Cause of loss", example="buffer_overflow")


class IngestBatchRequest(BaseModel):
    session_id: str = Field(..., description="Session identifier", example="5f0c2a9e")
    samples: List[SampleItem] = Field(default_factory=list)
    loss_records: List[LossRecordItem] = Field(default_factory=list)

    model_config = {
        "json_schema_extra": {
            "example": {
                "session_id": "5f0c2a9e",
                "samples": [
                    {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]},
                    {"sensor": 2, "index": 120000, "values": [400000, 150000]},
                ],
                "loss_records": [
                    {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}
                ],
            }
        }
    }


@app.get("/health", status_code=status.HTTP_200_OK)
def health():
    return {"status": "ok", "service": "biosignal-cloud-api"}


@app.post("/api/ingest", status_code=status.HTTP_200_OK)
def ingest(batch: IngestBatchRequest, background_tasks: BackgroundTasks):
    try:
        samples_raw = [s.model_dump() for s in batch.samples]
        losses_raw = [l.model_dump() for l in batch.loss_records]

        n_samples, n_losses = storage.ingest_batch(
            session_id=batch.session_id,
            samples=samples_raw,
            loss_records=losses_raw,
        )

        background_tasks.add_task(worker.process_pending_windows, batch.session_id)

        return {
            "status": "ok",
            "session_id": batch.session_id,
            "inserted_samples": n_samples,
            "inserted_losses": n_losses,
        }
    except Exception as e:
        logger.error("Ingestion failed for session %s: %s", batch.session_id, e, exc_info=True)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Ingestion failed: {e}")


@app.get("/api/status", status_code=status.HTTP_200_OK)
def status_endpoint():
    try:
        return storage.get_status()
    except Exception as e:
        logger.error("Status query failed: %s", e, exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get("/api/export")
def export_data(session: str = Query(..., description="Session ID to export")):
    def line_generator():
        for line in storage.export_session_samples(session):
            yield line + "\n"
        for line in storage.export_session_losses(session):
            yield line + "\n"

    return StreamingResponse(
        line_generator(),
        media_type="application/x-ndjson",
        headers={"Content-Disposition": f'inline; filename="export_{session}.ndjson"'},
    )


@app.get("/api/results")
def export_results(session: str = Query(..., description="Session ID to export results for")):
    def results_generator():
        for line in storage.export_results(session):
            yield line + "\n"

    return StreamingResponse(
        results_generator(),
        media_type="application/x-ndjson",
        headers={"Content-Disposition": f'inline; filename="results_{session}.ndjson"'},
    )


if __name__ == "__main__":
    uvicorn.run("cloud.api:app", host=config.host, port=config.port, log_level="info")
