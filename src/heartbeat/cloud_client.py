"""
HTTP client for Never Lose a Heartbeat cloud API.
"""

import logging
from typing import Generator, List, Optional

import requests

logger = logging.getLogger(__name__)


class CloudClient:
    """Communicates with the cloud ingestion service."""

    def __init__(self, base_url: str = "http://127.0.0.1:8000", timeout: float = 4.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def check_health(self) -> bool:
        """Probes the cloud /health endpoint. Returns True if healthy."""
        try:
            resp = self.session.get(f"{self.base_url}/health", timeout=self.timeout)
            return resp.status_code == 200
        except Exception as e:
            logger.debug("Cloud health check failed: %s", e)
            return False

    def ingest_batch(
        self,
        session_id: str,
        samples: List[dict],
        loss_records: List[dict],
    ) -> bool:
        """
        Uploads batch of samples and loss records to /api/ingest.
        Returns True only upon receiving verified 200 OK acknowledgment.
        """
        payload = {
            "session_id": session_id,
            "samples": samples,
            "loss_records": loss_records,
        }
        try:
            resp = self.session.post(
                f"{self.base_url}/api/ingest",
                json=payload,
                timeout=self.timeout,
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get("status") == "ok"
            logger.warning("Cloud ingest failed with status %d: %s", resp.status_code, resp.text)
            return False
        except Exception as e:
            logger.warning("Cloud ingest exception: %s", e)
            return False

    def get_cloud_status(self) -> Optional[dict]:
        """Fetches status summary from cloud."""
        try:
            resp = self.session.get(f"{self.base_url}/api/status", timeout=self.timeout)
            if resp.status_code == 200:
                return resp.json()
            return None
        except Exception as e:
            logger.debug("Failed to fetch cloud status: %s", e)
            return None


    def export_cloud_data(self, session_id: str) -> Generator[str, None, None]:
        """Streams cloud export JSON Lines."""
        resp = self.session.get(
            f"{self.base_url}/api/export",
            params={"session": session_id},
            stream=True,
            timeout=10.0,
        )
        resp.raise_for_status()
        for line in resp.iter_lines(decode_unicode=True):
            if line:
                yield line

    def export_cloud_results(self, session_id: str) -> Generator[str, None, None]:
        """Streams cloud results JSON Lines."""
        resp = self.session.get(
            f"{self.base_url}/api/results",
            params={"session": session_id},
            stream=True,
            timeout=10.0,
        )
        resp.raise_for_status()
        for line in resp.iter_lines(decode_unicode=True):
            if line:
                yield line
