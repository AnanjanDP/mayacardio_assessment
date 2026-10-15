"""
Configuration management for Never Lose a Heartbeat edge pipeline and cloud services.
"""

import logging
import os
from dataclasses import dataclass

logger = logging.getLogger(__name__)


def _load_env_file():
    env_file = os.path.join(os.getcwd(), ".env")
    if os.path.exists(env_file):
        try:
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip("'\"")
                    if k not in os.environ:
                        os.environ[k] = v
        except Exception as e:
            logger.debug("Failed reading .env file: %s", e)


_load_env_file()


@dataclass
class PipelineConfig:
    # Database
    db_path: str = os.getenv("PIPELINE_DB_PATH", "data/device.db")

    # Mock Service
    mock_host: str = os.getenv("PIPELINE_MOCK_HOST", "127.0.0.1")
    mock_port: int = int(os.getenv("PIPELINE_MOCK_PORT", "7100"))

    # Cloud Service
    cloud_url: str = os.getenv("PIPELINE_CLOUD_URL", "http://127.0.0.1:8000")

    # Operational Parameters
    poll_interval: float = float(os.getenv("PIPELINE_POLL_INTERVAL", "0.05"))
    batch_size: int = int(os.getenv("PIPELINE_BATCH_SIZE", "400"))
    sync_batch_size: int = int(os.getenv("PIPELINE_SYNC_BATCH_SIZE", "2000"))

    # Mode Timing (SLA <= 10s switch)
    offline_timeout: float = float(os.getenv("PIPELINE_OFFLINE_TIMEOUT", "3.0"))
    heartbeat_interval: float = float(os.getenv("PIPELINE_HEARTBEAT_INTERVAL", "2.0"))

    # SQLite Durability
    sqlite_synchronous: str = os.getenv("PIPELINE_SQLITE_SYNCHRONOUS", "FULL")  # FULL or NORMAL
    sqlite_journal_mode: str = os.getenv("PIPELINE_SQLITE_JOURNAL_MODE", "WAL")

    # Logging
    log_level: str = os.getenv("PIPELINE_LOG_LEVEL", "INFO")

