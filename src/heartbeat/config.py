"""
Configuration management for Never Lose a Heartbeat edge pipeline and cloud services.
"""

import os
from dataclasses import dataclass
from typing import Optional


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


@dataclass
class CloudConfig:
    db_url: str = os.getenv("CLOUD_DB_URL", "sqlite:///data/cloud.db")
    host: str = os.getenv("CLOUD_HOST", "0.0.0.0")
    port: int = int(os.getenv("CLOUD_PORT", "8000"))
    worker_interval: float = float(os.getenv("CLOUD_WORKER_INTERVAL", "0.5"))
