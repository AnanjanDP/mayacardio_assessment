"""
cloud/config.py: Configuration and structured logging for FastAPI cloud service.
Supports 12-factor cloud deployment (Render, Fly.io, Supabase, Neon).
"""

import json
import logging
import os
import sys
from typing import Optional


class CloudConfig:
    """Cloud application settings derived from environment variables."""

    def __init__(self):
        # Database URL: Supports SQLite fallback or PostgreSQL (Supabase / Neon)
        self.db_url: str = os.getenv("CLOUD_DB_URL", "sqlite:///data/cloud.db")

        # Standard port binding: prioritize PORT (Render / Fly.io / Heroku), then CLOUD_PORT, default 8000
        port_env = os.getenv("PORT") or os.getenv("CLOUD_PORT") or "8000"
        try:
            self.port: int = int(port_env)
        except ValueError:
            self.port = 8000

        # Host binding: prioritize HOST or CLOUD_HOST, default 0.0.0.0
        self.host: str = os.getenv("HOST") or os.getenv("CLOUD_HOST") or "0.0.0.0"

        # Worker interval for periodic processing sweep
        try:
            self.worker_interval: float = float(os.getenv("CLOUD_WORKER_INTERVAL", "1.0"))
        except ValueError:
            self.worker_interval = 1.0

        # Log format & level
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()
        self.log_format_json: bool = os.getenv("LOG_FORMAT", "text").lower() == "json"


def setup_cloud_logging(config: Optional[CloudConfig] = None) -> logging.Logger:
    """Configures structured production logging."""
    cfg = config or CloudConfig()
    level = getattr(logging, cfg.log_level, logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)

    # Avoid duplicate handlers if reloaded
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        if cfg.log_format_json:
            class JsonFormatter(logging.Formatter):
                def format(self, record):
                    log_obj = {
                        "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%SZ"),
                        "level": record.levelname,
                        "logger": record.name,
                        "message": record.getMessage(),
                    }
                    if record.exc_info:
                        log_obj["exception"] = self.formatException(record.exc_info)
                    return json.dumps(log_obj)

            handler.setFormatter(JsonFormatter())
        else:
            formatter = logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            handler.setFormatter(formatter)
        root.addHandler(handler)

    return logging.getLogger("cloud.api")
