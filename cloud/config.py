"""
cloud/config.py: Configuration and logging setup for FastAPI cloud service.
"""

import json
import logging
import os
import sys
from typing import Optional


class CloudConfig:
    def __init__(self):
        self.db_url: str = os.getenv("CLOUD_DB_URL", "sqlite:///data/cloud.db")
        port_env = os.getenv("PORT") or os.getenv("CLOUD_PORT") or "8000"
        try:
            self.port: int = int(port_env)
        except ValueError:
            self.port = 8000
        self.host: str = os.getenv("HOST") or os.getenv("CLOUD_HOST") or "0.0.0.0"
        try:
            self.worker_interval: float = float(os.getenv("CLOUD_WORKER_INTERVAL", "1.0"))
        except ValueError:
            self.worker_interval = 1.0
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()
        self.log_format_json: bool = os.getenv("LOG_FORMAT", "text").lower() == "json"


def setup_cloud_logging(config: Optional[CloudConfig] = None) -> logging.Logger:
    cfg = config or CloudConfig()
    level = getattr(logging, cfg.log_level, logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)

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
            handler.setFormatter(logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            ))
        root.addHandler(handler)

    return logging.getLogger("cloud.api")
