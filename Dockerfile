# Production Dockerfile for Never Lose a Heartbeat Cloud Backend
FROM python:3.11-slim as runtime

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src:/app"

WORKDIR /app

# Install minimal OS dependencies for healthchecks and PostgreSQL support
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl libpq5 && \
    rm -rf /var/lib/apt/lists/*

# Copy dependency specifications and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY src/ /app/src/
COPY cloud/ /app/cloud/

# Create non-privileged user and writable data directory for SQLite fallback
RUN useradd -u 10001 -m -s /bin/bash appuser && \
    mkdir -p /app/data && \
    chown -R appuser:appuser /app

USER appuser

# Standard port (overridden by Cloud environments via $PORT)
EXPOSE 8000

# Container healthcheck probe
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8000}/health || exit 1

# Launch uvicorn with dynamic $PORT support
CMD uvicorn cloud.api:app --host 0.0.0.0 --port ${PORT:-8000}
