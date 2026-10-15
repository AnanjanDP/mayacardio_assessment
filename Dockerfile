FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src:/app"

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl libpq5 && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/
COPY cloud/ /app/cloud/

RUN useradd -u 10001 -m -s /bin/bash appuser && \
    mkdir -p /app/data && \
    chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8000}/health || exit 1

CMD uvicorn cloud.api:app --host 0.0.0.0 --port ${PORT:-8000}
