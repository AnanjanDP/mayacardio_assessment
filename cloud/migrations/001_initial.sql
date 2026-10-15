-- 001_initial.sql: PostgreSQL schema for Never Lose a Heartbeat Cloud Database

CREATE TABLE IF NOT EXISTS cloud_sessions (
    session_id VARCHAR(64) PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS cloud_samples (
    session_id VARCHAR(64) NOT NULL,
    sensor INT NOT NULL,
    sample_index INT NOT NULL,
    val0 INT NOT NULL,
    val1 INT NOT NULL,
    val2 INT,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (session_id, sensor, sample_index)
);

CREATE TABLE IF NOT EXISTS cloud_loss_records (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(64) NOT NULL,
    sensor INT NOT NULL,
    from_index INT NOT NULL,
    to_index INT,
    reason TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    UNIQUE (session_id, sensor, from_index, to_index, reason)
);

CREATE INDEX IF NOT EXISTS idx_cloud_loss_lookup
ON cloud_loss_records (session_id, sensor);

CREATE TABLE IF NOT EXISTS cloud_processing_results (
    session_id VARCHAR(64) NOT NULL,
    window_index INT NOT NULL,
    mode VARCHAR(16) NOT NULL,
    missing_1 INT NOT NULL,
    missing_2 INT NOT NULL,
    complete INT NOT NULL,
    channels_json TEXT NOT NULL,
    magnitude DOUBLE PRECISION,
    heart_rate DOUBLE PRECISION,
    processed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (session_id, window_index, mode)
);

CREATE TABLE IF NOT EXISTS cloud_processed_windows (
    session_id VARCHAR(64) NOT NULL,
    window_index INT NOT NULL,
    mode VARCHAR(16) NOT NULL,
    PRIMARY KEY (session_id, window_index, mode)
);
