-- Runs on first initialization of the postgres data volume.
-- The default database (POSTGRES_DB=rico) is created by the entrypoint
-- before this script runs.

-- Airflow stores its metadata in a separate database on the same instance.
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

\c rico

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS screens_metadata (
    screen_id           BIGINT PRIMARY KEY,
    app_package         TEXT,
    category            TEXT,
    png_path            TEXT NOT NULL,
    hierarchy_json_path TEXT NOT NULL,
    extraction_payload  JSONB,
    prompt_version      TEXT,
    confidence          DOUBLE PRECISION,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS screens_embeddings (
    screen_id      BIGINT NOT NULL,
    model_name     TEXT NOT NULL,
    model_version  TEXT NOT NULL,
    embedding_kind TEXT NOT NULL CHECK (embedding_kind IN ('image', 'text')),
    vector         vector NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (screen_id, model_name, model_version, embedding_kind)
);

CREATE TABLE IF NOT EXISTS screens_review_queue (
    id          BIGSERIAL PRIMARY KEY,
    screen_id   BIGINT NOT NULL,
    reason      TEXT NOT NULL,
    raw_output  TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS screens_eval (
    id                       BIGSERIAL PRIMARY KEY,
    embedding_model_version  TEXT NOT NULL,
    n_queries                INTEGER NOT NULL,
    recall_at_5              DOUBLE PRECISION NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
