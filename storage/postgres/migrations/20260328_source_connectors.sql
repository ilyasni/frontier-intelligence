-- Frontier Intelligence source connector upgrade
-- Safe to run multiple times on existing databases.

ALTER TABLE sources DROP CONSTRAINT IF EXISTS sources_source_type_check;
ALTER TABLE sources
    ADD CONSTRAINT sources_source_type_check
    CHECK (source_type IN ('telegram', 'rss', 'web', 'api', 'email', 'habr'));

CREATE TABLE IF NOT EXISTS source_checkpoints (
    source_id TEXT PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    cursor_json JSONB DEFAULT '{}',
    etag TEXT,
    last_modified TEXT,
    last_seen_published_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_error TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS source_runs (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'error')),
    fetched_count INTEGER DEFAULT 0,
    emitted_count INTEGER DEFAULT 0,
    error_text TEXT
);

CREATE INDEX IF NOT EXISTS idx_source_runs_source_started
    ON source_runs(source_id, started_at DESC);
