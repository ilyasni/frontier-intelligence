-- RSI Фаза 0 — субстрат для ретроспективной петли.
-- Две append-only таблицы. Поведение пайплайна не меняется: только запись логов.
--   weak_signal_snapshots — снимок каждого weak-кандидата на каждом прогоне кластеризации,
--     включая подавленные weak-гейтом (которые сейчас теряются). Субстрат lookback 30/60/90.
--   relevance_decisions   — лог решений Relevance Filter (reject'ы) для аудита тихих false-negative.

CREATE TABLE IF NOT EXISTS weak_signal_snapshots (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    signal_key TEXT NOT NULL,
    title TEXT,
    stage TEXT NOT NULL,
    suppressed BOOLEAN NOT NULL DEFAULT FALSE,
    suppression_reason TEXT,
    signal_score FLOAT DEFAULT 0.0,
    confidence FLOAT DEFAULT 0.0,
    burst_score FLOAT DEFAULT 0.0,
    source_diversity FLOAT DEFAULT 0.0,
    source_count INTEGER DEFAULT 0,
    doc_count INTEGER DEFAULT 0,
    velocity_score FLOAT DEFAULT 0.0,
    acceleration_score FLOAT DEFAULT 0.0,
    keywords JSONB DEFAULT '[]'::jsonb,
    thresholds JSONB DEFAULT '{}'::jsonb,
    threshold_version TEXT NOT NULL,
    judge_verdict JSONB,
    detected_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_weak_snapshots_ws_detected
    ON weak_signal_snapshots(workspace_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_weak_snapshots_signal_key
    ON weak_signal_snapshots(workspace_id, signal_key, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_weak_snapshots_suppressed
    ON weak_signal_snapshots(workspace_id, suppressed, detected_at DESC);


CREATE TABLE IF NOT EXISTS relevance_decisions (
    id TEXT PRIMARY KEY,
    post_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    relevant BOOLEAN NOT NULL,
    score FLOAT DEFAULT 0.0,
    threshold FLOAT DEFAULT 0.0,
    category TEXT,
    reasoning TEXT,
    escalated BOOLEAN DEFAULT FALSE,
    model TEXT,
    source_id TEXT,
    content_excerpt TEXT,
    audit_status TEXT,
    audit_note TEXT,
    decided_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_relevance_decisions_ws_decided
    ON relevance_decisions(workspace_id, decided_at DESC);
CREATE INDEX IF NOT EXISTS idx_relevance_decisions_audit
    ON relevance_decisions(workspace_id, audit_status, decided_at DESC);
CREATE INDEX IF NOT EXISTS idx_relevance_decisions_post
    ON relevance_decisions(post_id);
