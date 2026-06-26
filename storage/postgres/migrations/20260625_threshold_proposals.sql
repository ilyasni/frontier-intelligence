-- Контур A (RSI): предложения по изменению порогов weak-signal детектора.
-- Ретро-петля пишет сюда строку (старый порог → новый + обоснование), человек утверждает
-- через MCP. Approve пишет новое значение в workspaces.extra->cluster_analysis (читается
-- кластеризацией на следующем прогоне — без редеплоя). Рекурсия разорвана человеком-гейтом.

CREATE TABLE IF NOT EXISTS threshold_proposals (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    threshold_key TEXT NOT NULL,
    current_value DOUBLE PRECISION NOT NULL,
    proposed_value DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'approved', 'rejected', 'superseded')),
    cohort_days INTEGER NOT NULL DEFAULT 0,
    direction TEXT,
    rationale TEXT,
    evidence JSONB DEFAULT '{}'::jsonb,
    threshold_version TEXT,
    run_id TEXT,
    created_by TEXT NOT NULL DEFAULT 'retrospective_loop',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    applied_value DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Не более одного активного (pending) предложения на (workspace, порог) — петля апсёртит его.
CREATE UNIQUE INDEX IF NOT EXISTS uq_threshold_proposals_pending
    ON threshold_proposals(workspace_id, threshold_key)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_threshold_proposals_status
    ON threshold_proposals(workspace_id, status, created_at DESC);
