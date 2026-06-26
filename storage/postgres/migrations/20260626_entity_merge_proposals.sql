-- Контур D+ (RSI): предложения семантического слияния концептов Neo4j.
-- Кандидаты-пары (акроним↔расшифровка, со-встречающиеся) подтверждаются LLM-судьёй другой
-- семьи и пишутся сюда (pending). Человек утверждает через MCP — approve сливает узлы графа.

CREATE TABLE IF NOT EXISTS entity_merge_proposals (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    norm_a TEXT NOT NULL,
    name_a TEXT NOT NULL,
    norm_b TEXT NOT NULL,
    name_b TEXT NOT NULL,
    canonical_norm TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    confidence DOUBLE PRECISION DEFAULT 0.0,
    reasoning TEXT,
    signal TEXT DEFAULT 'acronym',
    cooccurrence INTEGER DEFAULT 0,
    judge_provider TEXT,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'approved', 'rejected', 'superseded')),
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    run_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Одно активное предложение на пару (workspace, norm_a, norm_b).
CREATE UNIQUE INDEX IF NOT EXISTS uq_entity_merge_pair
    ON entity_merge_proposals(workspace_id, norm_a, norm_b);

CREATE INDEX IF NOT EXISTS idx_entity_merge_status
    ON entity_merge_proposals(workspace_id, status, confidence DESC);
