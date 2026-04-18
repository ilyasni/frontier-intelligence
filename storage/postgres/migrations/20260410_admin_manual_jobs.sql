CREATE TABLE IF NOT EXISTS admin_manual_jobs (
    id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    workspace_id TEXT,
    status TEXT NOT NULL,
    trigger TEXT DEFAULT 'manual',
    summary JSONB DEFAULT '{}',
    result JSONB,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_admin_manual_jobs_lookup
    ON admin_manual_jobs(job_name, workspace_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_admin_manual_jobs_status
    ON admin_manual_jobs(status, created_at DESC);
