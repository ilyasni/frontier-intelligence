ALTER TABLE workspaces
    ADD COLUMN IF NOT EXISTS extra JSONB DEFAULT '{}'::jsonb;

