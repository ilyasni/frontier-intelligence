-- Разрешить kind='crawl' для post_enrichments (crawl4ai).
-- Идемпотентно: снимает любой CHECK на колонке kind (если имя не стандартное) и создаёт целевой constraint.

DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT c.conname
    FROM pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    JOIN pg_namespace n ON t.relnamespace = n.oid
    WHERE n.nspname = 'public'
      AND t.relname = 'post_enrichments'
      AND c.contype = 'c'
      AND pg_get_constraintdef(c.oid) LIKE '%kind%'
  LOOP
    EXECUTE format('ALTER TABLE post_enrichments DROP CONSTRAINT %I', r.conname);
  END LOOP;
END $$;

ALTER TABLE post_enrichments
    DROP CONSTRAINT IF EXISTS post_enrichments_kind_check;

ALTER TABLE post_enrichments
    ADD CONSTRAINT post_enrichments_kind_check
    CHECK (kind IN ('concepts', 'vision', 'tags', 'crawl', 'valence'));
