-- posts.source_id: при удалении источника обнулять ссылку (ON DELETE SET NULL),
-- а не блокировать удаление (раньше FK без ON DELETE => DELETE источника с постами падал).
-- Посты сохраняются как «осиротевшие» (source_id = NULL). Идемпотентно, атомарно.
BEGIN;

-- 1. Разрешить NULL (иначе SET NULL нарушит NOT NULL).
ALTER TABLE posts ALTER COLUMN source_id DROP NOT NULL;

-- 2. Снять существующий FK на source_id (имя может быть автогенерённым posts_source_id_fkey).
DO $$
DECLARE
    fk_name text;
BEGIN
    SELECT conname INTO fk_name
    FROM pg_constraint
    WHERE conrelid = 'posts'::regclass
      AND contype = 'f'
      AND pg_get_constraintdef(oid) LIKE 'FOREIGN KEY (source_id)%';
    IF fk_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE posts DROP CONSTRAINT %I', fk_name);
    END IF;
END $$;

-- 3. Пересоздать FK с ON DELETE SET NULL.
ALTER TABLE posts
    ADD CONSTRAINT posts_source_id_fkey
    FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE SET NULL;

COMMIT;
