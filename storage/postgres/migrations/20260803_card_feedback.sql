-- Разметка редакторского выбора: раз в неделю автор берёт одну карточку из пятёрки,
-- остальные помечаются passed с общим reason. Одна строка на карточку — иначе пару
-- «выбрана / не выбрана» не восстановить, а ценность именно в паре.
--
-- Таблица append-only и самодостаточна: card_snapshot + relevance_at_pick +
-- own_stake_at_pick хранят копию того, что было показано. FK на таблицы сигналов НЕТ
-- намеренно: semantic_clusters перезаписывается каждый прогон, missing_signals целиком
-- DELETE-ается (worker/services/missing_signals.py:317) — через месяц entity_id укажет
-- в пустоту, а разметка обязана пережить источник. Целостность проверяется руками.
--
-- UNIQUE без workspace_id: карточки недельной пятёрки могут прийти из разных
-- воркспейсов, batch_id сквозной.
-- own_stake_at_pick nullable — до готовности задачи B пишется только релевантность.

CREATE TABLE IF NOT EXISTS card_feedback (
    id                TEXT PRIMARY KEY,
    workspace_id      TEXT NOT NULL REFERENCES workspaces(id),
    batch_id          TEXT NOT NULL,
    entity_kind       TEXT NOT NULL CHECK (entity_kind IN ('post','semantic','trend','emerging','missing')),
    entity_id         TEXT NOT NULL,
    verdict           TEXT NOT NULL CHECK (verdict IN ('chosen','passed')),
    reason            TEXT,
    relevance_at_pick DOUBLE PRECISION,
    own_stake_at_pick DOUBLE PRECISION,
    card_snapshot     JSONB NOT NULL DEFAULT '{}'::jsonb,
    reviewed_by       TEXT NOT NULL DEFAULT 'author',
    decided_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(batch_id, entity_kind, entity_id)
);

-- «Ровно одна chosen-строка на батч» — инвариант таблицы, а не соглашение вызывающего.
-- record_card_feedback принимает один workspace за вызов, а batch_id сквозной, поэтому
-- пятёрка из двух воркспейсов приходит двумя вызовами; без этого индекса второй вызов
-- (а равно повторная отправка расширенного батча) молча добавил бы вторую chosen-строку,
-- и пара «выбрана / не выбрана» перестала бы восстанавливаться. Таблица append-only —
-- исправить такую строку потом нечем, поэтому проверка стоит в БД, а не только в коде.
CREATE UNIQUE INDEX IF NOT EXISTS uq_card_feedback_batch_chosen
    ON card_feedback(batch_id) WHERE verdict = 'chosen';

CREATE INDEX IF NOT EXISTS idx_card_feedback_ws_decided ON card_feedback(workspace_id, decided_at DESC);
CREATE INDEX IF NOT EXISTS idx_card_feedback_batch      ON card_feedback(batch_id);
CREATE INDEX IF NOT EXISTS idx_card_feedback_verdict    ON card_feedback(workspace_id, verdict, decided_at DESC);
