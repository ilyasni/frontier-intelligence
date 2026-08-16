-- Убирает подставные finished_at у прогонов, закрытых задним числом 02.08.2026.
--
-- Тогдашняя редакция scripts/close-orphan-cluster-runs.sql ставила осиротевшим строкам
-- finished_at = started_at «как заглушку». Пока длительность в таблице всё равно была
-- фикцией (finished_at писался NOW() = начало транзакции), это ничего не портило.
--
-- После 16.08.2026 портит: finished_at - started_at стал настоящей длительностью
-- (20260816_cluster_runs_duration.sql), и 93 строки с нулём читаются как «прогон
-- отработал мгновенно» — ровно та подпись, которую та миграция и убирала. Флаг
-- summary.duration_unmeasured на них стоит, но он объясняет, что цифре нельзя верить,
-- а не то, что прогон вообще не финишировал.
--
-- NULL — единственное правдивое значение: процесс умер по сигналу (memcg-SIGKILL не
-- даёт выполниться except), момент смерти нигде не сохранён. После этого такие строки
-- становятся неотличимы от сирот, закрытых новой редакцией скрипта, — и правильно,
-- это один и тот же случай.
--
-- Идемпотентна: scripts/server-apply-sql-migrations.sh прогоняет все файлы при каждом
-- запуске, и на втором проходе условие finished_at = started_at не выполнит ни одна
-- строка. Отбор узкий намеренно — на 16.08 равенство finished_at = started_at во всей
-- таблице встречалось ровно у этих 93 строк и ни у одной другой. Бэкап снят в
-- docs/ops/backups/cluster_runs_stub_finish_20260816.csv.

UPDATE cluster_runs
   SET finished_at = NULL,
       updated_at  = clock_timestamp(),
       summary     = COALESCE(summary, '{}'::jsonb) || jsonb_build_object(
           'retro_stub_finish_removed_at', clock_timestamp(),
           'retro_reason',
           'Осиротевший прогон: процесс умер, не записав статус. finished_at был '
           'подставлен равным started_at при закрытии 02.08.2026 и снят 16.08.2026 — '
           'подстановка выдавала нулевую длительность за измеренную. Реальное время '
           'отказа неизвестно.'
       )
 WHERE status = 'error'
   AND summary ? 'retro_closed'
   AND finished_at IS NOT NULL
   AND finished_at = started_at;
