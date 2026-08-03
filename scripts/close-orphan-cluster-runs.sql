-- Закрывает осиротевшие прогоны cluster_runs, навсегда зависшие в status='running'.
--
-- Причина сирот: обработчик ошибки в admin/backend/scheduler.py выполняет
-- UPDATE cluster_runs SET status='error' внутри УЖЕ аборченной транзакции.
-- Он падает сам на InFailedSQLTransactionError, исходное исключение теряется,
-- строка навсегда остаётся 'running', а APScheduler рапортует успех.
-- Поэтому за всю историю таблицы нет НИ ОДНОЙ строки со status='error'.
--
-- Скрипт не чинит сам баг — только приводит таблицу в честный вид.
-- Запускать, убедившись что активных job-субпроцессов нет:
--   docker exec frontier-intelligence-admin-1 ps aux | grep manual_jobs

\echo '=== ДО: распределение по статусам ==='
SELECT status, count(*) FROM cluster_runs GROUP BY status ORDER BY status;

\echo ''
\echo '=== Бэкап затрагиваемых строк в /tmp/cluster_runs_orphans_backup.csv ==='
\copy (SELECT id, workspace_id, stage, status, started_at, finished_at, created_at, updated_at, summary FROM cluster_runs WHERE status = 'running' AND finished_at IS NULL ORDER BY started_at) TO '/tmp/cluster_runs_orphans_backup.csv' WITH CSV HEADER

BEGIN;

UPDATE cluster_runs
   SET status      = 'error',
       finished_at = started_at,
       updated_at  = now(),
       summary     = COALESCE(summary, '{}'::jsonb) || jsonb_build_object(
           'retro_closed', true,
           'retro_closed_at', now(),
           'retro_reason',
           'Осиротевший прогон: error-хендлер планировщика упал в аборченной транзакции, '
           'поэтому status никогда не стал error. finished_at выставлен равным started_at '
           'как заглушка — реальное время отказа неизвестно.'
       )
 WHERE status = 'running'
   AND finished_at IS NULL;

COMMIT;

\echo ''
\echo '=== ПОСЛЕ: распределение по статусам ==='
SELECT status, count(*) FROM cluster_runs GROUP BY status ORDER BY status;

\echo ''
\echo '=== Осталось зависших (должно быть 0) ==='
SELECT count(*) AS still_stuck FROM cluster_runs WHERE status = 'running' AND finished_at IS NULL;
