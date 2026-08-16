-- Закрывает осиротевшие прогоны cluster_runs, навсегда зависшие в status='running'.
--
-- Исходная причина (2026-07/08): обработчик ошибки выполнял UPDATE ... SET status='error'
-- внутри УЖЕ аборченной транзакции, падал сам на InFailedSQLTransactionError, исходное
-- исключение терялось, строка навсегда оставалась 'running', а APScheduler рапортовал
-- успех. Это починено 02.08.2026 (_mark_run_failed делает rollback перед записью), и
-- строки status='error' с тех пор появляются — на 16.08 их 102.
--
-- Оставшийся источник сирот — смерть по сигналу: при memcg-SIGKILL питоновский `except`
-- не выполняется вообще, поэтому записать статус некому в принципе. Осиротевший
-- 'running' — это и есть подпись OOM-убийства. Пересоздание контейнера с работающим
-- заданием даёт такую же строку.
--
-- Скрипт не чинит причину — только приводит таблицу в честный вид.
--
-- ПЕРЕД запуском убедиться, что активных job-субпроцессов нет. `ps` в admin-образе НЕТ,
-- и `docker exec ... ps aux | grep manual_jobs` молча падает, печатая пустоту — то есть
-- выглядит как «заданий нет» при любом положении дел. Смотреть через /proc:
--   docker compose exec -T admin sh -c \
--     'for p in /proc/[0-9]*; do tr "\0" " " < $p/cmdline; echo; done' | grep manual_jobs

\echo '=== ДО: распределение по статусам ==='
SELECT status, count(*) FROM cluster_runs GROUP BY status ORDER BY status;

\echo ''
\echo '=== Бэкап затрагиваемых строк в /tmp/cluster_runs_orphans_backup.csv ==='
\copy (SELECT id, workspace_id, stage, status, started_at, finished_at, created_at, updated_at, summary FROM cluster_runs WHERE status = 'running' AND finished_at IS NULL ORDER BY started_at) TO '/tmp/cluster_runs_orphans_backup.csv' WITH CSV HEADER

BEGIN;

-- finished_at СОЗНАТЕЛЬНО остаётся NULL.
--
-- Прежняя редакция ставила finished_at = started_at «как заглушку». После 16.08.2026
-- так делать нельзя: с этого дня finished_at - started_at действительно является
-- длительностью прогона (до этого писался NOW() = начало транзакции, и все замеры были
-- нулями — см. 20260816_cluster_runs_duration.sql). Заглушка добавила бы 29 строк с
-- нулевой длительностью, то есть ровно ту подпись, которую та починка убрала, и
-- следующий читатель не отличил бы «умер, время неизвестно» от «отработал мгновенно».
--
-- NULL — единственное честное значение: прогон не финишировал, момент смерти нигде не
-- сохранён. Запросы длительности отфильтровывают такие строки сами (finished_at IS NOT
-- NULL), а факт закрытия виден по status='error' и summary.retro_closed.
UPDATE cluster_runs
   SET status      = 'error',
       updated_at  = clock_timestamp(),
       summary     = COALESCE(summary, '{}'::jsonb) || jsonb_build_object(
           'retro_closed', true,
           'retro_closed_at', clock_timestamp(),
           'retro_reason',
           'Осиротевший прогон: процесс умер, не записав статус (memcg-SIGKILL не даёт '
           'выполниться except, пересоздание контейнера — тоже). finished_at оставлен '
           'NULL: реальное время отказа неизвестно, а подставлять started_at нельзя — '
           'это выдало бы нулевую длительность за измеренную.'
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
