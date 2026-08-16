-- Длительность прогонов в cluster_runs не измерялась с самого появления таблицы.
--
-- _finish_run писал `finished_at = NOW()`, а NOW() в PostgreSQL — синоним
-- transaction_timestamp(): момент НАЧАЛА транзакции, неизменный до её конца.
-- Прогон делает всю работу внутри одной длинной транзакции, поэтому в finished_at
-- попадало время, когда транзакция только открылась.
--
-- Замер 16.08.2026: signal-analysis у disruption шёл ~13 минут, в таблице
-- finished_at - started_at = 00:00:00.009907. У stage='full' цифра выглядела
-- правдоподобно (00:09:28) и была неверна иначе: промежуточный commit после
-- _replace_signal_series открывал транзакцию с _finish_run позже, и в замер
-- попадала только фаза семантической кластеризации — без фазы signal-analysis.
--
-- Исправлено в коде тем же числом: обе точки замера пишутся clock_timestamp()
-- (worker/services/semantic_clustering.py::_create_run, ::_finish_run).
--
-- Исторические строки восстановить нечем: настоящее время окончания нигде не
-- сохранялось, а updated_at писался тем же NOW() в том же UPDATE и совпадает
-- с finished_at до микросекунды. Поэтому они не чинятся, а помечаются.

-- 1. Пометка исторических строк.
--
-- Отсечка — литерал, а не now(): scripts/server-apply-sql-migrations.sh прогоняет
-- ВСЕ файлы миграций при каждом запуске, и подвижная граница на втором прогоне
-- пометила бы уже исправленные строки как неизмеренные. Значение отсечки —
-- фактический момент выката образов с исправлением (worker/admin пересозданы
-- 16.08.2026 15:03:10 UTC); строки с более ранним started_at записаны старым
-- кодом по определению.
--
-- Строки в status='running' не помечаются: у них finished_at пуст, длительности
-- нет вовсе, и путать «не измерено» с «не закончилось» не нужно.
UPDATE cluster_runs
   SET summary = COALESCE(summary, '{}'::jsonb) || '{"duration_unmeasured": true}'::jsonb
 WHERE finished_at IS NOT NULL
   AND started_at < TIMESTAMPTZ '2026-08-16 15:05:00+00'
   AND NOT (COALESCE(summary, '{}'::jsonb) ? 'duration_unmeasured');

-- 2. Свойство колонки описано в самой схеме, а не только в коде и доке: тот, кто
-- придёт считать длительности запросом к БД, читает \d+ cluster_runs, а не докстроку
-- питоновского хелпера.
COMMENT ON COLUMN cluster_runs.finished_at IS
    'Момент закрытия прогона, clock_timestamp(). До 16.08.2026 писался NOW() '
    '(= начало транзакции), поэтому finished_at - started_at у более ранних строк '
    'не является длительностью; такие строки помечены summary.duration_unmeasured. '
    'В длительность не входит индексация трендов в Qdrant — она идёт после commit.';

COMMENT ON COLUMN cluster_runs.started_at IS
    'Момент создания строки прогона, clock_timestamp().';

-- 3. DEFAULT на случай INSERT без явной колонки: NOW() здесь дал бы ту же
-- транзакционную заморозку. Сегодня _create_run передаёт started_at явно,
-- но дефолт обязан быть верным сам по себе.
ALTER TABLE cluster_runs ALTER COLUMN started_at SET DEFAULT clock_timestamp();
