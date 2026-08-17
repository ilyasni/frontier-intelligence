#!/usr/bin/env bash
# Экспортирует свежесть аналитического слоя в node_exporter textfile collector.
#
# Зачем: провал scheduled clustering / signal-analysis логируется как УСПЕХ —
# error-хендлер пишет UPDATE cluster_runs в уже аборченной транзакции, падает сам,
# прогон навсегда остаётся в status='running', APScheduler рапортует "executed
# successfully". Поэтому алертить по статусу джоба бесполезно: надо смотреть,
# сдвинулись ли данные. 31.07–02.08.2026 аналитика стояла 54 часа незамеченной.
#
# Ставится в cron на хосте, например каждые 10 минут:
#   */10 * * * * /opt/frontier-intelligence/scripts/export-analysis-freshness.sh >/dev/null 2>&1

set -euo pipefail

PROJECT_DIR=/opt/frontier-intelligence
OUT_DIR="$PROJECT_DIR/prometheus/textfile"
OUT="$OUT_DIR/frontier_analysis.prom"
TMP="$OUT.$$"
PG_CONTAINER=frontier-intelligence-postgres-1

mkdir -p "$OUT_DIR"

emit() {
    echo '# HELP frontier_analysis_last_update_timestamp_seconds Unix time of the most recent analysis-layer row write.'
    echo '# TYPE frontier_analysis_last_update_timestamp_seconds gauge'
    echo '# HELP frontier_analysis_rows_total Rows in the analysis-layer table for this workspace.'
    echo '# TYPE frontier_analysis_rows_total gauge'

    # Пять таблиц, а не две. Прежняя редакция смотрела только на emerging_signals
    # и semantic_clusters, поэтому 21 день без единого тренда у `design` и 10 дней
    # у `ai_products_media` прошли молча: метрика по этим воркспейсам всё это время
    # показывала свежесть 2-3 часа — по ДРУГИМ таблицам.
    #
    # Выборка идёт от списка воркспейсов (CROSS JOIN), а не от GROUP BY по самой
    # таблице. Разница принципиальная: воркспейс, у которого строк НЕТ вовсе
    # (auto_hmi в trend_clusters, любой в card_feedback), при GROUP BY просто не
    # попадал в результат — серии не возникало, а `max by (table)` по несуществующей
    # серии не считается, и алерт не мог сработать никогда. Отсутствие данных обязано
    # выражаться нулём, а не отсутствием ряда.
    #
    # Отсюда и вторая метрика. Свежесть отвечает на вопрос «слой шевелится?», но у
    # пустой таблицы timestamp взять физически неоткуда — max(NULL) не существует.
    # frontier_analysis_rows_total — единственный способ отличить «таблица пуста»
    # от «экспортёр не дошёл до этой таблицы». Ноль по card_feedback ОЖИДАЕМ
    # (редакторская петля не запускалась ни разу, гейт калибровки недостижим, пока
    # свёрнута ось own_stake) и алерта на него намеренно нет — это наблюдаемость,
    # а не тревога.
    docker exec "$PG_CONTAINER" psql -U "${POSTGRES_USER:-frontier}" -d "${POSTGRES_DB:-frontier}" \
        -At -F'|' -c "
            WITH ws AS (SELECT id FROM workspaces WHERE is_active),
            agg AS (
                SELECT 'emerging_signals' AS tbl, workspace_id, count(*) AS n,
                       max(updated_at) AS ts FROM emerging_signals GROUP BY workspace_id
                UNION ALL
                SELECT 'semantic_clusters', workspace_id, count(*),
                       max(updated_at) FROM semantic_clusters GROUP BY workspace_id
                UNION ALL
                SELECT 'trend_clusters', workspace_id, count(*),
                       max(updated_at) FROM trend_clusters GROUP BY workspace_id
                UNION ALL
                SELECT 'missing_signals', workspace_id, count(*),
                       max(updated_at) FROM missing_signals GROUP BY workspace_id
                UNION ALL
                SELECT 'card_feedback', workspace_id, count(*),
                       max(created_at) FROM card_feedback GROUP BY workspace_id
            ),
            tables AS (
                SELECT * FROM (VALUES
                    ('emerging_signals'),('semantic_clusters'),('trend_clusters'),
                    ('missing_signals'),('card_feedback')
                ) AS t(tbl)
            )
            SELECT t.tbl, ws.id, COALESCE(a.n, 0),
                   COALESCE(extract(epoch from a.ts)::bigint::text, '')
              FROM tables t
             CROSS JOIN ws
              LEFT JOIN agg a ON a.tbl = t.tbl AND a.workspace_id = ws.id
             ORDER BY t.tbl, ws.id
        " </dev/null | while IFS='|' read -r tbl ws rows ts; do
        if [ -z "${tbl:-}" ] || [ -z "${ws:-}" ]; then
            continue
        fi
        # Счётчик строк печатается ВСЕГДА и первым — он и есть отличие «пусто»
        # от «не измеряли». Пустой ts пропускается только для метрики свежести.
        printf 'frontier_analysis_rows_total{table="%s",workspace="%s"} %s\n' \
            "$tbl" "$ws" "${rows:-0}"
        if [ -n "${ts:-}" ]; then
            printf 'frontier_analysis_last_update_timestamp_seconds{table="%s",workspace="%s"} %s\n' \
                "$tbl" "$ws" "$ts"
        fi
    done

    # Материал, который кластеризация не увидит никогда (пункт 75 реестра).
    #
    # Выборка кластеризации требует `p.published_at IS NOT NULL` наравне с
    # `embedding_status='done'` и порогом релевантности. Пост без даты публикации
    # исключён предикатом — не «редко проходит», а не участвует вовсе.
    #
    # Замер 07.08.2026: у web-источников таких 1126 из 1186 (95%), у rss 2 из 207787,
    # у telegram и api ноль. Отказ до сих пор был невидим целиком: `source_runs`
    # пишет `success`, `fetched_count` растёт, пост сохраняется и находится поиском.
    #
    # Алерта на эту серию НЕТ и пока не будет: доля уже высока, правило зажглось бы
    # в первую же минуту и превратилось бы в фон — та же болезнь, что лечил пункт 56.
    # Сначала решение владельца по развилке (пункт 75), потом порог. До тех пор это
    # наблюдаемость, а не тревога.
    #
    # Ноль печатается ЯВНО: отсутствие серии неотличимо от «экспортёр не дошёл».
    docker exec "$PG_CONTAINER" psql -U "${POSTGRES_USER:-frontier}" -d "${POSTGRES_DB:-frontier}" \
        -At -F'|' -c "
            WITH ws AS (SELECT id FROM workspaces WHERE is_active),
            agg AS (
                SELECT p.workspace_id,
                       count(*) FILTER (WHERE p.published_at IS NULL) AS no_date,
                       count(*) AS total
                  FROM posts p
                 WHERE p.created_at > now() - interval '7 days'
                 GROUP BY p.workspace_id
            )
            SELECT ws.id, COALESCE(a.no_date, 0), COALESCE(a.total, 0)
              FROM ws LEFT JOIN agg a ON a.workspace_id = ws.id
             ORDER BY ws.id
        " </dev/null | while IFS='|' read -r ws no_date total; do
        if [ -z "${ws:-}" ]; then
            continue
        fi
        printf 'frontier_posts_without_published_at{workspace="%s"} %s\n' "$ws" "${no_date:-0}"
        printf 'frontier_posts_ingested_7d{workspace="%s"} %s\n' "$ws" "${total:-0}"
    done

    # То же самое по ИСТОЧНИКАМ, долей и в двух окнах (пункт 75 реестра).
    #
    # Разбивки по воркспейсу выше недостаточно, и это видно на числах: у
    # `disruption` без даты 1129 постов из 285 879 — 0.39%, шум. Внутри этих
    # 0.39% сидят ТРИНАДЦАТЬ источников со 100%: web_plusworld (394 поста, все),
    # web_ux_journal (252), web_nngroup_articles (103) и далее. Отказ адресный,
    # значит и метрика обязана быть адресной, иначе он тонет в знаменателе.
    #
    # Замер по всей базе 16.08.2026: web 1199 из 1262 (95%), rss 36 из 223 899
    # (0.02%), api и telegram — ноль. Двадцать один источник даёт хотя бы один
    # пост без даты, восемнадцать из них — 100%.
    #
    # ── Почему два окна, а не порог на факт ──────────────────────────────────
    # Порог на факт («доля выше половины») сработать-то сработает. Перемотка
    # предлагаемого условия по реальным данным за 45 суток (created_at как
    # момент оценки): без сравнения с прошлым правило было бы firing 46 суток
    # из 46, 116 источнико-суток. Входа, при котором оно молчит, НЕ существует —
    # ровно болезнь пункта 56, где карантинный алерт OpenRouter висел 6 суток
    # из 7 на штатном суточном сбросе квоты.
    #
    # Поэтому второе окно `prior` — вся история источника ДО начала окна `7d`.
    # Тревожит не состояние, а СКАЧОК. Та же перемотка того же набора: 18 суток
    # из 46, 70 источнико-суток, ноль прямо сейчас. Все срабатывания попадают в
    # два всплеска (02–12.07 и 06–12.08) — обе даты совпадают с включением
    # партий новых web-источников, то есть правило показывает событие, а не фон.
    #
    # ── Два несимметричных решения о нулях ───────────────────────────────────
    # `prior` печатается ВСЕГДА, и у источника без истории он равен нулю. Иначе
    # новорождённый источник не даёт пары для вычитания, и правило молчит именно
    # в том случае, ради которого написано: `auto_web_ieee_spectrum_autonomous`
    # включили 05.08, он выдал 33 поста подряд без даты, и это ровно тот отказ,
    # который сейчас разбирается. Ноль здесь — утверждение по существу: «до этой
    # недели ни одного поста без даты от него не приходило».
    #
    # `7d`, наоборот, печатается только при непустом знаменателе. Доля от нуля
    # постов — не ноль, а отсутствие измерения; напечатанный «0.0» читался бы на
    # графике как «источник здоров», хотя он просто молчит. Знаменатель при этом
    # печатается всегда — по нему и отличается «нечего мерить» от «не измеряли».
    echo '# HELP frontier_source_dateless_ratio Доля постов источника без published_at в окне.'
    echo '# TYPE frontier_source_dateless_ratio gauge'
    echo '# HELP frontier_source_posts_in_window Постов источника в окне (знаменатель доли).'
    echo '# TYPE frontier_source_posts_in_window gauge'
    docker exec "$PG_CONTAINER" psql -U "${POSTGRES_USER:-frontier}" -d "${POSTGRES_DB:-frontier}" \
        -At -F'|' -c "
            WITH src AS (
                SELECT id, workspace_id, source_type FROM sources WHERE is_enabled
            ),
            agg AS (
                SELECT p.source_id,
                       count(*) FILTER (WHERE p.created_at >  now() - interval '7 days')
                           AS recent_n,
                       count(*) FILTER (WHERE p.created_at >  now() - interval '7 days'
                                          AND p.published_at IS NULL)
                           AS recent_null,
                       count(*) FILTER (WHERE p.created_at <= now() - interval '7 days')
                           AS prior_n,
                       count(*) FILTER (WHERE p.created_at <= now() - interval '7 days'
                                          AND p.published_at IS NULL)
                           AS prior_null
                  FROM posts p
                 GROUP BY p.source_id
            )
            SELECT s.id, s.workspace_id, s.source_type,
                   COALESCE(a.recent_n, 0), COALESCE(a.recent_null, 0),
                   COALESCE(a.prior_n, 0),  COALESCE(a.prior_null, 0)
              FROM src s LEFT JOIN agg a ON a.source_id = s.id
             ORDER BY s.id
        " </dev/null | while IFS='|' read -r sid ws stype recent_n recent_null prior_n prior_null; do
        if [ -z "${sid:-}" ]; then
            continue
        fi
        labels="source_id=\"$sid\",workspace=\"$ws\",source_type=\"$stype\""
        printf 'frontier_source_posts_in_window{%s,window="7d"} %s\n' \
            "$labels" "${recent_n:-0}"
        printf 'frontier_source_posts_in_window{%s,window="prior"} %s\n' \
            "$labels" "${prior_n:-0}"
        # prior — всегда, включая ноль у источника без истории (см. выше).
        printf 'frontier_source_dateless_ratio{%s,window="prior"} %s\n' "$labels" \
            "$(awk -v d="${prior_null:-0}" -v n="${prior_n:-0}" \
                'BEGIN{printf "%.4f", (n > 0 ? d/n : 0)}')"
        if [ "${recent_n:-0}" -gt 0 ]; then
            printf 'frontier_source_dateless_ratio{%s,window="7d"} %s\n' "$labels" \
                "$(awk -v d="${recent_null:-0}" -v n="$recent_n" 'BEGIN{printf "%.4f", d/n}')"
        fi
    done

    # Остальные поля, отсутствие которых так же молча выкидывает пост из аналитики.
    #
    # Проверка симметричная к дате публикации: у `_fetch_posts` три предиката, и
    # спрашивать надо про каждый. Замер по всей базе 16.08.2026 (337 735 постов):
    #
    #   • published_at IS NULL ......................... 1235
    #   • relevance_score IS NULL ......................... 49
    #   • нет строки в indexing_status ..................... 0
    #   • embedding_status <> 'done' ПРИ релевантности >=0.6  0 (за 30 суток)
    #
    # Последние две строки — важный отрицательный результат. INNER JOIN на
    # indexing_status выглядит как второй тихий отсев, но не отсеивает ничего:
    # строка есть у каждого поста. А `embedding_status='dropped'` (20 161 за
    # 30 суток) совпадает с низкой релевантностью ТОЧНО — ни одного релевантного
    # поста без эмбеддинга нет. То есть эмбеддинг не самостоятельные ворота, а
    # следствие релевантности, и отдельная метрика по нему только шумела бы.
    #
    # А вот `relevance_score IS NULL` — тот же класс, что пропавшая дата, только
    # мельче: обогащение упало, оценки нет, `COALESCE(relevance_score,0) >= 0.6`
    # исключает пост навсегда. Все 49 приходятся на embedding_status error (42)
    # и pending (7), то есть это осадок неудачных обогащений, а не норма.
    #
    # Алерта нет ни на одну из трёх серий, и это осознанно: множества маленькие и
    # статичные, порог на них был бы выбран из воздуха. Здесь важно, чтобы осадок
    # был ВИДЕН и рос заметно, а не чтобы будил. Печатается лайфтайм, а не окно:
    # осадок по построению накапливается, и семисуточное окно показывало бы ноль
    # ровно тогда, когда накоплено больше всего.
    echo '# HELP frontier_posts_unanalyzable Посты, навсегда исключённые из выборки кластеризации, по причине.'
    echo '# TYPE frontier_posts_unanalyzable gauge'
    docker exec "$PG_CONTAINER" psql -U "${POSTGRES_USER:-frontier}" -d "${POSTGRES_DB:-frontier}" \
        -At -F'|' -c "
            WITH ws AS (SELECT id FROM workspaces WHERE is_active),
            agg AS (
                SELECT p.workspace_id,
                       count(*) FILTER (WHERE p.published_at IS NULL)    AS no_date,
                       count(*) FILTER (WHERE p.relevance_score IS NULL) AS no_relevance,
                       count(*) FILTER (
                           WHERE NOT EXISTS (
                               SELECT 1 FROM indexing_status i WHERE i.post_id = p.id
                           )
                       ) AS no_indexing_row
                  FROM posts p
                 GROUP BY p.workspace_id
            )
            SELECT ws.id, COALESCE(a.no_date, 0), COALESCE(a.no_relevance, 0),
                   COALESCE(a.no_indexing_row, 0)
              FROM ws LEFT JOIN agg a ON a.workspace_id = ws.id
             ORDER BY ws.id
        " </dev/null | while IFS='|' read -r ws no_date no_relevance no_indexing; do
        if [ -z "${ws:-}" ]; then
            continue
        fi
        # Ноль печатается ЯВНО по каждой причине: серия, которой нет, неотличима
        # от «экспортёр до неё не дошёл», а `no_indexing_row` сейчас ноль везде —
        # и именно его исчезновение из вывода означало бы, что проверять перестали.
        printf 'frontier_posts_unanalyzable{workspace="%s",reason="no_published_at"} %s\n' \
            "$ws" "${no_date:-0}"
        printf 'frontier_posts_unanalyzable{workspace="%s",reason="no_relevance_score"} %s\n' \
            "$ws" "${no_relevance:-0}"
        printf 'frontier_posts_unanalyzable{workspace="%s",reason="no_indexing_row"} %s\n' \
            "$ws" "${no_indexing:-0}"
    done

    # Свежесть ежедневной петли разбора алертов (docs/runbooks/alert-triage-daily.md).
    #
    # Считается по времени записи последнего дайджеста НА СЕРВЕРЕ, а не изнутри самой
    # петли: 03.08 и 04.08.2026 она умерла два дня подряд (0xC000013A, два `start`
    # без парного `exit=`), и никакого сигнала не было именно потому, что сообщать
    # о своей смерти было некому. Наблюдатель обязан жить снаружи наблюдаемого.
    #
    # mtime, а не дата из имени файла: имя несёт дату дня, то есть полночь, и возраст
    # «свежего» дайджеста стартовал бы с 9 часов — порог пришлось бы задирать. Каталог
    # исключён из rsync (.rsync-exclude), поэтому синхронизация mtime не сдвигает.
    echo '# HELP frontier_alert_triage_last_digest_timestamp_seconds Unix time of the newest alert-triage digest on the server.'
    echo '# TYPE frontier_alert_triage_last_digest_timestamp_seconds gauge'
    newest_digest=$(find "$PROJECT_DIR/docs/ops/alert-digests" -maxdepth 1 -type f -name '*.md' \
        -printf '%T@\n' 2>/dev/null | sort -rn | head -1 | cut -d. -f1)
    if [ -n "${newest_digest:-}" ]; then
        printf 'frontier_alert_triage_last_digest_timestamp_seconds %s\n' "$newest_digest"
    fi

    # Исход последнего прогона петли НА РАБОЧЕЙ МАШИНЕ. Свежести дайджеста мало.
    # 07.08–17.08.2026 петля запускалась исправно каждое утро (LastRunTime свежий,
    # пропусков ноль) и падала за ТРИ СЕКУНДЫ на `Not logged in`. Увидели на
    # одиннадцатые сутки: единственным сигналом был возраст дайджеста с порогом 26ч,
    # а он отвечает «результата давно нет» и не отличает «не запускалась» от
    # «запустилась и умерла». Причины разные, и действия разные: первая лечится
    # командой /login за минуту, вторая — разбором лога.
    #
    # Отчёт кладёт сам раннер (.claude/run-alert-triage.ps1) по ssh после КАЖДОГО
    # прогона, удачного или нет. Раннер живёт вне репозитория (`.claude/` в
    # .gitignore и .rsync-exclude), поэтому метрику эмитим здесь — иначе её имя
    # пришлось бы объявлять внешним в tests/test_alert_rules_contract.py и потерять
    # ту самую защиту от опечатки в имени.
    #
    # Значения проверяем на целочисленность перед печатью: битая строка в textfile
    # роняет разбор ВСЕГО файла, то есть унесла бы с собой и все метрики свежести.
    run_report="$PROJECT_DIR/runtime/alert-triage-last-run"
    if [ -f "$run_report" ]; then
        report_field() {
            local key="$1" val
            val=$(grep -m1 "^${key}=" "$run_report" 2>/dev/null | cut -d= -f2- | tr -d '[:space:]')
            case "$val" in
                ''|*[!0-9]*) return 1 ;;
                *) printf '%s' "$val" ;;
            esac
        }

        if exit_code=$(report_field exit_code); then
            echo '# HELP frontier_alert_triage_last_exit_code Exit code of the last alert-triage run on the workstation.'
            echo '# TYPE frontier_alert_triage_last_exit_code gauge'
            printf 'frontier_alert_triage_last_exit_code %s\n' "$exit_code"
        fi
        if run_seconds=$(report_field duration_seconds); then
            echo '# HELP frontier_alert_triage_last_duration_seconds Wall-clock seconds the last alert-triage run took.'
            echo '# TYPE frontier_alert_triage_last_duration_seconds gauge'
            printf 'frontier_alert_triage_last_duration_seconds %s\n' "$run_seconds"
        fi
        if finished_at=$(report_field finished_at); then
            echo '# HELP frontier_alert_triage_last_run_timestamp_seconds Unix time the last alert-triage run finished.'
            echo '# TYPE frontier_alert_triage_last_run_timestamp_seconds gauge'
            printf 'frontier_alert_triage_last_run_timestamp_seconds %s\n' "$finished_at"
        fi
    fi

    # Покрытие кластеризации: какая доля ПОДХОДЯЩИХ постов в окне вообще попала
    # в семантический кластер. Свежести мало — она отвечает на вопрос «слой
    # шевелится?», но не на «сколько корпуса он видит». 04.08.2026 у disruption
    # свежесть была в норме (прогон каждую ночь), при этом кластеризация видела
    # 40% окна: выборка берёт N самых свежих постов, а поток вдвое больше N.
    # Окно у каждого воркспейса своё: код берёт max(semantic_window, trend_window).
    echo '# HELP frontier_clustering_coverage_ratio Доля подходящих постов окна, попавших в семантический кластер.'
    echo '# TYPE frontier_clustering_coverage_ratio gauge'
    echo '# HELP frontier_clustering_eligible_posts Подходящих постов в окне кластеризации.'
    echo '# TYPE frontier_clustering_eligible_posts gauge'
    echo '# HELP frontier_clustering_window_days Окно выборки: max(semantic_cluster_window_days, trend_cluster_window_days).'
    echo '# TYPE frontier_clustering_window_days gauge'
    echo '# HELP frontier_clustering_max_posts Потолок выборки semantic_cluster_max_posts у воркспейса.'
    echo '# TYPE frontier_clustering_max_posts gauge'

    # Потолок и окно экспортируются рядом с покрытием намеренно. Покрытие —
    # ПИЛА: знаменатель (подходящие посты окна) растёт непрерывно, числитель
    # прыгает раз в сутки после ночного прогона, поэтому внутри суток серия
    # всегда убывает. Строить на ней алерт «покрытие падает» нельзя — он
    # срабатывает на здоровом процессе (проверено 04.08.2026: правило встало
    # в pending через 15 минут после появления метрики). Настоящий инвариант
    # проверяется этими двумя числами: если суточный приток больше потолка,
    # выборка «N самых свежих» физически не может охватить всё.
    docker exec "$PG_CONTAINER" psql -U "${POSTGRES_USER:-frontier}" -d "${POSTGRES_DB:-frontier}" \
        -At -F'|' -c "
            WITH cfg AS (
              SELECT w.id,
                     GREATEST(
                       COALESCE((w.extra->'cluster_analysis'->>'semantic_cluster_window_days')::int, 7),
                       COALESCE((w.extra->'cluster_analysis'->>'trend_cluster_window_days')::int, 30)
                     ) AS window_days,
                     COALESCE((w.extra->'cluster_analysis'->>'semantic_cluster_max_posts')::int, 400)
                       AS max_posts
                FROM workspaces w WHERE w.is_active
            )
            SELECT c.id, c.window_days, c.max_posts,
                   count(p.id),
                   count(p.id) FILTER (WHERE COALESCE(p.semantic_cluster_id,'') <> '')
              FROM cfg c
              LEFT JOIN posts p
                ON p.workspace_id = c.id
               AND p.published_at IS NOT NULL
               AND COALESCE(p.relevance_score,0) >= 0.6
               AND p.published_at > now() - make_interval(days => c.window_days)
               AND EXISTS (SELECT 1 FROM indexing_status i
                            WHERE i.post_id = p.id AND i.embedding_status = 'done')
             GROUP BY c.id, c.window_days, c.max_posts
        " </dev/null | while IFS='|' read -r ws window_days max_posts eligible clustered; do
        if [ -z "${ws:-}" ] || [ "${eligible:-0}" = "0" ]; then
            continue
        fi
        printf 'frontier_clustering_window_days{workspace="%s"} %s\n' "$ws" "$window_days"
        printf 'frontier_clustering_max_posts{workspace="%s"} %s\n' "$ws" "$max_posts"
        printf 'frontier_clustering_eligible_posts{workspace="%s"} %s\n' "$ws" "$eligible"
        printf 'frontier_clustering_coverage_ratio{workspace="%s"} %s\n' \
            "$ws" "$(awk -v c="$clustered" -v e="$eligible" 'BEGIN{printf "%.4f", c/e}')"
    done
}

# Пишем атомарно: textfile collector читает каталог на каждом скрейпе и на
# частично записанном файле выдал бы parse error вместо метрики.
emit > "$TMP"
mv "$TMP" "$OUT"
chmod 0644 "$OUT"
