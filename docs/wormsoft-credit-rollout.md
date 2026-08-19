# Wormsoft credit accounting — rollout

> Статус 19.08.2026: актуально для тарифа Payed `3 000 000 credits / 4h`.

## Что произошло 19.08

За четыре часа к моменту алерта Prometheus видел примерно `1 783 793` input и `534 330`
output token для `agent/medium`, плюс небольшой vision-трафик. Старый guard сложил tokens и
получил около `2.32M` «credits». По подтверждённым ставкам фактическая оценка — около
`589 083 credits`, то есть `19.64%` пакета. В окне оставалось примерно `2.411M credits`, а
локальный soft-cap уже отправлял кандидатов в fallback.

Число `77.288` в Telegram — не credits и не процент: это Prometheus `increase()` для
счётчика локальных throttle events, экстраполированный между scrape-точками. Фактически было
76 отклонённых Wormsoft-кандидатов; они ушли в Polza fallback без зафиксированных provider errors.
Сообщение `RESOLVED` сохранило последнее вычисленное значение annotation (`21.356`); это не
остаток credits и не новая ошибка после восстановления.

## Что изменилось

Guard больше не считает `1 token = 1 credit`. Расход считается по requested billing alias:

```text
non_cached_input = max(prompt_tokens - cached_prompt_tokens, 0)

credits =
    non_cached_input × input_rate
  + cached_prompt_tokens × cache_rate
  + completion_tokens × output_rate
```

Snapshot ставок от 19.08.2026 собран из authenticated
`/api/money/token-pricing` для subscription aliases и из public
`/api/public/pricing` для прямых моделей, которых нет в account snapshot. Эти источники
нельзя безусловно взаимозаменять: публичные ставки aliases отличаются от ставок аккаунта.

События расхода хранятся в точном trailing window в Redis sorted set:

```text
llm:budget:credit_window:v2:wormsoft:14400
llm:budget:credit_window:v2:wormsoft:14400:total
```

ZSET хранит события, а `:total` — атомарно обновляемую Lua-скриптом сумму. Поэтому каждый
запрос не вычитывает всё четырёхчасовое окно, и параллельные записи не теряют credits.

Старые `llm:budget:credit_window:wormsoft:*` намеренно не мигрируются: там находятся
raw tokens, ошибочно записанные как credits.

## Почему rollout двухступенчатый

Новый namespace после выкладки пуст. Одновременно Wormsoft не публикует API с live remaining
credits и точным временем следующего начисления. Поэтому сначала нужно наполнить новый счётчик
без enforcement и сверить его с кабинетом.

Отключённый `WORMSOFT_CREDIT_THROTTLE_ENABLED` теперь отключает только блокировку. Учёт,
FinOps и метрики продолжают работать — это штатный shadow-режим rollout.

## Этап 1 — shadow accounting

На сервере сохранить:

```dotenv
WORMSOFT_CREDIT_THROTTLE_ENABLED=false
WORMSOFT_CREDIT_WINDOW_LIMIT=3000000
WORMSOFT_CREDIT_WINDOW_SECONDS=14400
WORMSOFT_CREDIT_SOFT_CAP_RATIO=0.8
WORMSOFT_CREDIT_HARD_CAP_RATIO=0.95
WORMSOFT_CREDIT_SOFT_CAP_SHADOW_RATIO=0.7
WORMSOFT_CREDIT_FAIL_CLOSED=false
```

После sync/build нужно **recreate**, а не только `restart`: Settings читаются при старте
контейнера, а routing hot-reload эти поля не обновляет.

Проверить минимум одно полное четырёхчасовое окно:

```promql
max(
  frontier_wormsoft_credit_utilization_ratio
  and on(service)
  (time() - frontier_wormsoft_credit_window_refresh_timestamp_seconds < 300)
)
```

```promql
max(
  frontier_wormsoft_credit_window_usage
  and on(service)
  (time() - frontier_wormsoft_credit_window_refresh_timestamp_seconds < 300)
)
```

```promql
sum by (task, kind) (
  increase(frontier_wormsoft_credits_estimated_total{
    kind!="total"
  }[4h])
)
```

```promql
sum(increase(frontier_rate_limit_events_total{upstream="wormsoft"}[4h]))
```

Сверить с кабинетом Wormsoft:

- изменение credits за тот же интервал;
- фактическую фазу четырёхчасового начисления;
- requested alias против фактически возвращённой underlying model;
- отсутствие 429/402 и неожиданного Polza spillover.

Не использовать общий `summary.actual_cost_total` в legacy FinOps для сравнения провайдеров:
там пока смешиваются Wormsoft credits и raw token units других adapters. Для этого rollout
источник правды — Wormsoft window/credit metrics выше и отдельный объём Polza fallbacks.

## Этап 2 — enforcement

После успешной сверки:

```dotenv
WORMSOFT_CREDIT_THROTTLE_ENABLED=true
```

Снова recreate `worker`, `mcp` и `admin`. Primary traffic продолжает работать после раннего
порога `0.8` и блокируется только на hard cap `0.95`; shadow traffic отсекается на `0.7`, чтобы
не съедать остаток пакета. Пять процентов (150 000 credits) оставлены как запас на in-flight
ответы. Не поднимать hard cap, пока нет атомарной pre-call reservation.

После sync новых правил перечитать Prometheus и проверить, что они загружены:

```bash
curl -fsS -X POST http://127.0.0.1:9090/-/reload
curl -fsS http://127.0.0.1:9090/api/v1/rules | grep -F FrontierWormsoftCreditUtilizationHigh
```

## Rollback

Если estimator расходится с кабинетом или растут ошибки:

```dotenv
WORMSOFT_CREDIT_THROTTLE_ENABLED=false
```

Recreate сервисы. Учёт продолжится, но guard перестанет отклонять Wormsoft-кандидатов.

Не компенсировать расхождение умножением `WINDOW_LIMIT`: коэффициент зависит от модели и
input/output mix. Для `agent/medium` raw-token guard завышал расход, а для дорогого output
`agent/high` может, наоборот, занижать.

## Известная граница

Локальный guard использует настоящий trailing 4h window. Если кабинет подтвердит, что credits
не расходуются как rolling allowance, а жёстко сбрасываются/начисляются в фиксированную фазу,
нужно добавить подтверждённый `window_offset` либо учитывать published reset timestamp. До такой
сверки trailing window остаётся более консервативным вариантом.

Ставки — датированный snapshot, а не договорённость API навсегда. Перед включением enforcement
нужно сравнить их с account/public pricing; затем алерт свежести admin snapshot контролирует
доступность источника, но автоматический hot-reload ставок пока не реализован.

Запись фактических credits после ответа атомарна, но guard пока не резервирует будущую стоимость
in-flight запроса до отправки в Wormsoft. Поэтому primary hard cap пока оставлен на `0.95`:
150 000 credits — консервативный запас на параллельные ответы. Поднимать его выше стоит только
после измерения максимального in-flight burst или реализации reserve/commit/release.
