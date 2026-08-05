import logging

logger = logging.getLogger(__name__)

try:
    from prometheus_client import Counter, Gauge, disable_created_metrics, start_http_server

    disable_created_metrics()

    TELEGRAM_CLIENT_RESETS_TOTAL = Counter(
        "frontier_telegram_client_resets_total",
        "Total Telegram client resets after stale or transport/runtime failures.",
        ["service", "cause"],
    )
    CRAWL_SESSION_RECREATES_TOTAL = Counter(
        "frontier_crawl_session_recreates_total",
        "Total crawl HTTP session recreations.",
        ["service", "cause"],
    )
    RATE_LIMIT_EVENTS_TOTAL = Counter(
        "frontier_rate_limit_events_total",
        "Total upstream rate limit events.",
        ["service", "upstream", "operation"],
    )
    SEARXNG_REQUESTS_TOTAL = Counter(
        "frontier_searxng_requests_total",
        "Total SearXNG requests.",
        ["service", "mode", "status"],
    )
    NOVELTY_JUDGE_TOTAL = Counter(
        "frontier_novelty_judge_total",
        "Cross-family novelty-judge verdicts on weak candidates (RSI contour B).",
        ["service", "verdict"],
    )
    RELEVANCE_AUDIT_GAUGE = Gauge(
        "frontier_relevance_audit",
        "Relevance-filter false-negative audit metrics (RSI contour C).",
        ["service", "workspace", "metric"],
    )
    GRAPH_HEALTH_GAUGE = Gauge(
        "frontier_graph_health",
        "Neo4j concept-graph health metrics (RSI contour D).",
        ["service", "workspace", "metric"],
    )
    # Исход каждого прогона джоба планировщика. Три метрики выше выставляются
    # ВНУТРИ дочернего процесса (admin.backend.manual_jobs), у которого свой
    # REGISTRY и никакого HTTP-сервера, — поэтому в экспозиции admin они
    # присутствовали именами и не отдавали ни одного сэмпла, а два алерта на них
    # физически не могли сработать. Родитель теперь перепубликовывает их из JSON
    # ребёнка, и вместе с этим считает сами прогоны: провал ребёнка иначе не виден
    # в метриках вовсе (manual_jobs при исключении пишет в stderr и возвращает 1,
    # то есть на провальных прогонах перепубликовывать просто нечего).
    ADMIN_JOB_RUNS_TOTAL = Counter(
        "frontier_admin_job_runs_total",
        "Scheduler job subprocess runs by outcome.",
        ["service", "job", "outcome"],
    )
    # Единственный счётчик СОБСТВЕННЫХ стадий конвейера. Одно имя с меткой `stage`,
    # а не восемь отдельных: кардинальность ~6 воркспейсов x 6 стадий x 5 исходов
    # ≈ 180 серий, что для этого стека пренебрежимо, зато разрез строится одним
    # запросом и новая стадия не требует нового имени.
    #
    # Зачем вообще: телеметрия проекта была построена вокруг ВНЕШНИХ зависимостей
    # (LLM-провайдеры, Redis Streams, S3), а собственные стадии не считал никто.
    # Статус писался только в PostgreSQL (indexing_status.embedding_status), то есть
    # СОСТОЯНИЕ можно было посчитать задним числом SQL-запросом, а ПОТОКА не
    # существовало: ни rate(), ни доли дропа, ни всплеска ошибок. 04.08.2026 на
    # 190 977 done приходилось 122 600 dropped, а 29 из 32 ошибок за всю историю
    # случились в последние сутки — и этого не видел ни один дашборд.
    PIPELINE_STAGE_TOTAL = Counter(
        "frontier_pipeline_stage_total",
        "Pipeline stage outcomes for our own processing steps.",
        ["service", "stage", "workspace", "outcome"],
    )
    # Исходы краула отдельным семейством, а не меткой stage у счётчика выше:
    # у краула своя ось `reason` из девяти значений, и смешивать её с общей
    # разбивкой стадий значило бы либо плодить пустые комбинации, либо потерять
    # причину отказа. Границу между семействами держит тест.
    #
    # До 2026-08-05 все неуспехи краула были тупиком: `enrich_url` возвращает
    # None на девяти разных развилках, вызывающий пишет log.warning и делает XACK.
    # Отличить «у поста нечего краулить» от «источник отдал 403» было нельзя ни
    # из данных, ни из метрик. За сутки: 1403 успеха против 1497 неуспехов, и
    # класс `timeout` (399) в прежних замерах не считали вовсе — из-за этого
    # доля отказов гуляла между 41% и 35% от замера к замеру.
    CRAWL_OUTCOMES_TOTAL = Counter(
        "frontier_crawl_outcomes_total",
        "crawl4ai fetch outcomes by reason.",
        ["service", "outcome", "reason"],
    )
    LAST_POST_AGE_SECONDS = Gauge(
        "frontier_last_post_age_seconds",
        "Age in seconds of the freshest post per workspace (data-silence detector).",
        ["service", "workspace"],
    )
    SOURCE_FRESHNESS_HOURS = Gauge(
        "frontier_source_freshness_hours",
        "Hours since the newest post per enabled source (silent-stale-feed detector; "
        "catches feeds that respond green but serve only ancient content).",
        ["service", "workspace", "source_id", "source_name", "source_type"],
    )
    LLM_PROMPT_TOKENS_TOTAL = Counter(
        "frontier_llm_prompt_tokens_total",
        "Total prompt tokens reported by LLM providers.",
        ["service", "task", "provider", "requested_model", "actual_model"],
    )
    LLM_COMPLETION_TOKENS_TOTAL = Counter(
        "frontier_llm_completion_tokens_total",
        "Total completion tokens reported by LLM providers.",
        ["service", "task", "provider", "requested_model", "actual_model"],
    )
    LLM_BILLABLE_TOKENS_TOTAL = Counter(
        "frontier_llm_billable_tokens_total",
        "Total billable tokens reported by LLM providers.",
        ["service", "task", "provider", "requested_model", "actual_model"],
    )
    LLM_REQUESTS_TOTAL = Counter(
        "frontier_llm_requests_total",
        "Total LLM requests across providers.",
        ["service", "task", "provider", "requested_model", "actual_model", "status"],
    )
    LLM_FALLBACKS_TOTAL = Counter(
        "frontier_llm_fallbacks_total",
        "Total LLM provider/model fallbacks.",
        [
            "service",
            "task",
            "from_provider",
            "from_requested_model",
            "from_actual_model",
            "to_provider",
            "to_model",
            "reason",
        ],
    )
    LLM_THROTTLE_EVENTS_TOTAL = Counter(
        "frontier_llm_throttle_events_total",
        "Total local runtime throttle events by provider and reason.",
        ["service", "provider", "reason"],
    )
    LLM_COST_ESTIMATE_TOTAL = Counter(
        "frontier_llm_cost_estimate_total",
        "Cumulative estimated LLM cost by provider and task family.",
        ["service", "provider", "task_family", "execution_role"],
    )
    LLM_COST_ACTUAL_TOTAL = Counter(
        "frontier_llm_cost_actual_total",
        "Cumulative actual LLM cost by provider and task family.",
        ["service", "provider", "task_family", "execution_role"],
    )
    LLM_COST_DRIFT_TOTAL = Gauge(
        "frontier_llm_cost_drift_total",
        "Cumulative actual-minus-estimated LLM cost drift by provider and task family.",
        ["service", "provider", "task_family", "execution_role"],
    )
    LLM_FINOPS_RUNTIME_ACTUAL_TOTAL = Gauge(
        "frontier_llm_finops_runtime_actual_total",
        "Current runtime actual LLM cost total by provider from admin finops reconciliation.",
        ["service", "provider"],
    )
    LLM_FINOPS_RUNTIME_ESTIMATED_TOTAL = Gauge(
        "frontier_llm_finops_runtime_estimated_total",
        "Current runtime estimated LLM cost total by provider from admin finops reconciliation.",
        ["service", "provider"],
    )
    LLM_FINOPS_RUNTIME_DRIFT_TOTAL = Gauge(
        "frontier_llm_finops_runtime_drift_total",
        "Current runtime cost drift total by provider from admin finops reconciliation.",
        ["service", "provider"],
    )
    LLM_FINOPS_PUBLISHED_REMAINING = Gauge(
        "frontier_llm_finops_published_remaining",
        "Published remaining budget or balance by provider when available.",
        ["service", "provider"],
    )
    LLM_FINOPS_RECONCILIATION_GAP = Gauge(
        "frontier_llm_finops_reconciliation_gap",
        "Gap between runtime and published provider accounting where units are comparable.",
        ["service", "provider", "kind"],
    )
    LLM_FINOPS_STATUS = Gauge(
        "frontier_llm_finops_status",
        "One-hot finops reconciliation status by provider.",
        ["service", "provider", "status"],
    )
    GIGACHAT_PROMPT_TOKENS_TOTAL = Counter(
        "frontier_gigachat_prompt_tokens_total",
        "Total prompt tokens reported by GigaChat.",
        ["service", "task", "model"],
    )
    GIGACHAT_COMPLETION_TOKENS_TOTAL = Counter(
        "frontier_gigachat_completion_tokens_total",
        "Total completion tokens reported by GigaChat.",
        ["service", "task", "model"],
    )
    GIGACHAT_PRECACHED_PROMPT_TOKENS_TOTAL = Counter(
        "frontier_gigachat_precached_prompt_tokens_total",
        "Total cached prompt tokens reported by GigaChat.",
        ["service", "task", "model"],
    )
    GIGACHAT_BILLABLE_TOKENS_TOTAL = Counter(
        "frontier_gigachat_billable_tokens_total",
        "Total billable tokens reported by GigaChat.",
        ["service", "task", "model"],
    )
    GIGACHAT_REQUESTS_TOTAL = Counter(
        "frontier_gigachat_requests_total",
        "Total GigaChat requests.",
        ["service", "task", "model", "status"],
    )
    GIGACHAT_ESCALATIONS_TOTAL = Counter(
        "frontier_gigachat_escalations_total",
        "Total GigaChat model escalations.",
        ["service", "task", "from_model", "to_model"],
    )
    GIGACHAT_BALANCE_TOKENS = Gauge(
        "frontier_gigachat_balance_tokens",
        "Remaining GigaChat package token balance by usage/model family.",
        ["service", "usage"],
    )
    GIGACHAT_BALANCE_REFRESH_TIMESTAMP = Gauge(
        "frontier_gigachat_balance_refresh_timestamp_seconds",
        "Unix timestamp of the last successful GigaChat balance refresh.",
        ["service"],
    )
    OPENROUTER_VISION_REQUESTS_TOTAL = Counter(
        "frontier_openrouter_vision_requests_total",
        "Total OpenRouter vision requests.",
        ["service", "status"],
    )
    OPENROUTER_VISION_FALLBACKS_TOTAL = Counter(
        "frontier_openrouter_vision_fallbacks_total",
        "Total OpenRouter vision fallbacks to secondary providers.",
        ["service", "to_provider", "reason"],
    )
    OPENROUTER_VISION_QUARANTINE = Gauge(
        "frontier_openrouter_vision_quarantine",
        "Whether OpenRouter free vision is currently quarantined.",
        ["service"],
    )
    OPENROUTER_VISION_RPD_USED = Gauge(
        "frontier_openrouter_vision_rpd_used",
        "Reserved OpenRouter free vision requests for the current UTC day.",
        ["service"],
    )
    POLZA_VISION_REQUESTS_TOTAL = Counter(
        "frontier_polza_vision_requests_total",
        "Total Polza vision requests.",
        ["service", "status"],
    )
    OPENROUTER_CATALOG_AVAILABLE = Gauge(
        "frontier_openrouter_catalog_available",
        "Whether the latest OpenRouter catalog snapshot is usable.",
        ["service"],
    )
    OPENROUTER_CATALOG_REFRESH_TIMESTAMP = Gauge(
        "frontier_openrouter_catalog_refresh_timestamp_seconds",
        "Unix timestamp of the latest OpenRouter catalog snapshot.",
        ["service"],
    )
    OPENROUTER_CATALOG_MODEL_COUNT = Gauge(
        "frontier_openrouter_catalog_model_count",
        "Count of OpenRouter free models grouped by capability snapshot.",
        ["service", "kind"],
    )
    OPENROUTER_KEY_AVAILABLE = Gauge(
        "frontier_openrouter_key_available",
        "Whether the latest OpenRouter key snapshot is usable.",
        ["service"],
    )
    OPENROUTER_KEY_REFRESH_TIMESTAMP = Gauge(
        "frontier_openrouter_key_refresh_timestamp_seconds",
        "Unix timestamp of the latest OpenRouter key snapshot.",
        ["service"],
    )
    OPENROUTER_KEY_LIMIT_REMAINING = Gauge(
        "frontier_openrouter_key_limit_remaining",
        "Remaining OpenRouter credit limit reported by the current key.",
        ["service"],
    )
    OPENROUTER_CREDIT_BALANCE = Gauge(
        "frontier_openrouter_credit_balance",
        "OpenRouter account credit balance in USD (total_credits - total_usage from /credits).",
        ["service"],
    )
    OPENROUTER_KEY_USAGE_DAILY = Gauge(
        "frontier_openrouter_key_usage_daily",
        "Current UTC-day usage reported by the OpenRouter key endpoint.",
        ["service", "kind"],
    )
    OPENROUTER_KEY_FREE_TIER = Gauge(
        "frontier_openrouter_key_free_tier",
        "Whether the current OpenRouter key is still considered free tier.",
        ["service"],
    )
    OPENROUTER_HEALTH_SUCCESS_RATE = Gauge(
        "frontier_openrouter_health_success_rate",
        "Rolling OpenRouter model health success rate.",
        ["service", "model_id"],
    )
    OPENROUTER_HEALTH_LATENCY_MS = Gauge(
        "frontier_openrouter_health_latency_ms",
        "Rolling OpenRouter model latency in milliseconds.",
        ["service", "model_id"],
    )
    OPENROUTER_MODEL_QUARANTINE = Gauge(
        "frontier_openrouter_model_quarantine",
        "Whether an OpenRouter model is currently quarantined.",
        ["service", "model_id"],
    )
    OPENROUTER_PICKER_DECISIONS_TOTAL = Counter(
        "frontier_openrouter_picker_decisions_total",
        "Total OpenRouter picker decisions by task family and model.",
        ["service", "task_family", "model_id"],
    )
    OPENROUTER_PICKER_SKIP_TOTAL = Counter(
        "frontier_openrouter_picker_skip_total",
        "Total OpenRouter picker skips by task family and reason.",
        ["service", "task_family", "reason"],
    )
    OPENROUTER_RPD_USED = Gauge(
        "frontier_openrouter_rpd_used",
        "Reserved OpenRouter requests per model for the current UTC day.",
        ["service", "model_id"],
    )
    OPENROUTER_RPM_USED = Gauge(
        "frontier_openrouter_rpm_used",
        "Reserved OpenRouter requests per model for the current UTC minute.",
        ["service", "model_id"],
    )
    WORMSOFT_LIMITS_AVAILABLE = Gauge(
        "frontier_wormsoft_limits_available",
        "Whether the last Wormsoft limits/pricing refresh completed successfully.",
        ["service"],
    )
    WORMSOFT_LIMITS_REFRESH_TIMESTAMP = Gauge(
        "frontier_wormsoft_limits_refresh_timestamp_seconds",
        "Unix timestamp of the last successful Wormsoft limits/pricing refresh.",
        ["service"],
    )
    WORMSOFT_SUBSCRIPTION_LIMIT_CREDITS = Gauge(
        "frontier_wormsoft_subscription_limit_credits",
        "Wormsoft subscription credit allowance per refresh window.",
        ["service", "plan"],
    )
    WORMSOFT_SUBSCRIPTION_WINDOW_SECONDS = Gauge(
        "frontier_wormsoft_subscription_window_seconds",
        "Wormsoft subscription refresh window in seconds.",
        ["service", "plan"],
    )
    WORMSOFT_SUBSCRIPTION_PRICE_RUB = Gauge(
        "frontier_wormsoft_subscription_price_rub",
        "Wormsoft subscription price in RUB per billing period.",
        ["service", "plan"],
    )
    WORMSOFT_MODEL_PRICE_CREDITS_PER_MILLION = Gauge(
        "frontier_wormsoft_model_price_credits_per_million",
        "Wormsoft model pricing in credits per 1M tokens.",
        ["service", "model", "kind"],
    )
    ADMIN_SCHEDULER_RUNNING = Gauge(
        "frontier_admin_scheduler_running",
        "Whether the admin APScheduler is running.",
        ["service"],
    )
    ADMIN_MANUAL_JOBS_RUNNING = Gauge(
        "frontier_admin_manual_jobs_running",
        "Count of running admin manual jobs by job name.",
        ["service", "job_name"],
    )
    ADMIN_MANUAL_JOB_OLDEST_RUNNING_AGE_SECONDS = Gauge(
        "frontier_admin_manual_job_oldest_running_age_seconds",
        "Age of the oldest running admin manual job in seconds.",
        ["service", "job_name"],
    )
    ADMIN_MANUAL_JOBS_RECENT_FAILURES = Gauge(
        "frontier_admin_manual_jobs_recent_failures",
        "Count of admin manual job failures in the recent observation window.",
        ["service", "job_name"],
    )
    REDIS_STREAM_LAG = Gauge(
        "frontier_redis_stream_lag",
        "Redis stream consumer-group lag reported by XINFO GROUPS.",
        ["service", "stream", "group"],
    )
    REDIS_STREAM_PENDING = Gauge(
        "frontier_redis_stream_pending",
        "Redis stream pending message count reported by XPENDING/XINFO GROUPS.",
        ["service", "stream", "group"],
    )
    REDIS_STREAM_OLDEST_PENDING_AGE_SECONDS = Gauge(
        "frontier_redis_stream_oldest_pending_age_seconds",
        "Age of the oldest pending Redis stream message in seconds.",
        ["service", "stream", "group"],
    )
    REDIS_STREAM_CONSUMER_PENDING = Gauge(
        "frontier_redis_stream_consumer_pending",
        "Redis stream pending count per consumer.",
        ["service", "stream", "group", "consumer"],
    )
    REDIS_STREAM_CONSUMER_IDLE_SECONDS = Gauge(
        "frontier_redis_stream_consumer_idle_seconds",
        "Redis stream consumer idle age in seconds.",
        ["service", "stream", "group", "consumer"],
    )
    # DLQ объявлена в коде с апреля 2026 и до сих пор не наблюдалась ничем:
    # ключа в Redis нет (poison-сообщений пока не случалось), и «пусто» было
    # неотличимо от «механизм сломан». Серия обязана существовать со значением 0.
    REDIS_DLQ_LENGTH = Gauge(
        "frontier_redis_dlq_length",
        "Length of a dead-letter stream; zero is published explicitly for a missing key.",
        ["service", "stream"],
    )
    # Число consumer-групп у стрима. Ноль при растущем entries-added — продюсер,
    # пишущий в пустоту: события вытесняются триммингом непрочитанными, а lag и
    # pending при этом нулевые, потому что отставать нечему.
    REDIS_STREAM_GROUPS = Gauge(
        "frontier_redis_stream_groups",
        "Number of consumer groups attached to a Redis stream.",
        ["service", "stream"],
    )
    REDIS_STREAM_ENTRIES_ADDED = Gauge(
        "frontier_redis_stream_entries_added",
        "Total entries ever added to a Redis stream (XINFO STREAM entries-added).",
        ["service", "stream"],
    )
    # 1, если MAXLEN срезал записи ДО того, как группа успела их прочитать.
    # Именно этот класс потери не виден ни по lag, ни по pending.
    REDIS_STREAM_DELIVERY_GAP = Gauge(
        "frontier_redis_stream_delivery_gap",
        "1 when trimming removed entries a consumer group had not delivered yet.",
        ["service", "stream", "group"],
    )
    _PROMETHEUS_AVAILABLE = True
except Exception:  # pragma: no cover - fallback for environments without dependency
    _PROMETHEUS_AVAILABLE = False
    TELEGRAM_CLIENT_RESETS_TOTAL = None
    CRAWL_SESSION_RECREATES_TOTAL = None
    RATE_LIMIT_EVENTS_TOTAL = None
    SEARXNG_REQUESTS_TOTAL = None
    NOVELTY_JUDGE_TOTAL = None
    RELEVANCE_AUDIT_GAUGE = None
    GRAPH_HEALTH_GAUGE = None
    ADMIN_JOB_RUNS_TOTAL = None
    PIPELINE_STAGE_TOTAL = None
    CRAWL_OUTCOMES_TOTAL = None
    LAST_POST_AGE_SECONDS = None
    SOURCE_FRESHNESS_HOURS = None
    LLM_PROMPT_TOKENS_TOTAL = None
    LLM_COMPLETION_TOKENS_TOTAL = None
    LLM_BILLABLE_TOKENS_TOTAL = None
    LLM_REQUESTS_TOTAL = None
    LLM_FALLBACKS_TOTAL = None
    LLM_THROTTLE_EVENTS_TOTAL = None
    LLM_COST_ESTIMATE_TOTAL = None
    LLM_COST_ACTUAL_TOTAL = None
    LLM_COST_DRIFT_TOTAL = None
    LLM_FINOPS_RUNTIME_ACTUAL_TOTAL = None
    LLM_FINOPS_RUNTIME_ESTIMATED_TOTAL = None
    LLM_FINOPS_RUNTIME_DRIFT_TOTAL = None
    LLM_FINOPS_PUBLISHED_REMAINING = None
    LLM_FINOPS_RECONCILIATION_GAP = None
    LLM_FINOPS_STATUS = None
    GIGACHAT_PROMPT_TOKENS_TOTAL = None
    GIGACHAT_COMPLETION_TOKENS_TOTAL = None
    GIGACHAT_PRECACHED_PROMPT_TOKENS_TOTAL = None
    GIGACHAT_BILLABLE_TOKENS_TOTAL = None
    GIGACHAT_REQUESTS_TOTAL = None
    GIGACHAT_ESCALATIONS_TOTAL = None
    GIGACHAT_BALANCE_TOKENS = None
    GIGACHAT_BALANCE_REFRESH_TIMESTAMP = None
    OPENROUTER_VISION_REQUESTS_TOTAL = None
    OPENROUTER_VISION_FALLBACKS_TOTAL = None
    OPENROUTER_VISION_QUARANTINE = None
    OPENROUTER_VISION_RPD_USED = None
    POLZA_VISION_REQUESTS_TOTAL = None
    OPENROUTER_CATALOG_AVAILABLE = None
    OPENROUTER_CATALOG_REFRESH_TIMESTAMP = None
    OPENROUTER_CATALOG_MODEL_COUNT = None
    OPENROUTER_KEY_AVAILABLE = None
    OPENROUTER_KEY_REFRESH_TIMESTAMP = None
    OPENROUTER_KEY_LIMIT_REMAINING = None
    OPENROUTER_CREDIT_BALANCE = None
    OPENROUTER_KEY_USAGE_DAILY = None
    OPENROUTER_KEY_FREE_TIER = None
    OPENROUTER_HEALTH_SUCCESS_RATE = None
    OPENROUTER_HEALTH_LATENCY_MS = None
    OPENROUTER_MODEL_QUARANTINE = None
    OPENROUTER_PICKER_DECISIONS_TOTAL = None
    OPENROUTER_PICKER_SKIP_TOTAL = None
    OPENROUTER_RPD_USED = None
    OPENROUTER_RPM_USED = None
    WORMSOFT_LIMITS_AVAILABLE = None
    WORMSOFT_LIMITS_REFRESH_TIMESTAMP = None
    WORMSOFT_SUBSCRIPTION_LIMIT_CREDITS = None
    WORMSOFT_SUBSCRIPTION_WINDOW_SECONDS = None
    WORMSOFT_SUBSCRIPTION_PRICE_RUB = None
    WORMSOFT_MODEL_PRICE_CREDITS_PER_MILLION = None
    ADMIN_SCHEDULER_RUNNING = None
    ADMIN_MANUAL_JOBS_RUNNING = None
    ADMIN_MANUAL_JOB_OLDEST_RUNNING_AGE_SECONDS = None
    ADMIN_MANUAL_JOBS_RECENT_FAILURES = None
    REDIS_STREAM_LAG = None
    REDIS_STREAM_PENDING = None
    REDIS_STREAM_OLDEST_PENDING_AGE_SECONDS = None
    REDIS_STREAM_CONSUMER_PENDING = None
    REDIS_STREAM_CONSUMER_IDLE_SECONDS = None
    REDIS_DLQ_LENGTH = None
    REDIS_STREAM_GROUPS = None
    REDIS_STREAM_ENTRIES_ADDED = None
    REDIS_STREAM_DELIVERY_GAP = None


def start_metrics_server(port: int) -> None:
    if not _PROMETHEUS_AVAILABLE:
        logger.warning("Prometheus client not available; metrics server disabled")
        return
    start_http_server(port)
    logger.info("Prometheus metrics server started on port %d", port)


def note_telegram_client_reset(service: str, cause: str) -> None:
    if TELEGRAM_CLIENT_RESETS_TOTAL is not None:
        TELEGRAM_CLIENT_RESETS_TOTAL.labels(service=service, cause=cause).inc()


def note_crawl_session_recreate(service: str, cause: str) -> None:
    if CRAWL_SESSION_RECREATES_TOTAL is not None:
        CRAWL_SESSION_RECREATES_TOTAL.labels(service=service, cause=cause).inc()


def note_rate_limit_event(service: str, upstream: str, operation: str) -> None:
    if RATE_LIMIT_EVENTS_TOTAL is not None:
        RATE_LIMIT_EVENTS_TOTAL.labels(
            service=service, upstream=upstream, operation=operation
        ).inc()


def note_novelty_judge(service: str, verdict: str, count: int = 1) -> None:
    """Отметить вердикт(ы) novelty-судьи.

    `count` нужен родительскому процессу: он перепубликовывает итог дочернего
    прогона одним числом, а не по одному вердикту за раз. Инкремент нулём
    пропускается — Counter от этого не появится в экспозиции, но и лишней серии
    с нулём не создаст.
    """
    if NOVELTY_JUDGE_TOTAL is not None and count > 0:
        NOVELTY_JUDGE_TOTAL.labels(service=service, verdict=verdict).inc(count)


def set_relevance_audit_metric(service: str, workspace: str, metric: str, value: float) -> None:
    if RELEVANCE_AUDIT_GAUGE is not None:
        RELEVANCE_AUDIT_GAUGE.labels(service=service, workspace=workspace, metric=metric).set(value)


def set_graph_health_metric(service: str, workspace: str, metric: str, value: float) -> None:
    if GRAPH_HEALTH_GAUGE is not None:
        GRAPH_HEALTH_GAUGE.labels(service=service, workspace=workspace, metric=metric).set(value)


def note_admin_job_run(job: str, outcome: str, *, service: str = "admin") -> None:
    """Отметить исход прогона джоба планировщика (ok / failed / timeout)."""
    if ADMIN_JOB_RUNS_TOTAL is not None:
        ADMIN_JOB_RUNS_TOTAL.labels(service=service, job=job, outcome=outcome).inc()


def note_crawl_outcome(reason: str, outcome: str = "failed", *, service: str = "crawl4ai") -> None:
    """Отметить исход одной попытки краула.

    `outcome` — крупная категория (`saved` / `empty` / `failed`), `reason` —
    конкретная развилка. Дефолт `failed` намеренный: успешных точек в коде две,
    неуспешных девять, и забытый аргумент должен давать пессимистичный ответ,
    а не оптимистичный.
    """
    if CRAWL_OUTCOMES_TOTAL is not None:
        CRAWL_OUTCOMES_TOTAL.labels(service=service, outcome=outcome, reason=reason).inc()


def note_pipeline_stage(
    service: str, stage: str, outcome: str, workspace: str = "", count: int = 1
) -> None:
    """Отметить исход стадии конвейера.

    Пустой `workspace` намеренно превращается в `unknown`, а не отбрасывается:
    в части точек (например откат до сохранения поста) воркспейс ещё неизвестен,
    и терять там событие целиком хуже, чем потерять разрез по нему. Пустая метка
    выглядела бы в выдаче как отдельный воркспейс с именем «» — `unknown`
    честнее и заметнее.
    """
    if PIPELINE_STAGE_TOTAL is not None and count > 0:
        PIPELINE_STAGE_TOTAL.labels(
            service=service,
            stage=stage,
            workspace=workspace or "unknown",
            outcome=outcome,
        ).inc(count)


def set_last_post_age(service: str, ages_by_workspace: dict[str, float]) -> None:
    if LAST_POST_AGE_SECONDS is None:
        return
    LAST_POST_AGE_SECONDS.clear()
    for workspace, age_seconds in (ages_by_workspace or {}).items():
        workspace_id = str(workspace or "")
        if not workspace_id or age_seconds is None:
            continue
        LAST_POST_AGE_SECONDS.labels(service=service, workspace=workspace_id).set(float(age_seconds))


def set_source_freshness(service: str, rows: list[dict]) -> None:
    """Per-source content freshness in hours. Emits only enabled sources with a known
    newest publish date; clears each refresh so disabled/removed sources drop out."""
    if SOURCE_FRESHNESS_HOURS is None:
        return
    SOURCE_FRESHNESS_HOURS.clear()
    for row in rows or []:
        hours = row.get("freshness_hours")
        if hours is None:
            continue
        SOURCE_FRESHNESS_HOURS.labels(
            service=service,
            workspace=str(row.get("workspace_id") or ""),
            source_id=str(row.get("source_id") or ""),
            source_name=str(row.get("source_name") or ""),
            source_type=str(row.get("source_type") or ""),
        ).set(float(hours))


def note_searxng_request(service: str, mode: str, status: str) -> None:
    if SEARXNG_REQUESTS_TOTAL is not None:
        SEARXNG_REQUESTS_TOTAL.labels(
            service=service,
            mode=mode,
            status=status,
        ).inc()


def note_llm_usage(
    service: str,
    task: str,
    provider: str,
    requested_model: str,
    actual_model: str,
    *,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    billable_tokens: int = 0,
) -> None:
    if LLM_PROMPT_TOKENS_TOTAL is None:
        return
    labels = {
        "service": service,
        "task": task,
        "provider": provider,
        "requested_model": requested_model,
        "actual_model": actual_model,
    }
    LLM_PROMPT_TOKENS_TOTAL.labels(**labels).inc(prompt_tokens)
    LLM_COMPLETION_TOKENS_TOTAL.labels(**labels).inc(completion_tokens)
    LLM_BILLABLE_TOKENS_TOTAL.labels(**labels).inc(billable_tokens)


def note_llm_request(
    service: str,
    task: str,
    provider: str,
    requested_model: str,
    actual_model: str,
    status: str,
) -> None:
    if LLM_REQUESTS_TOTAL is not None:
        LLM_REQUESTS_TOTAL.labels(
            service=service,
            task=task,
            provider=provider,
            requested_model=requested_model,
            actual_model=actual_model,
            status=status,
        ).inc()


def note_llm_fallback(
    service: str,
    task: str,
    *,
    from_provider: str,
    from_requested_model: str,
    from_actual_model: str,
    to_provider: str,
    to_model: str,
    reason: str,
) -> None:
    if LLM_FALLBACKS_TOTAL is not None:
        LLM_FALLBACKS_TOTAL.labels(
            service=service,
            task=task,
            from_provider=from_provider,
            from_requested_model=from_requested_model,
            from_actual_model=from_actual_model,
            to_provider=to_provider,
            to_model=to_model,
            reason=reason,
        ).inc()


def note_llm_throttle_event(service: str, provider: str, reason: str) -> None:
    if LLM_THROTTLE_EVENTS_TOTAL is not None:
        LLM_THROTTLE_EVENTS_TOTAL.labels(
            service=service,
            provider=provider,
            reason=str(reason or "unknown"),
        ).inc()


def note_llm_cost(
    service: str,
    provider: str,
    task_family: str,
    execution_role: str,
    *,
    estimated_cost: float | None = None,
    actual_cost: float | None = None,
    cost_drift: float | None = None,
) -> None:
    labels = {
        "service": service,
        "provider": provider,
        "task_family": task_family,
        "execution_role": execution_role,
    }
    if LLM_COST_ESTIMATE_TOTAL is not None and estimated_cost is not None:
        LLM_COST_ESTIMATE_TOTAL.labels(**labels).inc(float(estimated_cost))
    if LLM_COST_ACTUAL_TOTAL is not None and actual_cost is not None:
        LLM_COST_ACTUAL_TOTAL.labels(**labels).inc(float(actual_cost))
    if LLM_COST_DRIFT_TOTAL is not None and cost_drift is not None:
        LLM_COST_DRIFT_TOTAL.labels(**labels).inc(float(cost_drift))


def set_llm_finops_snapshot(service: str, payload: dict) -> None:
    reconciliations = list(payload.get("reconciliations") or [])
    if LLM_FINOPS_STATUS is not None:
        LLM_FINOPS_STATUS.clear()
    if LLM_FINOPS_RECONCILIATION_GAP is not None:
        LLM_FINOPS_RECONCILIATION_GAP.clear()
    for item in reconciliations:
        provider = str(item.get("provider") or "")
        if not provider:
            continue
        if LLM_FINOPS_RUNTIME_ACTUAL_TOTAL is not None:
            LLM_FINOPS_RUNTIME_ACTUAL_TOTAL.labels(service=service, provider=provider).set(
                float(item.get("runtime_actual_cost_total") or 0.0)
            )
        if LLM_FINOPS_RUNTIME_ESTIMATED_TOTAL is not None:
            LLM_FINOPS_RUNTIME_ESTIMATED_TOTAL.labels(service=service, provider=provider).set(
                float(item.get("runtime_estimated_cost_total") or 0.0)
            )
        if LLM_FINOPS_RUNTIME_DRIFT_TOTAL is not None:
            LLM_FINOPS_RUNTIME_DRIFT_TOTAL.labels(service=service, provider=provider).set(
                float(item.get("runtime_cost_drift_total") or 0.0)
            )
        if LLM_FINOPS_PUBLISHED_REMAINING is not None:
            LLM_FINOPS_PUBLISHED_REMAINING.labels(service=service, provider=provider).set(
                float(item.get("published_remaining") or 0.0)
            )
        gap_kind = str(item.get("gap_kind") or "none")
        if LLM_FINOPS_RECONCILIATION_GAP is not None:
            LLM_FINOPS_RECONCILIATION_GAP.labels(
                service=service,
                provider=provider,
                kind=gap_kind,
            ).set(float(item.get("gap_value") or 0.0))
        status = str(item.get("status") or "unknown")
        if LLM_FINOPS_STATUS is not None:
            LLM_FINOPS_STATUS.labels(
                service=service,
                provider=provider,
                status=status,
            ).set(1)


def note_gigachat_usage(
    service: str,
    task: str,
    model: str,
    *,
    actual_model: str | None = None,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    precached_prompt_tokens: int = 0,
    billable_tokens: int = 0,
) -> None:
    if GIGACHAT_PROMPT_TOKENS_TOTAL is None:
        return
    labels = {"service": service, "task": task, "model": model}
    GIGACHAT_PROMPT_TOKENS_TOTAL.labels(**labels).inc(prompt_tokens)
    GIGACHAT_COMPLETION_TOKENS_TOTAL.labels(**labels).inc(completion_tokens)
    GIGACHAT_PRECACHED_PROMPT_TOKENS_TOTAL.labels(**labels).inc(precached_prompt_tokens)
    GIGACHAT_BILLABLE_TOKENS_TOTAL.labels(**labels).inc(billable_tokens)
    note_llm_usage(
        service,
        task,
        "gigachat",
        model,
        actual_model or model,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        billable_tokens=billable_tokens,
    )


def note_gigachat_request(
    service: str,
    task: str,
    model: str,
    status: str,
    *,
    actual_model: str | None = None,
) -> None:
    if GIGACHAT_REQUESTS_TOTAL is not None:
        GIGACHAT_REQUESTS_TOTAL.labels(
            service=service, task=task, model=model, status=status
        ).inc()
    note_llm_request(service, task, "gigachat", model, actual_model or model, status)


def note_gigachat_escalation(service: str, task: str, from_model: str, to_model: str) -> None:
    if GIGACHAT_ESCALATIONS_TOTAL is not None:
        GIGACHAT_ESCALATIONS_TOTAL.labels(
            service=service, task=task, from_model=from_model, to_model=to_model
        ).inc()


def set_gigachat_balance(service: str, usage: str, value: int) -> None:
    if GIGACHAT_BALANCE_TOKENS is not None:
        GIGACHAT_BALANCE_TOKENS.labels(service=service, usage=usage).set(value)


def note_gigachat_balance_refresh(service: str, timestamp: float) -> None:
    if GIGACHAT_BALANCE_REFRESH_TIMESTAMP is not None:
        GIGACHAT_BALANCE_REFRESH_TIMESTAMP.labels(service=service).set(timestamp)


def note_openrouter_vision_request(service: str, status: str) -> None:
    if OPENROUTER_VISION_REQUESTS_TOTAL is not None:
        OPENROUTER_VISION_REQUESTS_TOTAL.labels(service=service, status=status).inc()


def note_openrouter_vision_fallback(service: str, to_provider: str, reason: str) -> None:
    if OPENROUTER_VISION_FALLBACKS_TOTAL is not None:
        OPENROUTER_VISION_FALLBACKS_TOTAL.labels(
            service=service,
            to_provider=to_provider,
            reason=reason,
        ).inc()


def set_openrouter_vision_quarantine(service: str, is_quarantined: bool) -> None:
    if OPENROUTER_VISION_QUARANTINE is not None:
        OPENROUTER_VISION_QUARANTINE.labels(service=service).set(1 if is_quarantined else 0)


def set_openrouter_vision_rpd_used(service: str, used: int) -> None:
    if OPENROUTER_VISION_RPD_USED is not None:
        OPENROUTER_VISION_RPD_USED.labels(service=service).set(int(used or 0))


def note_polza_vision_request(service: str, status: str) -> None:
    if POLZA_VISION_REQUESTS_TOTAL is not None:
        POLZA_VISION_REQUESTS_TOTAL.labels(service=service, status=status).inc()


def set_openrouter_catalog_snapshot(service: str, payload: dict) -> None:
    if OPENROUTER_CATALOG_AVAILABLE is None:
        return
    models = list(payload.get("models") or [])
    OPENROUTER_CATALOG_AVAILABLE.labels(service=service).set(1 if models else 0)
    fetched_at = payload.get("fetched_at")
    if fetched_at:
        OPENROUTER_CATALOG_REFRESH_TIMESTAMP.labels(service=service).set(float(fetched_at))
    vision_count = sum(1 for model in models if model.get("supports_vision"))
    structured_count = sum(1 for model in models if model.get("supports_structured"))
    tools_count = sum(1 for model in models if model.get("supports_tools"))
    OPENROUTER_CATALOG_MODEL_COUNT.labels(service=service, kind="all").set(len(models))
    OPENROUTER_CATALOG_MODEL_COUNT.labels(service=service, kind="vision").set(vision_count)
    OPENROUTER_CATALOG_MODEL_COUNT.labels(service=service, kind="structured").set(structured_count)
    OPENROUTER_CATALOG_MODEL_COUNT.labels(service=service, kind="tools").set(tools_count)


def set_openrouter_key_snapshot(service: str, payload: dict) -> None:
    if OPENROUTER_KEY_AVAILABLE is None:
        return
    available = bool(payload.get("available"))
    OPENROUTER_KEY_AVAILABLE.labels(service=service).set(1 if available else 0)
    OPENROUTER_KEY_FREE_TIER.labels(service=service).set(1 if payload.get("is_free_tier") else 0)
    fetched_at = payload.get("fetched_at")
    if fetched_at and OPENROUTER_KEY_REFRESH_TIMESTAMP is not None:
        OPENROUTER_KEY_REFRESH_TIMESTAMP.labels(service=service).set(float(fetched_at))
    limit_remaining = payload.get("limit_remaining")
    OPENROUTER_KEY_LIMIT_REMAINING.labels(service=service).set(float(limit_remaining or 0.0))
    # Account balance comes from /credits, not the key's limit_remaining (which is
    # null for an uncapped key). Only publish when known so a transient /credits
    # failure retains the last good value instead of flapping to 0.
    credit_balance = payload.get("credit_balance")
    if credit_balance is not None and OPENROUTER_CREDIT_BALANCE is not None:
        OPENROUTER_CREDIT_BALANCE.labels(service=service).set(float(credit_balance))
    OPENROUTER_KEY_USAGE_DAILY.labels(service=service, kind="credits").set(
        float(payload.get("usage_daily") or 0.0)
    )
    OPENROUTER_KEY_USAGE_DAILY.labels(service=service, kind="byok_credits").set(
        float(payload.get("byok_usage_daily") or 0.0)
    )


def set_openrouter_model_health(
    service: str,
    model_id: str,
    *,
    success_rate: float,
    latency_ms: float,
    is_quarantined: bool,
) -> None:
    if OPENROUTER_HEALTH_SUCCESS_RATE is not None:
        OPENROUTER_HEALTH_SUCCESS_RATE.labels(service=service, model_id=model_id).set(
            float(success_rate)
        )
    if OPENROUTER_HEALTH_LATENCY_MS is not None:
        OPENROUTER_HEALTH_LATENCY_MS.labels(service=service, model_id=model_id).set(
            float(latency_ms)
        )
    if OPENROUTER_MODEL_QUARANTINE is not None:
        OPENROUTER_MODEL_QUARANTINE.labels(service=service, model_id=model_id).set(
            1 if is_quarantined else 0
        )


def note_openrouter_picker_decision(service: str, task_family: str, model_id: str) -> None:
    if OPENROUTER_PICKER_DECISIONS_TOTAL is not None:
        OPENROUTER_PICKER_DECISIONS_TOTAL.labels(
            service=service,
            task_family=task_family,
            model_id=model_id,
        ).inc()


def note_openrouter_picker_skip(service: str, task_family: str, reason: str) -> None:
    if OPENROUTER_PICKER_SKIP_TOTAL is not None:
        OPENROUTER_PICKER_SKIP_TOTAL.labels(
            service=service,
            task_family=task_family,
            reason=reason,
        ).inc()


def set_openrouter_model_usage(
    service: str,
    model_id: str,
    *,
    rpd_used: int | None = None,
    rpm_used: int | None = None,
) -> None:
    if rpd_used is not None and OPENROUTER_RPD_USED is not None:
        OPENROUTER_RPD_USED.labels(service=service, model_id=model_id).set(int(rpd_used))
    if rpm_used is not None and OPENROUTER_RPM_USED is not None:
        OPENROUTER_RPM_USED.labels(service=service, model_id=model_id).set(int(rpm_used))


def set_wormsoft_limits_snapshot(service: str, payload: dict) -> None:
    if WORMSOFT_LIMITS_AVAILABLE is None:
        return
    WORMSOFT_LIMITS_AVAILABLE.labels(service=service).set(1 if payload.get("available") else 0)
    fetched_at = payload.get("fetched_at")
    if fetched_at:
        WORMSOFT_LIMITS_REFRESH_TIMESTAMP.labels(service=service).set(float(fetched_at))
    WORMSOFT_SUBSCRIPTION_LIMIT_CREDITS.clear()
    WORMSOFT_SUBSCRIPTION_WINDOW_SECONDS.clear()
    WORMSOFT_SUBSCRIPTION_PRICE_RUB.clear()
    WORMSOFT_MODEL_PRICE_CREDITS_PER_MILLION.clear()
    for item in payload.get("plans") or []:
        plan = str(item.get("id") or "")
        if not plan:
            continue
        WORMSOFT_SUBSCRIPTION_LIMIT_CREDITS.labels(service=service, plan=plan).set(
            int(item.get("amount") or 0)
        )
        WORMSOFT_SUBSCRIPTION_WINDOW_SECONDS.labels(service=service, plan=plan).set(
            int(item.get("seconds") or 0)
        )
        WORMSOFT_SUBSCRIPTION_PRICE_RUB.labels(service=service, plan=plan).set(
            float(item.get("price") or 0.0)
        )
    for model, pricing in (payload.get("pricing") or {}).items():
        for kind in ("input", "output", "cache"):
            value = pricing.get(kind)
            if value is None:
                continue
            WORMSOFT_MODEL_PRICE_CREDITS_PER_MILLION.labels(
                service=service,
                model=str(model),
                kind=kind,
            ).set(float(value))


def set_admin_scheduler_running(service: str, is_running: bool) -> None:
    if ADMIN_SCHEDULER_RUNNING is not None:
        ADMIN_SCHEDULER_RUNNING.labels(service=service).set(1 if is_running else 0)


def set_admin_manual_job_metrics(
    service: str,
    snapshot: list[dict],
) -> None:
    if ADMIN_MANUAL_JOBS_RUNNING is None:
        return
    ADMIN_MANUAL_JOBS_RUNNING.clear()
    ADMIN_MANUAL_JOB_OLDEST_RUNNING_AGE_SECONDS.clear()
    ADMIN_MANUAL_JOBS_RECENT_FAILURES.clear()
    for item in snapshot:
        job_name = str(item.get("job_name") or "")
        if not job_name:
            continue
        ADMIN_MANUAL_JOBS_RUNNING.labels(service=service, job_name=job_name).set(
            int(item.get("running") or 0)
        )
        ADMIN_MANUAL_JOB_OLDEST_RUNNING_AGE_SECONDS.labels(
            service=service,
            job_name=job_name,
        ).set(float(item.get("oldest_running_age_seconds") or 0.0))
        ADMIN_MANUAL_JOBS_RECENT_FAILURES.labels(service=service, job_name=job_name).set(
            int(item.get("recent_failures") or 0)
        )


def set_redis_stream_metrics(service: str, snapshot: dict) -> None:
    if REDIS_STREAM_LAG is None:
        return

    # Гейджи с ДИНАМИЧЕСКИМ набором меток обязаны очищаться перед заполнением,
    # иначе исчезнувшая метка остаётся в реестре навсегда со своим последним
    # значением. Проверено 2026-08-05: после удаления 97 призрачных консьюмеров
    # `count(frontier_redis_stream_consumer_idle_seconds)` по-прежнему показывал
    # 107 серий, а удалённый `stream:posts:enriched` продолжал отдавать
    # `groups = 0` — уборка состоялась, метрика об этом не узнала.
    #
    # То есть метрика, заведённая ради наблюдения за мусором, сама копила мусор
    # и показывала его как живой. Тот же приём уже применён в
    # set_admin_manual_job_metrics — здесь его просто не сделали.
    #
    # Чистятся только те семейства, чьи метки приходят из снапшота. Счётчики
    # (Counter) не трогаем: у них сброс означал бы потерю монотонности.
    for _dynamic in (
        REDIS_STREAM_LAG,
        REDIS_STREAM_PENDING,
        REDIS_STREAM_OLDEST_PENDING_AGE_SECONDS,
        REDIS_STREAM_CONSUMER_PENDING,
        REDIS_STREAM_CONSUMER_IDLE_SECONDS,
        REDIS_DLQ_LENGTH,
        REDIS_STREAM_GROUPS,
        REDIS_STREAM_ENTRIES_ADDED,
        REDIS_STREAM_DELIVERY_GAP,
    ):
        if _dynamic is not None:
            _dynamic.clear()

    for stream_item in snapshot.get("streams", []):
        stream = str(stream_item.get("stream") or "")
        group = str(stream_item.get("group") or "")
        REDIS_STREAM_LAG.labels(service=service, stream=stream, group=group).set(
            int(stream_item.get("lag") or 0)
        )
        REDIS_STREAM_PENDING.labels(service=service, stream=stream, group=group).set(
            int(stream_item.get("pending") or 0)
        )
        REDIS_STREAM_OLDEST_PENDING_AGE_SECONDS.labels(
            service=service,
            stream=stream,
            group=group,
        ).set(float(stream_item.get("oldest_pending_age_seconds") or 0.0))
        for consumer in stream_item.get("consumers", []):
            consumer_name = str(consumer.get("name") or "")
            labels = {
                "service": service,
                "stream": stream,
                "group": group,
                "consumer": consumer_name,
            }
            REDIS_STREAM_CONSUMER_PENDING.labels(**labels).set(int(consumer.get("pending") or 0))
            REDIS_STREAM_CONSUMER_IDLE_SECONDS.labels(**labels).set(
                float(consumer.get("idle_seconds") or 0.0)
            )

    # DLQ: ноль печатается ВСЕГДА, в том числе для несуществующего ключа.
    # Отсутствие серии — это «неизвестно», а нам нужно «пусто».
    for dlq_item in snapshot.get("dlq", []) or []:
        stream = str(dlq_item.get("stream") or "")
        if not stream:
            continue
        REDIS_DLQ_LENGTH.labels(service=service, stream=stream).set(
            int(dlq_item.get("length") or 0)
        )

    # Здоровье стримов: осиротевший продюсер и потеря при тримминге. Обе величины
    # невыразимы через lag/pending — там, где они интересны, и lag, и pending
    # равны нулю по построению.
    for health_item in snapshot.get("health", []) or []:
        stream = str(health_item.get("stream") or "")
        if not stream:
            continue
        REDIS_STREAM_GROUPS.labels(service=service, stream=stream).set(
            int(health_item.get("groups") or 0)
        )
        REDIS_STREAM_ENTRIES_ADDED.labels(service=service, stream=stream).set(
            int(health_item.get("entries_added") or 0)
        )
        for gap_item in health_item.get("gaps", []) or []:
            group = str(gap_item.get("group") or "")
            if not group:
                continue
            REDIS_STREAM_DELIVERY_GAP.labels(
                service=service, stream=stream, group=group
            ).set(1 if gap_item.get("delivery_gap") else 0)
