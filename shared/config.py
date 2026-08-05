from functools import lru_cache

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from shared.embedding_models import expected_embedding_dim


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # PostgreSQL
    database_url: str = Field(..., alias="DATABASE_URL")

    # Redis
    redis_url: str = Field("redis://redis:6379", alias="REDIS_URL")
    redis_stream_maxlen: int = Field(100_000, alias="REDIS_STREAM_MAXLEN")

    # Qdrant
    qdrant_url: str = Field("http://qdrant:6333", alias="QDRANT_URL")
    qdrant_collection: str = Field("frontier_docs", alias="QDRANT_COLLECTION")
    qdrant_collection_alias: str = Field("", alias="QDRANT_COLLECTION_ALIAS")
    qdrant_trends_collection: str = Field("trend_clusters", alias="QDRANT_TRENDS_COLLECTION")
    qdrant_trends_collection_alias: str = Field("", alias="QDRANT_TRENDS_COLLECTION_ALIAS")
    qdrant_filter_embedding_version: bool = Field(
        True,
        alias="QDRANT_FILTER_EMBEDDING_VERSION",
    )
    qdrant_enforce_collection_schema: bool = Field(
        True,
        alias="QDRANT_ENFORCE_COLLECTION_SCHEMA",
    )

    # own_stake — вторая ось карточки (ТЗ B).
    # Корпус автора лежит в ОТДЕЛЬНОЙ коллекции без workspace_id: _build_payload_filter
    # к ней не применяется, hybrid_search и get_frontier_brief в неё не ходят.
    # Здесь хранится только база имени; фактическое имя коллекции версионируется
    # моделью эмбеддингов через qdrant_collection_name_for_embedding.
    qdrant_own_corpus_collection: str = Field("own_corpus", alias="QDRANT_OWN_CORPUS_COLLECTION")
    own_stake_enabled: bool = Field(False, alias="OWN_STAKE_ENABLED")
    # Сколько ближайших чанков корпуса запрашивать на карточку. Агрегация — max,
    # поэтому top_k > 1 нужен только как страховка от дублей в корпусе.
    own_stake_top_k: int = Field(3, alias="OWN_STAKE_TOP_K")
    # ПЛЕЙСХОЛДЕРЫ, а не измеренные величины. Вывести их из кода нельзя: own_stake —
    # честный косинус в [0,1], а score в выдаче — RRF-ранг (sparse включён по умолчанию),
    # несопоставимый между запросами. Калибруются по накопленным парам «выбрана /
    # не выбрана» из задачи C (таблица card_feedback, ~60 пар за 12 недель); до тех пор
    # квадрант читать как грубую отсечку, а не как измерение.
    # own_stake_high = 0.60 — верхняя граница фонового косинуса между несвязанными
    # русскими текстами у EmbeddingsGigaR; выше — кандидат в «свой замер есть».
    # relevance_high = 0.40 — единственное наблюдавшееся живое значение score (0.41)
    # из разбора выдачи; это RRF-балл, а не порог качества.
    own_stake_high: float = Field(0.60, alias="OWN_STAKE_HIGH")
    relevance_high: float = Field(0.40, alias="RELEVANCE_HIGH")

    # Neo4j
    neo4j_url: str = Field("bolt://neo4j:7687", alias="NEO4J_URL")
    neo4j_user: str = Field("neo4j", alias="NEO4J_USER")
    neo4j_password: str = Field("", alias="NEO4J_PASSWORD")

    # GigaChat / gpt2giga-proxy
    runtime_mode: str = Field("custom", alias="FRONTIER_RUNTIME_MODE")
    gpt2giga_enable_images: bool = Field(True, alias="GPT2GIGA_ENABLE_IMAGES")
    openai_api_base: str = Field("http://gpt2giga-proxy:8090/v1", alias="OPENAI_API_BASE")
    gigachat_proxy_url: str = Field("http://gpt2giga-proxy:8090", alias="GIGACHAT_PROXY_URL")
    gigachat_base_url: str = Field(
        "https://gigachat.devices.sberbank.ru/api/v1",
        alias="GIGACHAT_BASE_URL",
    )
    gigachat_auth_url: str = Field(
        "https://ngw.devices.sberbank.ru:9443/api/v2/oauth",
        alias="GIGACHAT_AUTH_URL",
    )
    gigachat_credentials: str = Field("", alias="GIGACHAT_CREDENTIALS")
    gigachat_scope: str = Field("GIGACHAT_API_PERS", alias="GIGACHAT_SCOPE")
    gigachat_verify_ssl_certs: bool = Field(False, alias="GIGACHAT_VERIFY_SSL_CERTS")
    gigachat_embeddings_model: str = Field("EmbeddingsGigaR", alias="GIGACHAT_EMBEDDINGS_MODEL")
    wormsoft_api_base: str = Field("https://ai.wormsoft.ru/api/gpt", alias="WORMSOFT_API_BASE")
    wormsoft_api_key: str = Field("", alias="WORMSOFT_API_KEY")
    wormsoft_model_default: str = Field("wormsoft/agent/medium", alias="WORMSOFT_MODEL_DEFAULT")
    wormsoft_model_mcp_synthesis: str = Field("", alias="WORMSOFT_MODEL_MCP_SYNTHESIS")
    # Пусто — не добавлять Wormsoft в цепочку vision (см. default_routing_policy_v2).
    wormsoft_vision_model: str = Field("", alias="WORMSOFT_VISION_MODEL")
    wormsoft_max_simultaneous_requests: int = Field(
        1,
        alias="WORMSOFT_MAX_SIMULTANEOUS_REQUESTS",
    )
    wormsoft_min_request_interval_ms: int = Field(
        2000,
        alias="WORMSOFT_MIN_REQUEST_INTERVAL_MS",
    )
    wormsoft_max_retries: int = Field(0, alias="WORMSOFT_MAX_RETRIES")
    wormsoft_connect_timeout_sec: float = Field(5.0, alias="WORMSOFT_CONNECT_TIMEOUT_SEC")
    wormsoft_read_timeout_sec: float = Field(45.0, alias="WORMSOFT_READ_TIMEOUT_SEC")
    wormsoft_write_timeout_sec: float = Field(45.0, alias="WORMSOFT_WRITE_TIMEOUT_SEC")
    wormsoft_pool_timeout_sec: float = Field(10.0, alias="WORMSOFT_POOL_TIMEOUT_SEC")
    wormsoft_max_connections: int = Field(20, alias="WORMSOFT_MAX_CONNECTIONS")
    wormsoft_max_keepalive_connections: int = Field(
        5,
        alias="WORMSOFT_MAX_KEEPALIVE_CONNECTIONS",
    )
    wormsoft_shared_guard_enabled: bool = Field(True, alias="WORMSOFT_SHARED_GUARD_ENABLED")
    wormsoft_quarantine_rate_limit_sec: int = Field(
        120,
        alias="WORMSOFT_QUARANTINE_RATE_LIMIT_SEC",
    )
    wormsoft_quarantine_upstream_error_sec: int = Field(
        180,
        alias="WORMSOFT_QUARANTINE_UPSTREAM_ERROR_SEC",
    )
    wormsoft_failure_burst_threshold: int = Field(
        3,
        alias="WORMSOFT_FAILURE_BURST_THRESHOLD",
    )
    wormsoft_failure_burst_window_sec: int = Field(
        300,
        alias="WORMSOFT_FAILURE_BURST_WINDOW_SEC",
    )
    openrouter_api_key: str = Field("", alias="OPENROUTER_API_KEY")
    openrouter_base_url: str = Field("https://openrouter.ai/api/v1", alias="OPENROUTER_BASE_URL")
    openrouter_referrer: str = Field(
        "https://frontier-intelligence.local",
        alias="OPENROUTER_REFERRER",
    )
    openrouter_text_model: str = Field("openrouter/free", alias="OPENROUTER_TEXT_MODEL")
    openrouter_vision_model: str = Field("openrouter/free", alias="OPENROUTER_VISION_MODEL")
    openrouter_free_rpm_throttle: int = Field(18, alias="OPENROUTER_FREE_RPM_THROTTLE")
    openrouter_free_rpd_soft_cap: int = Field(850, alias="OPENROUTER_FREE_RPD_SOFT_CAP")
    openrouter_free_quarantine_5xx_sec: int = Field(
        900,
        alias="OPENROUTER_FREE_QUARANTINE_5XX_SEC",
    )
    openrouter_health_probe_timeout_sec: float = Field(
        20.0,
        alias="OPENROUTER_HEALTH_PROBE_TIMEOUT_SEC",
    )
    openrouter_health_probe_max_tokens: int = Field(
        1,
        alias="OPENROUTER_HEALTH_PROBE_MAX_TOKENS",
    )
    openrouter_health_probe_batch_size: int = Field(
        8,
        alias="OPENROUTER_HEALTH_PROBE_BATCH_SIZE",
    )
    openrouter_picker_sticky_sec: int = Field(600, alias="OPENROUTER_PICKER_STICKY_SEC")
    openrouter_rpd_safety_buffer: float = Field(0.1, alias="OPENROUTER_RPD_SAFETY_BUFFER")
    openrouter_max_simultaneous_requests: int = Field(
        4,
        alias="OPENROUTER_MAX_SIMULTANEOUS_REQUESTS",
    )
    openrouter_min_request_interval_ms: int = Field(
        100,
        alias="OPENROUTER_MIN_REQUEST_INTERVAL_MS",
    )
    openrouter_fail_safe_enabled: bool = Field(
        True,
        alias="OPENROUTER_FAIL_SAFE_ENABLED",
    )
    openrouter_fail_safe_stale_sec: int = Field(
        1800,
        alias="OPENROUTER_FAIL_SAFE_STALE_SEC",
    )
    openrouter_quarantine_5xx_threshold: int = Field(
        3,
        alias="OPENROUTER_QUARANTINE_5XX_THRESHOLD",
    )
    openrouter_quarantine_5xx_window_sec: int = Field(
        300,
        alias="OPENROUTER_QUARANTINE_5XX_WINDOW_SEC",
    )
    llm_circuit_provider_quarantine_sec: int = Field(
        300,
        alias="LLM_CIRCUIT_PROVIDER_QUARANTINE_SEC",
    )
    llm_circuit_model_quarantine_sec: int = Field(
        180,
        alias="LLM_CIRCUIT_MODEL_QUARANTINE_SEC",
    )
    llm_circuit_rate_limit_quarantine_sec: int = Field(
        120,
        alias="LLM_CIRCUIT_RATE_LIMIT_QUARANTINE_SEC",
    )
    llm_circuit_failure_threshold: int = Field(
        3,
        alias="LLM_CIRCUIT_FAILURE_THRESHOLD",
    )
    llm_circuit_failure_window_sec: int = Field(
        300,
        alias="LLM_CIRCUIT_FAILURE_WINDOW_SEC",
    )
    llm_shadow_eval_enabled: bool = Field(True, alias="LLM_SHADOW_EVAL_ENABLED")
    llm_shadow_eval_timeout_sec: float = Field(15.0, alias="LLM_SHADOW_EVAL_TIMEOUT_SEC")
    llm_shadow_eval_max_concurrency: int = Field(
        2,
        alias="LLM_SHADOW_EVAL_MAX_CONCURRENCY",
    )
    llm_runtime_shadow_daily_request_soft_cap: int = Field(
        250,
        alias="LLM_RUNTIME_SHADOW_DAILY_REQUEST_SOFT_CAP",
    )
    llm_runtime_shadow_daily_request_limit: int = Field(
        0,
        alias="LLM_RUNTIME_SHADOW_DAILY_REQUEST_LIMIT",
    )
    llm_runtime_embeddings_daily_request_soft_cap: int = Field(
        0,
        alias="LLM_RUNTIME_EMBEDDINGS_DAILY_REQUEST_SOFT_CAP",
    )
    llm_runtime_embeddings_daily_request_limit: int = Field(
        0,
        alias="LLM_RUNTIME_EMBEDDINGS_DAILY_REQUEST_LIMIT",
    )
    llm_runtime_provider_openrouter_daily_request_soft_cap: int = Field(
        0,
        alias="LLM_RUNTIME_PROVIDER_OPENROUTER_DAILY_REQUEST_SOFT_CAP",
    )
    llm_runtime_provider_openrouter_daily_request_limit: int = Field(
        0,
        alias="LLM_RUNTIME_PROVIDER_OPENROUTER_DAILY_REQUEST_LIMIT",
    )
    llm_runtime_provider_wormsoft_daily_request_soft_cap: int = Field(
        0,
        alias="LLM_RUNTIME_PROVIDER_WORMSOFT_DAILY_REQUEST_SOFT_CAP",
    )
    llm_runtime_provider_wormsoft_daily_request_limit: int = Field(
        0,
        alias="LLM_RUNTIME_PROVIDER_WORMSOFT_DAILY_REQUEST_LIMIT",
    )
    llm_runtime_provider_polza_daily_request_soft_cap: int = Field(
        0,
        alias="LLM_RUNTIME_PROVIDER_POLZA_DAILY_REQUEST_SOFT_CAP",
    )
    llm_runtime_provider_polza_daily_request_limit: int = Field(
        0,
        alias="LLM_RUNTIME_PROVIDER_POLZA_DAILY_REQUEST_LIMIT",
    )
    llm_runtime_provider_gigachat_daily_request_soft_cap: int = Field(
        0,
        alias="LLM_RUNTIME_PROVIDER_GIGACHAT_DAILY_REQUEST_SOFT_CAP",
    )
    llm_runtime_provider_gigachat_daily_request_limit: int = Field(
        0,
        alias="LLM_RUNTIME_PROVIDER_GIGACHAT_DAILY_REQUEST_LIMIT",
    )
    wormsoft_credit_throttle_enabled: bool = Field(
        False,
        alias="WORMSOFT_CREDIT_THROTTLE_ENABLED",
    )
    wormsoft_credit_window_seconds: int = Field(
        18000,
        alias="WORMSOFT_CREDIT_WINDOW_SECONDS",
    )
    wormsoft_credit_window_limit: float = Field(
        500000.0,
        alias="WORMSOFT_CREDIT_WINDOW_LIMIT",
    )
    wormsoft_credit_soft_cap_ratio: float = Field(
        0.8,
        alias="WORMSOFT_CREDIT_SOFT_CAP_RATIO",
    )
    wormsoft_credit_hard_cap_ratio: float = Field(
        0.98,
        alias="WORMSOFT_CREDIT_HARD_CAP_RATIO",
    )
    wormsoft_credit_soft_cap_shadow_ratio: float = Field(
        0.7,
        alias="WORMSOFT_CREDIT_SOFT_CAP_SHADOW_RATIO",
    )
    polza_api_key: str = Field("", alias="POLZA_API_KEY")
    polza_base_url: str = Field("https://polza.ai/api/v1", alias="POLZA_BASE_URL")
    polza_text_model: str = Field("deepseek/deepseek-v3.2", alias="POLZA_TEXT_MODEL")
    polza_synthesis_model: str = Field(
        "deepseek/deepseek-v3.2",
        alias="POLZA_SYNTHESIS_MODEL",
    )
    polza_vision_model: str = Field("", alias="POLZA_VISION_MODEL")
    gigachat_model_lite: str = Field("GigaChat-2", alias="GIGACHAT_MODEL_LITE")
    gigachat_model: str = Field("GigaChat-2", alias="GIGACHAT_MODEL")
    gigachat_model_pro: str = Field("GigaChat-2-Pro", alias="GIGACHAT_MODEL_PRO")
    gigachat_model_max: str = Field("GigaChat-2-Max", alias="GIGACHAT_MODEL_MAX")
    # Пусто → релевантность идёт в gigachat_model_pro (JSON-инструкции стабильнее, см. доки GigaChat 2 Lite/Pro/Max)
    gigachat_model_relevance: str = Field("", alias="GIGACHAT_MODEL_RELEVANCE")
    gigachat_model_concepts: str = Field("", alias="GIGACHAT_MODEL_CONCEPTS")
    gigachat_model_valence: str = Field("", alias="GIGACHAT_MODEL_VALENCE")
    # Пусто → vision через gigachat_model_pro (должна поддерживать изображения, иначе 422)
    gigachat_model_vision: str = Field("", alias="GIGACHAT_VISION_MODEL")
    gigachat_model_mcp_synthesis: str = Field("", alias="GIGACHAT_MODEL_MCP_SYNTHESIS")
    gigachat_session_cache_enabled: bool = Field(True, alias="GIGACHAT_SESSION_CACHE_ENABLED")
    gigachat_escalation_enabled: bool = Field(True, alias="GIGACHAT_ESCALATION_ENABLED")
    gigachat_token_budget_relevance: int = Field(1500, alias="GIGACHAT_TOKEN_BUDGET_RELEVANCE")
    gigachat_token_budget_concepts: int = Field(1500, alias="GIGACHAT_TOKEN_BUDGET_CONCEPTS")
    gigachat_token_budget_valence: int = Field(1200, alias="GIGACHAT_TOKEN_BUDGET_VALENCE")
    gigachat_token_budget_embed: int = Field(1200, alias="GIGACHAT_TOKEN_BUDGET_EMBED")
    gigachat_token_budget_vision_prompt: int = Field(600, alias="GIGACHAT_TOKEN_BUDGET_VISION_PROMPT")
    gigachat_relevance_gray_zone: float = Field(0.1, alias="GIGACHAT_RELEVANCE_GRAY_ZONE")
    gigachat_rc_joint_enabled: bool = Field(False, alias="GIGACHAT_RC_JOINT_ENABLED")
    gigachat_rc_joint_workspaces: str = Field("", alias="GIGACHAT_RC_JOINT_WORKSPACES")
    gigachat_rc_joint_sources: str = Field("", alias="GIGACHAT_RC_JOINT_SOURCES")
    gigachat_token_budget_relevance_concepts: int = Field(
        1800,
        alias="GIGACHAT_TOKEN_BUDGET_RELEVANCE_CONCEPTS",
    )
    gigachat_balance_alert_threshold: int = Field(
        100_000,
        alias="GIGACHAT_BALANCE_ALERT_THRESHOLD",
    )
    gigachat_max_simultaneous_requests: int = Field(1, alias="GIGACHAT_MAX_SIMULTANEOUS_REQUESTS")
    gigachat_min_request_interval_ms: int = Field(250, alias="GIGACHAT_MIN_REQUEST_INTERVAL_MS")
    # Опционально: базовый URL сервиса PaddleOCR (docker compose --profile paddleocr → http://paddleocr:8008)
    paddleocr_url: str = Field("", alias="PADDLEOCR_URL")
    embed_dim: int = Field(2560, alias="EMBED_DIM")

    # S3
    s3_endpoint_url: str = Field("https://s3.cloud.ru", alias="S3_ENDPOINT_URL")
    s3_bucket_name: str = Field("", alias="S3_BUCKET_NAME")
    s3_access_key_id: str = Field("", alias="S3_ACCESS_KEY_ID")
    s3_secret_access_key: str = Field("", alias="S3_SECRET_ACCESS_KEY")
    s3_region: str = Field("ru-central-1", alias="S3_REGION")
    s3_addressing_style: str = Field("path", alias="S3_ADDRESSING_STYLE")
    aws_signature_version: str = Field("s3v4", alias="AWS_SIGNATURE_VERSION")
    s3_connect_timeout_sec: int = Field(5, alias="S3_CONNECT_TIMEOUT_SEC")
    s3_read_timeout_sec: int = Field(30, alias="S3_READ_TIMEOUT_SEC")
    s3_max_retry_attempts: int = Field(3, alias="S3_MAX_RETRY_ATTEMPTS")

    # Telegram
    tg_api_id_0: int = Field(0, alias="TG_API_ID_0")
    tg_api_hash_0: str = Field("", alias="TG_API_HASH_0")
    tg_api_id_1: int = Field(0, alias="TG_API_ID_1")
    tg_api_hash_1: str = Field("", alias="TG_API_HASH_1")
    telegram_bot_token: str = Field("", alias="TELEGRAM_BOT_TOKEN")
    telegram_alert_chat_id: str = Field(
        "",
        validation_alias=AliasChoices("ALERT_TELEGRAM_CHAT_ID", "TELEGRAM_ALERT_CHAT_ID"),
    )
    telegram_alert_proxy_url: str = Field("", alias="TELEGRAM_ALERT_PROXY_URL")
    alertmanager_webhook_token: str = Field("", alias="ALERTMANAGER_WEBHOOK_TOKEN")

    # ── IMAP (email-коннектор) ───────────────────────────────────────────────
    # Пароль ящика НИКОГДА не живёт в конфиге источника: тот едет из
    # git-трекаемого config/sources.yml и лежит в PostgreSQL открытым текстом
    # (sources.extra), то есть попадает в бэкапы и в выдачу админского API.
    # Источник несёт только ИМЯ ключа (extra.fetch.password_key), значение —
    # отсюда. Один ящик закрывается IMAP_PASSWORD; несколько — JSON-картой
    # IMAP_PASSWORDS вида {"alerts": "...", "digest": "..."}. Карта объявлена
    # строкой, а не dict: значения приезжают из .env, где вложенных структур нет.
    imap_password: str = Field("", alias="IMAP_PASSWORD")
    imap_passwords: str = Field("", alias="IMAP_PASSWORDS")

    # SearXNG
    searxng_url: str = Field("http://searxng:8080", alias="SEARXNG_URL")
    searxng_enabled: bool = Field(True, alias="SEARXNG_ENABLED")
    searxng_user: str = Field("", alias="SEARXNG_USER")
    searxng_password: str = Field("", alias="SEARXNG_PASSWORD")
    searxng_timeout_seconds: float = Field(8.0, alias="SEARXNG_TIMEOUT_SECONDS")
    searxng_cache_ttl: int = Field(3600, alias="SEARXNG_CACHE_TTL")
    # TTL для ПУСТОЙ выдачи — отдельный и намеренно короткий. Пустой ответ почти
    # всегда значит «набор движков лежит», и кэшировать его на час означает прятать
    # починку: 03.08.2026 после правки searxng/settings.yml те же темы ещё час
    # отдавали [] из кэша, и правка выглядела нерабочей. Две минуты хватает, чтобы
    # не долбить мёртвый апстрим, и не переживает цикл «поправил → перезапустил →
    # проверил». Плановые прогоны gap-анализа идут кратно реже, им это ничего не стоит.
    # Объявлено полем, а не читается через getattr: у Settings model_config
    # extra="ignore", поэтому необъявленный ключ из .env молча выбрасывается — ровно
    # так searxng_engines_fingerprint оказался константой при любом содержимом .env.
    searxng_empty_cache_ttl: int = Field(120, alias="SEARXNG_EMPTY_CACHE_TTL")
    searxng_max_results: int = Field(5, alias="SEARXNG_MAX_RESULTS")
    searxng_categories: str = Field("general,news", alias="SEARXNG_CATEGORIES")
    # Отпечаток набора движков: единственный инвалидатор кэша поиска
    # (worker/services/searxng_client.py::_cache_key). Сам набор живёт в
    # searxng/settings.yml, а тот смонтирован ТОЛЬКО в контейнер searxng — вывести
    # отпечаток из своего окружения клиент не может, поэтому значение объявляет
    # оператор. Правило эксплуатации: поменял searxng/settings.yml — положи сюда
    # новое значение (удобнее всего sha256 самого файла), иначе закэшированные
    # ответы (в том числе пустые) продолжат отдаваться со старым ключом.
    # Поля не существовало до 2026-08-04, и это был не «пока не добавили»:
    # model_config стоит на extra="ignore", то есть SEARXNG_ENGINES_FINGERPRINT из
    # .env отбрасывался ещё ДО того, как клиент пытался его прочесть, и отпечаток
    # был константной пустой строкой при любом содержимом .env.
    searxng_engines_fingerprint: str = Field("", alias="SEARXNG_ENGINES_FINGERPRINT")
    missing_signals_enabled: bool = Field(True, alias="MISSING_SIGNALS_ENABLED")
    missing_signals_window_days: int = Field(30, alias="MISSING_SIGNALS_WINDOW_DAYS")
    missing_signals_topic_limit: int = Field(8, alias="MISSING_SIGNALS_TOPIC_LIMIT")
    # 0.35 калибровался под неограниченную сумму в _frontier_frequency: presence
    # там был min(1.0, sum/3.5), поэтому любая тема с суммой >= 1.83 отсеивалась
    # всегда (замерено 2026-08-03: три из трёх design_lenses воркспейса design).
    # После нормировки обе стороны вычитания лежат в [0, 1], и порог означает
    # ровно «пол по внешним свидетельствам». Значение выбрано между двумя
    # посчитанными точками _external_signal_strength при SEARXNG_MAX_RESULTS=5 и
    # типичных для живого SearXNG оценках 0.4-0.5:
    #   2 результата с ОДНОГО домена -> 0.303 (не проходит)
    #   2 результата с ДВУХ доменов  -> 0.363 (проходит)
    # То есть MISSING_SIGNALS_MIN_EXTERNAL_RESULTS=2 не противоречит арифметике,
    # но одно-доменная выдача сама по себе порогом не считается. Осмысленный
    # коридор — (0.303, 0.363]; при 0.30 (значение до 2026-08-03) одно-доменные
    # выдачи проходили с запасом 0.003, и фильтр вырождался в «SearXNG вообще
    # ответил». Одно-доменная выдача всё же пройдёт, если средняя оценка
    # результатов выше 1.0 (слагаемое min(avg/3,1)*0.15), — на живых числах
    # такого не встречается.
    missing_signals_min_gap_score: float = Field(0.33, alias="MISSING_SIGNALS_MIN_GAP_SCORE")
    # Доля корпуса, при которой тема считается покрытой полностью (presence = 1.0).
    # Заменяет литерал 3.5 из _gap_score, который был привязан к старой шкале.
    # Условие прохождения: frontier_frequency <= saturation * (external_strength -
    # min_gap_score). На замеренной таблице design (64 кластера, external на
    # практическом потолке 0.8728) при 0.30 отсекаются «design» (presence 0.94,
    # gap 0.0) и «interaction design» (presence 0.57, gap 0.2998 — при прежнем
    # пороге 0.30 отсев держался на 0.0002, ножевой край; при 0.33 запас 0.03),
    # проходят «service design» 0.4041, «visual culture» 0.7688 и «automotive HMI
    # design» 0.8728.
    # ВНИМАНИЕ (2026-08-03): колонка presence в этой таблице замерена при
    # IDF-взвешенном пороге покрытия _MIN_TOPIC_OVERLAP = 0.30. С тех пор вес
    # токена — редкость 1 - df/N, а порог 0.40, и мера перестала зависеть от
    # размера корпуса отдельно от долей df/N. Сами настройки не тронуты и значат
    # ровно то же, но конкретные presence/gap по темам требуют перезамера на живых
    # данных — направление сдвига не монотонно (совпадение по общему токену стало
    # весить чуть больше, по различающему чуть меньше). Что НЕ зависит от той
    # таблицы и потому осталось в силе — обоснование min_gap_score ниже: оно
    # держится на коридоре external_strength (0.303, 0.363], а его правка не
    # касалась.
    missing_signals_presence_saturation: float = Field(
        0.30,
        alias="MISSING_SIGNALS_PRESENCE_SATURATION",
    )
    missing_signals_min_external_results: int = Field(
        2,
        alias="MISSING_SIGNALS_MIN_EXTERNAL_RESULTS",
    )
    missing_signals_max_evidence_urls: int = Field(
        5,
        alias="MISSING_SIGNALS_MAX_EVIDENCE_URLS",
    )
    missing_signals_time_range: str = Field("month", alias="MISSING_SIGNALS_TIME_RANGE")
    missing_signals_language: str = Field("auto", alias="MISSING_SIGNALS_LANGUAGE")

    # App
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    mcp_port: int = Field(8100, alias="MCP_PORT")
    mcp_internal_url: str = Field("http://mcp:8100", alias="MCP_INTERNAL_URL")
    admin_port: int = Field(8101, alias="ADMIN_PORT")
    allowed_origins: list[str] = Field(default=["*"], alias="ALLOWED_ORIGINS")
    admin_scheduler_enabled: bool = Field(True, alias="ADMIN_SCHEDULER_ENABLED")
    admin_scheduler_timezone: str = Field("UTC", alias="ADMIN_SCHEDULER_TIMEZONE")
    admin_source_score_refresh_cron: str = Field(
        "17 */6 * * *",
        alias="ADMIN_SOURCE_SCORE_REFRESH_CRON",
    )
    admin_semantic_cluster_cron: str = Field(
        "35 3 * * *",
        alias="ADMIN_SEMANTIC_CLUSTER_CRON",
    )
    admin_signal_cluster_cron: str = Field(
        "20 */8 * * *",
        alias="ADMIN_SIGNAL_CLUSTER_CRON",
    )
    admin_retrospective_review_cron: str = Field(
        "10 4 * * *",
        alias="ADMIN_RETROSPECTIVE_REVIEW_CRON",
    )
    admin_novelty_judge_cron: str = Field(
        "40 4 * * *",
        alias="ADMIN_NOVELTY_JUDGE_CRON",
    )
    admin_relevance_audit_cron: str = Field(
        "0 5 * * *",
        alias="ADMIN_RELEVANCE_AUDIT_CRON",
    )
    admin_graph_maintenance_cron: str = Field(
        "20 5 * * *",
        alias="ADMIN_GRAPH_MAINTENANCE_CRON",
    )
    admin_entity_resolution_cron: str = Field(
        "40 5 * * *",
        alias="ADMIN_ENTITY_RESOLUTION_CRON",
    )
    # Контур D+ (RSI): семантический entity-resolution (акроним↔расшифровка + LLM-судья).
    entity_resolution_enabled: bool = Field(True, alias="ENTITY_RESOLUTION_ENABLED")
    entity_resolution_max_per_run: int = Field(30, alias="ENTITY_RESOLUTION_MAX_PER_RUN")
    entity_resolution_min_cooccurrence: int = Field(2, alias="ENTITY_RESOLUTION_MIN_COOCCURRENCE")
    entity_resolution_min_confidence: float = Field(0.7, alias="ENTITY_RESOLUTION_MIN_CONFIDENCE")
    # Контур B (RSI): кросс-семейный novelty-judge для weak-кандидатов.
    # Не-Giga семья (primary идёт на Gemma+GigaChat) — судит малый набор weak-снимков.
    novelty_judge_enabled: bool = Field(True, alias="NOVELTY_JUDGE_ENABLED")
    novelty_judge_model: str = Field("deepseek-ai/deepseek-v4-pro", alias="NOVELTY_JUDGE_MODEL")
    novelty_judge_fallback_model: str = Field(
        "deepseek/deepseek-v3.2", alias="NOVELTY_JUDGE_FALLBACK_MODEL"
    )
    novelty_judge_max_per_run: int = Field(15, alias="NOVELTY_JUDGE_MAX_PER_RUN")
    # Output-токены судьи: DeepSeek-v4-pro — reasoning-модель, ей нужен бюджет на «думанье» + JSON;
    # при 400 контент пустой (всё съедает reasoning), при 1500+ возвращает валидный JSON.
    novelty_judge_token_budget: int = Field(2000, alias="NOVELTY_JUDGE_TOKEN_BUDGET")
    novelty_judge_threshold: float = Field(0.6, alias="NOVELTY_JUDGE_THRESHOLD")
    admin_gigachat_balance_refresh_cron: str = Field(
        "*/5 * * * *",
        alias="ADMIN_GIGACHAT_BALANCE_REFRESH_CRON",
    )
    admin_wormsoft_limits_refresh_cron: str = Field(
        "*/10 * * * *",
        alias="ADMIN_WORMSOFT_LIMITS_REFRESH_CRON",
    )
    admin_openrouter_catalog_refresh_cron: str = Field(
        "*/15 * * * *",
        alias="ADMIN_OPENROUTER_CATALOG_REFRESH_CRON",
    )
    admin_openrouter_key_refresh_cron: str = Field(
        "*/5 * * * *",
        alias="ADMIN_OPENROUTER_KEY_REFRESH_CRON",
    )
    admin_openrouter_health_refresh_cron: str = Field(
        "*/5 * * * *",
        alias="ADMIN_OPENROUTER_HEALTH_REFRESH_CRON",
    )
    admin_openrouter_reconcile_cron: str = Field(
        "* * * * *",
        alias="ADMIN_OPENROUTER_RECONCILE_CRON",
    )
    admin_trend_alert_cron: str = Field(
        "25 * * * *",
        alias="ADMIN_TREND_ALERT_CRON",
    )
    admin_xray_health_cron: str = Field(
        "*/5 * * * *",
        alias="ADMIN_XRAY_HEALTH_CRON",
    )
    admin_scheduler_misfire_grace_seconds: int = Field(
        1800,
        alias="ADMIN_SCHEDULER_MISFIRE_GRACE_SECONDS",
    )
    admin_scheduler_max_jitter_seconds: int = Field(
        120,
        alias="ADMIN_SCHEDULER_MAX_JITTER_SECONDS",
    )

    # Worker tuning
    indexing_consumer_group: str = Field("enrichment_workers", alias="INDEXING_CONSUMER_GROUP")
    indexing_batch_size: int = Field(32, alias="INDEXING_BATCH_SIZE")
    # Одновременных process_event в батче (релевантность+концепты+embed) — меньше 429 от GigaChat
    indexing_max_concurrency: int = Field(1, alias="INDEXING_MAX_CONCURRENCY")
    indexing_max_retries: int = Field(5, alias="INDEXING_MAX_RETRIES")
    # PEL delivery-count cap for the enrichment reclaim path. Deliberately higher
    # than indexing_max_retries: the Redis PEL counter also grows on worker
    # restarts and slow-message reclaims, and a poison message (one that dies
    # before the guarded retry path and never bumps the app-level retry_count)
    # is only detectable via this real redelivery counter. Past this many
    # redeliveries the message is force-dropped to indexing_dlq_stream.
    indexing_max_deliveries: int = Field(20, alias="INDEXING_MAX_DELIVERIES")
    indexing_dlq_stream: str = Field("stream:posts:parsed:dlq", alias="INDEXING_DLQ_STREAM")
    indexing_backoff_ms: int = Field(2000, alias="INDEXING_BACKOFF_MS")
    sparse_vectors_enabled: bool = Field(True, alias="SPARSE_VECTORS_ENABLED")

    # Relevance
    default_relevance_threshold: float = Field(0.6, alias="DEFAULT_RELEVANCE_THRESHOLD")

    # Semantic clustering
    semantic_cluster_max_posts: int = Field(400, alias="SEMANTIC_CLUSTER_MAX_POSTS")
    semantic_cluster_window_days: int = Field(7, alias="SEMANTIC_CLUSTER_WINDOW_DAYS")
    semantic_dedupe_similarity_threshold: float = Field(0.92, alias="SEMANTIC_DEDUPE_SIMILARITY_THRESHOLD")
    semantic_dedupe_max_gap_hours: int = Field(96, alias="SEMANTIC_DEDUPE_MAX_GAP_HOURS")
    semantic_merge_enabled: bool = Field(True, alias="SEMANTIC_MERGE_ENABLED")
    semantic_merge_similarity_threshold: float = Field(0.78, alias="SEMANTIC_MERGE_SIMILARITY_THRESHOLD")
    semantic_merge_title_overlap_threshold: float = Field(0.4, alias="SEMANTIC_MERGE_TITLE_OVERLAP_THRESHOLD")
    semantic_merge_concept_overlap_threshold: float = Field(0.5, alias="SEMANTIC_MERGE_CONCEPT_OVERLAP_THRESHOLD")
    semantic_merge_max_gap_hours: int = Field(168, alias="SEMANTIC_MERGE_MAX_GAP_HOURS")
    semantic_cluster_cooling_hours: int = Field(48, alias="SEMANTIC_CLUSTER_COOLING_HOURS")
    semantic_cluster_archive_hours: int = Field(24 * 14, alias="SEMANTIC_CLUSTER_ARCHIVE_HOURS")
    trend_cluster_similarity_threshold: float = Field(0.87, alias="TREND_CLUSTER_SIMILARITY_THRESHOLD")
    trend_cluster_max_gap_hours: int = Field(24 * 30, alias="TREND_CLUSTER_MAX_GAP_HOURS")
    trend_cluster_window_days: int = Field(30, alias="TREND_CLUSTER_WINDOW_DAYS")
    trend_cluster_min_semantic_clusters: int = Field(2, alias="TREND_CLUSTER_MIN_SEMANTIC_CLUSTERS")
    trend_cluster_min_docs: int = Field(4, alias="TREND_CLUSTER_MIN_DOCS")
    trend_cluster_stable_threshold: float = Field(0.58, alias="TREND_CLUSTER_STABLE_THRESHOLD")
    trend_cluster_emerging_threshold: float = Field(0.42, alias="TREND_CLUSTER_EMERGING_THRESHOLD")
    trend_cluster_min_source_diversity: float = Field(0.2, alias="TREND_CLUSTER_MIN_SOURCE_DIVERSITY")
    trend_alerts_enabled: bool = Field(True, alias="TREND_ALERTS_ENABLED")
    trend_alert_window_hours: int = Field(48, alias="TREND_ALERT_WINDOW_HOURS")
    trend_alert_min_signal_score: float = Field(0.8, alias="TREND_ALERT_MIN_SIGNAL_SCORE")
    trend_alert_change_point_min_signal_score: float = Field(
        0.74,
        alias="TREND_ALERT_CHANGE_POINT_MIN_SIGNAL_SCORE",
    )
    trend_alert_min_change_point_strength: float = Field(
        0.7,
        alias="TREND_ALERT_MIN_CHANGE_POINT_STRENGTH",
    )
    trend_alert_min_doc_count: int = Field(5, alias="TREND_ALERT_MIN_DOC_COUNT")
    trend_alert_min_source_count: int = Field(3, alias="TREND_ALERT_MIN_SOURCE_COUNT")
    trend_alert_max_per_run: int = Field(2, alias="TREND_ALERT_MAX_PER_RUN")
    trend_alert_max_per_7d: int = Field(2, alias="TREND_ALERT_MAX_PER_7D")
    # Russian LLM briefs (title_ru/insight/opportunity) for stable trend clusters.
    trend_brief_enabled: bool = Field(True, alias="TREND_BRIEF_ENABLED")
    trend_brief_max_per_run: int = Field(20, alias="TREND_BRIEF_MAX_PER_RUN")
    cluster_min_evidence_count: int = Field(2, alias="CLUSTER_MIN_EVIDENCE_COUNT")
    signal_short_window_hours: int = Field(24, alias="SIGNAL_SHORT_WINDOW_HOURS")
    signal_analysis_window_days: int = Field(3, alias="SIGNAL_ANALYSIS_WINDOW_DAYS")
    signal_baseline_window_days: int = Field(14, alias="SIGNAL_BASELINE_WINDOW_DAYS")
    signal_velocity_weight: float = Field(0.14, alias="SIGNAL_VELOCITY_WEIGHT")
    signal_acceleration_weight: float = Field(0.1, alias="SIGNAL_ACCELERATION_WEIGHT")
    change_point_method: str = Field("window", alias="CHANGE_POINT_METHOD")
    change_point_penalty: str = Field("auto", alias="CHANGE_POINT_PENALTY")
    change_point_min_size: int = Field(2, alias="CHANGE_POINT_MIN_SIZE")
    change_point_jump: int = Field(1, alias="CHANGE_POINT_JUMP")
    change_point_recent_hours: int = Field(48, alias="CHANGE_POINT_RECENT_HOURS")
    signal_merge_similarity_threshold: float = Field(0.72, alias="SIGNAL_MERGE_SIMILARITY_THRESHOLD")
    signal_merge_doc_overlap_threshold: float = Field(0.25, alias="SIGNAL_MERGE_DOC_OVERLAP_THRESHOLD")
    persist_weak_signals: bool = Field(True, alias="PERSIST_WEAK_SIGNALS")
    weak_signal_min_score: float = Field(0.42, alias="WEAK_SIGNAL_MIN_SCORE")
    weak_signal_min_confidence: float = Field(0.52, alias="WEAK_SIGNAL_MIN_CONFIDENCE")
    weak_signal_min_source_diversity: float = Field(0.2, alias="WEAK_SIGNAL_MIN_SOURCE_DIVERSITY")
    weak_signal_min_source_count: int = Field(1, alias="WEAK_SIGNAL_MIN_SOURCE_COUNT")
    signal_min_source_count: int = Field(1, alias="SIGNAL_MIN_SOURCE_COUNT")
    april_fools_guard_enabled: bool = Field(True, alias="APRIL_FOOLS_GUARD_ENABLED")
    april_fools_guard_penalty: float = Field(0.45, alias="APRIL_FOOLS_GUARD_PENALTY")
    april_fools_guard_stage_block_ratio: float = Field(0.34, alias="APRIL_FOOLS_GUARD_STAGE_BLOCK_RATIO")
    cluster_evaluation_fixture_path: str = Field(
        "tests/fixtures/cluster_analysis_golden_set.json",
        alias="CLUSTER_EVALUATION_FIXTURE_PATH",
    )

    # Consumer claim / housekeeping
    indexing_claim_idle_ms: int = Field(600_000, alias="INDEXING_CLAIM_IDLE_MS")        # 10 min
    indexing_consumer_cleanup_interval: int = Field(1800, alias="INDEXING_CONSUMER_CLEANUP_INTERVAL")  # 30 min
    vision_enabled: bool = Field(True, alias="VISION_ENABLED")
    vision_routing_enabled: bool = Field(True, alias="VISION_ROUTING_ENABLED")
    vision_claim_idle_ms: int = Field(600_000, alias="VISION_CLAIM_IDLE_MS")
    vision_max_delivery_count: int = Field(5, alias="VISION_MAX_DELIVERY_COUNT")
    vision_dlq_stream: str = Field("stream:posts:vision:dlq", alias="VISION_DLQ_STREAM")
    redis_stream_lag_alert_threshold: int = Field(1000, alias="REDIS_STREAM_LAG_ALERT_THRESHOLD")
    redis_stream_pending_alert_threshold: int = Field(
        100,
        alias="REDIS_STREAM_PENDING_ALERT_THRESHOLD",
    )
    redis_stream_oldest_pending_age_alert_seconds: int = Field(
        900,
        alias="REDIS_STREAM_OLDEST_PENDING_AGE_ALERT_SECONDS",
    )
    prometheus_url: str = Field("http://prometheus:9090", alias="PROMETHEUS_URL")
    xray_profile_registry_path: str = Field(
        "/runtime/xray-profiles.json",
        alias="XRAY_PROFILE_REGISTRY_PATH",
    )
    xray_active_profile_path: str = Field(
        "/runtime/xray-active-profile.txt",
        alias="XRAY_ACTIVE_PROFILE_PATH",
    )
    xray_previous_profile_path: str = Field(
        "/runtime/xray-previous-profile.txt",
        alias="XRAY_PREVIOUS_PROFILE_PATH",
    )
    xray_reload_trigger_path: str = Field(
        "/runtime/xray-reload.trigger",
        alias="XRAY_RELOAD_TRIGGER_PATH",
    )
    xray_probe_proxy_url: str = Field("socks5://xray:10808", alias="XRAY_PROBE_PROXY_URL")
    crawl_browser_proxy_url: str = Field("socks5://xray:10808", alias="CRAWL_BROWSER_PROXY_URL")
    xray_probe_targets: list[str] = Field(
        default=[
            "https://example.com",
            "https://www.google.com/generate_204",
            "https://www.cloudflare.com/cdn-cgi/trace",
        ],
        alias="XRAY_PROBE_TARGETS",
    )
    xray_source_smoke_targets: list[str] = Field(
        default=[
            "https://medium.com/feed/tag/future",
            "https://news.google.com/rss",
            "https://www.mobilityhouse.com/usa_en/our-company/newsroom",
        ],
        alias="XRAY_SOURCE_SMOKE_TARGETS",
    )
    xray_degradation_failure_ratio: float = Field(0.66, alias="XRAY_DEGRADATION_FAILURE_RATIO")
    xray_degradation_consecutive_threshold: int = Field(
        2,
        alias="XRAY_DEGRADATION_CONSECUTIVE_THRESHOLD",
    )
    xray_alert_cooldown_seconds: int = Field(1800, alias="XRAY_ALERT_COOLDOWN_SECONDS")
    xray_auto_remediation_enabled: bool = Field(
        False,
        alias="XRAY_AUTO_REMEDIATION_ENABLED",
    )
    xray_auto_remediation_webhook_url: str = Field(
        "",
        alias="XRAY_AUTO_REMEDIATION_WEBHOOK_URL",
    )
    xray_auto_remediation_cooldown_seconds: int = Field(
        1800,
        alias="XRAY_AUTO_REMEDIATION_COOLDOWN_SECONDS",
    )

    @property
    def configured_embedding_dim_matches_model(self) -> bool:
        expected = expected_embedding_dim(self.gigachat_embeddings_model)
        return expected is None or expected == self.embed_dim


@lru_cache
def get_settings() -> Settings:
    return Settings()
