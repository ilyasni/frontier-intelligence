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
    qdrant_trends_collection: str = Field("trend_clusters", alias="QDRANT_TRENDS_COLLECTION")

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

    # SearXNG
    searxng_url: str = Field("http://searxng:8080", alias="SEARXNG_URL")
    searxng_enabled: bool = Field(True, alias="SEARXNG_ENABLED")
    searxng_user: str = Field("", alias="SEARXNG_USER")
    searxng_password: str = Field("", alias="SEARXNG_PASSWORD")
    searxng_timeout_seconds: float = Field(8.0, alias="SEARXNG_TIMEOUT_SECONDS")
    searxng_cache_ttl: int = Field(3600, alias="SEARXNG_CACHE_TTL")
    searxng_max_results: int = Field(5, alias="SEARXNG_MAX_RESULTS")
    searxng_categories: str = Field("general,news", alias="SEARXNG_CATEGORIES")
    missing_signals_enabled: bool = Field(True, alias="MISSING_SIGNALS_ENABLED")
    missing_signals_window_days: int = Field(30, alias="MISSING_SIGNALS_WINDOW_DAYS")
    missing_signals_topic_limit: int = Field(8, alias="MISSING_SIGNALS_TOPIC_LIMIT")
    missing_signals_min_gap_score: float = Field(0.35, alias="MISSING_SIGNALS_MIN_GAP_SCORE")
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
    admin_gigachat_balance_refresh_cron: str = Field(
        "*/5 * * * *",
        alias="ADMIN_GIGACHAT_BALANCE_REFRESH_CRON",
    )
    admin_trend_alert_cron: str = Field(
        "25 * * * *",
        alias="ADMIN_TREND_ALERT_CRON",
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

    @property
    def configured_embedding_dim_matches_model(self) -> bool:
        expected = expected_embedding_dim(self.gigachat_embeddings_model)
        return expected is None or expected == self.embed_dim


@lru_cache
def get_settings() -> Settings:
    return Settings()
