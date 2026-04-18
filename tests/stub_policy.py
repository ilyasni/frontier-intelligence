"""
Политика глобальных MagicMock в sys.modules (tests/conftest.py).

Рекомендация pytest: предпочитать monkeypatch.setattr / фикстуры для конкретных тестов,
а не подмену целых пакетов-драйверов — иначе интеграционные тесты «молча» проходят на моках.

См. pytest: how-to monkeypatch (локальные подмены вместо глобальных побочных эффектов).
"""

from __future__ import annotations

# Корни имён модулей, которые нельзя регистрировать через sys.modules.setdefault(..., MagicMock()).
# Расширяй при появлении интеграционных тестов с реальным драйвером (redis, asyncpg, …).
FORBIDDEN_ROOT_MODULES: frozenset[str] = frozenset({
    "neo4j",
})

# Модули, целиком подменяемые plain MagicMock() в conftest (единый список для ревью и теста политики).
GLOBAL_MAGICMOCK_STUB_MODULES: tuple[str, ...] = (
    "socks",
    "crawl4ai",
    "qdrant_client",
    "qdrant_client.models",
    "fastembed",
    "asyncpg",
    "langchain_core",
    "langchain_core.prompts",
    "langchain_openai",
    "croniter",
)
