"""Общие guard'ы для MCP-инструментов: SSRF-защита enqueue-URL и allowlist workspace.

Живёт в mcp/, чтобы не редактировать shared/ или worker/. Логирование — stdlib logging
(как остальной mcp), конфиг workspace читается один раз с ленивой инициализацией (module cache).
"""
from __future__ import annotations

import asyncio
import ipaddress
import logging
import re
import socket
from pathlib import Path
from urllib.parse import urlparse

from fastapi import HTTPException

logger = logging.getLogger(__name__)

# ── SSRF guard ──────────────────────────────────────────────────────────────
#
# Внутренние DNS-имена сервисов стека (docker-compose network). crawl4ai-консьюмер
# ходит по enqueue-URL напрямую, поэтому запрос к ним = обращение к внутренней сети.
# Совпадает по духу с worker/services/searxng_client.py::_PRIVATE_HOSTS, но список
# шире (весь стек), т.к. это точка приёма произвольного пользовательского URL.
_BLOCKED_HOSTNAMES = frozenset(
    {
        "localhost",
        "redis",
        "qdrant",
        "neo4j",
        "postgres",
        "admin",
        "mcp",
        "mcp-gateway",
        "gpt2giga-proxy",
        "prometheus",
        "alertmanager",
        "grafana",
        "searxng",
        "crawl4ai",
        "worker",
        "ingest",
        "paddleocr",
        "xray",
    }
)


def _is_disallowed_ip(host: str) -> bool:
    """True, если host — это IP-литерал из приватного/служебного диапазона."""
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    return bool(
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_reserved
        or address.is_multicast
        or address.is_unspecified
    )


async def _resolve_addresses(host: str) -> list[str]:
    """Неблокирующий getaddrinfo (getaddrinfo — блокирующий, уводим в executor)."""
    loop = asyncio.get_event_loop()
    try:
        infos = await loop.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:  # не резолвится — не пускаем (fail-closed)
        logger.warning("ssrf_guard dns_resolve_failed host=%s err=%s", host[:80], exc)
        raise HTTPException(status_code=400, detail="url host does not resolve") from exc
    return [sockaddr[0] for (*_, sockaddr) in infos if sockaddr and sockaddr[0]]


async def assert_public_http_url(url: str) -> None:
    """Отклонить URL, ведущий к приватному/внутреннему адресу (SSRF-защита).

    Проверяет: схема ∈ {http, https}; host не является внутренним DNS-именем стека;
    ни один из резолвнутых IP не является приватным/loopback/link-local/reserved/
    multicast. Блокирующий resolve вынесен в executor. При отказе — HTTPException 400.
    """
    parsed = urlparse((url or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise HTTPException(status_code=400, detail="url must be http(s)")

    host = (parsed.hostname or "").strip().lower().rstrip(".")
    if not host:
        raise HTTPException(status_code=400, detail="url must have a host")

    if host in _BLOCKED_HOSTNAMES:
        raise HTTPException(status_code=400, detail="url resolves to a private/internal address")

    # host может быть уже IP-литералом (в т.ч. IPv6 в скобках, urlparse отдаёт без скобок)
    if _is_disallowed_ip(host):
        raise HTTPException(status_code=400, detail="url resolves to a private/internal address")

    for ip in await _resolve_addresses(host):
        if _is_disallowed_ip(ip):
            logger.warning("ssrf_guard blocked host=%s ip=%s", host[:80], ip)
            raise HTTPException(
                status_code=400, detail="url resolves to a private/internal address"
            )


# ── Workspace allowlist ─────────────────────────────────────────────────────
#
# Слаги workspace форвардятся как фильтр в Qdrant/PG/Neo4j. Валидируем против
# config/workspaces.yml (единый источник bootstrap'а). Кэш модульного уровня —
# читаем файл один раз. НЕ хардкодим список: fallback-набор используется только
# если файл недоступен в образе (mcp/Dockerfile делает `COPY config/ /app/config/`,
# поэтому в штатной сборке читается файл, а fallback — страховка).

# Кандидаты пути к workspaces.yml: /app/config (как bootstrap_configs) и repo-relative
# (mcp/guards.py → ../config/workspaces.yml) для локального dev/тестов.
_WORKSPACES_CONFIG_CANDIDATES = (
    Path("/app/config/workspaces.yml"),
    Path(__file__).resolve().parent.parent / "config" / "workspaces.yml",
)

# Fallback только на случай отсутствия файла в образе — держим синхронно с config/workspaces.yml.
_FALLBACK_WORKSPACE_SLUGS = frozenset(
    {"disruption", "ai_trends", "ai_research", "ai_products_media", "design", "auto_hmi"}
)

_ID_LINE_RE = re.compile(r"^\s*-\s*id\s*:\s*[\"']?([A-Za-z0-9_-]+)[\"']?\s*$")

_workspace_allowlist: frozenset[str] | None = None


def _parse_workspace_slugs(raw_text: str) -> set[str]:
    """Извлечь слаги из workspaces.yml.

    Пробуем PyYAML (если установлен — worker/admin-образы), иначе построчный разбор
    `- id: <slug>` под `workspaces:` (mcp-образ без PyYAML). Структура файла плоская
    и регулярная, поэтому построчный разбор безопасен.
    """
    try:
        import yaml  # noqa: PLC0415 — опциональная зависимость, есть не во всех образах

        data = yaml.safe_load(raw_text) or {}
        entries = data.get("workspaces", []) if isinstance(data, dict) else []
        return {
            str(entry["id"]).strip()
            for entry in entries
            if isinstance(entry, dict) and entry.get("id")
        }
    except ModuleNotFoundError:
        pass
    except Exception:  # битый yaml — не глушим молча
        logger.warning("workspace_allowlist yaml_parse_failed, falling back to line scan")

    slugs: set[str] = set()
    for line in raw_text.splitlines():
        match = _ID_LINE_RE.match(line)
        if match:
            slugs.add(match.group(1).strip())
    return slugs


def _load_workspace_allowlist() -> frozenset[str]:
    for path in _WORKSPACES_CONFIG_CANDIDATES:
        try:
            raw_text = path.read_text(encoding="utf-8")
        except (FileNotFoundError, OSError):
            continue
        slugs = _parse_workspace_slugs(raw_text)
        if slugs:
            logger.info("workspace_allowlist loaded path=%s count=%d", path, len(slugs))
            return frozenset(slugs)
        logger.warning("workspace_allowlist empty_after_parse path=%s", path)

    logger.warning(
        "workspace_allowlist config not found in %s; using baked-in fallback %s "
        "(mcp/Dockerfile does COPY config/ — отсутствие файла значит устаревший образ)",
        [str(p) for p in _WORKSPACES_CONFIG_CANDIDATES],
        sorted(_FALLBACK_WORKSPACE_SLUGS),
    )
    return _FALLBACK_WORKSPACE_SLUGS


def get_workspace_allowlist() -> frozenset[str]:
    """Разрешённые workspace-слаги (кэш модульного уровня, ленивая инициализация)."""
    global _workspace_allowlist
    if _workspace_allowlist is None:
        _workspace_allowlist = _load_workspace_allowlist()
    return _workspace_allowlist


def assert_known_workspace(workspace: str | None) -> None:
    """Отклонить неизвестный workspace-слаг (tenancy allowlist).

    None/пусто пропускается (инструменты с опциональным workspace-фильтром трактуют
    его как «без фильтра»). Непустое значение обязано быть в allowlist, иначе 400.
    """
    if workspace is None:
        return
    slug = str(workspace).strip()
    if not slug:
        return
    if slug not in get_workspace_allowlist():
        logger.warning("unknown_workspace rejected slug=%s", slug[:64])
        raise HTTPException(status_code=400, detail="unknown workspace")
