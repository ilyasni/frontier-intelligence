"""Shared source type definitions, presets, and config normalization helpers."""
from __future__ import annotations

from copy import deepcopy
from typing import Any

SOURCE_TYPE_TELEGRAM = "telegram"
SOURCE_TYPE_RSS = "rss"
SOURCE_TYPE_WEB = "web"
SOURCE_TYPE_API = "api"
SOURCE_TYPE_EMAIL = "email"
SOURCE_TYPE_HABR = "habr"  # deprecated alias kept for compatibility

CANONICAL_SOURCE_TYPES = (
    SOURCE_TYPE_TELEGRAM,
    SOURCE_TYPE_RSS,
    SOURCE_TYPE_WEB,
    SOURCE_TYPE_API,
    SOURCE_TYPE_EMAIL,
)

ACCEPTED_SOURCE_TYPES = CANONICAL_SOURCE_TYPES + (SOURCE_TYPE_HABR,)

RSS_PRESETS: dict[str, dict[str, str]] = {
    "habr": {
        "name": "Habr Featured",
        "url": "https://habr.com/ru/rss/flows/featured/",
    },
    "habr_design_articles": {
        "name": "Habr Design Articles",
        "url": "https://habr.com/ru/flows/design/articles/",
    },
    "habr_ai_hub": {
        "name": "Habr Artificial Intelligence Hub",
        "url": "https://habr.com/ru/hubs/artificial_intelligence/",
    },
    "vc": {
        "name": "VC.ru",
        "url": "https://vc.ru/rss/all",
    },
    "techcrunch": {
        "name": "TechCrunch",
        "url": "https://techcrunch.com/feed/",
    },
    "wired_main": {
        "name": "WIRED Main",
        "url": "https://www.wired.com/feed/rss",
    },
    "wired_ai": {
        "name": "WIRED AI",
        "url": "https://www.wired.com/feed/tag/ai/latest/rss",
    },
    "wired_business": {
        "name": "WIRED Business",
        "url": "https://www.wired.com/feed/category/business/latest/rss",
    },
    "wired_backchannel": {
        "name": "WIRED Backchannel",
        "url": "https://www.wired.com/feed/category/backchannel/latest/rss",
    },
    "medium_ai": {
        "name": "Medium Artificial Intelligence",
        "url": "https://medium.com/feed/tag/artificial-intelligence",
    },
    "medium_future": {
        "name": "Medium Future",
        "url": "https://medium.com/feed/tag/future",
    },
    "medium_design": {
        "name": "Medium Design",
        "url": "https://medium.com/feed/tag/design",
    },
    "medium_mobility": {
        "name": "Medium Mobility",
        "url": "https://medium.com/feed/tag/mobility",
    },
    "medium_autonomous_vehicles": {
        "name": "Medium Autonomous Vehicles",
        "url": "https://medium.com/feed/tag/autonomous-vehicles",
    },
    "medium_product_design": {
        "name": "Medium Product Design",
        "url": "https://medium.com/feed/tag/product-design",
    },
    "medium_ux": {
        "name": "Medium UX",
        "url": "https://medium.com/feed/tag/ux",
    },
    "medium_ui": {
        "name": "Medium UI",
        "url": "https://medium.com/feed/tag/ui",
    },
    "medium_ux_ui": {
        "name": "Medium UX/UI",
        "url": "https://medium.com/feed/tag/ux-ui",
    },
    "medium_user_experience": {
        "name": "Medium User Experience",
        "url": "https://medium.com/feed/tag/user-experience",
    },
    "ux_collective": {
        "name": "UX Collective",
        "url": "https://uxdesign.cc/feed",
    },
    "muzli": {
        "name": "Muzli",
        "url": "https://medium.muz.li/feed",
    },
    "baymard_subscribe": {
        "name": "Baymard Subscribe",
        "url": "https://baymard.com/blog/subscribe",
    },
    "mit_tech_review": {
        "name": "MIT Technology Review",
        "url": "https://www.technologyreview.com/feed/",
    },
    "arxiv_cs_ai": {
        "name": "arXiv cs.AI",
        "url": "https://rss.arxiv.org/rss/cs.AI",
    },
    "arxiv_cs_lg": {
        "name": "arXiv cs.LG",
        "url": "https://rss.arxiv.org/rss/cs.LG",
    },
    "arxiv_cs_hc": {
        "name": "arXiv cs.HC",
        "url": "https://rss.arxiv.org/rss/cs.HC",
    },
    "arxiv_cs_ro": {
        "name": "arXiv cs.RO",
        "url": "https://rss.arxiv.org/rss/cs.RO",
    },
    "arxiv_cs_ai_cs_ro": {
        "name": "arXiv cs.AI + cs.RO",
        "url": "https://rss.arxiv.org/rss/cs.AI+cs.RO",
    },
    "insideevs_all": {
        "name": "InsideEVs All Articles",
        "url": "https://insideevs.com/rss/articles/all/",
    },
    "insideevs_autonomous": {
        "name": "InsideEVs Autonomous Vehicles",
        "url": "https://insideevs.com/rss/category/autonomous-vehicles/",
    },
    "insideevs_design": {
        "name": "InsideEVs Design",
        "url": "https://insideevs.com/rss/category/design/",
    },
    "insideevs_ux": {
        "name": "InsideEVs User Experience",
        "url": "https://insideevs.com/rss/category/user-experience/",
    },
    "electrek": {
        "name": "Electrek",
        "url": "https://electrek.co/feed",
    },
    "tgstat": {
        "name": "TGStat Feed",
        "url": "https://tgstat.ru/rss",
    },
}

DEFAULT_EXTRA_TEMPLATE: dict[str, Any] = {
    "preset": "custom",
    "quality_tier": "standard",
    "expected_signal_types": [],
    "source_authority": 0.5,
    "source_region": "global",
    "market_scope": "global",
    "fetch": {
        "lookback_hours": 24,
        "max_items_per_run": 50,
        "timeout_sec": 20,
        "use_conditional_get": True,
    },
    "filters": {
        "include_keywords": [],
        "exclude_keywords": [],
        "lang_allow": [],
    },
    "parse": {
        "full_content": False,
        "extract_author": True,
        "extract_tags": True,
        "listing_selector": "",
        "link_selector": "",
        "title_selector": "",
        "date_selector": "",
        "article_selector": "",
        "field_map": {},
    },
    "vision": {
        "mode": "full",
        "max_media_bytes": 9_000_000,
    },
    "dedupe": {
        "strategy": "guid_or_url",
        "canonicalize_url": True,
    },
}

SOURCE_DEFAULT_OVERRIDES: dict[str, dict[str, Any]] = {
    SOURCE_TYPE_TELEGRAM: {
        "fetch": {
            "lookback_hours": 24,
            "max_items_per_run": 200,
            "timeout_sec": 20,
        },
    },
    SOURCE_TYPE_WEB: {
        "fetch": {
            "max_items_per_run": 20,
        },
        "parse": {
            "full_content": True,
        },
    },
    SOURCE_TYPE_API: {
        "fetch": {
            "timeout_sec": 30,
        },
        "parse": {
            "format": "json",
            "field_map": {
                "items_path": "",
                "id": "id",
                "url": "url",
                "title": "title",
                "content": "content",
                "summary": "summary",
                "author": "author",
                "published_at": "published_at",
                "tags": "tags",
                "linked_urls": "linked_urls",
                "next_cursor": "",
            },
        },
    },
    SOURCE_TYPE_EMAIL: {
        "fetch": {
            "protocol": "imap",
            "mailbox": "INBOX",
            "search": "ALL",
            "username": "",
            "password": "",
            "host": "",
            "port": 993,
            "use_ssl": True,
        },
        "parse": {
            "summary_from_email": True,
        },
    },
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def canonical_source_type(source_type: str) -> str:
    normalized = (source_type or "").strip().lower()
    if normalized == SOURCE_TYPE_HABR:
        return SOURCE_TYPE_RSS
    return normalized


def default_extra_for_source(source_type: str, preset: str = "custom") -> dict[str, Any]:
    normalized_type = canonical_source_type(source_type)
    extra = deepcopy(DEFAULT_EXTRA_TEMPLATE)
    extra["preset"] = preset or "custom"
    return _deep_merge(extra, SOURCE_DEFAULT_OVERRIDES.get(normalized_type, {}))


def normalize_source_extra(source_type: str, extra: dict[str, Any] | None) -> dict[str, Any]:
    incoming = extra if isinstance(extra, dict) else {}
    preset = str(incoming.get("preset") or "custom")
    normalized = default_extra_for_source(source_type, preset=preset)
    normalized = _deep_merge(normalized, incoming)
    for key in ("fetch", "filters", "parse", "vision", "dedupe"):
        if not isinstance(normalized.get(key), dict):
            normalized[key] = deepcopy(DEFAULT_EXTRA_TEMPLATE[key])
    if not isinstance(normalized.get("expected_signal_types"), list):
        normalized["expected_signal_types"] = []
    try:
        normalized["source_authority"] = float(normalized.get("source_authority", 0.5) or 0.5)
    except (TypeError, ValueError):
        normalized["source_authority"] = 0.5
    normalized["source_authority"] = max(0.0, min(1.0, normalized["source_authority"]))
    normalized["quality_tier"] = str(normalized.get("quality_tier") or "standard")
    normalized["source_region"] = str(normalized.get("source_region") or "global").strip().lower()
    normalized["market_scope"] = str(normalized.get("market_scope") or "global").strip().lower()

    vision = normalized.get("vision") or {}
    if not isinstance(vision, dict):
        vision = {}
    mode = str(vision.get("mode") or "full").strip().lower()
    if mode not in {"full", "ocr_only", "skip"}:
        mode = "full"
    try:
        max_media_bytes = int(vision.get("max_media_bytes") or 9_000_000)
    except (TypeError, ValueError):
        max_media_bytes = 9_000_000
    normalized["vision"] = {
        **vision,
        "mode": mode,
        "max_media_bytes": max(0, max_media_bytes),
    }
    return normalized


def apply_source_preset(
    source_type: str,
    url: str | None,
    extra: dict[str, Any],
) -> tuple[str | None, dict[str, Any]]:
    normalized_type = canonical_source_type(source_type)
    normalized_extra = normalize_source_extra(normalized_type, extra)
    preset = str(normalized_extra.get("preset") or "custom")
    if normalized_type == SOURCE_TYPE_RSS and preset in RSS_PRESETS:
        url = url or RSS_PRESETS[preset]["url"]
    return url, normalized_extra


def validate_source_payload(
    source_type: str,
    url: str | None,
    tg_channel: str | None,
    extra: dict[str, Any] | None,
) -> tuple[str, str | None, str | None, dict[str, Any]]:
    normalized_type = canonical_source_type(source_type)
    if normalized_type not in CANONICAL_SOURCE_TYPES:
        raise ValueError(f"Unsupported source_type={source_type!r}")

    normalized_url, normalized_extra = apply_source_preset(normalized_type, url, extra or {})

    if normalized_type == SOURCE_TYPE_TELEGRAM:
        if not tg_channel:
            raise ValueError("tg_channel required for telegram sources")
    elif normalized_type in (SOURCE_TYPE_RSS, SOURCE_TYPE_WEB, SOURCE_TYPE_API):
        if not normalized_url:
            raise ValueError(f"url required for {normalized_type} sources")
    elif normalized_type == SOURCE_TYPE_EMAIL:
        fetch = normalized_extra.get("fetch") or {}
        if not fetch.get("host") or not fetch.get("username"):
            raise ValueError("email sources require extra.fetch.host and extra.fetch.username")

    return normalized_type, normalized_url, tg_channel, normalized_extra
