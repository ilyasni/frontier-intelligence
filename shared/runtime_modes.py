"""Runtime mode definitions shared by admin, worker, and MCP."""
from __future__ import annotations

from copy import deepcopy
from typing import Any

RUNTIME_MODE_REDIS_KEY = "frontier:runtime:mode"
RUNTIME_MODE_DB_KEY = "runtime_mode"

RUNTIME_MODE_CUSTOM = "custom"
RUNTIME_MODE_FULL_VISION = "full-vision"
RUNTIME_MODE_NO_VISION = "no-vision"
RUNTIME_MODE_GIGACHAT_2_ONLY = "gigachat-2-only"

_COMMON_GIGACHAT_2 = {
    "gigachat_model": "GigaChat-2",
    "gigachat_model_lite": "GigaChat-2",
    "gigachat_model_relevance": "GigaChat-2",
    "gigachat_model_concepts": "GigaChat-2",
    "gigachat_model_valence": "GigaChat-2",
    "gigachat_model_mcp_synthesis": "GigaChat-2",
    "gigachat_session_cache_enabled": True,
    "gigachat_token_budget_relevance": 1500,
    "gigachat_token_budget_concepts": 1500,
    "gigachat_token_budget_valence": 1200,
    "gigachat_token_budget_relevance_concepts": 1800,
    "gigachat_token_budget_embed": 1200,
    "gigachat_token_budget_vision_prompt": 600,
    "gigachat_relevance_gray_zone": 0.1,
    "gigachat_max_simultaneous_requests": 1,
    "gigachat_min_request_interval_ms": 250,
    "indexing_max_concurrency": 1,
}

RUNTIME_MODE_DEFINITIONS: dict[str, dict[str, Any]] = {
    RUNTIME_MODE_CUSTOM: {
        "label": "Custom env",
        "description": "Use environment variables as configured on the running containers.",
        "overrides": {},
    },
    RUNTIME_MODE_FULL_VISION: {
        "label": "Full Vision",
        "description": "Analyze images with GigaChat Vision and optional PaddleOCR.",
        "overrides": {
            **_COMMON_GIGACHAT_2,
            "runtime_mode": RUNTIME_MODE_FULL_VISION,
            "vision_enabled": True,
            "gpt2giga_enable_images": True,
            "gigachat_model_pro": "GigaChat-2-Pro",
            "gigachat_model_max": "GigaChat-2-Max",
            "gigachat_model_vision": "GigaChat-2-Pro",
            "gigachat_escalation_enabled": True,
            "paddleocr_url": "http://paddleocr:8008",
        },
    },
    RUNTIME_MODE_NO_VISION: {
        "label": "No Vision",
        "description": "Skip image analysis; keep text enrichment and Pro/Max fallbacks.",
        "overrides": {
            **_COMMON_GIGACHAT_2,
            "runtime_mode": RUNTIME_MODE_NO_VISION,
            "vision_enabled": False,
            "gpt2giga_enable_images": False,
            "gigachat_model_pro": "GigaChat-2-Pro",
            "gigachat_model_max": "GigaChat-2-Max",
            "gigachat_model_vision": "",
            "gigachat_escalation_enabled": True,
            "paddleocr_url": "",
        },
    },
    RUNTIME_MODE_GIGACHAT_2_ONLY: {
        "label": "GigaChat-2 only",
        "description": "Skip image analysis and keep every chat task on regular GigaChat-2.",
        "overrides": {
            **_COMMON_GIGACHAT_2,
            "runtime_mode": RUNTIME_MODE_GIGACHAT_2_ONLY,
            "vision_enabled": False,
            "gpt2giga_enable_images": False,
            "gigachat_model_pro": "GigaChat-2",
            "gigachat_model_max": "GigaChat-2",
            "gigachat_model_vision": "GigaChat-2",
            "gigachat_escalation_enabled": False,
            "paddleocr_url": "",
        },
    },
}


def normalize_runtime_mode(mode: str | None) -> str:
    normalized = str(mode or "").strip().lower()
    aliases = {
        "full": RUNTIME_MODE_FULL_VISION,
        "vision": RUNTIME_MODE_FULL_VISION,
        "text-only": RUNTIME_MODE_NO_VISION,
        "no_vision": RUNTIME_MODE_NO_VISION,
        "giga-only": RUNTIME_MODE_GIGACHAT_2_ONLY,
        "economy": RUNTIME_MODE_GIGACHAT_2_ONLY,
    }
    normalized = aliases.get(normalized, normalized)
    if normalized in RUNTIME_MODE_DEFINITIONS:
        return normalized
    return RUNTIME_MODE_CUSTOM


def runtime_mode_options() -> list[dict[str, str]]:
    return [
        {
            "id": mode,
            "label": str(definition["label"]),
            "description": str(definition["description"]),
        }
        for mode, definition in RUNTIME_MODE_DEFINITIONS.items()
    ]


def runtime_overrides_for_mode(mode: str | None) -> dict[str, Any]:
    normalized = normalize_runtime_mode(mode)
    return deepcopy(RUNTIME_MODE_DEFINITIONS[normalized]["overrides"])


def resolve_runtime_value(settings: Any, overrides: dict[str, Any], name: str, default: Any = None) -> Any:
    if name in overrides:
        return overrides[name]
    return getattr(settings, name, default)


def effective_runtime_snapshot(settings: Any, mode: str | None) -> dict[str, Any]:
    overrides = runtime_overrides_for_mode(mode)

    def value(name: str, default: Any = None) -> Any:
        return resolve_runtime_value(settings, overrides, name, default)

    return {
        "runtime_mode": normalize_runtime_mode(value("runtime_mode", mode)),
        "vision_enabled": bool(value("vision_enabled", True)),
        "gpt2giga_enable_images": bool(value("gpt2giga_enable_images", True)),
        "paddleocr_url": str(value("paddleocr_url", "") or ""),
        "gigachat_model": str(value("gigachat_model", "GigaChat-2") or ""),
        "gigachat_model_lite": str(value("gigachat_model_lite", "GigaChat-2") or ""),
        "gigachat_model_pro": str(value("gigachat_model_pro", "GigaChat-2-Pro") or ""),
        "gigachat_model_max": str(value("gigachat_model_max", "GigaChat-2-Max") or ""),
        "gigachat_model_relevance": str(value("gigachat_model_relevance", "") or ""),
        "gigachat_model_concepts": str(value("gigachat_model_concepts", "") or ""),
        "gigachat_model_valence": str(value("gigachat_model_valence", "") or ""),
        "gigachat_model_mcp_synthesis": str(value("gigachat_model_mcp_synthesis", "") or ""),
        "gigachat_model_vision": str(value("gigachat_model_vision", "") or ""),
        "gigachat_escalation_enabled": bool(value("gigachat_escalation_enabled", True)),
        "indexing_max_concurrency": int(value("indexing_max_concurrency", 1) or 1),
    }
