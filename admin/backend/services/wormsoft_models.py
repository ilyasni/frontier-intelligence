"""Wormsoft model catalog lookup for the admin UI."""

from __future__ import annotations

from typing import Any

import httpx

from shared.config import get_settings


async def fetch_wormsoft_models() -> dict[str, Any]:
    settings = get_settings()
    base_url = str(settings.wormsoft_api_base or "").rstrip("/")
    key_present = bool(settings.wormsoft_api_key)
    if not base_url:
        return {
            "available": False,
            "status": "missing_base_url",
            "key_present": key_present,
            "models": [],
        }
    if not key_present:
        return {
            "available": False,
            "status": "missing_api_key",
            "key_present": False,
            "models": [],
        }

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.get(
                f"{base_url}/models",
                headers={"Authorization": f"Bearer {settings.wormsoft_api_key}"},
            )
            resp.raise_for_status()
            payload = resp.json()
    except Exception as exc:
        return {
            "available": False,
            "status": "request_error",
            "error": str(exc),
            "key_present": True,
            "models": [],
        }

    raw_models = payload if isinstance(payload, list) else payload.get("data") or payload.get("models") or []
    models: list[dict[str, str]] = []
    for item in raw_models:
        if isinstance(item, str):
            model_id = item.strip()
            if not model_id:
                continue
            models.append({"id": model_id, "label": model_id, "provider": "wormsoft"})
            continue
        if not isinstance(item, dict):
            continue
        model_id = str(item.get("id") or item.get("name") or "").strip()
        if not model_id:
            continue
        models.append(
            {
                "id": model_id,
                "label": str(item.get("name") or model_id).strip() or model_id,
                "provider": "wormsoft",
            }
        )

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in models:
        key = item["id"]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    return {
        "available": True,
        "status": "ok",
        "key_present": True,
        "models": deduped,
        "count": len(deduped),
        "base_url": base_url,
    }
