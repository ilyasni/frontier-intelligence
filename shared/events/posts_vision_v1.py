import json
import uuid
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


def normalize_vision_stream_fields(data: dict) -> dict:
    """
    Сырой dict из Redis Stream: RedisClient.xadd сериализует list/dict через
    json.dumps → в XREADGROUP значения — строки (см. redis-py streams).
    """
    out = dict(data)
    raw = out.get("media_s3_keys")
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw.strip() or "[]")
            out["media_s3_keys"] = [str(x) for x in parsed] if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            out["media_s3_keys"] = []
    elif raw is None:
        out["media_s3_keys"] = []
    elif isinstance(raw, list):
        out["media_s3_keys"] = [str(x) for x in raw]
    out["grouped_id"] = _normalize_grouped_id(out.get("grouped_id"))
    for fld, default in (("album_item_index", 0), ("album_total_items", 1)):
        out[fld] = _coerce_int(out.get(fld), default)
    out["vision_mode"] = str(out.get("vision_mode") or "full").strip().lower() or "full"
    out["max_media_bytes"] = _coerce_int(out.get("max_media_bytes"), 9_000_000)
    return out


def _normalize_grouped_id(v: object) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        s = v.strip()
        if not s or s.lower() in {"null", "none"}:
            return ""
        return s
    return str(v)


def _coerce_int(v: object, default: int) -> int:
    if isinstance(v, int) and not isinstance(v, bool):
        return v
    if isinstance(v, str) and v.strip():
        try:
            return int(v.strip(), 10)
        except ValueError:
            return default
    return default


class PostVisionEvent(BaseModel):
    """Событие vision: допускает лишние поля из Redis (игнор)."""

    model_config = ConfigDict(extra="ignore")

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    post_id: str
    workspace_id: str
    source_id: str
    grouped_id: str
    media_s3_keys: list[str] = Field(default_factory=list)
    album_item_index: int = 0
    album_total_items: int = 1
    vision_mode: str = "full"  # full | ocr_only | skip
    max_media_bytes: int = 9_000_000

    @model_validator(mode="before")
    @classmethod
    def _normalize_redis_stream_payload(cls, data: Any) -> Any:
        """До разбора полей — привести wire-формат Redis к типам модели."""
        if isinstance(data, dict):
            return normalize_vision_stream_fields(data)
        return data
