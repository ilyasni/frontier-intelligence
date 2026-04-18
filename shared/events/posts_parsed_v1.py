import json
import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class PostParsedEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    post_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    workspace_id: str
    source_id: str
    external_id: str
    content: str
    grouped_id: Optional[str] = None
    has_media: bool = False
    media_urls: list[str] = Field(default_factory=list)
    # Внешние URL из текста/entities — для stream:posts:crawl (не t.me permalink)
    linked_urls: list[str] = Field(default_factory=list)
    published_at: Optional[datetime] = None
    url: Optional[str] = None
    author: Optional[str] = None
    extra: dict = Field(default_factory=dict)

    @field_validator("media_urls", "linked_urls", mode="before")
    @classmethod
    def _coerce_str_list(cls, v: object) -> list[str]:
        if v is None:
            return []
        if isinstance(v, list):
            return [str(x) for x in v]
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return []
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return [str(x) for x in parsed]
            except json.JSONDecodeError:
                pass
            return []
        return []

    @field_validator("extra", mode="before")
    @classmethod
    def _coerce_extra_dict(cls, v: object) -> dict:
        if v is None:
            return {}
        if isinstance(v, dict):
            return v
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return {}
            try:
                parsed = json.loads(s)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                pass
            return {}
        return {}

    @field_validator("has_media", mode="before")
    @classmethod
    def _coerce_bool(cls, v: object) -> bool:
        if isinstance(v, str):
            return v.lower() in ("1", "true", "yes")
        return bool(v)

    @field_validator("published_at", mode="before")
    @classmethod
    def _coerce_published_at(cls, v: object) -> object:
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip()
            if not s or s.lower() in {"null", "none"}:
                return None
        return v

    @field_validator("grouped_id", mode="before")
    @classmethod
    def _coerce_grouped_id(cls, v: object) -> object:
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip()
            if not s or s.lower() in {"null", "none"}:
                return None
            return s
        return str(v)
