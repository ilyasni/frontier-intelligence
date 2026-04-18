from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator

SignalValence = Literal["positive", "neutral", "negative"]

COUNTER_SIGNAL_TYPES = (
    "failure",
    "closure",
    "rejection",
    "delay",
    "recall",
    "lawsuit",
    "ban",
)


class SearchRequest(BaseModel):
    model_config = {"extra": "forbid"}

    query: str
    workspace: str = Field(default="disruption")
    limit: int = Field(default=10, ge=1, le=50)
    synthesize: bool = False
    lang: str | None = Field(default=None, min_length=2, max_length=16)
    days_back: int | None = Field(default=None, ge=1, le=365)
    valence: SignalValence | None = None
    signal_type: str | None = Field(default=None, min_length=2, max_length=32)
    source_region: str | None = Field(default=None, min_length=2, max_length=24)
    entities: list[str] | None = None

    @field_validator("query")
    @classmethod
    def _validate_query(cls, value: str) -> str:
        text = (value or "").strip()
        if not text:
            raise ValueError("query must not be empty")
        return text

    @field_validator("lang", "signal_type", "source_region", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip().lower()
        return text or None

    @field_validator("entities", mode="before")
    @classmethod
    def _normalize_entities(cls, value: object) -> list[str] | None:
        if value is None:
            return None
        if isinstance(value, str):
            items = [chunk.strip() for chunk in value.split(",")]
        elif isinstance(value, list):
            items = [str(chunk).strip() for chunk in value]
        else:
            return None
        normalized = [item for item in items if item]
        return normalized[:20] or None


class BalancedSearchRequest(BaseModel):
    model_config = {"extra": "forbid"}

    query: str
    workspace: str = Field(default="disruption")
    limit: int = Field(default=10, ge=1, le=50)
    synthesize: bool = True
    lang: str | None = Field(default=None, min_length=2, max_length=16)
    source_region: str | None = Field(default=None, min_length=2, max_length=24)
    entities: list[str] | None = None
    days_back: int | None = Field(default=7, ge=1, le=365)

    @field_validator("query")
    @classmethod
    def _validate_query(cls, value: str) -> str:
        text = (value or "").strip()
        if not text:
            raise ValueError("query must not be empty")
        return text

    @field_validator("lang", "source_region", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip().lower()
        return text or None

    @field_validator("entities", mode="before")
    @classmethod
    def _normalize_entities(cls, value: object) -> list[str] | None:
        if value is None:
            return None
        if isinstance(value, str):
            items = [chunk.strip() for chunk in value.split(",")]
        elif isinstance(value, list):
            items = [str(chunk).strip() for chunk in value]
        else:
            return None
        normalized = [item for item in items if item]
        return normalized[:20] or None
