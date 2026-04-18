"""Semantic search over persisted trend clusters."""
from __future__ import annotations

from typing import Literal

from fastapi import APIRouter
from pydantic import BaseModel, Field, field_validator

from shared.config import get_settings
from worker.integrations.qdrant_client import QdrantFrontierClient

from .search_frontier import _get_embedding

router = APIRouter()
SignalStage = Literal["weak", "emerging", "stable", "fading"]


class TrendClusterSearchRequest(BaseModel):
    model_config = {"extra": "forbid"}

    query: str
    workspace: str = Field(default="disruption", min_length=1)
    limit: int = Field(default=10, ge=1, le=50)
    pipeline: Literal["stable", "reactive"] | None = "stable"
    stages: list[SignalStage] | None = None
    days_back: int | None = Field(default=None, ge=1, le=365)

    @field_validator("query")
    @classmethod
    def _validate_query(cls, value: str) -> str:
        text = (value or "").strip()
        if not text:
            raise ValueError("query must not be empty")
        return text


@router.post("")
async def search_trend_clusters(req: TrendClusterSearchRequest) -> dict:
    settings = get_settings()
    vector = await _get_embedding(req.query, settings)
    qdrant = QdrantFrontierClient()
    try:
        results = await qdrant.search_trend_clusters(
            vector,
            req.workspace,
            limit=req.limit,
            pipeline=req.pipeline,
            signal_stage=req.stages,
            days_back=req.days_back,
        )
    finally:
        await qdrant.close()
    return {
        "results": results,
        "applied_filters": {
            "workspace": req.workspace,
            "limit": req.limit,
            "pipeline": req.pipeline,
            "stages": req.stages or [],
            "days_back": req.days_back,
        },
    }
