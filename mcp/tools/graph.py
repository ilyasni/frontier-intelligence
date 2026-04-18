"""Concept graph MCP tools backed by Neo4j."""
from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel, Field, field_validator

from worker.integrations.neo4j_client import Neo4jFrontierClient

router = APIRouter()


class ConceptGraphRequest(BaseModel):
    model_config = {"extra": "forbid"}

    workspace: str = Field(default="disruption", min_length=1)
    concept: str | None = Field(default=None, max_length=100)
    depth: int = Field(default=2, ge=1, le=4)
    limit: int = Field(default=50, ge=1, le=200)

    @field_validator("concept", mode="before")
    @classmethod
    def _normalize_concept(cls, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


@router.post("")
async def get_concept_graph(req: ConceptGraphRequest) -> dict:
    neo4j = Neo4jFrontierClient()
    try:
        graph = await neo4j.get_concept_graph(
            workspace_id=req.workspace,
            concept=req.concept,
            depth=req.depth,
            limit=req.limit,
        )
    finally:
        await neo4j.close()
    return {
        "workspace": req.workspace,
        "concept": req.concept,
        "depth": req.depth,
        "limit": req.limit,
        "graph": graph,
    }
