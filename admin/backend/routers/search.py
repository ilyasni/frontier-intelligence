"""Search router — proxy to MCP search tools."""
from __future__ import annotations

import httpx
from fastapi import APIRouter

from shared.config import get_settings
from shared.search_contracts import BalancedSearchRequest, SearchRequest

router = APIRouter()


@router.post("")
async def search(req: SearchRequest):
    settings = get_settings()
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            f"{settings.mcp_internal_url}/tools/search_frontier",
            json=req.model_dump(exclude_none=True),
        )
        return resp.json()


@router.post("/balanced")
async def search_balanced(req: BalancedSearchRequest):
    settings = get_settings()
    async with httpx.AsyncClient(timeout=90) as client:
        resp = await client.post(
            f"{settings.mcp_internal_url}/tools/search_balanced",
            json=req.model_dump(exclude_none=True),
        )
        return resp.json()
