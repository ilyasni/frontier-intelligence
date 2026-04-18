from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from worker.chains.relevance_chain import RelevanceChain
from worker.gigachat_client import GigaChatResponse, GigaChatUsage


@pytest.mark.asyncio
async def test_relevance_chain_escalates_from_lite_to_pro_on_invalid_json(monkeypatch):
    calls = []

    async def _chat(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            return GigaChatResponse(content="not-json", model="GigaChat-2-Lite")
        return GigaChatResponse(
            content='{"score": 0.91, "category": "technology", "reasoning": "fit"}',
            model="GigaChat-2-Pro",
            usage=GigaChatUsage(prompt_tokens=50, completion_tokens=10, total_tokens=60),
        )

    client = SimpleNamespace(
        chat=AsyncMock(side_effect=_chat),
        budget_text=AsyncMock(return_value=SimpleNamespace(text="trimmed text", truncated=False)),
    )

    chain = RelevanceChain(client)
    result = await chain.run("long text", "Disruption", ["technology"], threshold=0.6)

    assert result["relevant"] is True
    assert result["category"] == "technology"
    assert result["_escalated"] is True
    assert len(calls) == 2
    assert calls[0]["task"] == "relevance"
    assert calls[1]["model_override"] == "GigaChat-2-Pro"


@pytest.mark.asyncio
async def test_relevance_chain_accepts_non_other_borderline_score_after_escalation():
    calls = []

    async def _chat(**kwargs):
        calls.append(kwargs)
        return GigaChatResponse(
            content='{"score": 0.5, "category": "technology", "reasoning": "borderline but useful"}',
            model="GigaChat-2-Pro" if len(calls) > 1 else "GigaChat-2",
            usage=GigaChatUsage(prompt_tokens=40, completion_tokens=10, total_tokens=50),
        )

    client = SimpleNamespace(
        chat=AsyncMock(side_effect=_chat),
        budget_text=AsyncMock(return_value=SimpleNamespace(text="trimmed text", truncated=False)),
    )

    chain = RelevanceChain(client)
    chain._settings.gigachat_relevance_gray_zone = 0.1
    result = await chain.run("borderline tech news", "AI Trends", ["technology", "design"], threshold=0.6)

    assert result["score"] == 0.5
    assert result["category"] == "technology"
    assert result["relevant"] is True
    assert result["_escalated"] is True


@pytest.mark.asyncio
async def test_relevance_chain_keeps_other_category_borderline_dropped():
    client = SimpleNamespace(
        chat=AsyncMock(
            return_value=GigaChatResponse(
                content='{"score": 0.5, "category": "other", "reasoning": "generic"}',
                model="GigaChat-2",
            )
        ),
        budget_text=AsyncMock(return_value=SimpleNamespace(text="trimmed text", truncated=False)),
    )

    chain = RelevanceChain(client)
    result = await chain.run("generic post", "Disruption", ["technology", "design"], threshold=0.6)

    assert result["score"] == 0.5
    assert result["category"] == "other"
    assert result["relevant"] is False
