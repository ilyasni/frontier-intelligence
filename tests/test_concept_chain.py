from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from worker.chains.concept_chain import ConceptChain
from worker.gigachat_client import GigaChatResponse, GigaChatUsage


@pytest.mark.asyncio
async def test_concept_chain_escalates_to_pro_when_lite_returns_empty_result():
    calls = []

    async def _chat(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            return GigaChatResponse(content='{"concepts": []}', model="GigaChat-2-Lite")
        return GigaChatResponse(
            content='{"concepts":[{"name":"GigaChat","category":"technology","weight":5}]}',
            model="GigaChat-2-Pro",
            usage=GigaChatUsage(prompt_tokens=40, completion_tokens=12, total_tokens=52),
        )

    client = SimpleNamespace(
        chat=AsyncMock(side_effect=_chat),
        budget_text=AsyncMock(return_value=SimpleNamespace(text="trimmed text", truncated=False)),
    )

    chain = ConceptChain(client)
    result = await chain.run("source text")

    assert result == [{"name": "GigaChat", "category": "technology", "weight": 5}]
    assert chain.last_meta["escalated"] is True
    assert len(calls) == 2
    assert calls[0]["task"] == "concepts"
    assert calls[1]["model_override"] == "GigaChat-2-Pro"
