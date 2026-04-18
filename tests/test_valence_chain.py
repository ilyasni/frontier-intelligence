from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from worker.chains.valence_chain import ValenceChain
from worker.gigachat_client import GigaChatResponse, GigaChatUsage


@pytest.mark.asyncio
async def test_valence_chain_escalates_to_pro_on_invalid_json():
    calls = []

    async def _chat(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            return GigaChatResponse(content="not-json", model="GigaChat-2")
        return GigaChatResponse(
            content='{"valence":"negative","signal_type":"closure","confidence":0.83,"reasoning":"shutdown"}',
            model="GigaChat-2-Pro",
            usage=GigaChatUsage(prompt_tokens=35, completion_tokens=11, total_tokens=46),
        )

    client = SimpleNamespace(
        chat=AsyncMock(side_effect=_chat),
        budget_text=AsyncMock(return_value=SimpleNamespace(text="trimmed text", truncated=False)),
    )

    chain = ValenceChain(client)
    result = await chain.run("shutdown signal")

    assert result["valence"] == "negative"
    assert result["signal_type"] == "closure"
    assert result["confidence"] == 0.83
    assert chain.last_meta["escalated"] is True
    assert len(calls) == 2
    assert calls[0]["task"] == "valence"
    assert calls[1]["model_override"] == "GigaChat-2-Pro"
