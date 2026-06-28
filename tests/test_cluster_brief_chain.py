from types import SimpleNamespace

import pytest

from worker.chains.cluster_brief_chain import ClusterBriefChain


class _FakeClient:
    def __init__(self, content: str):
        self._content = content
        self.last_call: dict = {}

    async def chat(self, *, system: str, user: str, task: str, max_tokens: int):
        self.last_call = {"system": system, "user": user, "task": task}
        return SimpleNamespace(
            content=self._content, provider="wormsoft", model="wormsoft/agent/high", usage=None
        )


@pytest.mark.asyncio
async def test_chain_parses_russian_brief() -> None:
    client = _FakeClient(
        '{"title_ru": "Apple поднимает цены", '
        '"insight": "Дорожает железо, кроме iPhone.", '
        '"opportunity": "Следить за реакцией рынка."}'
    )
    chain = ClusterBriefChain(client)

    brief = await chain.run(
        title="Apple raises Mac and iPad prices",
        keywords=["Apple", "iPad"],
        evidence_titles=["Mac price hike", "iPad gets pricier"],
    )

    assert brief == {
        "title_ru": "Apple поднимает цены",
        "insight": "Дорожает железо, кроме iPhone.",
        "opportunity": "Следить за реакцией рынка.",
    }
    # Routes through the synthesis task so the router selects wormsoft.
    assert client.last_call["task"] == "mcp_synthesis"
    assert "Apple raises Mac and iPad prices" in client.last_call["user"]


@pytest.mark.asyncio
async def test_chain_handles_markdown_fenced_json() -> None:
    client = _FakeClient(
        '```json\n{"title_ru": "Заголовок", "insight": "Суть.", "opportunity": ""}\n```'
    )
    chain = ClusterBriefChain(client)

    brief = await chain.run(title="Some English headline")

    assert brief is not None
    assert brief["title_ru"] == "Заголовок"
    assert brief["opportunity"] == ""


@pytest.mark.asyncio
async def test_chain_returns_none_on_garbage() -> None:
    chain = ClusterBriefChain(_FakeClient("not json at all"))
    assert await chain.run(title="headline") is None


@pytest.mark.asyncio
async def test_chain_skips_empty_title() -> None:
    chain = ClusterBriefChain(_FakeClient("{}"))
    assert await chain.run(title="   ") is None
    assert chain.last_meta["skip_reason"] == "empty_title"
