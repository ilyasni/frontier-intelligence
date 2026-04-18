from types import SimpleNamespace

from worker.chains.relevance_concepts_chain import RelevanceConceptsChain
from worker.gigachat_client import GigaChatResponse, GigaChatUsage


class _DummyClient:
    async def budget_text(self, text, model, token_budget):
        return SimpleNamespace(text=text, truncated=False)

    async def chat(self, **kwargs):
        return GigaChatResponse(
            content='{"score":0.7,"category":"technology","reasoning":"relevant","concepts":[{"name":"Waymo","category":"technology","weight":4}]}',
            model="GigaChat-2",
            usage=GigaChatUsage(total_tokens=42),
        )


async def test_relevance_concepts_chain_returns_relevance_and_concepts() -> None:
    chain = RelevanceConceptsChain(_DummyClient())

    rel, concepts = await chain.run(
        "Waymo launches paid robotaxi rides",
        "Disruption",
        ["technology", "design"],
        0.6,
    )

    assert rel["relevant"] is True
    assert rel["category"] == "technology"
    assert rel["score"] == 0.7
    assert concepts == [{"name": "Waymo", "category": "technology", "weight": 4}]
    assert chain.last_meta["model"] == "GigaChat-2"
