import pytest

from worker.token_budget import fit_text_to_token_budget


@pytest.mark.asyncio
async def test_fit_text_to_token_budget_trims_by_token_counter():
    async def _counter(_model: str, text: str) -> int | None:
        return len(text) // 2

    source = "a" * 200
    result = await fit_text_to_token_budget(source, "GigaChat-2-Lite", 40, _counter)

    assert result.truncated is True
    assert result.estimated_tokens is not None
    assert result.estimated_tokens <= 40
    assert len(result.text) < len(source)


@pytest.mark.asyncio
async def test_fit_text_to_token_budget_falls_back_to_char_budget_when_counter_unavailable():
    async def _counter(_model: str, _text: str) -> int | None:
        return None

    source = "b" * 500
    result = await fit_text_to_token_budget(source, "GigaChat-2-Lite", 50, _counter)

    assert result.truncated is True
    assert result.estimated_tokens is None
    assert len(result.text) == 200
