"""Helpers to keep GigaChat prompts inside stable token budgets."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable


TokenCounter = Callable[[str, str], Awaitable[int | None]]


@dataclass(frozen=True)
class BudgetedText:
    text: str
    estimated_tokens: int | None
    truncated: bool


async def fit_text_to_token_budget(
    text: str,
    model: str,
    token_budget: int,
    counter: TokenCounter,
    *,
    fallback_chars_per_token: int = 4,
    min_chars: int = 1,
    max_rounds: int = 6,
) -> BudgetedText:
    """Trim text to fit the requested token budget with a best-effort token counter."""
    clean = text.strip()
    if not clean or token_budget <= 0:
        return BudgetedText(text="", estimated_tokens=0, truncated=bool(clean))

    estimated = await counter(model, clean)
    if estimated is not None and estimated <= token_budget:
        return BudgetedText(text=clean, estimated_tokens=estimated, truncated=False)

    fallback_limit = max(min_chars, token_budget * fallback_chars_per_token)
    if estimated is None:
        trimmed = clean[:fallback_limit].strip()
        return BudgetedText(
            text=trimmed,
            estimated_tokens=None,
            truncated=len(trimmed) < len(clean),
        )

    low = min_chars
    high = min(len(clean), max(low, fallback_limit))
    best = clean[:high].strip()
    best_estimate = await counter(model, best)

    if best_estimate is not None and best_estimate <= token_budget:
        low = len(best)
        high = len(clean)

    for _ in range(max_rounds):
        if low >= high:
            break
        mid = (low + high + 1) // 2
        candidate = clean[:mid].strip()
        if not candidate:
            break
        candidate_tokens = await counter(model, candidate)
        if candidate_tokens is None:
            break
        if candidate_tokens <= token_budget:
            best = candidate
            best_estimate = candidate_tokens
            low = mid
        else:
            high = mid - 1

    return BudgetedText(
        text=best,
        estimated_tokens=best_estimate,
        truncated=len(best) < len(clean),
    )
