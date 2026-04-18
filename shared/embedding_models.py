"""Known GigaChat embedding model specs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic.fields import FieldInfo


@dataclass(frozen=True)
class EmbeddingModelSpec:
    model: str
    dim: int
    context_tokens: int
    tier: str


EMBEDDING_MODEL_SPECS: dict[str, EmbeddingModelSpec] = {
    "Embeddings": EmbeddingModelSpec(
        model="Embeddings",
        dim=1024,
        context_tokens=512,
        tier="base",
    ),
    "Embeddings-2": EmbeddingModelSpec(
        model="Embeddings-2",
        dim=1024,
        context_tokens=512,
        tier="base_plus",
    ),
    "EmbeddingsGigaR": EmbeddingModelSpec(
        model="EmbeddingsGigaR",
        dim=2560,
        context_tokens=4096,
        tier="advanced",
    ),
    "GigaEmbeddings-3B-2025-09": EmbeddingModelSpec(
        model="GigaEmbeddings-3B-2025-09",
        dim=2048,
        context_tokens=4096,
        tier="advanced",
    ),
}


def _normalize_model_name(model: Any) -> str:
    if isinstance(model, FieldInfo) or model is None:
        return ""
    return str(model).strip()


def get_embedding_model_spec(model: str) -> EmbeddingModelSpec | None:
    return EMBEDDING_MODEL_SPECS.get(_normalize_model_name(model))


def expected_embedding_dim(model: str) -> int | None:
    spec = get_embedding_model_spec(model)
    return spec.dim if spec else None
