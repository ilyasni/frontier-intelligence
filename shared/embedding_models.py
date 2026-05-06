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
    distance_metric: str = "cosine"
    query_prefix: str = ""
    document_prefix: str = ""


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
        query_prefix="search_query: ",
        document_prefix="search_document: ",
    ),
    "GigaEmbeddings-3B-2025-09": EmbeddingModelSpec(
        model="GigaEmbeddings-3B-2025-09",
        dim=2048,
        context_tokens=4096,
        tier="advanced",
        query_prefix="search_query: ",
        document_prefix="search_document: ",
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


def embedding_profile(model: str) -> dict[str, Any]:
    spec = get_embedding_model_spec(model)
    if spec is None:
        return {}
    return {
        "dimension": spec.dim,
        "context_tokens": spec.context_tokens,
        "tier": spec.tier,
        "distance_metric": spec.distance_metric,
        "query_prefix": spec.query_prefix,
        "document_prefix": spec.document_prefix,
        "index_family": f"dense-{spec.dim}",
    }


def embedding_input_text(
    model: str,
    text: str,
    *,
    purpose: str = "document",
) -> str:
    raw_text = str(text or "")
    spec = get_embedding_model_spec(model)
    if spec is None:
        return raw_text
    normalized_purpose = str(purpose or "document").strip().lower()
    if normalized_purpose == "query" and spec.query_prefix:
        return f"{spec.query_prefix}{raw_text}"
    if normalized_purpose == "document" and spec.document_prefix:
        return f"{spec.document_prefix}{raw_text}"
    return raw_text
