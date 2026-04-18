"""Sparse BM25 via fastembed for Qdrant hybrid search."""
from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any, Optional

from shared.config import get_settings

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from qdrant_client.models import SparseVector

_sparse_model: Any = None
_SparseTextEmbedding: Any = None
HAS_SPARSE: bool = False
_sparse_init_attempted: bool = False

try:
    from fastembed import SparseTextEmbedding

    _SparseTextEmbedding = SparseTextEmbedding
    HAS_SPARSE = True
except Exception as exc:  # pragma: no cover
    _SparseTextEmbedding = None
    _sparse_model = None
    HAS_SPARSE = False
    logger.warning("fastembed not available, sparse vectors disabled: %s", exc)

try:
    if HAS_SPARSE and not get_settings().sparse_vectors_enabled:
        HAS_SPARSE = False
        logger.info("Sparse vectors disabled by SPARSE_VECTORS_ENABLED=false")
except Exception:
    HAS_SPARSE = False


def _get_sparse_model() -> Any:
    global _sparse_model, _sparse_init_attempted, HAS_SPARSE

    if not HAS_SPARSE or _SparseTextEmbedding is None:
        return None
    if _sparse_model is not None:
        return _sparse_model
    if _sparse_init_attempted:
        return None

    _sparse_init_attempted = True

    kwargs: dict[str, Any] = {"model_name": "Qdrant/bm25"}
    cache_dir = os.getenv("HF_HUB_CACHE") or os.getenv("HF_HOME")
    if cache_dir:
        kwargs["cache_dir"] = cache_dir

    try:
        try:
            _sparse_model = _SparseTextEmbedding(**kwargs)
        except TypeError:
            kwargs.pop("cache_dir", None)
            _sparse_model = _SparseTextEmbedding(**kwargs)
        return _sparse_model
    except Exception as exc:  # pragma: no cover
        HAS_SPARSE = False
        logger.warning("fastembed BM25 init failed, sparse vectors disabled for now: %s", exc)
        return None


def sparse_encode(text: str) -> Optional["SparseVector"]:
    """Return BM25 sparse vector or None if sparse mode is unavailable."""
    model = _get_sparse_model()
    if model is None:
        return None
    try:
        from qdrant_client.models import SparseVector

        result = list(model.embed([text[:2000]]))[0]
        return SparseVector(indices=result.indices.tolist(), values=result.values.tolist())
    except Exception as exc:
        logger.warning("Sparse encode failed: %s", exc)
        return None
