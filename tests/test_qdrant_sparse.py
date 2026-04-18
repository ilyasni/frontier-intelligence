import importlib


def test_sparse_model_init_fails_closed(monkeypatch) -> None:
    module = importlib.import_module("shared.qdrant_sparse")

    calls = {"count": 0}

    class _BrokenSparseModel:
        def __init__(self, **kwargs):
            calls["count"] += 1
            raise RuntimeError("network unavailable")

    monkeypatch.setattr(module, "_SparseTextEmbedding", _BrokenSparseModel)
    monkeypatch.setattr(module, "_sparse_model", None)
    monkeypatch.setattr(module, "_sparse_init_attempted", False)
    monkeypatch.setattr(module, "HAS_SPARSE", True)

    assert module._get_sparse_model() is None
    assert module._get_sparse_model() is None
    assert calls["count"] == 1
    assert module.HAS_SPARSE is False
