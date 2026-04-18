from shared.embedding_models import expected_embedding_dim, get_embedding_model_spec


def test_embedding_model_specs_cover_current_default():
    spec = get_embedding_model_spec("EmbeddingsGigaR")
    assert spec is not None
    assert spec.dim == 2560
    assert spec.context_tokens == 4096


def test_expected_embedding_dim_for_new_candidate_model():
    assert expected_embedding_dim("GigaEmbeddings-3B-2025-09") == 2048
