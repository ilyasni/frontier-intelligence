from shared.embedding_models import (
    embedding_input_text,
    embedding_profile,
    expected_embedding_dim,
    get_embedding_model_spec,
)


def test_embedding_model_specs_cover_current_default():
    spec = get_embedding_model_spec("EmbeddingsGigaR")
    assert spec is not None
    assert spec.dim == 2560
    assert spec.context_tokens == 4096


def test_expected_embedding_dim_for_new_candidate_model():
    assert expected_embedding_dim("GigaEmbeddings-3B-2025-09") == 2048


def test_embedding_profile_exposes_distance_and_prefixes() -> None:
    profile = embedding_profile("EmbeddingsGigaR")

    assert profile["distance_metric"] == "cosine"
    assert profile["query_prefix"] == "search_query: "
    assert profile["document_prefix"] == "search_document: "


def test_embedding_input_text_applies_purpose_prefix() -> None:
    assert embedding_input_text("EmbeddingsGigaR", "hello", purpose="query").startswith(
        "search_query: "
    )
    assert embedding_input_text("EmbeddingsGigaR", "hello", purpose="document").startswith(
        "search_document: "
    )
