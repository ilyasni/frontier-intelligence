from scripts import qdrant_alias_cutover as cutover


def test_preflight_errors_detect_schema_and_ratio_gaps() -> None:
    errors = cutover._preflight_errors(
        expected_dimension=2560,
        expected_distance="cosine",
        expects_sparse=True,
        current_count=100,
        target_count=70,
        target_dimension=2048,
        target_distance="dot",
        target_sparse_enabled=False,
        min_ratio=0.95,
    )

    assert "target_sparse_vector_missing" in errors
    assert any("target_dimension_mismatch" in item for item in errors)
    assert any("target_distance_mismatch" in item for item in errors)
    assert any("target_point_ratio_too_low" in item for item in errors)


def test_preflight_errors_accept_ready_target() -> None:
    errors = cutover._preflight_errors(
        expected_dimension=2560,
        expected_distance="cosine",
        expects_sparse=True,
        current_count=100,
        target_count=100,
        target_dimension=2560,
        target_distance="cosine",
        target_sparse_enabled=True,
        min_ratio=0.95,
    )

    assert errors == []


def test_alias_swap_operations_create_only_when_alias_missing(monkeypatch) -> None:
    monkeypatch.setattr(cutover, "CreateAlias", lambda **kwargs: {"create_alias": kwargs})
    monkeypatch.setattr(
        cutover,
        "CreateAliasOperation",
        lambda **kwargs: {"op": "create", **kwargs},
    )

    operations = cutover._alias_swap_operations(
        alias_name="frontier_docs_active",
        current_target=None,
        target_collection="frontier_docs__embeddingsgigar__dense_2560",
    )

    assert len(operations) == 1
    assert operations[0]["op"] == "create"


def test_alias_swap_operations_delete_then_create_on_cutover(monkeypatch) -> None:
    monkeypatch.setattr(cutover, "CreateAlias", lambda **kwargs: {"create_alias": kwargs})
    monkeypatch.setattr(cutover, "DeleteAlias", lambda **kwargs: {"delete_alias": kwargs})
    monkeypatch.setattr(
        cutover,
        "CreateAliasOperation",
        lambda **kwargs: {"op": "create", **kwargs},
    )
    monkeypatch.setattr(
        cutover,
        "DeleteAliasOperation",
        lambda **kwargs: {"op": "delete", **kwargs},
    )

    operations = cutover._alias_swap_operations(
        alias_name="frontier_docs_active",
        current_target="frontier_docs",
        target_collection="frontier_docs__embeddingsgigar__dense_2560",
    )

    assert [item["op"] for item in operations] == ["delete", "create"]
