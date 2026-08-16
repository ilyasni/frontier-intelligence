"""The prior-signal lookup in `_signal_results` must keep `next()` semantics.

`_signal_results` used to find a group's prior signal with a `next(...)` scan over
`[*existing_trends, *existing_emerging]` that was rebuilt — along with a fresh `set()`
of every row's doc_ids — inside the per-group loop. That is now an index built once by
`_existing_signal_lookup`. These tests pin "same answer", because every pre-existing
`_signal_results` test passes empty lists for both and would not notice a change.
"""
import random
from typing import Any

import pytest

from worker.services.semantic_clustering import _existing_signal_for, _existing_signal_lookup

_WORKSPACES = ("disruption", "design", "ai_trends", "auto_hmi")


def _reference_existing(
    existing_trends: list[dict[str, Any]],
    existing_emerging: list[dict[str, Any]],
    workspace_id: str,
    doc_ids: list[str],
) -> dict[str, Any] | None:
    """The scan this replaced, kept verbatim as the oracle."""
    return next(
        (
            row
            for row in [*existing_trends, *existing_emerging]
            if row.get("workspace_id") == workspace_id
            and set(row.get("doc_ids") or []) & set(doc_ids)
        ),
        None,
    )


# The replacement is imported, never re-spelled here: an earlier draft of this file
# kept its own copy of the selection expression, so mutating the production one changed
# nothing and every test below still passed. Tests must call the code that ships.
_lookup = _existing_signal_for


def _random_rows(rng: random.Random, count: int, kind: str) -> list[dict[str, Any]]:
    key_field = "cluster_key" if kind == "trend" else "signal_key"
    return [
        {
            "id": f"{kind}-{idx}",
            "workspace_id": rng.choice(_WORKSPACES),
            key_field: f"{kind}-key-{idx}",
            # Empty and repeated doc_ids occur in real rows; both must be harmless.
            "doc_ids": [f"p{rng.randrange(40)}" for _ in range(rng.randrange(0, 5))],
        }
        for idx in range(count)
    ]


@pytest.mark.parametrize("seed", range(25))
def test_lookup_matches_the_scan_it_replaced(seed: int) -> None:
    rng = random.Random(seed)
    trends = _random_rows(rng, rng.randrange(0, 12), "trend")
    emerging = _random_rows(rng, rng.randrange(0, 12), "emerging")
    rows, owner = _existing_signal_lookup(trends, emerging)
    for _ in range(30):
        workspace_id = rng.choice(_WORKSPACES)
        doc_ids = sorted({f"p{rng.randrange(40)}" for _ in range(rng.randrange(1, 6))})
        # Identity, not equality: the very same row object must be selected.
        assert _lookup(rows, owner, workspace_id, doc_ids) is _reference_existing(
            trends, emerging, workspace_id, doc_ids
        ), f"seed={seed} workspace={workspace_id} doc_ids={doc_ids}"


def test_earliest_owner_wins_regardless_of_doc_id_order() -> None:
    # The failure mode a naive rewrite walks into: returning the owner of the first
    # doc_id that happens to be indexed instead of the earliest row in scan order.
    # `doc_ids` arrives sorted, so "p1" is visited first, but "p9"'s row comes first
    # in the scan and must win. This is what min(owners) buys.
    first = {"id": "t1", "workspace_id": "disruption", "cluster_key": "a", "doc_ids": ["p9"]}
    second = {"id": "t2", "workspace_id": "disruption", "cluster_key": "b", "doc_ids": ["p1"]}
    rows, owner = _existing_signal_lookup([first, second], [])
    assert _lookup(rows, owner, "disruption", ["p1", "p9"]) is first


def test_trend_outranks_emerging_signal_for_the_same_doc() -> None:
    trend = {"id": "t1", "workspace_id": "disruption", "cluster_key": "ck", "doc_ids": ["p1"]}
    signal = {"id": "e1", "workspace_id": "disruption", "signal_key": "sk", "doc_ids": ["p1"]}
    rows, owner = _existing_signal_lookup([trend], [signal])
    assert _lookup(rows, owner, "disruption", ["p1"]) is trend


def test_newer_row_wins_within_one_list() -> None:
    # _existing orders by detected_at DESC, so position 0 is the newest row.
    newer = {"id": "t1", "workspace_id": "disruption", "cluster_key": "a", "doc_ids": ["p1"]}
    older = {"id": "t2", "workspace_id": "disruption", "cluster_key": "b", "doc_ids": ["p1"]}
    rows, owner = _existing_signal_lookup([newer, older], [])
    assert _lookup(rows, owner, "disruption", ["p1"]) is newer


def test_workspace_isolates_rows_sharing_a_doc_id() -> None:
    other = {"id": "t1", "workspace_id": "design", "cluster_key": "a", "doc_ids": ["p1"]}
    rows, owner = _existing_signal_lookup([other], [])
    assert _lookup(rows, owner, "disruption", ["p1"]) is None
    assert _lookup(rows, owner, "design", ["p1"]) is other
    assert _lookup(rows, owner, "design", ["p9"]) is None


def test_missing_and_duplicated_doc_ids_do_not_break_the_index() -> None:
    without = {"id": "t1", "workspace_id": "disruption", "cluster_key": "a", "doc_ids": None}
    repeated = {"id": "t2", "workspace_id": "disruption", "cluster_key": "b", "doc_ids": ["p1", "p1"]}
    rows, owner = _existing_signal_lookup([without, repeated], [])
    assert _lookup(rows, owner, "disruption", ["p1"]) is repeated


def test_empty_inputs_yield_an_empty_index() -> None:
    rows, owner = _existing_signal_lookup([], [])
    assert rows == []
    assert owner == {}
    assert _lookup(rows, owner, "disruption", ["p1"]) is None
