from worker.services.entity_resolution import acronym_of, find_acronym_candidates


def test_acronym_of() -> None:
    assert acronym_of("Human-Machine Interface") == "hmi"
    assert acronym_of("Large Language Model") == "llm"
    assert acronym_of("AI") is None          # single word
    assert acronym_of("") is None
    assert acronym_of("the End of X") == "ex"  # stopwords the/of skipped
    assert acronym_of("retrieval augmented generation") == "rag"


def test_find_acronym_candidates() -> None:
    concepts = [
        {"name": "HMI", "norm": "hmi", "mentions": 5},
        {"name": "Human-Machine Interface", "norm": "humanmachineinterface", "mentions": 3},
        {"name": "Large Language Model", "norm": "largelanguagemodel", "mentions": 10},
        {"name": "LLM", "norm": "llm", "mentions": 20},
        {"name": "Random Thing", "norm": "randomthing", "mentions": 1},  # acronym 'rt' absent
    ]
    cands = find_acronym_candidates(concepts)
    keys = {(c["acronym_norm"], c["expansion_norm"]) for c in cands}
    assert ("hmi", "humanmachineinterface") in keys
    assert ("llm", "largelanguagemodel") in keys
    assert len(cands) == 2
    # sorted by combined mentions desc: LLM pair (30) before HMI pair (8)
    assert cands[0]["expansion_norm"] == "largelanguagemodel"


def test_no_self_pair() -> None:
    # a concept whose own norm equals its acronym must not pair with itself
    concepts = [{"name": "A B", "norm": "ab", "mentions": 1}]
    assert find_acronym_candidates(concepts) == []
