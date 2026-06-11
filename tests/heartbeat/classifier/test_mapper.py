"""Column mapper tests (Pass 1)."""

from __future__ import annotations

from seeknal.heartbeat.classifier.mapper import (
    DEFAULT_UNIT_SENSITIVE_KEYWORDS,
    Mapper,
    _levenshtein,
)


def test_levenshtein_basic():
    assert _levenshtein("kitten", "sitting") == 3
    assert _levenshtein("abc", "abc") == 0


def test_exact_match():
    mapper = Mapper()
    mapping = mapper.map_columns(["a", "b"], ["a", "b"])
    assert mapping.mapping == {"a": "a", "b": "b"}
    assert mapping.unmapped_file_cols == []


def test_snake_normalization():
    mapper = Mapper()
    mapping = mapper.map_columns(["OrderId"], ["order_id"])
    assert mapping.mapping == {"OrderId": "order_id"}


def test_alias_lookup():
    mapper = Mapper()
    mapping = mapper.map_columns(
        ["orderid"],
        ["order_id"],
        aliases={"order_id": ["orderid", "order_no"]},
    )
    assert mapping.mapping == {"orderid": "order_id"}


def test_fuzzy_within_distance_2():
    mapper = Mapper()
    mapping = mapper.map_columns(["customr"], ["customer"])
    assert mapping.mapping == {"customr": "customer"}


def test_fuzzy_too_far():
    mapper = Mapper()
    mapping = mapper.map_columns(["zzzz"], ["customer"])
    assert mapping.mapping == {}
    assert mapping.unmapped_file_cols == ["zzzz"]


def test_unit_sensitive_flag():
    mapper = Mapper()
    mapping = mapper.map_columns(["amount"], ["amount"])
    assert mapping.has_unit_uncertainty is True


def test_unit_sensitive_indonesian_default():
    """Bilingual default keyword list (Architect Tension C)."""
    mapper = Mapper()
    mapping = mapper.map_columns(["jumlah"], ["jumlah"])
    assert mapping.has_unit_uncertainty is True


def test_coverage_calculation():
    mapper = Mapper()
    # "a" maps to "a"; "xyz_unmapped" has no candidate within Levenshtein 2.
    mapping = mapper.map_columns(["a", "xyz_unmapped"], ["a", "different_col"])
    assert mapping.coverage == 0.5  # one mapped, one unmapped file_col


def test_default_keyword_list_includes_indonesian():
    assert "jumlah" in DEFAULT_UNIT_SENSITIVE_KEYWORDS
    assert "amount" in DEFAULT_UNIT_SENSITIVE_KEYWORDS
