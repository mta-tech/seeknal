"""Fingerprint extraction tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from seeknal.heartbeat.classifier.fingerprint import (
    _infer_type,
    _normalize_name,
    compute_fingerprint,
    similarity,
)
from seeknal.heartbeat.classifier.models import Fingerprint


def test_normalize_name():
    assert _normalize_name("OrderId") == "order_id"
    assert _normalize_name("order-id") == "order_id"
    assert _normalize_name(" Customer Name ") == "customer_name"


def test_infer_type_integer():
    assert _infer_type(["1", "2", "3"]) == "int"


def test_infer_type_float():
    assert _infer_type(["1.0", "2.5", "3.1"]) == "float"


def test_infer_type_date():
    assert _infer_type(["2024-01-01", "2024-02-15"]) == "date"


def test_infer_type_string():
    assert _infer_type(["alice", "bob"]) == "string"


def test_compute_fingerprint_csv(tmp_path: Path):
    f = tmp_path / "sales.csv"
    f.write_text(
        "order_id,customer_id,amount,order_date\n"
        "A001,C1,99.50,2024-01-01\n"
        "A002,C2,42.00,2024-02-01\n"
    )
    fp = compute_fingerprint(f)
    assert fp.column_names == ["order_id", "customer_id", "amount", "order_date"]
    assert fp.column_count == 4
    assert fp.row_count_sample == 2


def test_similarity_identical():
    fp = Fingerprint(
        column_names=["a", "b"],
        column_types=["int", "string"],
        value_regexes=["integer", ""],
    )
    assert similarity(fp, fp) == pytest.approx(1.0)


def test_similarity_disjoint():
    a = Fingerprint(column_names=["x"], column_types=["int"], value_regexes=[""])
    b = Fingerprint(column_names=["y"], column_types=["string"], value_regexes=[""])
    assert similarity(a, b) < 0.6


def test_similarity_partial():
    a = Fingerprint(
        column_names=["a", "b", "c"],
        column_types=["int", "int", "string"],
        value_regexes=["", "", ""],
    )
    b = Fingerprint(
        column_names=["a", "b", "d"],
        column_types=["int", "int", "string"],
        value_regexes=["", "", ""],
    )
    score = similarity(a, b)
    assert 0.4 < score < 1.0
