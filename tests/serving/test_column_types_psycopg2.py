"""Tests for psycopg2 Column description entries normalization (Section D)."""

from __future__ import annotations

from types import SimpleNamespace
import pytest
from psycopg2.extensions import Column

from seeknal.serving.adapters import normalize_column_type


@pytest.mark.parametrize(
    "type_code,precision,scale,expected",
    [
        (16, None, None, "boolean"),
        (21, None, None, "smallint"),
        (23, None, None, "integer"),
        (20, None, None, "bigint"),
        (700, None, None, "float"),
        (701, None, None, "double"),
        (1700, 10, 2, "decimal(10,2)"),
        (1700, None, None, "decimal"),
        (25, None, None, "varchar"),
        (1043, None, None, "varchar"),
        (1082, None, None, "date"),
        (1114, None, None, "timestamp"),
        (1184, None, None, "timestamptz"),
        (1083, None, None, "time"),
        (17, None, None, "binary"),
        (114, None, None, "json"),
        (3802, None, None, "json"),
        (99999, None, None, "unknown"),
    ],
)
@pytest.mark.parametrize("kind", ["cube", "lakebase", "psycopg2", "postgres"])
def test_real_psycopg2_column_and_required_property(kind, type_code, precision, scale, expected):
    """normalize_column_type(kind, col) == normalize_column_type(kind, equivalent_tuple)."""
    col = Column("col_name", type_code, 10, 4, precision, scale, True)
    equiv_tuple = (col.name, col.type_code, col.display_size, col.internal_size, col.precision, col.scale, col.null_ok)

    res_col = normalize_column_type(kind, col)
    res_tuple = normalize_column_type(kind, equiv_tuple)

    assert res_col == expected
    assert res_col == res_tuple


@pytest.mark.parametrize("kind", ["cube", "lakebase"])
def test_attribute_stub_full_and_partial(kind):
    # Full attribute stub
    stub_full = SimpleNamespace(
        name="price",
        type_code=1700,
        display_size=12,
        internal_size=None,
        precision=12,
        scale=4,
        null_ok=True,
    )
    equiv_full = (
        stub_full.name,
        stub_full.type_code,
        stub_full.display_size,
        stub_full.internal_size,
        stub_full.precision,
        stub_full.scale,
        stub_full.null_ok,
    )
    assert normalize_column_type(kind, stub_full) == "decimal(12,4)"
    assert normalize_column_type(kind, stub_full) == normalize_column_type(kind, equiv_full)

    # Partial stub with only type_code (missing display_size, precision, scale)
    stub_minimal = SimpleNamespace(name="created_at", type_code=1114)
    assert normalize_column_type(kind, stub_minimal) == "timestamp"

    # Stub with invalid type_code
    stub_invalid = SimpleNamespace(name="bad", type_code="not_an_int")
    assert normalize_column_type(kind, stub_invalid) == "unknown"


def test_tuple_entries_and_other_kinds_unchanged():
    # Tuples for DuckDB
    assert normalize_column_type("duckdb", ("a", "INTEGER", None, None, None, None, None)) == "integer"
    assert normalize_column_type("duckdb", ("b", "DECIMAL(10,2)", None, None, None, None, None)) == "decimal(10,2)"

    # Tuples for StarRocks
    assert normalize_column_type("starrocks", ("c", 3, None, None, None, None, None)) == "integer"
    assert normalize_column_type("starrocks", ("d", 246, None, None, 10, 2, None)) == "decimal(10,2)"

    # Tuples for Cube and Lakebase
    assert normalize_column_type("cube", ("e", 20, None, None, None, None, None)) == "bigint"
    assert normalize_column_type("lakebase", ("f", 1114, None, None, None, None, None)) == "timestamp"

    # Direct raw values
    assert normalize_column_type("cube", 20) == "bigint"
    assert normalize_column_type("duckdb", "varchar") == "varchar"
