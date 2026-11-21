"""Tests for DuckDB 1.4.x / 2.0.x Arrow conversion compatibility."""

from __future__ import annotations

import json
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import pytest

from seeknal.iceberg.ingest_writer import _read_source_to_arrow
from seeknal.tasks.duckdb import DuckDBTask
from seeknal.utils.duckdb_arrow import result_to_arrow_reader, result_to_arrow_table
from seeknal.workflow.materialization.iceberg_mutations import _freeze_arrow_batch


def _sample_table() -> pa.Table:
    schema = pa.schema(
        [
            pa.field("z_id", pa.int64()),
            pa.field("a_name", pa.string()),
            pa.field("m_score", pa.float64()),
        ]
    )
    return pa.Table.from_pylist(
        [
            {"z_id": 10, "a_name": "alpha", "m_score": 1.5},
            {"z_id": 20, "a_name": "beta", "m_score": None},
        ],
        schema=schema,
    )


def _table_as_reader(table: pa.Table) -> pa.RecordBatchReader:
    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())


# ---------------------------------------------------------------------------
# Fake DuckDB 2.0 / 1.4 connection & relation shapes
# ---------------------------------------------------------------------------


class _FakeDuckDB20Connection:
    """DuckDB 2.0 connection result: exposes only to_arrow_table / to_arrow_reader."""

    def __init__(self, table: pa.Table) -> None:
        self._table = table
        self.calls: list[str] = []

    def to_arrow_table(self) -> pa.Table:
        self.calls.append("to_arrow_table")
        return self._table

    def to_arrow_reader(self) -> pa.RecordBatchReader:
        self.calls.append("to_arrow_reader")
        return _table_as_reader(self._table)


class _FakeDuckDB14Connection:
    """DuckDB 1.4 connection result: exposes only fetch_arrow_table / arrow."""

    def __init__(self, table: pa.Table, *, arrow_returns_reader: bool) -> None:
        self._table = table
        self._arrow_returns_reader = arrow_returns_reader
        self.calls: list[str] = []

    def fetch_arrow_table(self) -> pa.Table:
        self.calls.append("fetch_arrow_table")
        return self._table

    def arrow(self) -> pa.Table | pa.RecordBatchReader:
        self.calls.append("arrow")
        if self._arrow_returns_reader:
            return _table_as_reader(self._table)
        return self._table


class _FakeFetchTableOnlyResult:
    """Minimal legacy result with only fetch_arrow_table()."""

    def __init__(self, table: pa.Table) -> None:
        self._table = table

    def fetch_arrow_table(self) -> pa.Table:
        return self._table


class _FakeRelationArrowReader:
    """Relation whose arrow() returns a RecordBatchReader."""

    def __init__(self, table: pa.Table) -> None:
        self._table = table

    def arrow(self) -> pa.RecordBatchReader:
        return _table_as_reader(self._table)


class _FakeRelationArrowTable:
    """Relation whose arrow() returns a pyarrow.Table."""

    def __init__(self, table: pa.Table) -> None:
        self._table = table

    def arrow(self) -> pa.Table:
        return self._table


class _FakeRelationWithDeprecatedMethods:
    """DuckDB 2.0 relation shape: has both to_arrow_* and deprecated fetch_arrow_*."""

    def __init__(self, table: pa.Table) -> None:
        self._table = table

    def to_arrow_table(self) -> pa.Table:
        return self._table

    def to_arrow_reader(self) -> pa.RecordBatchReader:
        return _table_as_reader(self._table)

    def fetch_arrow_table(self) -> pa.Table:  # pragma: no cover - must not be called
        raise AssertionError("fetch_arrow_table should not be called when to_arrow_table exists")

    def fetch_arrow_reader(self) -> pa.RecordBatchReader:  # pragma: no cover - must not be called
        raise AssertionError("fetch_arrow_reader should not be called when to_arrow_reader exists")

    def arrow(self) -> pa.RecordBatchReader:  # pragma: no cover - must not be called
        raise AssertionError("arrow should not be called when to_arrow_* exists")


def test_fake_duckdb_20_connection_shape():
    expected = _sample_table()
    fake_for_table = _FakeDuckDB20Connection(expected)
    table = result_to_arrow_table(fake_for_table)
    assert isinstance(table, pa.Table)
    assert table.equals(expected)
    assert fake_for_table.calls == ["to_arrow_table"]

    fake_for_reader = _FakeDuckDB20Connection(expected)
    reader = result_to_arrow_reader(fake_for_reader)
    assert isinstance(reader, pa.RecordBatchReader)
    assert reader.read_all().equals(expected)
    assert fake_for_reader.calls == ["to_arrow_reader"]


@pytest.mark.parametrize("arrow_returns_reader", [True, False])
def test_fake_duckdb_14_connection_shape(arrow_returns_reader: bool):
    expected = _sample_table()
    fake_for_table = _FakeDuckDB14Connection(
        expected, arrow_returns_reader=arrow_returns_reader
    )
    table = result_to_arrow_table(fake_for_table)
    assert isinstance(table, pa.Table)
    assert table.equals(expected)
    assert fake_for_table.calls == ["fetch_arrow_table"]

    fake_for_reader = _FakeDuckDB14Connection(
        expected, arrow_returns_reader=arrow_returns_reader
    )
    reader = result_to_arrow_reader(fake_for_reader)
    assert isinstance(reader, pa.RecordBatchReader)
    assert reader.schema == expected.schema
    assert reader.read_all().equals(expected)
    assert fake_for_reader.calls == ["arrow"]


def test_fake_fetch_arrow_table_only_converts_to_reader():
    expected = _sample_table()
    fake = _FakeFetchTableOnlyResult(expected)
    reader = result_to_arrow_reader(fake)
    assert isinstance(reader, pa.RecordBatchReader)
    assert reader.schema == expected.schema
    assert reader.read_all().equals(expected)


def test_fake_relation_arrow_returns_reader_vs_table():
    expected = _sample_table()

    rel_reader = _FakeRelationArrowReader(expected)
    tbl_from_reader = result_to_arrow_table(rel_reader)
    assert isinstance(tbl_from_reader, pa.Table)
    assert tbl_from_reader.equals(expected)

    rdr_from_reader = result_to_arrow_reader(rel_reader)
    assert isinstance(rdr_from_reader, pa.RecordBatchReader)
    assert rdr_from_reader.read_all().equals(expected)

    rel_table = _FakeRelationArrowTable(expected)
    tbl_from_table = result_to_arrow_table(rel_table)
    assert isinstance(tbl_from_table, pa.Table)
    assert tbl_from_table.equals(expected)

    rdr_from_table = result_to_arrow_reader(rel_table)
    assert isinstance(rdr_from_table, pa.RecordBatchReader)
    assert rdr_from_table.schema == expected.schema
    assert rdr_from_table.read_all().equals(expected)


def test_prefers_to_arrow_methods_over_deprecated_fallbacks():
    expected = _sample_table()
    rel = _FakeRelationWithDeprecatedMethods(expected)
    assert result_to_arrow_table(rel).equals(expected)
    reader = result_to_arrow_reader(rel)
    assert isinstance(reader, pa.RecordBatchReader)
    assert reader.read_all().equals(expected)


def test_empty_table_fallback_preserves_schema_in_reader():
    empty = _sample_table().slice(0, 0)
    reader = result_to_arrow_reader(_FakeRelationArrowTable(empty))
    assert isinstance(reader, pa.RecordBatchReader)
    assert reader.schema == empty.schema
    materialized = reader.read_all()
    assert materialized.num_rows == 0
    assert materialized.column_names == ["z_id", "a_name", "m_score"]


def test_unsupported_object_raises_type_error():
    with pytest.raises(TypeError, match="pyarrow.Table"):
        result_to_arrow_table(object())
    with pytest.raises(TypeError, match="pyarrow.RecordBatchReader"):
        result_to_arrow_reader(object())


# ---------------------------------------------------------------------------
# Real DuckDB tests
# ---------------------------------------------------------------------------


QUERY = (
    "SELECT * FROM (VALUES "
    "(CAST(10 AS BIGINT), 'alpha', CAST(1.5 AS DOUBLE)), "
    "(CAST(20 AS BIGINT), 'beta', CAST(NULL AS DOUBLE))"
    ") AS t(z_id, a_name, m_score)"
)
EXPECTED_ROWS = [
    {"z_id": 10, "a_name": "alpha", "m_score": 1.5},
    {"z_id": 20, "a_name": "beta", "m_score": None},
]


def _assert_arrow_table_matches(table: pa.Table) -> None:
    assert isinstance(table, pa.Table)
    assert table.column_names == ["z_id", "a_name", "m_score"]
    assert table.schema.field("z_id").type == pa.int64()
    assert pa.types.is_string(table.schema.field("a_name").type) or pa.types.is_large_string(
        table.schema.field("a_name").type
    )
    assert table.schema.field("m_score").type == pa.float64()
    assert table.to_pylist() == EXPECTED_ROWS


def test_real_duckdb_execute_table_and_reader():
    con = duckdb.connect(":memory:")
    try:
        table = result_to_arrow_table(con.execute(QUERY))
        _assert_arrow_table_matches(table)

        reader = result_to_arrow_reader(con.execute(QUERY))
        assert isinstance(reader, pa.RecordBatchReader)
        assert reader.schema.names == ["z_id", "a_name", "m_score"]
        materialized = reader.read_all()
        _assert_arrow_table_matches(materialized)
    finally:
        con.close()


def test_real_duckdb_sql_and_table_relations():
    con = duckdb.connect(":memory:")
    try:
        sql_table = result_to_arrow_table(con.sql(QUERY))
        _assert_arrow_table_matches(sql_table)

        sql_reader = result_to_arrow_reader(con.sql(QUERY))
        assert isinstance(sql_reader, pa.RecordBatchReader)
        _assert_arrow_table_matches(sql_reader.read_all())

        con.execute(f"CREATE TABLE sample_rel AS {QUERY}")
        rel_table = result_to_arrow_table(con.table("sample_rel"))
        _assert_arrow_table_matches(rel_table)

        rel_reader = result_to_arrow_reader(con.table("sample_rel"))
        assert isinstance(rel_reader, pa.RecordBatchReader)
        _assert_arrow_table_matches(rel_reader.read_all())

        frozen = _freeze_arrow_batch(con, "sample_rel")
        _assert_arrow_table_matches(frozen)
    finally:
        con.close()


def test_real_duckdb_read_source_to_arrow(tmp_path):
    csv_path = tmp_path / "items.csv"
    csv_path.write_text("z_id,a_name,m_score\n1,one,2.5\n2,two,3.5\n", encoding="utf-8")
    csv_table = _read_source_to_arrow(csv_path)
    assert isinstance(csv_table, pa.Table)
    assert csv_table.column_names == ["z_id", "a_name", "m_score"]
    assert csv_table.to_pylist() == [
        {"z_id": 1, "a_name": "one", "m_score": 2.5},
        {"z_id": 2, "a_name": "two", "m_score": 3.5},
    ]

    jsonl_path = tmp_path / "items.jsonl"
    jsonl_path.write_text(
        "\n".join(
            [
                json.dumps({"z_id": 1, "a_name": "one"}),
                json.dumps({"z_id": 2, "a_name": "two"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    jsonl_table = _read_source_to_arrow(jsonl_path)
    assert isinstance(jsonl_table, pa.Table)
    assert jsonl_table.to_pylist() == [
        {"z_id": 1, "a_name": "one"},
        {"z_id": 2, "a_name": "two"},
    ]

    parquet_path = tmp_path / "items.parquet"
    pq.write_table(_sample_table(), parquet_path)
    parquet_table = _read_source_to_arrow(parquet_path)
    _assert_arrow_table_matches(parquet_table)


def test_real_duckdb_task_pipeline_paths(tmp_path):
    parquet_path = tmp_path / "task_input.parquet"
    pq.write_table(_sample_table(), parquet_path)

    # Path input + first-stage drop_columns + computed column + subsequent drop_columns
    path_result = (
        DuckDBTask(name="from_path")
        .add_input(path=str(parquet_path))
        .drop_columns(["m_score"])
        .add_new_column("z_id * 2", "doubled")
        .add_new_column("z_id + 1", "temp_col")
        .drop_columns(["temp_col"])
        .transform()
    )
    assert isinstance(path_result, pa.Table)
    assert path_result.column_names == ["z_id", "a_name", "doubled"]
    assert path_result.to_pylist() == [
        {"z_id": 10, "a_name": "alpha", "doubled": 20},
        {"z_id": 20, "a_name": "beta", "doubled": 40},
    ]

    # SQL input + SQL stage
    sql_result = (
        DuckDBTask(name="from_sql")
        .add_input(sql=QUERY)
        .add_sql("SELECT z_id, a_name FROM __THIS__ WHERE z_id > 10")
        .transform()
    )
    assert isinstance(sql_result, pa.Table)
    assert sql_result.column_names == ["z_id", "a_name"]
    assert sql_result.to_pylist() == [{"z_id": 20, "a_name": "beta"}]

    # Arrow Table input with zero stages (direct relation -> reader -> table)
    df_result = DuckDBTask(name="from_arrow").add_input(dataframe=_sample_table()).transform()
    _assert_arrow_table_matches(df_result)
