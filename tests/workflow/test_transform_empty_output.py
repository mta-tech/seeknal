"""Regression tests for empty SQL transform intermediate outputs."""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from seeknal.dag.manifest import Node, NodeType
from seeknal.workflow.executors.base import ExecutionContext, ExecutorExecutionError
from seeknal.workflow.executors.transform_executor import TransformExecutor


def _context(tmp_path: Path, connection=None) -> ExecutionContext:
    return ExecutionContext(
        project_name="empty-output-regression",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
        duckdb_connection=connection or duckdb.connect(":memory:"),
    )


def _node(
    name: str,
    sql: str,
    *,
    inputs: list[dict[str, str]] | None = None,
) -> Node:
    config: dict[str, object] = {"transform": sql}
    if inputs is not None:
        config["inputs"] = inputs
    return Node(
        id=f"transform.{name}",
        name=name,
        node_type=NodeType.TRANSFORM,
        config=config,
    )


def _intermediate_path(context: ExecutionContext, node: Node) -> Path:
    return context.target_path / "intermediate" / f"{node.id.replace('.', '_')}.parquet"


def _parquet_rows(path: Path) -> list[tuple]:
    with duckdb.connect(":memory:") as connection:
        return connection.execute(
            "SELECT * FROM read_parquet(?)",
            [str(path)],
        ).fetchall()


def _parquet_schema(path: Path) -> list[tuple[str, str]]:
    with duckdb.connect(":memory:") as connection:
        rows = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [str(path)],
        ).fetchall()
    return [(row[0], row[1]) for row in rows]


def _direct_duckdb_copy(tmp_path: Path, name: str, expression: str) -> Path:
    output = tmp_path / f"direct_{name}.parquet"
    escaped_output = str(output).replace("'", "''")
    with duckdb.connect(":memory:") as connection:
        connection.execute(
            f"COPY (SELECT {expression} AS value) "
            f"TO '{escaped_output}' (FORMAT PARQUET)"
        )
    return output


def _direct_duckdb_ctas_copy(tmp_path: Path, name: str, select_sql: str) -> Path:
    output = tmp_path / f"direct_ctas_{name}.parquet"
    escaped_output = str(output).replace("'", "''")
    with duckdb.connect(":memory:") as connection:
        connection.execute(f"CREATE TEMP TABLE native_result AS {select_sql}")
        connection.execute(
            f"COPY native_result TO '{escaped_output}' (FORMAT PARQUET)"
        )
    return output


def _temporary_output_tables(context: ExecutionContext) -> list[str]:
    connection = context.get_duckdb_connection()
    return [
        row[0]
        for row in connection.execute(
            """
            SELECT table_name
            FROM duckdb_tables()
            WHERE starts_with(table_name, '_seeknal_output_')
            ORDER BY table_name
            """
        ).fetchall()
    ]


class _CopyFailingConnection:
    """Delegate to DuckDB except for intermediate parquet writes."""

    def __init__(self, connection) -> None:
        self._connection = connection

    def execute(self, query: str, *args, **kwargs):
        if query.lstrip().upper().startswith("COPY"):
            raise RuntimeError("simulated COPY failure")
        return self._connection.execute(query, *args, **kwargs)

    def __getattr__(self, name: str):
        return getattr(self._connection, name)


def test_fresh_empty_transform_writes_typed_parquet(tmp_path):
    context = _context(tmp_path)
    node = _node(
        "empty_upstream",
        "SELECT CAST(1 AS INTEGER) AS customer_id, CAST('x' AS VARCHAR) AS label WHERE FALSE",
    )

    result = TransformExecutor(node, context).run()
    output = _intermediate_path(context, node)

    assert result.row_count == 0
    assert output.exists()
    assert _parquet_rows(output) == []
    assert _parquet_schema(output) == [
        ("customer_id", "INTEGER"),
        ("label", "VARCHAR"),
    ]


def test_empty_transform_overwrites_stale_parquet_across_connections(tmp_path):
    initial_context = _context(tmp_path)
    initial_node = _node(
        "incremental_customers",
        "SELECT CAST(42 AS INTEGER) AS customer_id",
    )
    TransformExecutor(initial_node, initial_context).run()
    output = _intermediate_path(initial_context, initial_node)
    assert _parquet_rows(output) == [(42,)]

    empty_context = _context(tmp_path)
    empty_node = _node(
        "incremental_customers",
        "SELECT CAST(42 AS INTEGER) AS customer_id WHERE FALSE",
    )
    result = TransformExecutor(empty_node, empty_context).run()

    assert result.row_count == 0
    assert _parquet_rows(output) == []
    assert _parquet_schema(output) == [("customer_id", "INTEGER")]


def test_chained_downstream_receives_empty_named_ref(tmp_path):
    upstream_context = _context(tmp_path)
    upstream = _node(
        "empty_upstream",
        "SELECT CAST(1 AS INTEGER) AS customer_id WHERE FALSE",
    )
    TransformExecutor(upstream, upstream_context).run()

    downstream_context = _context(tmp_path)
    downstream = _node(
        "empty_downstream",
        "SELECT * FROM ref('transform.empty_upstream')",
        inputs=[{"ref": "transform.empty_upstream"}],
    )
    result = TransformExecutor(downstream, downstream_context).run()
    output = _intermediate_path(downstream_context, downstream)

    assert result.row_count == 0
    assert output.exists()
    assert _parquet_rows(output) == []
    assert _parquet_schema(output) == [("customer_id", "INTEGER")]


def test_chained_empty_downstream_keeps_upstream_view_valid_on_shared_connection(
    tmp_path,
):
    context = _context(tmp_path)
    upstream = _node(
        "shared_empty_upstream",
        "SELECT CAST(1 AS INTEGER) AS customer_id WHERE FALSE",
    )
    TransformExecutor(upstream, context).run()

    downstream = _node(
        "shared_empty_downstream",
        "SELECT * FROM ref('transform.shared_empty_upstream')",
        inputs=[{"ref": "transform.shared_empty_upstream"}],
    )
    result = TransformExecutor(downstream, context).run()
    connection = context.get_duckdb_connection()

    assert result.row_count == 0
    assert connection.execute(
        "SELECT * FROM transform.shared_empty_upstream"
    ).fetchall() == []
    assert connection.execute(
        "SELECT * FROM transform.shared_empty_downstream"
    ).fetchall() == []


def test_copy_failure_fails_transform(tmp_path):
    real_connection = duckdb.connect(":memory:")
    context = _context(tmp_path, _CopyFailingConnection(real_connection))
    node = _node("copy_failure", "SELECT CAST(1 AS INTEGER) AS customer_id")

    with pytest.raises(ExecutorExecutionError, match="simulated COPY failure"):
        TransformExecutor(node, context).run()

    assert _temporary_output_tables(context) == []


def test_atomic_replace_failure_preserves_previous_output_and_cleans_temp(
    tmp_path,
    monkeypatch,
):
    context = _context(tmp_path)
    node = _node("atomic_output", "SELECT CAST(1 AS INTEGER) AS customer_id")
    output = _intermediate_path(context, node)
    output.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as connection:
        connection.execute(
            f"COPY (SELECT CAST(7 AS INTEGER) AS customer_id) TO '{output}' (FORMAT PARQUET)"
        )
    previous_bytes = output.read_bytes()
    files_before = set(output.parent.iterdir())

    def fail_replace(source, destination):
        raise OSError("simulated atomic replace failure")

    monkeypatch.setattr(os, "replace", fail_replace)

    with pytest.raises(ExecutorExecutionError, match="simulated atomic replace failure"):
        TransformExecutor(node, context).run()

    assert output.read_bytes() == previous_bytes
    assert set(output.parent.iterdir()) == files_before
    assert _temporary_output_tables(context) == []


def test_successful_atomic_write_cleans_temp_file(tmp_path):
    context = _context(tmp_path)
    node = _node("atomic_success", "SELECT CAST(9 AS INTEGER) AS customer_id")
    output = _intermediate_path(context, node)

    result = TransformExecutor(node, context).run()

    assert result.row_count == 1
    assert _parquet_rows(output) == [(9,)]
    assert set(output.parent.iterdir()) == {output}
    assert _temporary_output_tables(context) == []


def test_empty_transform_preserves_decimal_and_timestamptz_schema(tmp_path):
    context = _context(tmp_path)
    node = _node(
        "typed_empty",
        """
        SELECT
            CAST(0 AS DECIMAL(18,2)) AS balance,
            CAST('2026-10-12 08:30:00+07:00' AS TIMESTAMPTZ) AS observed_at
        WHERE FALSE
        """,
    )
    output = _intermediate_path(context, node)

    result = TransformExecutor(node, context).run()

    assert result.row_count == 0
    assert _parquet_rows(output) == []
    assert _parquet_schema(output) == [
        ("balance", "DECIMAL(18,2)"),
        ("observed_at", "TIMESTAMP WITH TIME ZONE"),
    ]


def test_nonempty_transform_preserves_high_precision_decimal(tmp_path):
    context = _context(tmp_path)
    node = _node(
        "precise_decimal",
        """
        SELECT CAST(
            '12345678901234567890.123456789012345678'
            AS DECIMAL(38,18)
        ) AS precise_value
        """,
    )
    output = _intermediate_path(context, node)

    result = TransformExecutor(node, context).run()

    assert result.row_count == 1
    assert _parquet_schema(output) == [("precise_value", "DECIMAL(38,18)")]
    assert _parquet_rows(output) == [
        (Decimal("12345678901234567890.123456789012345678"),)
    ]


@pytest.mark.parametrize(
    ("name", "expression", "expected_type"),
    [
        (
            "uuid",
            "CAST('123e4567-e89b-12d3-a456-426614174000' AS UUID)",
            "UUID",
        ),
        (
            "timetz",
            "CAST('08:30:00+07:00' AS TIMETZ)",
            "TIME WITH TIME ZONE",
        ),
        (
            "bit",
            "CAST('10101' AS BIT)",
            "VARCHAR",
        ),
    ],
    ids=["uuid", "timetz", "bit"],
)
def test_transform_matches_native_duckdb_copy_types(
    tmp_path,
    name,
    expression,
    expected_type,
):
    expected = _direct_duckdb_copy(tmp_path, name, expression)
    context = _context(tmp_path)
    node = _node(f"native_{name}", f"SELECT {expression} AS value")
    output = _intermediate_path(context, node)

    result = TransformExecutor(node, context).run()

    assert result.row_count == 1
    assert _parquet_schema(output) == _parquet_schema(expected) == [
        ("value", expected_type)
    ]
    assert _parquet_rows(output) == _parquet_rows(expected)
    assert _temporary_output_tables(context) == []


def test_transform_matches_native_ctas_duplicate_column_deduplication(tmp_path):
    sql = "SELECT 1 AS x, 2 AS x"
    expected = _direct_duckdb_ctas_copy(tmp_path, "duplicate_columns", sql)
    context = _context(tmp_path)
    node = _node("duplicate_columns", sql)
    output = _intermediate_path(context, node)

    result = TransformExecutor(node, context).run()

    assert result.row_count == 1
    assert _parquet_schema(output) == _parquet_schema(expected) == [
        ("x", "INTEGER"),
        ("x_1", "INTEGER"),
    ]
    assert _parquet_rows(output) == _parquet_rows(expected) == [(1, 2)]
    assert _temporary_output_tables(context) == []


def test_intermediate_write_supports_workspace_path_with_apostrophe(tmp_path):
    workspace = tmp_path / "customer's workspace"
    context = _context(workspace)
    node = _node(
        "quoted_path",
        "SELECT CAST(1 AS INTEGER) AS customer_id WHERE FALSE",
    )
    output = _intermediate_path(context, node)

    result = TransformExecutor(node, context).run()

    assert result.row_count == 0
    assert output.exists()
    assert _parquet_rows(output) == []
    assert _parquet_schema(output) == [("customer_id", "INTEGER")]


def test_multi_statement_writes_last_select_after_source_table_is_dropped(tmp_path):
    context = _context(tmp_path)
    node = _node(
        "multi_statement",
        """
        CREATE TEMP TABLE transient_result AS
            SELECT CAST(11 AS INTEGER) AS customer_id;
        SELECT * FROM transient_result;
        DROP TABLE transient_result;
        """,
    )
    output = _intermediate_path(context, node)

    result = TransformExecutor(node, context).run()

    assert result.row_count == 1
    assert result.metadata["statements_executed"] == 3
    assert _parquet_rows(output) == [(11,)]
    assert _parquet_schema(output) == [("customer_id", "INTEGER")]


def test_cte_insert_is_not_captured_as_pipeline_select_output(tmp_path):
    context = _context(tmp_path)
    node = _node(
        "cte_insert",
        """
        CREATE TEMP TABLE sink (i INTEGER);
        WITH x AS (SELECT 1 AS i)
        INSERT INTO sink SELECT * FROM x;
        SELECT * FROM sink;
        """,
    )
    output = _intermediate_path(context, node)

    result = TransformExecutor(node, context).run()

    assert result.row_count == 1
    assert result.metadata["statements_executed"] == 3
    assert _parquet_rows(output) == [(1,)]
    assert _parquet_schema(output) == [("i", "INTEGER")]


def test_cte_update_returning_is_not_captured_as_pipeline_select_output(tmp_path):
    context = _context(tmp_path)
    node = _node(
        "cte_update",
        """
        CREATE TEMP TABLE sink (i INTEGER);
        INSERT INTO sink VALUES (1);
        WITH x AS (SELECT 1 AS delta)
        UPDATE sink
        SET i = sink.i + x.delta
        FROM x
        RETURNING sink.i;
        SELECT * FROM sink;
        """,
    )
    output = _intermediate_path(context, node)

    result = TransformExecutor(node, context).run()

    assert result.row_count == 1
    assert result.metadata["statements_executed"] == 4
    assert _parquet_rows(output) == [(2,)]
    assert _parquet_schema(output) == [("i", "INTEGER")]


def test_select_classifier_rejects_cte_insert_and_update(tmp_path):
    context = _context(tmp_path)
    executor = TransformExecutor(
        _node("classifier", "SELECT 1"),
        context,
    )
    connection = context.get_duckdb_connection()

    assert executor._is_select_statement(
        "WITH x AS (SELECT 1 AS i) INSERT INTO sink SELECT * FROM x",
        connection,
    ) is False
    assert executor._is_select_statement(
        """
        WITH x AS (SELECT 1 AS delta)
        UPDATE sink SET i = sink.i + x.delta FROM x RETURNING sink.i
        """,
        connection,
    ) is False


def test_ddl_only_transform_succeeds_without_intermediate_output(tmp_path):
    context = _context(tmp_path)
    node = _node(
        "ddl_only",
        "CREATE TEMP TABLE created_table (customer_id INTEGER)",
    )
    output = _intermediate_path(context, node)

    result = TransformExecutor(node, context).run()

    assert result.is_success()
    assert result.row_count == 0
    assert result.metadata["statements_executed"] == 1
    assert not output.exists()


@pytest.mark.parametrize("mode", ["append", "upsert"])
def test_materialization_dispatch_receives_empty_transform_view(
    tmp_path,
    monkeypatch,
    mode,
):
    context = _context(tmp_path)
    node = _node(
        "empty_materialized",
        "SELECT CAST(1 AS INTEGER) AS customer_id WHERE FALSE",
    )
    node.config["materializations"] = [
        {
            "type": "iceberg",
            "table": "catalog.namespace.customers",
            "mode": mode,
        }
    ]
    observed: dict[str, object] = {}

    def dispatch(dispatcher, *, con, view_name, targets, node_id, env_name):
        observed["rows"] = con.execute(
            f"SELECT * FROM {view_name}"
        ).fetchall()
        observed["schema"] = [
            (row[0], row[1])
            for row in con.execute(f"DESCRIBE SELECT * FROM {view_name}").fetchall()
        ]
        observed["mode"] = targets[0]["mode"]
        return SimpleNamespace(
            all_succeeded=True,
            total=1,
            succeeded=1,
            failed=0,
            required_failed=0,
            serializable_results=[],
        )

    monkeypatch.setattr(
        "seeknal.workflow.executors.transform_executor.MaterializationDispatcher.dispatch",
        dispatch,
    )

    result = TransformExecutor(node, context).run()

    assert result.row_count == 0
    assert observed == {
        "rows": [],
        "schema": [("customer_id", "INTEGER")],
        "mode": mode,
    }
    assert result.metadata["materialization"]["success"] is True
