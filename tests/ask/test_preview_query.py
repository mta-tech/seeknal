"""Tests for the preview_query safety-hook tool."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.preview_query import preview_query


class _REPLStub:
    def __init__(self) -> None:
        self.conn = duckdb.connect(":memory:")

    def execute_oneshot(self, sql: str, limit=None):
        if limit is not None:
            sql = f"SELECT * FROM ({sql}) AS _q LIMIT {int(limit)}"
        result = self.conn.execute(sql)
        if not result.description:
            return [], []
        cols = [d[0] for d in result.description]
        rows = result.fetchall()
        return cols, rows


@pytest.fixture
def ctx(tmp_path: Path) -> ToolContext:
    repl = _REPLStub()
    repl.conn.execute("CREATE TABLE small AS SELECT range AS id FROM range(50)")
    repl.conn.execute("CREATE TABLE medium AS SELECT range AS id FROM range(15000)")
    repl.conn.execute("CREATE TABLE huge AS SELECT range AS id FROM range(150000)")
    # Wide table: 60 columns
    repl.conn.execute(
        "CREATE TABLE wide AS SELECT range AS id, "
        + ", ".join(f"'v' AS c{i}" for i in range(60))
        + " FROM range(5)"
    )
    # Fan-out demo: customers with many orders each
    repl.conn.execute("CREATE TABLE customers AS SELECT range AS cust_id FROM range(500)")
    repl.conn.execute(
        "CREATE TABLE orders AS "
        "SELECT range AS order_id, (range % 500) AS cust_id FROM range(5000)"
    )
    context = ToolContext(
        repl=repl, artifact_discovery=MagicMock(), project_path=tmp_path,
    )
    set_tool_context(context)
    return context


# ---------------------------------------------------------------------------
# OK paths
# ---------------------------------------------------------------------------


def test_small_query_ok(ctx):
    out = preview_query("SELECT * FROM small")
    assert "OK" in out
    assert "Estimated rows: **50**" in out


def test_limit_1_skipped(ctx):
    out = preview_query("SELECT * FROM huge LIMIT 1")
    assert "OK (aggregation / LIMIT 1 — hooks skipped)" in out


def test_pure_aggregation_skipped(ctx):
    out = preview_query("SELECT COUNT(*) FROM huge")
    assert "OK (aggregation / LIMIT 1 — hooks skipped)" in out


def test_run_hooks_false_skips(ctx):
    out = preview_query("SELECT * FROM huge", run_hooks=False)
    assert "OK (hooks skipped by caller)" in out


# ---------------------------------------------------------------------------
# WARN: >= 10k rows
# ---------------------------------------------------------------------------


def test_warn_on_large_result(ctx):
    out = preview_query("SELECT * FROM medium")
    assert "WARN" in out
    assert "15,000 rows" in out
    # Not blocked
    assert "BLOCK" not in out


# ---------------------------------------------------------------------------
# BLOCK: >= 100k rows
# ---------------------------------------------------------------------------


def test_block_on_huge_result(ctx):
    out = preview_query("SELECT * FROM huge")
    assert "BLOCK" in out
    assert "150,000 rows" in out
    assert "Execution blocked" in out
    assert "Do not call `execute_sql`" in out


# ---------------------------------------------------------------------------
# Column count warning
# ---------------------------------------------------------------------------


def test_column_count_warning(ctx):
    out = preview_query("SELECT * FROM wide")
    assert "Columns in projection: **61**" in out
    assert "WARN" in out
    assert "61 columns" in out


# ---------------------------------------------------------------------------
# Fan-out detection
# ---------------------------------------------------------------------------


def test_fanout_detected_on_many_to_many_join(ctx):
    # Cartesian-ish JOIN: customers (500) × orders (5000) with no constraint =
    # 500 * 5000 = 2.5M rows. That trips both the row threshold and fan-out.
    # Use a proper JOIN that multiplies — each customer has 10 orders on average,
    # so customers JOIN orders ON cust_id produces 5000 rows from a 500-row primary.
    sql = "SELECT c.cust_id, o.order_id FROM customers c JOIN orders o ON c.cust_id = o.cust_id"
    out = preview_query(sql)
    # Primary table has 500 rows, result has 5000 → 10x ratio
    assert "multiply rows" in out
    assert "500" in out and "5,000" in out


def test_no_fanout_warning_without_join(ctx):
    out = preview_query("SELECT * FROM medium")
    assert "multiply rows" not in out


# ---------------------------------------------------------------------------
# Reachability error
# ---------------------------------------------------------------------------


def test_error_on_unreachable_table(ctx):
    out = preview_query("SELECT * FROM does_not_exist")
    assert "ERROR" in out
    assert "list_tables" in out


def test_empty_sql_rejected(ctx):
    out = preview_query("")
    assert "ERROR: empty SQL" in out


# ---------------------------------------------------------------------------
# Safety
# ---------------------------------------------------------------------------


def test_primary_table_identifier_guard():
    # Direct unit test of the injection guard — only valid SQL identifiers
    # (with optional dotted qualifiers) should pass through to the FROM-clause
    # interpolation in the fan-out probe.
    from seeknal.ask.agents.tools.preview_query import _is_safe_identifier

    assert _is_safe_identifier("customers") is True
    assert _is_safe_identifier("main.customers") is True
    assert _is_safe_identifier('"customers"') is True
    # Anything with spaces, parens, semicolons, or injection characters fails.
    assert _is_safe_identifier("customers; DROP TABLE users") is False
    assert _is_safe_identifier("customers WHERE 1=1") is False
    assert _is_safe_identifier("(SELECT * FROM x)") is False
