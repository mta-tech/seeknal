"""Tests for the execute_sql tool — context-window guards."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.execute_sql import execute_sql


class _REPLStub:
    """Minimal REPL shim — forwards execute_oneshot to a real in-memory DuckDB."""

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
    # Seed tables used across tests
    repl.conn.execute("CREATE TABLE small AS SELECT range AS id, 'hi' AS v FROM range(3)")
    repl.conn.execute("CREATE TABLE big AS SELECT range AS id, 'x' AS v FROM range(1500)")
    repl.conn.execute(
        "CREATE TABLE wide AS "
        "SELECT range AS id, "
        + ", ".join(f"'v{i}' AS c{i}" for i in range(60))
        + " FROM range(2)"
    )
    repl.conn.execute(
        "CREATE TABLE longcells AS "
        "SELECT 1 AS id, repeat('a', 500) AS big_text"
    )
    context = ToolContext(
        repl=repl,
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
    )
    set_tool_context(context)
    return context


# ---------------------------------------------------------------------------
# Happy paths — no guards triggered
# ---------------------------------------------------------------------------


def test_small_result_returns_clean_table(ctx):
    out = execute_sql("SELECT * FROM small", limit=10)
    assert "| id | v |" in out
    assert "(3 rows)" in out
    # No truncation notices on a clean, within-budget query.
    assert "Result truncated" not in out


def test_zero_result_message(ctx):
    out = execute_sql("SELECT * FROM small WHERE id = 999")
    assert "no results" in out.lower() or "0 rows" in out


# ---------------------------------------------------------------------------
# Row hard cap — 500 — with accurate total row count
# ---------------------------------------------------------------------------


def test_row_hard_cap_shows_total_count(ctx):
    # big has 1500 rows; caller asks for 10000 → clamped to 500
    out = execute_sql("SELECT * FROM big", limit=10_000)
    # Footer reports both shown and true total
    assert "(500 of 1,500 rows shown)" in out
    # Explicit truncation notice present
    assert "Result truncated" in out
    assert "500" in out and "1,500" in out


def test_row_hard_cap_limit_beyond_hard_cap_warning(ctx):
    # Query fits under the cap but caller asked for too many — we still warn
    out = execute_sql("SELECT * FROM small", limit=10_000)
    # All 3 rows fit
    assert "(3 rows)" in out
    # Caller warned about the clamp anyway
    assert "clamped to 500" in out


def test_caller_limit_informs_without_alarm(ctx):
    # Caller asked for a modest limit that clamps the result — footer reports
    # "N of M rows shown" so the agent knows more exist, but no ⚠ fires because
    # the clamp was the caller's own choice, not the hard cap.
    out = execute_sql("SELECT * FROM small", limit=2)
    assert "2 of 3 rows shown" in out
    # No "Result truncated" alarm when the caller's own limit does the clamping
    assert "Result truncated" not in out


# ---------------------------------------------------------------------------
# Column hard cap — 50
# ---------------------------------------------------------------------------


def test_column_hard_cap(ctx):
    # wide has 61 columns (id + c0..c59)
    out = execute_sql("SELECT * FROM wide")
    # Column cap applied
    assert "showing 50 of 61 columns" in out
    assert "Result truncated" in out
    # Header has exactly 50 columns (50 pipes of content between outer pipes)
    header_line = next(line for line in out.splitlines() if line.startswith("|"))
    # The header line is "| c1 | c2 | ... |"; 50 columns = 50 cell separators
    assert header_line.count("|") == 51  # 50 cells between 51 pipes


# ---------------------------------------------------------------------------
# Cell cap — 200 chars per cell
# ---------------------------------------------------------------------------


def test_cell_length_cap(ctx):
    out = execute_sql("SELECT * FROM longcells")
    # The 500-char 'a's are truncated
    assert "…" in out
    assert "capped at 200 chars" in out
    # The row should contain 200 chars worth of the cell value including the …
    body_line = next(line for line in out.splitlines() if "…" in line)
    # Approximate: ellipsis + padded text around it; just verify long field shrunk
    assert len(body_line) < 260


# ---------------------------------------------------------------------------
# Byte budget — drops trailing rows to fit
# ---------------------------------------------------------------------------


def test_byte_budget_drops_rows(ctx, monkeypatch):
    # Force the byte budget tiny so the guard triggers on a small result
    monkeypatch.setattr(
        "seeknal.ask.agents.tools.execute_sql._BYTE_BUDGET", 120
    )
    out = execute_sql("SELECT * FROM big", limit=500)
    assert "output exceeded" in out
    assert "Result truncated" in out


# ---------------------------------------------------------------------------
# Robustness: invalid limit, pipes in cells, errors pass through
# ---------------------------------------------------------------------------


def test_invalid_limit_coerces_to_default(ctx):
    # Passing a non-integer limit shouldn't explode — should fall back to 100
    out = execute_sql("SELECT * FROM small", limit="oops")  # type: ignore[arg-type]
    assert "(3 rows)" in out


def test_pipes_in_cells_are_escaped(ctx):
    ctx.repl.conn.execute(
        "CREATE TABLE piped AS SELECT 1 AS id, 'a|b|c' AS raw"
    )
    out = execute_sql("SELECT * FROM piped")
    # Literal pipes in the cell must be escaped so the markdown row stays aligned
    assert "a\\|b\\|c" in out


def test_sql_error_returns_tool_error(ctx):
    out = execute_sql("SELECT * FROM table_that_doesnt_exist")
    # The tool's error formatter returns a JSON blob. Check the substring.
    assert "retryable" in out or "category" in out
