"""Tests for the execute_sql tool — context-window guards."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from seeknal.ask.agents.tools._context import (
    ToolContext,
    get_loaded_sql_pairs,
    mark_sql_pairs_checked,
    record_authoritative_sql_pair_result,
    reset_turn_governor,
    set_tool_context,
)
from seeknal.ask.agents.tools.execute_sql import execute_sql


class _REPLStub:
    """Minimal REPL shim — forwards execute_oneshot to a real in-memory DuckDB."""

    def __init__(self) -> None:
        self.conn = duckdb.connect(":memory:")
        self.calls: list[str] = []

    def execute_oneshot(self, sql: str, limit=None):
        self.calls.append(sql)
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


def test_blank_string_cells_are_labeled(ctx):
    ctx.repl.conn.execute(
        "CREATE TABLE blanks AS SELECT ' ' AS scale, 54 AS products"
    )
    out = execute_sql("SELECT * FROM blanks")

    assert "| [blank] | 54 |" in out


def test_sql_error_returns_tool_error(ctx):
    out = execute_sql("SELECT * FROM table_that_doesnt_exist")
    # The tool's error formatter returns a JSON blob. Check the substring.
    assert "retryable" in out or "category" in out


def test_missing_table_suggestion_is_retried(tmp_path: Path):
    class ReplWithSuggestion:
        def __init__(self):
            self.calls: list[str] = []

        def execute_oneshot(self, sql: str, limit=None):
            self.calls.append(sql)
            if " monthly_revenue" in sql:
                raise Exception(
                    'Catalog Error: Table with name monthly_revenue does not exist!\n'
                    'Did you mean "wh.analytics.monthly_revenue"?'
                )
            assert "wh.analytics.monthly_revenue" in sql
            return ["revenue"], [(123.0,)]

    repl = ReplWithSuggestion()
    set_tool_context(
        ToolContext(
            repl=repl,
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
        )
    )

    out = execute_sql("SELECT SUM(revenue) AS revenue FROM monthly_revenue")

    assert "| revenue |" in out
    assert "123.0" in out
    assert any("wh.analytics.monthly_revenue" in call for call in repl.calls)


def test_query_alias_executes(ctx):
    out = execute_sql(query="SELECT * FROM small ORDER BY id", limit=2)
    assert "| id | v |" in out
    assert "2 of 3 rows shown" in out


def test_missing_sql_returns_retryable_hint(ctx):
    out = execute_sql()
    assert "Missing SQL query" in out
    assert "retryable" in out
    assert "sql" in out


def test_function_style_ilike_is_repaired(ctx):
    ctx.repl.conn.execute("CREATE TABLE names AS SELECT 'Air Mineral' AS name")

    out = execute_sql("SELECT name FROM names WHERE ILIKE(name, '%air%')")

    assert "Air Mineral" in out
    assert "rewrote function-style ILIKE" in out


def test_order_by_integer_cast_is_repaired_to_try_cast(ctx):
    ctx.repl.conn.execute(
        "CREATE TABLE labels AS SELECT 'Tidak Diketahui' AS label UNION ALL SELECT '2' AS label"
    )
    out = execute_sql("SELECT label FROM labels ORDER BY CAST(label AS INTEGER)")

    assert "Tidak Diketahui" in out
    assert "TRY_CAST" in out or "rewrote ORDER BY CAST" in out


def test_repeated_failed_sql_is_blocked_by_failure_memory(ctx):
    first = execute_sql("SELECT * FROM table_that_doesnt_exist")
    second = execute_sql("SELECT * FROM table_that_doesnt_exist")

    assert "retryable" in first
    assert "already failed" in second


def test_repeated_successful_sql_reuses_cached_result(ctx):
    first = execute_sql("SELECT * FROM small ORDER BY id")
    calls_after_first = len(ctx.repl.calls)

    second = execute_sql("  SELECT   * FROM small ORDER BY id ; ")

    assert "| id | v |" in first
    assert "Reusing prior successful SQL result" in second
    assert "| id | v |" in second
    assert len(ctx.repl.calls) == calls_after_first


def test_refresh_bypasses_successful_sql_cache(ctx):
    execute_sql("SELECT * FROM small ORDER BY id")
    calls_after_first = len(ctx.repl.calls)

    second = execute_sql("SELECT * FROM small ORDER BY id", refresh=True)

    assert "| id | v |" in second
    assert len(ctx.repl.calls) > calls_after_first


def test_loaded_sql_pair_drift_is_warned_without_blocking(ctx):
    get_loaded_sql_pairs(ctx)["canonical"] = (
        "SELECT id, v FROM small ORDER BY id"
    )

    out = execute_sql("SELECT COUNT(*) AS total FROM small", allow_sql_pair_drift=True)

    assert "| total |" in out
    assert "SQL pair drift" in out


def test_loaded_sql_pair_drift_is_blocked_by_default(ctx):
    get_loaded_sql_pairs(ctx)["canonical"] = (
        "SELECT id, v FROM small ORDER BY id"
    )

    out = execute_sql("SELECT COUNT(*) AS total FROM small")

    assert "SQL differs from the SQL pair" in out
    assert not any("COUNT(*) AS total" in call for call in ctx.repl.calls)


def test_authoritative_sql_pair_explicit_override_lets_drift_through(ctx):
    """An authoritative pair must not silently lock out an explicit override.

    Previous behavior (pre-Fix-A) treated ``allow_sql_pair_drift=True`` as a
    no-op once a pair landed authoritatively for the turn, even for ordinary
    business questions. This caused the agent to be stuck on a broken pair's
    numbers. The override now means what it says: the agent is taking
    responsibility for the drift, and the query runs with a strong notice.
    """
    reset_turn_governor("How many records are there by month?")
    # Simulate read_sql_pair-style preload of canonical SQL plus an
    # authoritative landing recorded by execute_sql_pair(authoritative=True).
    from seeknal.ask.agents.tools._context import get_loaded_sql_pairs as _glp
    _glp(ctx)["canonical"] = "SELECT id, v FROM small ORDER BY id"
    record_authoritative_sql_pair_result(
        name="canonical",
        path="seeknal/sql_pairs/canonical.yml",
        result="| month | count |\n| --- | --- |\n| 2025-01 | 3 |\n\n(1 row)",
        ctx=ctx,
    )

    out = execute_sql(
        "SELECT COUNT(*) AS total FROM small",
        allow_sql_pair_drift=True,
    )

    assert "| total |" in out
    assert "SQL pair drift" in out
    assert any("COUNT(*) AS total" in call for call in ctx.repl.calls)


def test_authoritative_sql_pair_first_drift_attempt_is_retryable(ctx):
    """Fix A — Guard 3's first hit must be RETRYABLE, not TERMINAL.

    The agent should learn about the authoritative pair and the override
    flag once, retry intentionally, then have its query execute. Going
    straight to TERMINAL on the first attempt was the lock-in behavior.
    """
    reset_turn_governor("Berapa total jumlah NIE?")
    from seeknal.ask.agents.tools._context import get_loaded_sql_pairs as _glp
    _glp(ctx)["canonical"] = "SELECT id, v FROM small ORDER BY id"
    record_authoritative_sql_pair_result(
        name="canonical",
        path="seeknal/sql_pairs/canonical.yml",
        result="| nie | count |\n| --- | --- |\n| 12345 | 3 |\n\n(1 row)",
        ctx=ctx,
    )

    out = execute_sql("SELECT COUNT(*) AS total FROM small")

    assert "retryable_syntax" in out
    assert "allow_sql_pair_drift=True" in out
    assert not any("COUNT(*) AS total" in call for call in ctx.repl.calls)


def test_authoritative_sql_pair_second_drift_attempt_is_terminal(ctx):
    """Fix A — second un-overridden attempt escalates to TERMINAL.

    Each attempt uses a different drifting SQL so the per-signature
    repeated_failure_message guard (which would short-circuit duplicate
    queries) does not pre-empt the drift escalation logic.
    """
    reset_turn_governor("Berapa total jumlah NIE?")
    from seeknal.ask.agents.tools._context import get_loaded_sql_pairs as _glp
    _glp(ctx)["canonical"] = "SELECT id, v FROM small ORDER BY id"
    record_authoritative_sql_pair_result(
        name="canonical",
        path="seeknal/sql_pairs/canonical.yml",
        result="| nie | count |\n| --- | --- |\n| 12345 | 3 |\n\n(1 row)",
        ctx=ctx,
    )

    first = execute_sql("SELECT COUNT(*) AS total FROM small")
    second = execute_sql("SELECT COUNT(*) AS jumlah FROM small WHERE id > 0")

    assert "retryable_syntax" in first
    assert "terminal_bounded_evidence" in second
    assert not any("COUNT" in call for call in ctx.repl.calls)


def test_authoritative_sql_pair_allows_drift_for_advanced_question(ctx):
    reset_turn_governor("Model the correlation after getting the base query")
    get_loaded_sql_pairs(ctx)["canonical"] = "SELECT id, v FROM small ORDER BY id"
    record_authoritative_sql_pair_result(
        name="canonical",
        path="seeknal/sql_pairs/canonical.yml",
        result="| id | v |\n| --- | --- |\n| 1 | hi |\n\n(1 row)",
        ctx=ctx,
    )

    out = execute_sql(
        "SELECT COUNT(*) AS total FROM small",
        allow_sql_pair_drift=True,
    )

    assert "| total |" in out
    assert "SQL pair drift" in out


def test_ad_hoc_business_sql_requires_sql_pair_lookup_when_pairs_exist(ctx, tmp_path):
    pair = tmp_path / "seeknal" / "sql_pairs" / "answer.yml"
    pair.parent.mkdir(parents=True)
    pair.write_text("name: answer\nprompt: answer\nsql: SELECT 42 AS answer\n")
    reset_turn_governor("What is the answer?")

    out = execute_sql("SELECT COUNT(*) AS total FROM small")

    assert "Project SQL pairs exist" in out
    assert not any("COUNT(*) AS total" in call for call in ctx.repl.calls)


def test_ad_hoc_sql_allowed_after_sql_pair_lookup(ctx, tmp_path):
    pair = tmp_path / "seeknal" / "sql_pairs" / "answer.yml"
    pair.parent.mkdir(parents=True)
    pair.write_text("name: answer\nprompt: answer\nsql: SELECT 42 AS answer\n")
    reset_turn_governor("What is the answer?")
    mark_sql_pairs_checked(ctx)

    out = execute_sql("SELECT COUNT(*) AS total FROM small")

    assert "| total |" in out
class _SlowRepl:
    def execute_oneshot(self, sql: str, limit=None):
        import time

        time.sleep(2)
        return ["n"], [(1,)]


def test_execute_sql_returns_terminal_timeout(tmp_path):
    from unittest.mock import MagicMock

    from seeknal.ask.agents.tools._context import ToolContext, set_tool_context

    set_tool_context(
        ToolContext(
            repl=_SlowRepl(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
            sql_timeout_seconds=1,
        )
    )

    out = execute_sql(sql="SELECT 1")

    assert "terminal_timeout" in out
    assert "SQL execution timed out after 1 seconds" in out


# ---------------------------------------------------------------------------
# Fix B — zero-row SQL pair must not become authoritative
# ---------------------------------------------------------------------------


def test_empty_pair_result_is_not_authoritative(tmp_path):
    """A broken SQL pair that silently returns no rows must NOT lock the turn.

    Reproduces the run-2 wrong-numbers symptom: a pair with a wrongly quoted
    qualified table name like ``"warehouse.public.t_x"`` resolves as a missing
    relation in DuckDB but the surrounding UNION ALL collapses the failure to
    an empty result. Before Fix B, that empty result was recorded as
    authoritative and locked the agent to wrong numbers.
    """
    from unittest.mock import MagicMock

    from seeknal.ask.agents.tools._context import (
        ToolContext,
        get_authoritative_sql_pair_result,
        reset_turn_governor,
        set_tool_context,
    )
    from seeknal.ask.agents.tools.execute_sql_pair import execute_sql_pair

    class _EmptyRepl:
        def execute_oneshot(self, sql, limit=None):
            return ["jumlah_nie"], []  # zero rows — silent UNION-ALL failure shape

    ctx = ToolContext(
        repl=_EmptyRepl(),
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
    )
    set_tool_context(ctx)
    reset_turn_governor("Berapa total jumlah NIE?")

    pair = tmp_path / "seeknal" / "sql_pairs" / "broken.yml"
    pair.parent.mkdir(parents=True)
    pair.write_text(
        'name: broken\n'
        'prompt: Berapa total jumlah NIE\n'
        'sql: |\n'
        '  SELECT nomor FROM "warehouse.public.t_x" "main"\n'
    )

    out = execute_sql_pair("broken", authoritative=True)

    assert "AUTHORITATIVE_RESULT" not in out
    assert "not authoritative" in out
    assert get_authoritative_sql_pair_result(ctx) is None


def test_null_only_aggregate_is_not_authoritative(tmp_path):
    """A pair whose only row is NULL/0 across all aggregate cells is suspect.

    This is the second silent-failure shape: COUNT/SUM over an empty
    intermediate select returns one row of all-NULL/0 instead of zero rows.
    """
    from unittest.mock import MagicMock

    from seeknal.ask.agents.tools._context import (
        ToolContext,
        get_authoritative_sql_pair_result,
        reset_turn_governor,
        set_tool_context,
    )
    from seeknal.ask.agents.tools.execute_sql_pair import execute_sql_pair

    class _NullRepl:
        def execute_oneshot(self, sql, limit=None):
            return ["jumlah_nie"], [(None,)]

    ctx = ToolContext(
        repl=_NullRepl(),
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
    )
    set_tool_context(ctx)
    reset_turn_governor("Berapa total jumlah NIE?")

    pair = tmp_path / "seeknal" / "sql_pairs" / "broken.yml"
    pair.parent.mkdir(parents=True)
    pair.write_text(
        'name: broken\n'
        'prompt: Berapa total jumlah NIE\n'
        'sql: |\n'
        '  SELECT COUNT(*) AS jumlah_nie FROM warehouse.public.empty\n'
    )

    out = execute_sql_pair("broken", authoritative=True)

    assert "AUTHORITATIVE_RESULT" not in out
    assert "not authoritative" in out
    assert get_authoritative_sql_pair_result(ctx) is None


# ---------------------------------------------------------------------------
# Fix C — filter-tweak questions release Guard 3
# ---------------------------------------------------------------------------


def test_filter_tweak_question_releases_guard3(ctx):
    """Asking for the same metric with an added exclusion is post-SQL analysis.

    Before Fix C, ``_question_requests_post_sql_analysis`` only matched
    explicit analytical keywords (forecast, chart, model, ...). A user asking
    "kecuali test accounts" or "tanpa tahun 1900" was dropped into Guard 3
    lockout. The expanded keyword list now treats those as legitimate
    follow-up scope so Guard 3 is dormant — Guard 2 (the polite RETRYABLE
    drift nudge) still applies, which is the desired behavior.
    """
    from seeknal.ask.agents.tools._context import (
        get_loaded_sql_pairs as _glp,
        should_synthesize_after_authoritative_sql_pair,
    )

    reset_turn_governor("Berapa total NIE kecuali test account?")
    _glp(ctx)["canonical"] = "SELECT id, v FROM small ORDER BY id"
    record_authoritative_sql_pair_result(
        name="canonical",
        path="seeknal/sql_pairs/canonical.yml",
        result="| nie | count |\n| --- | --- |\n| 12345 | 3 |\n\n(1 row)",
        ctx=ctx,
    )

    # Guard 3 must NOT lock — the question is a filter tweak.
    assert not should_synthesize_after_authoritative_sql_pair(ctx)
    # Override works (Guard 3 dormant means the override is honored).
    overridden = execute_sql(
        "SELECT COUNT(*) AS total FROM small",
        allow_sql_pair_drift=True,
    )
    assert "| total |" in overridden
    assert "SQL pair drift" in overridden


# ---------------------------------------------------------------------------
# Fix D — advisory mode never escalates Guard 3
# ---------------------------------------------------------------------------


def test_advisory_mode_never_locks_authoritative(ctx):
    """``sql_pairs.mode: advisory`` keeps pairs as cheatsheets only."""
    from seeknal.ask.agents.tools._context import (
        get_loaded_sql_pairs as _glp,
        should_synthesize_after_authoritative_sql_pair,
    )

    reset_turn_governor("Berapa total jumlah NIE?")
    ctx.sql_pair_mode = "advisory"
    _glp(ctx)["canonical"] = "SELECT id, v FROM small ORDER BY id"
    record_authoritative_sql_pair_result(
        name="canonical",
        path="seeknal/sql_pairs/canonical.yml",
        result="| nie | count |\n| --- | --- |\n| 12345 | 3 |\n\n(1 row)",
        ctx=ctx,
    )

    assert not should_synthesize_after_authoritative_sql_pair(ctx)

    out = execute_sql("SELECT COUNT(*) AS total FROM small")

    assert "| total |" in out
    assert "SQL pair drift" in out


def test_get_sql_pair_mode_reads_yaml_section(tmp_path):
    """End-to-end: ``seeknal_agent.yml`` → ToolContext.sql_pair_mode."""
    from seeknal.ask.config import get_sql_pair_mode

    assert get_sql_pair_mode({}) == "authoritative"
    assert get_sql_pair_mode({"sql_pairs": {"mode": "advisory"}}) == "advisory"
    # Unknown mode falls back to authoritative (typo-safe).
    assert get_sql_pair_mode({"sql_pairs": {"mode": "lenient"}}) == "authoritative"


# ---------------------------------------------------------------------------
# Fix E — context/sql_pairs no longer triggers Guard 1
# ---------------------------------------------------------------------------


def test_context_sql_pairs_dir_does_not_trigger_lookup_guard(ctx, tmp_path):
    """Files under ``context/sql_pairs/`` must not force a lookup.

    That directory is reserved for generated-source context per CLAUDE.md;
    treating it as a curated-cheatsheet trigger conflated two storage layers.
    """
    pair = tmp_path / "context" / "sql_pairs" / "generated.yml"
    pair.parent.mkdir(parents=True)
    pair.write_text("name: generated\nprompt: any\nsql: SELECT 1\n")
    reset_turn_governor("Berapa total jumlah NIE?")

    out = execute_sql("SELECT COUNT(*) AS total FROM small")

    assert "Project SQL pairs exist" not in out
    assert "| total |" in out


# ---------------------------------------------------------------------------
# Phase B (issue #64) — EXTRACT pushdown integration
# ---------------------------------------------------------------------------


def test_ac_b1_extract_pushdown_lint_notice_in_result(ctx):
    """AC-B1: execute_sql result contains the EXTRACT-pushdown lint notice."""
    ctx.repl.conn.execute(
        "CREATE TABLE pg_t AS SELECT DATE '2023-06-15' AS tanggal"
    )
    out = execute_sql("SELECT * FROM pg_t WHERE EXTRACT(YEAR FROM tanggal) = 2023")
    # Result must surface the EXTRACT rewrite to the user, matching the
    # existing ILIKE/TRY_CAST notice style.
    assert "ℹ SQL lint: rewrote EXTRACT(...) to a pushdown-safe date range" in out


def test_ac_b2_extract_pushdown_cache_hit_skips_execute(ctx):
    """AC-B2: second invocation of same SQL hits the cache; execute called once."""
    from unittest.mock import patch

    ctx.repl.conn.execute(
        "CREATE TABLE pg_t2 AS SELECT DATE '2023-06-15' AS tanggal"
    )
    sql = "SELECT * FROM pg_t2 WHERE EXTRACT(YEAR FROM tanggal) = 2023"

    # Run once to populate the cache.
    first = execute_sql(sql)
    assert "ℹ SQL lint" in first

    # Now patch the executor and call again — it must NOT be invoked.
    with patch(
        "seeknal.ask.agents.tools.execute_sql._execute_oneshot_with_timeout"
    ) as mocked:
        second = execute_sql(sql)
        mocked.assert_not_called()
    # Cached result keeps the lint notice via the cached string.
    assert "ℹ SQL lint" in second
    assert "Reusing prior successful SQL result" in second


def test_ac_b3_execute_sql_pair_delegation_emits_lint_notice(ctx, tmp_path):
    """AC-B3: execute_sql_pair with EXTRACT YAML produces the same lint notice."""
    from seeknal.ask.agents.tools.execute_sql_pair import execute_sql_pair

    ctx.repl.conn.execute(
        "CREATE TABLE pg_t3 AS SELECT DATE '2023-06-15' AS tanggal"
    )
    pair = tmp_path / "seeknal" / "sql_pairs" / "extract_year.yml"
    pair.parent.mkdir(parents=True)
    pair.write_text(
        "name: extract_year\n"
        "prompt: rows in 2023\n"
        "sql: |\n"
        "  SELECT * FROM pg_t3 WHERE EXTRACT(YEAR FROM tanggal) = 2023\n"
    )

    out = execute_sql_pair("extract_year")
    assert "ℹ SQL lint: rewrote EXTRACT(...) to a pushdown-safe date range" in out


def test_ac_b4_drift_notice_does_not_fire_for_extract_pair(ctx, tmp_path):
    """AC-B4: drift notice is silent when pair SQL and agent SQL match post-rewrite."""
    from seeknal.ask.agents.tools._context import get_loaded_sql_pairs

    ctx.repl.conn.execute(
        "CREATE TABLE pg_t4 AS SELECT DATE '2023-06-15' AS tanggal"
    )
    pair_sql = "SELECT * FROM pg_t4 WHERE EXTRACT(YEAR FROM tanggal) = 2023"
    get_loaded_sql_pairs(ctx)["extract_year"] = pair_sql

    # Same EXTRACT form — drift normalization rewrites both sides identically.
    out = execute_sql(pair_sql, allow_sql_pair_drift=True)
    assert "⚠ SQL pair drift" not in out
    assert "| tanggal |" in out
