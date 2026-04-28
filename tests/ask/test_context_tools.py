"""Tests for generated source-context and SQL-pair read tools."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from seeknal.ask.agents.tools._context import (
    ToolContext,
    get_authoritative_sql_pair_result,
    record_authoritative_sql_pair_result,
    reset_turn_governor,
    set_tool_context,
    should_synthesize_after_authoritative_sql_pair,
)
from seeknal.ask.agents.tools.list_ask_test_results import list_ask_test_results
from seeknal.ask.agents.tools.list_ask_tests import list_ask_tests
from seeknal.ask.agents.tools.list_source_context import list_source_context
from seeknal.ask.agents.tools.list_sql_pairs import list_sql_pairs
from seeknal.ask.agents.tools.read_ask_test import read_ask_test
from seeknal.ask.agents.tools.read_ask_test_result import read_ask_test_result
from seeknal.ask.agents.tools.read_source_context import read_source_context
from seeknal.ask.agents.tools.read_sql_pair import read_sql_pair
from seeknal.ask.agents.tools.execute_sql_pair import execute_sql_pair
from seeknal.ask.agents.tools.run_ask_test import run_ask_test
from seeknal.ask.agents.agent import _build_generated_source_context_index


class _Repl:
    attached = set()

    def __init__(self):
        self.calls: list[str] = []

    def execute_oneshot(self, sql: str, limit=None):
        self.calls.append(sql)
        return ["answer"], [(42,)]


def _set_ctx(tmp_path: Path):
    repl = _Repl()
    set_tool_context(
        ToolContext(
            repl=repl,
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
        )
    )
    return repl


def test_source_context_tools_list_and_read_generated_files(tmp_path: Path):
    _set_ctx(tmp_path)
    path = tmp_path / ".seeknal/context/sources/wh/tables/analytics.fact/columns.md"
    path.parent.mkdir(parents=True)
    path.write_text("# Columns for wh.analytics.fact\n\n- `id` (INTEGER)\n")

    listed = list_source_context(source="wh", query="columns")
    assert "wh/tables/analytics.fact/columns.md" in listed

    content = read_source_context("wh/tables/analytics.fact/columns.md")
    assert "Source context" in content
    assert "`id`" in content


def test_source_context_read_rejects_traversal(tmp_path: Path):
    _set_ctx(tmp_path)
    assert "cannot contain '..'" in read_source_context("../.env")


def test_sql_pair_tools_list_and_read_project_pairs(tmp_path: Path):
    _set_ctx(tmp_path)
    pair = tmp_path / "seeknal/sql_pairs/top_balai.yml"
    pair.parent.mkdir(parents=True)
    pair.write_text(
        "name: top_balai\n"
        "prompt: Which balai has the most inspections?\n"
        "sql: |\n"
        "  SELECT 1 AS total_pemeriksaan\n"
    )

    listed = list_sql_pairs(query="balai")
    assert "seeknal/sql_pairs/top_balai.yml" in listed
    assert "prompt:" in listed or "name:" in listed

    content = read_sql_pair("top_balai")
    assert "SQL pair" in content
    assert "total_pemeriksaan" in content


def test_sql_pair_read_handles_relative_project_path(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _set_ctx(Path("."))
    pair = tmp_path / "seeknal/sql_pairs/top_balai.yml"
    pair.parent.mkdir(parents=True)
    pair.write_text(
        "name: top_balai\n"
        "prompt: Which balai has the most inspections?\n"
        "sql: |\n"
        "  SELECT 1 AS total_pemeriksaan\n"
    )

    content = read_sql_pair("top_balai")

    assert "# SQL pair: seeknal/sql_pairs/top_balai.yml" in content
    assert "total_pemeriksaan" in content


def test_sql_pair_list_matches_spaces_against_slug_separators(tmp_path: Path):
    _set_ctx(tmp_path)
    pair = tmp_path / "seeknal/sql_pairs/bpom_rpo_jumlah_permohonan_bulanan.yml"
    pair.parent.mkdir(parents=True)
    pair.write_text(
        "name: bpom_rpo_jumlah_permohonan_bulanan\n"
        "prompt: Berapa jumlah permohonan per bulan tahun 2025?\n"
        "sql: |\n"
        "  SELECT 1 AS jumlah_permohonan\n"
    )

    listed = list_sql_pairs(query="jumlah permohonan")

    assert "bpom_rpo_jumlah_permohonan_bulanan.yml" in listed


def test_sql_pair_list_matches_natural_query_against_full_pair_text(tmp_path: Path):
    _set_ctx(tmp_path)
    pair = tmp_path / "seeknal/sql_pairs/bpom_rpo_jumlah_permohonan_bulanan.yml"
    pair.parent.mkdir(parents=True)
    pair.write_text(
        "name: bpom_rpo_jumlah_permohonan_bulanan\n"
        "prompt: Berapa jumlah permohonan per bulan tahun 2025?\n"
        "intent: Count monthly application volume.\n"
        "sql: |\n"
        "  SELECT 1 AS jumlah_permohonan\n"
    )

    listed = list_sql_pairs(query="berapa jumlah permohonan per bulan")

    assert "bpom_rpo_jumlah_permohonan_bulanan.yml" in listed


def test_execute_sql_pair_runs_stored_sql_once_as_non_authoritative_by_default(
    tmp_path: Path,
):
    repl = _set_ctx(tmp_path)
    pair = tmp_path / "seeknal/sql_pairs/answer.yml"
    pair.parent.mkdir(parents=True)
    pair.write_text(
        "name: answer\n"
        "prompt: What is the answer?\n"
        "sql: |\n"
        "  SELECT 42 AS answer\n"
    )

    out = execute_sql_pair("answer")

    assert "# Executed SQL pair: seeknal/sql_pairs/answer.yml" in out
    assert "SQL_PAIR_RESULT" in out
    assert "AUTHORITATIVE_RESULT" not in out
    assert "| answer |" in out
    assert "42" in out
    assert repl.calls == ["SELECT 42 AS answer"]
    assert get_authoritative_sql_pair_result() is None
    assert not should_synthesize_after_authoritative_sql_pair()


def test_execute_sql_pair_can_mark_exact_match_authoritative(tmp_path: Path):
    repl = _set_ctx(tmp_path)
    pair = tmp_path / "seeknal/sql_pairs/answer.yml"
    pair.parent.mkdir(parents=True)
    pair.write_text(
        "name: answer\n"
        "prompt: What is the answer?\n"
        "sql: |\n"
        "  SELECT 42 AS answer\n"
    )

    out = execute_sql_pair("answer", authoritative=True)

    assert "# Executed SQL pair: seeknal/sql_pairs/answer.yml" in out
    assert "AUTHORITATIVE_RESULT" in out
    assert "| answer |" in out
    assert "42" in out
    assert repl.calls == ["SELECT 42 AS answer"]
    assert get_authoritative_sql_pair_result() == {
        "name": "answer",
        "path": "seeknal/sql_pairs/answer.yml",
        "result": "| answer |\n| --- |\n| 42 |\n\n(1 row)",
    }
    assert should_synthesize_after_authoritative_sql_pair()


def test_authoritative_sql_pair_synthesizes_indonesian_jumlah_question(tmp_path: Path):
    _set_ctx(tmp_path)
    reset_turn_governor("berapa jumlah permohonan per bulan tahun 2025")
    record_authoritative_sql_pair_result(
        name="monthly",
        path="seeknal/sql_pairs/monthly.yml",
        result="| bulan | jumlah |\n| --- | --- |\n| 2025-01 | 1 |\n\n(1 row)",
    )

    assert should_synthesize_after_authoritative_sql_pair()


def test_sql_pair_read_rejects_missing_or_ambiguous(tmp_path: Path):
    _set_ctx(tmp_path)
    assert "not found" in read_sql_pair("missing")


def test_ask_test_tools_list_read_run_and_read_results(tmp_path: Path):
    _set_ctx(tmp_path)
    test_file = tmp_path / "seeknal/tests/answer.yml"
    test_file.parent.mkdir(parents=True)
    test_file.write_text(
        "name: answer\n" "prompt: What is the answer?\n" "sql: SELECT 42 AS answer\n"
    )

    listed = list_ask_tests(query="answer")
    assert "answer" in listed
    assert "seeknal/tests/answer.yml" in listed

    content = read_ask_test("answer")
    assert "Ask SQL test" in content
    assert "SELECT 42" in content

    run = run_ask_test("answer")
    assert "1/1 passed" in run
    assert "PASS `answer`" in run

    results = list_ask_test_results()
    assert "ask_sql_results_" in results

    latest = read_ask_test_result("latest")
    assert "Ask SQL test result" in latest
    assert '"passed": 1' in latest


def test_generated_source_context_index_lists_context_files(tmp_path: Path):
    path = tmp_path / ".seeknal/context/sources/wh/tables/analytics.orders/columns.md"
    path.parent.mkdir(parents=True)
    path.write_text("# Columns\n")

    index = _build_generated_source_context_index(tmp_path)

    assert index is not None
    assert "Generated Source Context Index" in index
    assert "wh/tables/analytics.orders/columns.md" in index
