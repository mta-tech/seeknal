"""Tests for project-local Seeknal Ask SQL QA runner."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from seeknal.ask.testing import (
    AskSqlTestResult,
    AskSqlTestSuiteResult,
    SqlOracleResult,
    check_answer,
    discover_ask_sql_tests,
    extract_answer_table,
    run_ask_sql_tests,
    run_agent_answer,
)


def test_discover_ask_sql_tests_from_seeknal_tests(tmp_path: Path):
    test_file = tmp_path / "seeknal/tests/total_revenue.yml"
    test_file.parent.mkdir(parents=True)
    test_file.write_text(
        "name: total_revenue\n"
        "prompt: What is total revenue?\n"
        "sql: |\n"
        "  SELECT 42 AS total_revenue\n",
        encoding="utf-8",
    )

    cases = discover_ask_sql_tests(tmp_path)

    assert len(cases) == 1
    assert cases[0].name == "total_revenue"
    assert cases[0].prompt == "What is total revenue?"
    assert "SELECT 42" in cases[0].sql


def test_run_ask_sql_tests_sql_only_executes_expected_sql(tmp_path: Path):
    test_file = tmp_path / "seeknal/tests/answer.yml"
    test_file.parent.mkdir(parents=True)
    test_file.write_text(
        "name: answer\n"
        "question: What is the answer?\n"
        "expected_sql: SELECT 42 AS answer\n",
        encoding="utf-8",
    )

    suite = run_ask_sql_tests(tmp_path, sql_only=True)

    assert suite.total == 1
    assert suite.failed == 0
    assert suite.results[0].expected_columns == ["answer"]
    assert suite.results[0].expected_rows == [[42]]
    assert suite.output_file is not None
    assert Path(suite.output_file).exists()


def test_run_ask_sql_tests_fails_invalid_project_test_yaml(tmp_path: Path):
    test_file = tmp_path / "seeknal/tests/broken.yml"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("name: broken\nprompt: Missing SQL\n", encoding="utf-8")

    suite = run_ask_sql_tests(tmp_path, sql_only=True, save=False)

    assert suite.failed == 1
    assert suite.results[0].mode == "load"
    assert "expected_sql" in suite.results[0].message


def test_run_ask_sql_tests_agent_mode_uses_generic_answer_assertions(
    tmp_path: Path, monkeypatch
):
    test_file = tmp_path / "seeknal/tests/revenue.yml"
    test_file.parent.mkdir(parents=True)
    test_file.write_text(
        "name: revenue\n" "prompt: What is revenue?\n" "sql: SELECT 1250 AS revenue\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "seeknal.ask.testing.run_agent_answer",
        lambda *_args, **_kwargs: "Revenue is 1,250.",
    )

    suite = run_ask_sql_tests(tmp_path, sql_only=False, save=False)

    assert suite.total == 1
    assert suite.failed == 0
    assert suite.results[0].answer == "Revenue is 1,250."


def test_check_answer_supports_project_owned_contains_assertions():
    result = check_answer(
        "Top category is AMDK with 12 products.",
        oracle=type("Oracle", (), {"rows": [["unused"]], "columns": ["category"]})(),
        assertions={
            "answer_contains": ["AMDK", "12"],
            "answer_not_contains": "cosmetics",
        },
    )

    assert result.passed is True


def test_check_answer_supports_dataframe_markdown_comparison():
    oracle = SqlOracleResult(
        columns=["year", "revenue"],
        rows=[[2024, 1250.0], [2025, 1300.0]],
    )
    answer = """
    | year | revenue |
    | --- | ---: |
    | 2025 | 1,300 |
    | 2024 | 1,250.00 |
    """

    result = check_answer(answer, oracle, assertions={"compare": "dataframe"})

    assert result.passed is True


def test_extract_answer_table_supports_json_data_shape():
    answer = '{"data": [{"year": 2024, "revenue": 1250}]}'

    columns, rows, error = extract_answer_table(answer, ["year", "revenue"])

    assert error is None
    assert columns == ["year", "revenue"]
    assert rows == [[2024, 1250]]


def test_run_ask_sql_tests_agent_mode_records_actual_rows_for_dataframe(
    tmp_path: Path, monkeypatch
):
    test_file = tmp_path / "seeknal/tests/revenue.yml"
    test_file.parent.mkdir(parents=True)
    test_file.write_text(
        "name: revenue\n"
        "prompt: What is revenue by year?\n"
        "sql: SELECT 2024 AS year, 1250 AS revenue\n"
        "assert:\n"
        "  compare: dataframe\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "seeknal.ask.testing.run_agent_answer",
        lambda *_args, **_kwargs: (
            "| year | revenue |\n| --- | ---: |\n| 2024 | 1,250 |"
        ),
    )

    suite = run_ask_sql_tests(tmp_path, sql_only=False, save=False)

    assert suite.failed == 0
    assert suite.results[0].actual_columns == ["year", "revenue"]
    assert suite.results[0].actual_rows == [[2024, 1250]]


def test_run_agent_answer_restores_existing_tool_context(tmp_path: Path, monkeypatch):
    from unittest.mock import MagicMock

    from seeknal.ask.agents.tools._context import (
        ToolContext,
        get_tool_context,
        set_tool_context,
    )

    original = ToolContext(
        repl=MagicMock(),
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
    )
    set_tool_context(original)

    monkeypatch.setattr(
        "seeknal.ask.agents.agent.create_agent",
        lambda *_args, **_kwargs: (object(), object(), [], {}),
    )
    monkeypatch.setattr(
        "seeknal.ask.agents.agent.ask",
        lambda *_args, **_kwargs: "answer",
    )

    assert run_agent_answer(tmp_path, "question") == "answer"
    assert get_tool_context() is original


def test_ask_test_cli_prints_summary(tmp_path: Path, monkeypatch):
    from seeknal.cli.ask import ask_app

    runner = CliRunner()
    suite = AskSqlTestSuiteResult(
        project_path=str(tmp_path),
        timestamp="2026-04-26T00:00:00+00:00",
        mode="sql-only",
        output_file=str(tmp_path / "results.json"),
        results=[
            AskSqlTestResult(
                name="answer",
                file="seeknal/tests/answer.yml",
                prompt="What is the answer?",
                passed=True,
                mode="sql-only",
                message="expected SQL passed",
            )
        ],
    )
    monkeypatch.setattr(
        "seeknal.ask.testing.run_ask_sql_tests", lambda *_a, **_kw: suite
    )

    result = runner.invoke(ask_app, ["test", "--project", str(tmp_path), "--sql-only"])

    assert result.exit_code == 0
    assert "Seeknal Ask tests" in result.output
    assert "1/1 passed" in result.output


def test_ask_test_cli_json_output(tmp_path: Path, monkeypatch):
    from seeknal.cli.ask import ask_app

    runner = CliRunner()
    suite = AskSqlTestSuiteResult(
        project_path=str(tmp_path),
        timestamp="2026-04-26T00:00:00+00:00",
        mode="sql-only",
        results=[],
    )
    monkeypatch.setattr(
        "seeknal.ask.testing.run_ask_sql_tests", lambda *_a, **_kw: suite
    )

    result = runner.invoke(ask_app, ["test", "--project", str(tmp_path), "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["summary"]["total"] == 0
