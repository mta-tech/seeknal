"""Tests for the subprocess sandbox execution."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from seeknal.ask.sandbox import execute_in_sandbox, _format_output, _get_sandbox_env


@pytest.fixture
def sandbox_project(tmp_path):
    """Create a minimal project with parquet data for sandbox testing."""
    import pandas as pd

    # Create project structure
    intermediate = tmp_path / "target" / "intermediate"
    intermediate.mkdir(parents=True)

    # Create a test parquet
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "amount": [100.0, 200.0, 300.0],
    })
    df.to_parquet(intermediate / "source_customers.parquet", index=False)

    return tmp_path


class TestExecuteInSandbox:
    """Tests that actually run uv subprocess."""

    def test_simple_expression(self, sandbox_project):
        result = execute_in_sandbox("1 + 2", sandbox_project)
        assert "3" in result

    def test_print_captured(self, sandbox_project):
        result = execute_in_sandbox("print('hello sandbox')", sandbox_project)
        assert "hello sandbox" in result

    def test_last_expression_value(self, sandbox_project):
        result = execute_in_sandbox("x = 10\nx * 3", sandbox_project)
        assert "30" in result

    def test_pandas_available(self, sandbox_project):
        result = execute_in_sandbox("print(type(pd).__name__)", sandbox_project)
        assert "module" in result

    def test_numpy_available(self, sandbox_project):
        result = execute_in_sandbox("print(type(np).__name__)", sandbox_project)
        assert "module" in result

    def test_conn_queries_parquet(self, sandbox_project):
        """The subprocess DuckDB connection should see registered parquets."""
        code = "conn.sql('SELECT count(*) as n FROM source_customers').df()['n'][0]"
        result = execute_in_sandbox(code, sandbox_project)
        assert "3" in result

    def test_conn_sql_to_dataframe(self, sandbox_project):
        code = "df = conn.sql('SELECT name FROM source_customers ORDER BY name').df()\nlist(df['name'])"
        result = execute_in_sandbox(code, sandbox_project)
        assert "Alice" in result
        assert "Bob" in result
        assert "Charlie" in result

    def test_pandas_operations(self, sandbox_project):
        code = (
            "df = conn.sql('SELECT * FROM source_customers').df()\n"
            "df['amount'].mean()"
        )
        result = execute_in_sandbox(code, sandbox_project)
        assert "200" in result

    def test_syntax_error(self, sandbox_project):
        result = execute_in_sandbox("def !!!", sandbox_project)
        assert "SyntaxError" in result or "Error" in result

    def test_runtime_error(self, sandbox_project):
        result = execute_in_sandbox("1 / 0", sandbox_project)
        assert "ZeroDivisionError" in result or "Error" in result

    def test_empty_code(self, sandbox_project):
        result = execute_in_sandbox("", sandbox_project)
        assert "No code provided" in result

    def test_no_output(self, sandbox_project):
        result = execute_in_sandbox("x = 42", sandbox_project)
        assert "no output" in result.lower() or "executed successfully" in result.lower()

    def test_timeout(self, sandbox_project):
        """Long-running code should be killed by timeout."""
        result = execute_in_sandbox(
            "import time; time.sleep(60)", sandbox_project, timeout=2
        )
        assert "timed out" in result.lower()

    def test_multiline_with_print_and_expr(self, sandbox_project):
        code = "x = 5\nprint(f'x is {x}')\nx * 2"
        result = execute_in_sandbox(code, sandbox_project)
        assert "x is 5" in result
        assert "10" in result

    def test_no_access_to_api_keys(self, sandbox_project):
        """Sandbox should strip sensitive env vars."""
        code = "import os\nos.environ.get('OPENAI_API_KEY', 'NOT_SET')"
        result = execute_in_sandbox(code, sandbox_project)
        assert "NOT_SET" in result or "None" in result


class TestFormatOutput:
    def test_stdout_only(self):
        data = {"stdout": "hello\n", "error": None, "value": None, "plots": []}
        assert "hello" in _format_output(data)

    def test_value_only(self):
        data = {"stdout": "", "error": None, "value": "42", "plots": []}
        assert "42" in _format_output(data)

    def test_error(self):
        data = {"stdout": "", "error": "ZeroDivisionError", "value": None, "plots": []}
        result = _format_output(data)
        assert "Error" in result
        assert "ZeroDivisionError" in result

    def test_plots(self):
        data = {"stdout": "", "error": None, "value": None, "plots": ["/tmp/plot.png"]}
        result = _format_output(data)
        assert "plot.png" in result

    def test_empty(self):
        data = {"stdout": "", "error": None, "value": None, "plots": []}
        result = _format_output(data)
        assert "no output" in result.lower() or "executed successfully" in result.lower()

    def test_stdout_and_value(self):
        data = {"stdout": "step 1 done\n", "error": None, "value": "result", "plots": []}
        result = _format_output(data)
        assert "step 1 done" in result
        assert "result" in result


class TestGetSandboxEnv:
    def test_strips_api_keys(self):
        with patch.dict("os.environ", {"OPENAI_API_KEY": "sk-test", "HOME": "/home"}):
            env = _get_sandbox_env()
            assert "OPENAI_API_KEY" not in env
            assert "HOME" in env

    def test_strips_tokens(self):
        with patch.dict("os.environ", {"GITHUB_TOKEN": "ghp_xxx", "PATH": "/usr/bin"}):
            env = _get_sandbox_env()
            assert "GITHUB_TOKEN" not in env
            assert "PATH" in env

    def test_strips_secrets(self):
        with patch.dict("os.environ", {"DB_SECRET": "xyz", "TERM": "xterm"}):
            env = _get_sandbox_env()
            assert "DB_SECRET" not in env
            assert "TERM" in env
