"""Tests for execute_python tool."""

import pytest
from unittest.mock import MagicMock

from seeknal.ask.agents.tools.execute_python import (
    _do_execute,
    _infer_error_hint,
    _imported_top_level_modules,
    _missing_module_from_error,
    execute_python,
    _split_last_expression,
)
from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools._safe_connection import SafeConnection


@pytest.fixture
def mock_conn():
    """Create a mock DuckDB connection."""
    conn = MagicMock()
    conn.sql.return_value.df.return_value = "mock_df"
    return conn


class TestSplitLastExpression:
    def test_expression_at_end(self):
        body, expr = _split_last_expression("x = 1\nx + 2")
        assert body is not None
        assert expr is not None

    def test_statement_at_end(self):
        body, expr = _split_last_expression("x = 1\ny = 2")
        assert isinstance(body, str)
        assert expr is None

    def test_single_expression(self):
        body, expr = _split_last_expression("42")
        assert body is None
        assert expr is not None

    def test_syntax_error(self):
        body, expr = _split_last_expression("def !!!")
        assert expr is None

    def test_empty_code(self):
        body, expr = _split_last_expression("")
        assert expr is None

    def test_function_call_expression(self):
        body, expr = _split_last_expression("x = [1,2,3]\nlen(x)")
        assert expr is not None


class TestDoExecute:
    def test_simple_expression(self, mock_conn):
        result = _do_execute("1 + 2", mock_conn)
        assert "3" in result

    def test_stdout_capture(self, mock_conn):
        result = _do_execute("print('hello world')", mock_conn)
        assert "hello world" in result

    def test_last_expression_captured(self, mock_conn):
        result = _do_execute("x = 10\nx * 2", mock_conn)
        assert "20" in result

    def test_multiline_code(self, mock_conn):
        code = "x = 5\ny = 10\nresult = x + y\nprint(f'Sum is {result}')"
        result = _do_execute(code, mock_conn)
        assert "Sum is 15" in result

    def test_syntax_error_returns_traceback(self, mock_conn):
        result = _do_execute("def !!!", mock_conn)
        assert "SyntaxError" in result or "Error" in result

    def test_runtime_error_returns_traceback(self, mock_conn):
        result = _do_execute("1 / 0", mock_conn)
        assert "ZeroDivisionError" in result or "Error" in result

    def test_empty_code(self, mock_conn):
        result = _do_execute("", mock_conn)
        assert "No code provided" in result

    def test_whitespace_only(self, mock_conn):
        result = _do_execute("   \n  ", mock_conn)
        assert "No code provided" in result

    def test_pandas_available(self, mock_conn):
        result = _do_execute("type(pd).__name__", mock_conn)
        assert "module" in result

    def test_numpy_available(self, mock_conn):
        result = _do_execute("type(np).__name__", mock_conn)
        assert "module" in result

    def test_conn_is_safe_connection(self, mock_conn):
        result = _do_execute("type(conn).__name__", mock_conn)
        assert "SafeConnection" in result

    def test_no_output_message(self, mock_conn):
        result = _do_execute("x = 42", mock_conn)
        assert "no output" in result.lower() or "executed successfully" in result.lower()


class TestSafeConnection:
    def test_sql_validates(self):
        mock_inner = MagicMock()
        safe = SafeConnection(mock_inner)

        # Valid SQL should pass through
        safe.sql("SELECT 1")
        mock_inner.sql.assert_called_once_with("SELECT 1")

    def test_sql_blocks_dangerous_functions(self):
        mock_inner = MagicMock()
        safe = SafeConnection(mock_inner)

        with pytest.raises(ValueError, match="not allowed"):
            safe.sql("SELECT read_csv('data.csv')")

    def test_execute_validates(self):
        mock_inner = MagicMock()
        safe = SafeConnection(mock_inner)

        safe.execute("SELECT 1")
        mock_inner.execute.assert_called_once_with("SELECT 1")

    def test_execute_blocks_write(self):
        mock_inner = MagicMock()
        safe = SafeConnection(mock_inner)

        with pytest.raises(ValueError):
            safe.execute("CREATE TABLE test (id INT)")

    def test_delegates_other_attributes(self):
        mock_inner = MagicMock()
        mock_inner.description = [("col1", "INT")]
        safe = SafeConnection(mock_inner)

        assert safe.description == [("col1", "INT")]


class TestInferErrorHint:
    """Regression tests for the error-hint classifier."""

    def test_module_not_found_hint(self):
        hint = _infer_error_hint("ModuleNotFoundError: No module named 'xgboost'", "")
        assert hint is not None
        assert "xgboost" in hint
        assert "retry" in hint.lower()

    def test_missing_module_parser(self):
        assert (
            _missing_module_from_error(
                "ModuleNotFoundError: No module named 'matplotlib.pyplot'"
            )
            == "matplotlib"
        )

    def test_imported_top_level_modules(self):
        code = "import matplotlib.pyplot as plt\nfrom sklearn.cluster import KMeans"
        assert _imported_top_level_modules(code) == {"matplotlib", "sklearn"}

    def test_name_error_hint(self):
        hint = _infer_error_hint("NameError: name 'df' is not defined", "print(df)")
        assert hint is not None
        assert "previous calls" in hint

    def test_sql_hash_comment_hint(self):
        code = 'conn.sql("SELECT * # bad FROM t").df()'
        hint = _infer_error_hint("ParserException: syntax error near #", code)
        assert hint is not None
        assert "# comment" in hint or "-- for SQL" in hint

    def test_catalog_exception_with_duckdb_connect_hint(self):
        """Regression: live verification caught the agent creating a new
        `duckdb.connect(':memory:')` connection that shadowed the sandbox's
        pre-loaded `conn` — leading to CatalogException on every query.
        The hint must tell the agent to remove its own connect() call.
        """
        code = (
            "import duckdb\n"
            "conn = duckdb.connect(':memory:')\n"
            "df = conn.execute('SELECT * FROM transform_daily_revenue').df()"
        )
        result = (
            "_duckdb.CatalogException: Catalog Error: Table with name "
            "transform_daily_revenue does not exist!"
        )
        hint = _infer_error_hint(result, code)
        assert hint is not None
        assert "duckdb.connect" in hint
        assert "pre-loaded" in hint or "ALREADY provides" in hint or "already provides" in hint.lower()

    def test_catalog_exception_without_duckdb_connect_hint(self):
        """If the agent DIDN'T call duckdb.connect, it's a stale table name —
        hint should point at SHOW TABLES / list_tables instead."""
        code = "df = conn.sql('SELECT * FROM transform_missing').df()"
        result = "CatalogException: Table with name transform_missing does not exist!"
        hint = _infer_error_hint(result, code)
        assert hint is not None
        assert "SHOW TABLES" in hint or "list_tables" in hint

    def test_unrelated_error_returns_none(self):
        hint = _infer_error_hint("IndexError: list out of range", "xs[99]")
        assert hint is None


@pytest.mark.asyncio
async def test_execute_python_marks_missing_module_terminal(tmp_path, monkeypatch):
    """Unavailable optional packages should not invite repeated retries."""

    async def fake_sandbox(code, project_path):
        return "Error:\nModuleNotFoundError: No module named 'matplotlib'"

    monkeypatch.setattr(
        "seeknal.ask.sandbox.async_execute_in_sandbox",
        fake_sandbox,
    )
    ctx = ToolContext(
        repl=MagicMock(),
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
        current_question="please plot the trend",
    )
    set_tool_context(ctx)

    out = await execute_python("import matplotlib\nprint('plot')")

    assert "terminal_dependency_unavailable" in out
    assert "retryable" in out
    assert "false" in out.lower()
    assert "matplotlib" in ctx.unavailable_python_modules


@pytest.mark.asyncio
async def test_execute_python_prevents_repeated_unavailable_import(tmp_path, monkeypatch):
    async def fake_sandbox(code, project_path):  # pragma: no cover - should not run
        raise AssertionError("sandbox should not be called for repeated import")

    monkeypatch.setattr(
        "seeknal.ask.sandbox.async_execute_in_sandbox",
        fake_sandbox,
    )
    ctx = ToolContext(
        repl=MagicMock(),
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
        unavailable_python_modules={"matplotlib"},
        current_question="please plot the trend",
    )
    set_tool_context(ctx)

    out = await execute_python("import matplotlib.pyplot as plt")

    assert "terminal_dependency_unavailable" in out
    assert "already failed" in out


@pytest.mark.asyncio
async def test_execute_python_blocks_unrequested_visualization(tmp_path, monkeypatch):
    async def fake_sandbox(code, project_path):  # pragma: no cover - should not run
        raise AssertionError("sandbox should not run for unrequested visualization")

    monkeypatch.setattr(
        "seeknal.ask.sandbox.async_execute_in_sandbox",
        fake_sandbox,
    )
    ctx = ToolContext(
        repl=MagicMock(),
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
        current_question="show the trend by segment",
    )
    set_tool_context(ctx)

    out = await execute_python("import matplotlib.pyplot as plt\nplt.plot([1, 2])")

    assert "terminal_dependency_unavailable" in out
    assert "Visualization was not explicitly requested" in out
