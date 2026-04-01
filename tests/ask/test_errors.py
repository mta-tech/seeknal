"""Tests for structured error taxonomy."""

import json

import pytest

from seeknal.ask.agents.tools.errors import (
    RETRYABLE_MISSING_REF,
    RETRYABLE_SYNTAX,
    RETRYABLE_TYPE,
    TERMINAL_CRASH,
    TERMINAL_SECURITY,
    TERMINAL_TIMEOUT,
    classify_duckdb_error,
    format_tool_error,
)


# ---------------------------------------------------------------------------
# format_tool_error
# ---------------------------------------------------------------------------


class TestFormatToolError:
    def test_retryable_category_sets_retryable_true(self):
        result = json.loads(format_tool_error(RETRYABLE_SYNTAX, "some error"))
        assert result["retryable"] is True
        assert result["category"] == RETRYABLE_SYNTAX
        assert result["message"] == "some error"
        assert result["hint"] is None

    def test_terminal_category_sets_retryable_false(self):
        result = json.loads(format_tool_error(TERMINAL_CRASH, "OOM"))
        assert result["retryable"] is False
        assert result["category"] == TERMINAL_CRASH

    def test_terminal_timeout_not_retryable(self):
        result = json.loads(format_tool_error(TERMINAL_TIMEOUT, "timed out"))
        assert result["retryable"] is False

    def test_terminal_security_not_retryable(self):
        result = json.loads(format_tool_error(TERMINAL_SECURITY, "denied"))
        assert result["retryable"] is False

    def test_hint_included_when_provided(self):
        result = json.loads(format_tool_error(RETRYABLE_SYNTAX, "err", hint="try X"))
        assert result["hint"] == "try X"

    def test_hint_is_none_when_not_provided(self):
        result = json.loads(format_tool_error(RETRYABLE_SYNTAX, "err"))
        assert result["hint"] is None

    def test_retryable_missing_ref_is_retryable(self):
        result = json.loads(format_tool_error(RETRYABLE_MISSING_REF, "err"))
        assert result["retryable"] is True

    def test_retryable_type_is_retryable(self):
        result = json.loads(format_tool_error(RETRYABLE_TYPE, "err"))
        assert result["retryable"] is True

    def test_returns_valid_json_string(self):
        raw = format_tool_error(RETRYABLE_SYNTAX, "test message")
        assert isinstance(raw, str)
        parsed = json.loads(raw)
        assert isinstance(parsed, dict)


# ---------------------------------------------------------------------------
# classify_duckdb_error
# ---------------------------------------------------------------------------


class TestClassifyDuckdbError:
    def test_catalog_error_is_missing_ref(self):
        msg = "Catalog Error: Table 'foo' does not exist"
        assert classify_duckdb_error(msg) == RETRYABLE_MISSING_REF

    def test_binder_error_column_is_missing_ref(self):
        msg = 'Binder Error: Referenced column "bar" not found in FROM clause'
        assert classify_duckdb_error(msg) == RETRYABLE_MISSING_REF

    def test_binder_error_type_is_type(self):
        msg = "Binder Error: Cannot compare type VARCHAR with type INTEGER"
        assert classify_duckdb_error(msg) == RETRYABLE_TYPE

    def test_binder_error_generic_is_missing_ref(self):
        msg = "Binder Error: something else entirely"
        assert classify_duckdb_error(msg) == RETRYABLE_MISSING_REF

    def test_parser_error_is_syntax(self):
        msg = "Parser Error: syntax error at end of input"
        assert classify_duckdb_error(msg) == RETRYABLE_SYNTAX

    def test_io_error_is_crash(self):
        msg = "IO Error: Cannot open file"
        assert classify_duckdb_error(msg) == TERMINAL_CRASH

    def test_out_of_memory_is_crash(self):
        msg = "OutOfMemoryError: not enough memory"
        assert classify_duckdb_error(msg) == TERMINAL_CRASH

    def test_unknown_error_defaults_to_syntax(self):
        msg = "Something completely unexpected happened"
        assert classify_duckdb_error(msg) == RETRYABLE_SYNTAX

    def test_case_insensitive_matching(self):
        msg = "catalog error: table 'x' not found"
        assert classify_duckdb_error(msg) == RETRYABLE_MISSING_REF
