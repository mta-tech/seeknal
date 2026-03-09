"""Tests for seeknal.ask.agents.tools.generate_report."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestDoGenerate:
    """Test the _do_generate helper."""

    def test_empty_title_returns_error(self):
        from seeknal.ask.agents.tools.generate_report import _do_generate

        result = _do_generate("", "[]", Path("/tmp/test"))
        assert "Error" in result
        assert "title" in result.lower()

    def test_invalid_json_returns_error(self):
        from seeknal.ask.agents.tools.generate_report import _do_generate

        result = _do_generate("Test", "not json", Path("/tmp/test"))
        assert "Error" in result
        assert "Invalid pages JSON" in result

    def test_empty_pages_returns_error(self):
        from seeknal.ask.agents.tools.generate_report import _do_generate

        result = _do_generate("Test", "[]", Path("/tmp/test"))
        assert "Error" in result
        assert "non-empty" in result

    def test_page_missing_fields_returns_error(self):
        from seeknal.ask.agents.tools.generate_report import _do_generate

        pages = json.dumps([{"name": "test"}])
        result = _do_generate("Test", pages, Path("/tmp/test"))
        assert "Error" in result
        assert "missing" in result.lower()

    def test_success_with_mocked_scaffold_build(self, tmp_path):
        from seeknal.ask.agents.tools.generate_report import _do_generate

        pages = json.dumps([{"name": "overview", "content": "# Test"}])

        with patch("seeknal.ask.report.scaffolder.scaffold_report") as mock_scaffold, \
             patch("seeknal.ask.report.builder.build_report") as mock_build:
            mock_scaffold.return_value = tmp_path / "target" / "reports" / "test"
            mock_build.return_value = str(tmp_path / "build" / "index.html")

            result = _do_generate("Test", pages, tmp_path)
            assert "Report built successfully" in result

    def test_scaffold_error_returns_error_string(self, tmp_path):
        from seeknal.ask.agents.tools.generate_report import _do_generate

        pages = json.dumps([{"name": "overview", "content": "# Test"}])

        with patch("seeknal.ask.report.scaffolder.scaffold_report") as mock_scaffold:
            mock_scaffold.side_effect = ValueError("SQL validation failed")

            result = _do_generate("Test", pages, tmp_path)
            assert "Error" in result
            assert "SQL validation failed" in result

    def test_build_failure_returns_error_string(self, tmp_path):
        from seeknal.ask.agents.tools.generate_report import _do_generate

        pages = json.dumps([{"name": "overview", "content": "# Test"}])

        with patch("seeknal.ask.report.scaffolder.scaffold_report") as mock_scaffold, \
             patch("seeknal.ask.report.builder.build_report") as mock_build:
            mock_scaffold.return_value = tmp_path / "target" / "reports" / "test"
            mock_build.return_value = "Evidence build failed:\nSome error"

            result = _do_generate("Test", pages, tmp_path)
            assert "Evidence build failed" in result

    def test_pages_not_list_returns_error(self):
        from seeknal.ask.agents.tools.generate_report import _do_generate

        result = _do_generate("Test", '{"name": "test"}', Path("/tmp"))
        assert "Error" in result
        assert "non-empty JSON array" in result

    def test_page_not_dict_returns_error(self):
        from seeknal.ask.agents.tools.generate_report import _do_generate

        result = _do_generate("Test", '["not a dict"]', Path("/tmp"))
        assert "Error" in result
        assert "object" in result
