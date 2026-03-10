"""Tests for save_report_exposure agent tool."""

import json
from pathlib import Path

import pytest
import yaml


class TestDoSave:
    """Test _do_save() core logic (no tool context needed)."""

    def test_saves_valid_exposure(self, tmp_path):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        result = _do_save(
            name="monthly_revenue",
            prompt="Analyze monthly revenue trends",
            inputs_json='["transform.revenue"]',
            output_format="markdown",
            project_path=tmp_path,
        )
        assert "saved" in result.lower()
        assert "monthly_revenue" in result

        # Verify YAML file
        yml_path = tmp_path / "seeknal" / "exposures" / "monthly_revenue.yml"
        assert yml_path.exists()
        data = yaml.safe_load(yml_path.read_text())
        assert data["kind"] == "exposure"
        assert data["type"] == "report"
        assert data["name"] == "monthly_revenue"
        assert data["params"]["prompt"] == "Analyze monthly revenue trends"
        assert data["params"]["format"] == "markdown"
        assert data["inputs"] == [{"ref": "transform.revenue"}]

    def test_invalid_name_returns_error(self, tmp_path):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        result = _do_save(
            name="Invalid-Name!",
            prompt="test",
            inputs_json="[]",
            output_format="markdown",
            project_path=tmp_path,
        )
        assert "error" in result.lower()
        assert "snake_case" in result.lower()

    def test_empty_name_returns_error(self, tmp_path):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        result = _do_save(
            name="",
            prompt="test",
            inputs_json="[]",
            output_format="markdown",
            project_path=tmp_path,
        )
        assert "error" in result.lower()

    def test_invalid_format_returns_error(self, tmp_path):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        result = _do_save(
            name="test_report",
            prompt="test",
            inputs_json="[]",
            output_format="pdf",
            project_path=tmp_path,
        )
        assert "error" in result.lower()
        assert "format" in result.lower()

    def test_empty_prompt_returns_error(self, tmp_path):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        result = _do_save(
            name="test_report",
            prompt="",
            inputs_json="[]",
            output_format="markdown",
            project_path=tmp_path,
        )
        assert "error" in result.lower()

    def test_invalid_json_inputs_returns_error(self, tmp_path):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        result = _do_save(
            name="test_report",
            prompt="test",
            inputs_json="not valid json",
            output_format="markdown",
            project_path=tmp_path,
        )
        assert "error" in result.lower()

    def test_non_array_inputs_returns_error(self, tmp_path):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        result = _do_save(
            name="test_report",
            prompt="test",
            inputs_json='{"key": "value"}',
            output_format="markdown",
            project_path=tmp_path,
        )
        assert "error" in result.lower()
        assert "array" in result.lower()

    def test_html_format(self, tmp_path):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        result = _do_save(
            name="html_report",
            prompt="test",
            inputs_json="[]",
            output_format="html",
            project_path=tmp_path,
        )
        assert "saved" in result.lower()
        data = yaml.safe_load(
            (tmp_path / "seeknal" / "exposures" / "html_report.yml").read_text()
        )
        assert data["params"]["format"] == "html"

    def test_both_format(self, tmp_path):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        result = _do_save(
            name="both_report",
            prompt="test",
            inputs_json="[]",
            output_format="both",
            project_path=tmp_path,
        )
        assert "saved" in result.lower()

    def test_overwrite_existing(self, tmp_path):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        # First save
        _do_save("my_report", "v1", "[]", "markdown", tmp_path)
        # Overwrite
        result = _do_save("my_report", "v2", "[]", "markdown", tmp_path)
        assert "overwriting" in result.lower()

        data = yaml.safe_load(
            (tmp_path / "seeknal" / "exposures" / "my_report.yml").read_text()
        )
        assert data["params"]["prompt"] == "v2"

    def test_multiple_inputs(self, tmp_path):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        inputs = '["transform.revenue", "transform.customers"]'
        result = _do_save("multi_input", "test", inputs, "markdown", tmp_path)
        assert "saved" in result.lower()

        data = yaml.safe_load(
            (tmp_path / "seeknal" / "exposures" / "multi_input.yml").read_text()
        )
        assert len(data["inputs"]) == 2
        assert data["inputs"][0]["ref"] == "transform.revenue"
        assert data["inputs"][1]["ref"] == "transform.customers"
