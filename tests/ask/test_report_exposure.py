"""Tests for seeknal.ask.report.exposure module."""

import json
from datetime import date
from pathlib import Path

import pytest
import yaml


class TestSaveRenderedMarkdown:
    """Test save_rendered_markdown()."""

    def test_saves_markdown_file(self, tmp_path):
        from seeknal.ask.report.exposure import save_rendered_markdown

        result = save_rendered_markdown(tmp_path, "Customer Analysis", "# Report\nData here.")
        assert result.exists()
        assert result.read_text() == "# Report\nData here."
        assert "target/reported/customer-analysis" in str(result)
        assert result.name == f"{date.today().isoformat()}.md"

    def test_slugifies_name(self, tmp_path):
        from seeknal.ask.report.exposure import save_rendered_markdown

        result = save_rendered_markdown(tmp_path, "My Complex Report!!!", "content")
        assert "my-complex-report" in str(result)

    def test_empty_content_raises(self, tmp_path):
        from seeknal.ask.report.exposure import save_rendered_markdown

        with pytest.raises(ValueError, match="empty"):
            save_rendered_markdown(tmp_path, "test", "")

    def test_whitespace_only_raises(self, tmp_path):
        from seeknal.ask.report.exposure import save_rendered_markdown

        with pytest.raises(ValueError, match="empty"):
            save_rendered_markdown(tmp_path, "test", "   \n  ")

    def test_path_traversal_blocked(self, tmp_path):
        """Slugifier sanitizes traversal attempts — result stays under target/."""
        from seeknal.ask.report.exposure import save_rendered_markdown

        # ../../etc/passwd gets slugified to "etc-passwd", staying under target/
        result = save_rendered_markdown(tmp_path, "../../etc/passwd", "content")
        target_dir = (tmp_path / "target").resolve()
        assert str(result.resolve()).startswith(str(target_dir))


class TestLoadReportExposure:
    """Test load_report_exposure()."""

    def _write_exposure(self, project_path, name, data):
        exposures_dir = project_path / "seeknal" / "exposures"
        exposures_dir.mkdir(parents=True, exist_ok=True)
        path = exposures_dir / f"{name}.yml"
        path.write_text(yaml.safe_dump(data, sort_keys=False))
        return path

    def test_loads_valid_exposure(self, tmp_path):
        from seeknal.ask.report.exposure import load_report_exposure

        data = {
            "kind": "exposure",
            "name": "test_report",
            "type": "report",
            "params": {"prompt": "Analyze revenue", "format": "markdown"},
            "inputs": [{"ref": "transform.revenue"}],
        }
        self._write_exposure(tmp_path, "test_report", data)

        result = load_report_exposure(tmp_path, "test_report")
        assert result["name"] == "test_report"
        assert result["params"]["prompt"] == "Analyze revenue"

    def test_not_found_raises(self, tmp_path):
        from seeknal.ask.report.exposure import load_report_exposure

        with pytest.raises(FileNotFoundError, match="nonexistent"):
            load_report_exposure(tmp_path, "nonexistent")

    def test_invalid_kind_raises(self, tmp_path):
        from seeknal.ask.report.exposure import load_report_exposure

        data = {
            "kind": "model",
            "name": "test",
            "type": "report",
            "params": {"prompt": "test"},
        }
        self._write_exposure(tmp_path, "test", data)

        with pytest.raises(ValueError, match="kind"):
            load_report_exposure(tmp_path, "test")

    def test_missing_prompt_raises(self, tmp_path):
        from seeknal.ask.report.exposure import load_report_exposure

        data = {
            "kind": "exposure",
            "name": "test",
            "type": "report",
            "params": {},
        }
        self._write_exposure(tmp_path, "test", data)

        with pytest.raises(ValueError, match="prompt"):
            load_report_exposure(tmp_path, "test")

    def test_invalid_format_raises(self, tmp_path):
        from seeknal.ask.report.exposure import load_report_exposure

        data = {
            "kind": "exposure",
            "name": "test",
            "type": "report",
            "params": {"prompt": "test", "format": "pdf"},
        }
        self._write_exposure(tmp_path, "test", data)

        with pytest.raises(ValueError, match="format"):
            load_report_exposure(tmp_path, "test")

    def test_scans_nested_yaml(self, tmp_path):
        from seeknal.ask.report.exposure import load_report_exposure

        # Place exposure in a nested directory (not direct path)
        nested = tmp_path / "seeknal" / "reports" / "custom"
        nested.mkdir(parents=True)
        data = {
            "kind": "exposure",
            "name": "nested_report",
            "type": "report",
            "params": {"prompt": "test"},
        }
        (nested / "my_report.yml").write_text(yaml.safe_dump(data, sort_keys=False))

        result = load_report_exposure(tmp_path, "nested_report")
        assert result["name"] == "nested_report"


class TestResolvePrompt:
    """Test resolve_prompt()."""

    def test_resolves_run_date(self, tmp_path):
        from seeknal.ask.report.exposure import resolve_prompt

        result = resolve_prompt(
            "Report for {{ run_date }}",
            tmp_path,
            inputs=[],
            params={},
        )
        assert date.today().isoformat() in result

    def test_resolves_params(self, tmp_path):
        from seeknal.ask.report.exposure import resolve_prompt

        result = resolve_prompt(
            "Analyze {{ params.metric }} for {{ params.region }}",
            tmp_path,
            inputs=[],
            params={"metric": "revenue", "region": "APAC"},
        )
        assert "revenue" in result
        assert "APAC" in result

    def test_strict_undefined_raises(self, tmp_path):
        from seeknal.ask.report.exposure import resolve_prompt

        with pytest.raises(Exception):
            resolve_prompt(
                "{{ undefined_var }}",
                tmp_path,
                inputs=[],
                params={},
            )

    def test_resolves_input_context(self, tmp_path):
        """Test that input refs are resolved when parquet exists."""
        from seeknal.ask.report.exposure import resolve_prompt

        try:
            import duckdb
            import pandas as pd
        except ImportError:
            pytest.skip("duckdb/pandas not available")

        # Create a test parquet
        intermediate = tmp_path / "target" / "intermediate"
        intermediate.mkdir(parents=True)
        df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
        df.to_parquet(intermediate / "transform_revenue.parquet")

        result = resolve_prompt(
            "Columns: {{ inputs.revenue.columns }}, Rows: {{ inputs.revenue.row_count }}",
            tmp_path,
            inputs=[{"ref": "transform.revenue"}],
            params={},
        )
        assert "id" in result
        assert "value" in result
        assert "3" in result

    def test_missing_table_graceful(self, tmp_path):
        from seeknal.ask.report.exposure import resolve_prompt

        result = resolve_prompt(
            "Columns: {{ inputs.missing.columns }}",
            tmp_path,
            inputs=[{"ref": "transform.missing"}],
            params={},
        )
        assert "not found" in result.lower() or "columns" in result.lower()
