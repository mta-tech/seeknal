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
        assert "non-empty array" in result

    def test_page_not_dict_returns_error(self):
        from seeknal.ask.agents.tools.generate_report import _do_generate

        result = _do_generate("Test", '["not a dict"]', Path("/tmp"))
        assert "Error" in result
        assert "object" in result

    def test_accepts_list_directly(self, tmp_path):
        """_do_generate should accept pages as a Python list, not just JSON string."""
        from seeknal.ask.agents.tools.generate_report import _do_generate

        pages = [{"name": "overview", "content": "# Test"}]

        with patch("seeknal.ask.report.scaffolder.scaffold_report") as mock_scaffold, \
             patch("seeknal.ask.report.builder.build_report") as mock_build:
            mock_scaffold.return_value = tmp_path / "target" / "reports" / "test"
            mock_build.return_value = str(tmp_path / "build" / "index.html")

            result = _do_generate("Test", pages, tmp_path)
            assert "Report built successfully" in result


class TestGenerateReportConfirmation:
    """Test the confirmed= gate on generate_report."""

    def test_default_returns_confirmation_gate(self):
        """Calling without confirmed returns gate message."""
        from seeknal.ask.agents.tools.generate_report import generate_report

        result = generate_report.invoke({"title": "Test", "page_content": "# Test"})
        assert "requires confirmation" in result
        assert "confirmed=True" in result

    def test_confirmed_false_returns_gate(self):
        """Explicit confirmed=False returns gate message."""
        from seeknal.ask.agents.tools.generate_report import generate_report

        result = generate_report.invoke({
            "title": "Test", "page_content": "# Test", "confirmed": False,
        })
        assert "requires confirmation" in result

    def test_confirmed_true_proceeds_to_generation(self, tmp_path):
        """confirmed=True proceeds to actual report generation."""
        from seeknal.ask.agents.tools.generate_report import generate_report
        from seeknal.ask.agents.tools._context import ToolContext, set_tool_context

        # Set up minimal tool context
        set_tool_context(ToolContext(
            repl=MagicMock(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
        ))

        with patch("seeknal.ask.report.scaffolder.scaffold_report") as mock_scaffold, \
             patch("seeknal.ask.report.builder.build_report") as mock_build:
            mock_scaffold.return_value = tmp_path / "target" / "reports" / "test"
            mock_build.return_value = str(tmp_path / "build" / "index.html")

            result = generate_report.invoke({
                "title": "Test", "page_content": "# Test", "confirmed": True,
            })
            assert "Report built successfully" in result
            mock_scaffold.assert_called_once()
            mock_build.assert_called_once()

    def test_gate_does_not_call_scaffold_or_build(self):
        """Gate should not invoke scaffold or build."""
        from seeknal.ask.agents.tools.generate_report import generate_report

        with patch("seeknal.ask.report.scaffolder.scaffold_report") as mock_scaffold, \
             patch("seeknal.ask.report.builder.build_report") as mock_build:
            generate_report.invoke({"title": "Test", "page_content": "# Test"})
            mock_scaffold.assert_not_called()
            mock_build.assert_not_called()

    def test_exposure_mode_auto_confirms(self, tmp_path):
        """In exposure mode, confirmed=False is auto-promoted to True."""
        from seeknal.ask.agents.tools.generate_report import generate_report
        from seeknal.ask.agents.tools._context import ToolContext, set_tool_context

        set_tool_context(ToolContext(
            repl=MagicMock(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
            exposure_mode=True,
        ))

        with patch("seeknal.ask.report.scaffolder.scaffold_report") as mock_scaffold, \
             patch("seeknal.ask.report.builder.build_report") as mock_build:
            mock_scaffold.return_value = tmp_path / "target" / "reports" / "test"
            mock_build.return_value = str(tmp_path / "build" / "index.html")

            result = generate_report.invoke({
                "title": "Test", "page_content": "# Test", "confirmed": False,
            })
            assert "Report built successfully" in result
            mock_scaffold.assert_called_once()

    def test_interactive_mode_still_gates(self, tmp_path):
        """In interactive mode (exposure_mode=False), gate still works."""
        from seeknal.ask.agents.tools.generate_report import generate_report
        from seeknal.ask.agents.tools._context import ToolContext, set_tool_context

        set_tool_context(ToolContext(
            repl=MagicMock(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
            exposure_mode=False,
        ))

        result = generate_report.invoke({
            "title": "Test", "page_content": "# Test", "confirmed": False,
        })
        assert "requires confirmation" in result


class TestFixEvidenceSyntax:
    """Test _fix_evidence_syntax post-processing for LLM output."""

    def test_fixes_double_braces_to_single(self):
        from seeknal.ask.agents.tools.generate_report import _fix_evidence_syntax

        content = '<BarChart data={{my_query}} x=col y=val />'
        fixed = _fix_evidence_syntax(content)
        assert fixed == '<BarChart data={my_query} x=col y=val />'

    def test_fixes_multiple_double_braces(self):
        from seeknal.ask.agents.tools.generate_report import _fix_evidence_syntax

        content = (
            '<BarChart data={{sales}} x=region y=total />\n'
            '<DataTable data={{customers}} />'
        )
        fixed = _fix_evidence_syntax(content)
        assert "data={sales}" in fixed
        assert "data={customers}" in fixed
        assert "{{" not in fixed

    def test_preserves_single_braces(self):
        from seeknal.ask.agents.tools.generate_report import _fix_evidence_syntax

        content = '<BarChart data={my_query} x=col y=val />'
        fixed = _fix_evidence_syntax(content)
        assert fixed == content

    def test_preserves_markdown_and_sql(self):
        from seeknal.ask.agents.tools.generate_report import _fix_evidence_syntax

        content = (
            '# Report\n\n'
            '```sql my_query\n'
            'SELECT * FROM table\n'
            '```\n\n'
            '<DataTable data={{my_query}} />'
        )
        fixed = _fix_evidence_syntax(content)
        assert "# Report" in fixed
        assert "```sql my_query" in fixed
        assert "data={my_query}" in fixed

    def test_strips_trailing_semicolons_in_sql(self):
        from seeknal.ask.agents.tools.generate_report import _fix_evidence_syntax

        content = (
            '```sql my_query\n'
            'SELECT * FROM table;\n'
            '```'
        )
        fixed = _fix_evidence_syntax(content)
        assert ";" not in fixed
        assert "SELECT * FROM table" in fixed

    def test_handles_empty_content(self):
        from seeknal.ask.agents.tools.generate_report import _fix_evidence_syntax

        assert _fix_evidence_syntax("") == ""

    def test_full_evidence_page_fix(self):
        """Simulates a typical LLM-generated page with common mistakes."""
        from seeknal.ask.agents.tools.generate_report import _fix_evidence_syntax

        content = (
            '# Customer Analysis\n\n'
            '```sql segment_data\n'
            'SELECT segment, COUNT(*) as cnt\n'
            'FROM entity_customer\n'
            'GROUP BY segment;\n'
            '```\n\n'
            '<BarChart data={{segment_data}} x=segment y=cnt />\n'
            '<DataTable data={{segment_data}} />\n'
        )
        fixed = _fix_evidence_syntax(content)
        # Double braces fixed
        assert "data={segment_data}" in fixed
        assert "{{" not in fixed
        # Semicolons stripped
        assert "GROUP BY segment\n" in fixed


class TestScaffoldAndBuildIntegration:
    """Integration tests for the scaffold → build pipeline."""

    def test_scaffold_creates_correct_structure(self, tmp_path):
        """Verify scaffold produces the expected Evidence project structure."""
        from seeknal.ask.report.scaffolder import scaffold_report

        # Create minimal parquet so data_bridge doesn't fail
        intermediate = tmp_path / "target" / "intermediate"
        intermediate.mkdir(parents=True)

        import pandas as pd
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        df.to_parquet(intermediate / "source_test.parquet", index=False)

        pages = [{"name": "overview", "content": "# Test\n\n```sql q1\nSELECT 1 AS x\n```\n"}]
        report_dir = scaffold_report(tmp_path, "Test Report", pages)

        # Verify structure
        assert (report_dir / "pages" / "index.md").exists()
        assert (report_dir / "evidence.config.yaml").exists()
        assert (report_dir / "sources" / "seeknal" / "connection.yaml").exists()
        assert (report_dir / "package.json").exists()
        assert (report_dir / "sources" / "seeknal" / ".report.duckdb").exists()

        # Verify package.json has Evidence dependencies
        import json
        pkg = json.loads((report_dir / "package.json").read_text())
        assert "@evidence-dev/evidence" in pkg["dependencies"]
        assert "@evidence-dev/duckdb" in pkg["dependencies"]
        assert pkg["scripts"]["build"] == "evidence build"

    def test_data_bridge_creates_views(self, tmp_path):
        """Verify data_bridge creates DuckDB views from parquets."""
        from seeknal.ask.report.data_bridge import create_duckdb_from_parquets

        intermediate = tmp_path / "target" / "intermediate"
        intermediate.mkdir(parents=True)

        import pandas as pd
        df = pd.DataFrame({"customer_id": ["C001"], "amount": [100.0]})
        df.to_parquet(intermediate / "transform_sales.parquet", index=False)

        db_path = tmp_path / "test.duckdb"
        create_duckdb_from_parquets(tmp_path, db_path)

        assert db_path.exists()

        # Verify view is queryable
        import duckdb
        conn = duckdb.connect(str(db_path), read_only=True)
        result = conn.sql("SELECT * FROM transform_sales").fetchall()
        assert len(result) == 1
        assert result[0][0] == "C001"
        conn.close()

    def test_symlink_cache_uses_relative_path(self, tmp_path):
        """Verify node_modules symlink uses relative path."""
        from seeknal.ask.report.builder import _ensure_node_modules

        report_dir = tmp_path / "target" / "reports" / "test-report"
        report_dir.mkdir(parents=True)

        # Create a fake cache with node_modules
        cache_dir = tmp_path / "target" / "reports" / ".evidence-cache"
        cache_modules = cache_dir / "node_modules"
        cache_modules.mkdir(parents=True)
        (cache_modules / ".bin").mkdir()

        # Write package.json
        (report_dir / "package.json").write_text('{"name": "test"}')

        result = _ensure_node_modules(report_dir)
        assert result == ""

        # Verify symlink exists and is relative
        nm = report_dir / "node_modules"
        assert nm.is_symlink()
        link_target = nm.readlink()
        # Should be relative: ../.evidence-cache/node_modules
        assert not link_target.is_absolute()
        assert ".evidence-cache" in str(link_target)
        # Verify it resolves correctly
        assert nm.resolve() == cache_modules.resolve()

    def test_broken_symlink_gets_replaced(self, tmp_path):
        """Verify broken symlinks are detected and replaced."""
        from seeknal.ask.report.builder import _ensure_node_modules

        report_dir = tmp_path / "target" / "reports" / "test-report"
        report_dir.mkdir(parents=True)

        # Create a broken symlink
        nm = report_dir / "node_modules"
        nm.symlink_to("/nonexistent/path")
        assert nm.is_symlink()
        assert not nm.exists()  # Broken

        # Create real cache
        cache_dir = tmp_path / "target" / "reports" / ".evidence-cache"
        cache_modules = cache_dir / "node_modules"
        cache_modules.mkdir(parents=True)

        (report_dir / "package.json").write_text('{"name": "test"}')

        result = _ensure_node_modules(report_dir)
        assert result == ""

        # Symlink should now point to the real cache
        assert nm.exists()
        assert nm.resolve() == cache_modules.resolve()


class TestCLIExposureOption:
    """Test the --exposure CLI option on ask report."""

    def test_exposure_option_exists(self):
        """Verify --exposure/-e is registered on the report command."""
        from seeknal.cli.ask import report_callback
        import inspect

        sig = inspect.signature(report_callback)
        params = sig.parameters
        assert "exposure" in params

    def test_run_exposure_loads_yaml(self, tmp_path):
        """Verify _run_exposure loads and validates YAML spec."""
        import yaml

        # Create exposure YAML
        exposures_dir = tmp_path / "seeknal" / "exposures"
        exposures_dir.mkdir(parents=True)
        (exposures_dir / "test_report.yml").write_text(yaml.safe_dump({
            "kind": "exposure",
            "name": "test_report",
            "type": "report",
            "params": {
                "prompt": "Analyze sales for {{ run_date }}",
                "format": "markdown",
            },
            "inputs": [],
        }))

        from seeknal.ask.report.exposure import load_report_exposure, resolve_prompt

        # Load should succeed
        spec = load_report_exposure(tmp_path, "test_report")
        assert spec["kind"] == "exposure"
        assert spec["type"] == "report"

        # Resolve should work
        from datetime import date
        prompt = resolve_prompt(
            spec["params"]["prompt"], tmp_path, [], spec["params"]
        )
        assert date.today().isoformat() in prompt
