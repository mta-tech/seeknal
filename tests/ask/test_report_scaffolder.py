"""Tests for seeknal.ask.report.scaffolder."""

import json
import warnings
from pathlib import Path

import pytest


@pytest.fixture
def project_dir(tmp_path):
    """Create a minimal seeknal project with parquet files."""
    intermediate = tmp_path / "target" / "intermediate"
    intermediate.mkdir(parents=True)

    # Create a small parquet file
    try:
        import duckdb
        conn = duckdb.connect(":memory:")
        conn.execute(
            f"COPY (SELECT 1 AS id, 'test' AS name) "
            f"TO '{intermediate / 'transform_users.parquet'}' (FORMAT PARQUET)"
        )
        conn.close()
    except ImportError:
        pytest.skip("duckdb required for scaffolder tests")

    return tmp_path


class TestSlugify:
    """Test slug generation from titles."""

    def test_simple_title(self):
        from seeknal.ask.report.scaffolder import _slugify

        assert _slugify("Customer Analysis") == "customer-analysis"

    def test_special_characters(self):
        from seeknal.ask.report.scaffolder import _slugify

        assert _slugify("Q1 2026: Revenue & Costs!") == "q1-2026-revenue-costs"

    def test_unicode(self):
        from seeknal.ask.report.scaffolder import _slugify

        assert _slugify("Análisis de datos") == "an-lisis-de-datos"

    def test_empty_title_defaults(self):
        from seeknal.ask.report.scaffolder import _slugify

        assert _slugify("   ") == "report"

    def test_long_title_truncated(self):
        from seeknal.ask.report.scaffolder import _slugify

        result = _slugify("a" * 100)
        assert len(result) <= 60

    def test_leading_trailing_hyphens_stripped(self):
        from seeknal.ask.report.scaffolder import _slugify

        assert _slugify("--hello--") == "hello"


class TestPageNameValidation:
    """Test page name sanitization."""

    def test_valid_name(self):
        from seeknal.ask.report.scaffolder import _validate_page_name

        assert _validate_page_name("overview") == "overview"

    def test_mixed_case_lowered(self):
        from seeknal.ask.report.scaffolder import _validate_page_name

        assert _validate_page_name("Overview") == "overview"

    def test_spaces_to_hyphens(self):
        from seeknal.ask.report.scaffolder import _validate_page_name

        assert _validate_page_name("my page") == "my-page"

    def test_empty_name_raises(self):
        from seeknal.ask.report.scaffolder import _validate_page_name

        with pytest.raises(ValueError, match="Invalid page name"):
            _validate_page_name("")

    def test_special_chars_sanitized(self):
        from seeknal.ask.report.scaffolder import _validate_page_name

        assert _validate_page_name("page@#$1") == "page-1"


class TestScaffoldReport:
    """Test full report scaffolding."""

    def test_creates_directory_structure(self, project_dir):
        from seeknal.ask.report.scaffolder import scaffold_report

        pages = [{"name": "overview", "content": "# Overview\nSome text"}]
        report_dir = scaffold_report(project_dir, "Test Report", pages)

        assert report_dir.exists()
        assert (report_dir / "pages" / "index.md").exists()
        assert (report_dir / "evidence.config.yaml").exists()
        assert (report_dir / "sources" / "seeknal" / "connection.yaml").exists()
        assert (report_dir / "package.json").exists()
        assert (report_dir / ".report.duckdb").exists()

    def test_first_page_is_index(self, project_dir):
        from seeknal.ask.report.scaffolder import scaffold_report

        pages = [
            {"name": "overview", "content": "# Overview"},
            {"name": "details", "content": "# Details"},
        ]
        report_dir = scaffold_report(project_dir, "Test", pages)

        assert (report_dir / "pages" / "index.md").exists()
        assert (report_dir / "pages" / "details.md").exists()
        assert not (report_dir / "pages" / "overview.md").exists()

    def test_empty_title_raises(self, project_dir):
        from seeknal.ask.report.scaffolder import scaffold_report

        with pytest.raises(ValueError, match="title cannot be empty"):
            scaffold_report(project_dir, "", [{"name": "a", "content": "b"}])

    def test_empty_pages_raises(self, project_dir):
        from seeknal.ask.report.scaffolder import scaffold_report

        with pytest.raises(ValueError, match="(?i)at least one page"):
            scaffold_report(project_dir, "Test", [])

    def test_page_missing_name_raises(self, project_dir):
        from seeknal.ask.report.scaffolder import scaffold_report

        with pytest.raises(ValueError, match="missing a 'name'"):
            scaffold_report(project_dir, "Test", [{"content": "text"}])

    def test_page_empty_content_raises(self, project_dir):
        from seeknal.ask.report.scaffolder import scaffold_report

        with pytest.raises(ValueError, match="empty content"):
            scaffold_report(project_dir, "Test", [{"name": "a", "content": ""}])

    def test_overwrite_existing_warns(self, project_dir):
        from seeknal.ask.report.scaffolder import scaffold_report

        pages = [{"name": "overview", "content": "# V1"}]
        scaffold_report(project_dir, "Test", pages)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            scaffold_report(project_dir, "Test", [{"name": "overview", "content": "# V2"}])
            assert any("already exists" in str(warning.message) for warning in w)

    def test_duckdb_has_views(self, project_dir):
        from seeknal.ask.report.scaffolder import scaffold_report

        import duckdb

        pages = [{"name": "overview", "content": "# Overview"}]
        report_dir = scaffold_report(project_dir, "Test", pages)

        conn = duckdb.connect(str(report_dir / ".report.duckdb"), read_only=True)
        tables = conn.execute("SHOW TABLES").fetchall()
        view_names = [t[0] for t in tables]
        conn.close()

        assert "transform_users" in view_names

    def test_sql_validation_blocks_dangerous_queries(self, project_dir):
        from seeknal.ask.report.scaffolder import scaffold_report

        dangerous_page = {
            "name": "bad",
            "content": "```sql data\nSELECT * FROM read_parquet('/etc/passwd')\n```",
        }
        with pytest.raises(ValueError, match="SQL validation failed"):
            scaffold_report(project_dir, "Test", [dangerous_page])

    def test_package_json_content(self, project_dir):
        from seeknal.ask.report.scaffolder import scaffold_report

        pages = [{"name": "overview", "content": "# Overview"}]
        report_dir = scaffold_report(project_dir, "My Report", pages)

        pkg = json.loads((report_dir / "package.json").read_text())
        assert "@evidence-dev/evidence" in pkg["dependencies"]
        assert "@evidence-dev/duckdb" in pkg["dependencies"]

    def test_connection_yaml_content(self, project_dir):
        from seeknal.ask.report.scaffolder import scaffold_report

        pages = [{"name": "overview", "content": "# Overview"}]
        report_dir = scaffold_report(project_dir, "Test", pages)

        conn_yaml = (report_dir / "sources" / "seeknal" / "connection.yaml").read_text()
        assert "type: duckdb" in conn_yaml
        assert ".report.duckdb" in conn_yaml
