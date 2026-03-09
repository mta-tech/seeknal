"""Tests for seeknal ask report CLI commands."""

from datetime import datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

runner = CliRunner()


@pytest.fixture
def project_dir(tmp_path):
    """Create a minimal seeknal project."""
    (tmp_path / "target" / "intermediate").mkdir(parents=True)
    return tmp_path


@pytest.fixture
def project_with_reports(tmp_path):
    """Create a project with existing reports."""
    (tmp_path / "target" / "intermediate").mkdir(parents=True)

    # Create two reports
    for name in ["customer-analysis", "revenue-report"]:
        report_dir = tmp_path / "target" / "reports" / name
        pages_dir = report_dir / "pages"
        pages_dir.mkdir(parents=True)
        (pages_dir / "index.md").write_text("# Test")

    # Add a built report
    build_dir = tmp_path / "target" / "reports" / "customer-analysis" / "build"
    build_dir.mkdir()
    (build_dir / "index.html").write_text("<html></html>")

    return tmp_path


class TestReportSubcommandRegistration:
    """Test that report subcommands are registered."""

    def test_report_help(self):
        from seeknal.cli.ask import ask_app

        result = runner.invoke(ask_app, ["report", "--help"])
        assert result.exit_code == 0
        assert "report" in result.output.lower()

    def test_report_list_help(self):
        from seeknal.cli.ask import ask_app

        result = runner.invoke(ask_app, ["report", "list", "--help"])
        assert result.exit_code == 0

    def test_report_serve_help(self):
        from seeknal.cli.ask import ask_app

        result = runner.invoke(ask_app, ["report", "serve", "--help"])
        assert result.exit_code == 0


class TestReportList:
    """Test seeknal ask report list."""

    def test_no_reports_dir(self, project_dir, monkeypatch):
        from seeknal.cli.ask import ask_app

        monkeypatch.setattr(
            "seeknal.ask.project.find_project_path",
            lambda: project_dir,
        )
        result = runner.invoke(ask_app, ["report", "list", "--project", str(project_dir)])
        assert "No reports found" in result.output

    def test_with_reports(self, project_with_reports, monkeypatch):
        from seeknal.cli.ask import ask_app

        result = runner.invoke(
            ask_app,
            ["report", "list", "--project", str(project_with_reports)],
        )
        assert "customer-analysis" in result.output
        assert "revenue-report" in result.output
        assert "yes" in result.output  # built column for customer-analysis


class TestReportServe:
    """Test seeknal ask report serve."""

    def test_nonexistent_report(self, project_dir, monkeypatch):
        from seeknal.cli.ask import ask_app

        result = runner.invoke(
            ask_app,
            ["report", "serve", "nonexistent", "--project", str(project_dir)],
        )
        assert result.exit_code == 1
        assert "not found" in result.output
