"""
Tests for seeknal diff CLI command.

Tests the diff command with various arguments, output modes,
and error cases.
"""

import pytest
from typer.testing import CliRunner
from seeknal.cli.main import app
from seeknal.workflow.diff_engine import write_applied_state_entry

runner = CliRunner()


@pytest.fixture
def project_path(tmp_path):
    """Create a project directory structure."""
    project = tmp_path / "test_project"
    project.mkdir()
    (project / "seeknal" / "sources").mkdir(parents=True)
    (project / "seeknal" / "transforms").mkdir(parents=True)
    (project / "target").mkdir(parents=True)
    return project


@pytest.fixture
def source_yaml():
    return """kind: source
name: orders
description: Order data
columns:
  order_id: integer
  order_date: string
  customer_id: integer
"""


@pytest.fixture
def modified_source_yaml():
    return """kind: source
name: orders
description: Order data
columns:
  order_id: integer
  order_date: date
  customer_id: integer
  discount_amount: float
"""


@pytest.fixture
def project_with_baseline(project_path, source_yaml):
    """Project with source file and applied_state baseline."""
    source_file = project_path / "seeknal" / "sources" / "orders.yml"
    source_file.write_text(source_yaml)

    write_applied_state_entry(
        project_path,
        "source.orders",
        "seeknal/sources/orders.yml",
        source_yaml,
    )
    return project_path


class TestDiffCommandNoArgs:
    def test_no_changes(self, project_with_baseline, monkeypatch):
        monkeypatch.chdir(project_with_baseline)
        result = runner.invoke(app, ["diff", "--project-path", str(project_with_baseline)])
        assert result.exit_code == 0
        assert "No changes detected" in result.output

    def test_modified_file_shown(self, project_with_baseline, modified_source_yaml, monkeypatch):
        monkeypatch.chdir(project_with_baseline)
        (project_with_baseline / "seeknal" / "sources" / "orders.yml").write_text(
            modified_source_yaml
        )
        result = runner.invoke(app, ["diff", "--project-path", str(project_with_baseline)])
        assert result.exit_code == 0
        assert "Modified" in result.output or "modified" in result.output
        assert "orders" in result.output

    def test_new_file_shown(self, project_with_baseline, monkeypatch):
        monkeypatch.chdir(project_with_baseline)
        (project_with_baseline / "seeknal" / "sources" / "products.yml").write_text(
            "kind: source\nname: products\n"
        )
        result = runner.invoke(app, ["diff", "--project-path", str(project_with_baseline)])
        assert result.exit_code == 0
        assert "New" in result.output or "new" in result.output

    def test_deleted_file_shown(self, project_with_baseline, monkeypatch):
        monkeypatch.chdir(project_with_baseline)
        (project_with_baseline / "seeknal" / "sources" / "orders.yml").unlink()
        result = runner.invoke(app, ["diff", "--project-path", str(project_with_baseline)])
        assert result.exit_code == 0
        assert "Deleted" in result.output or "deleted" in result.output


class TestDiffCommandSingleNode:
    def test_unchanged_node(self, project_with_baseline, monkeypatch):
        monkeypatch.chdir(project_with_baseline)
        result = runner.invoke(app, [
            "diff", "sources/orders", "--project-path", str(project_with_baseline)
        ])
        assert result.exit_code == 0
        assert "No changes detected" in result.output

    def test_modified_node_shows_diff(self, project_with_baseline, modified_source_yaml, monkeypatch):
        monkeypatch.chdir(project_with_baseline)
        (project_with_baseline / "seeknal" / "sources" / "orders.yml").write_text(
            modified_source_yaml
        )
        result = runner.invoke(app, [
            "diff", "sources/orders", "--project-path", str(project_with_baseline)
        ])
        assert result.exit_code == 0
        # Should show unified diff markers
        assert "---" in result.output
        assert "+++" in result.output

    def test_invalid_node_identifier(self, project_with_baseline, monkeypatch):
        monkeypatch.chdir(project_with_baseline)
        result = runner.invoke(app, [
            "diff", "../etc/passwd", "--project-path", str(project_with_baseline)
        ])
        assert result.exit_code == 1

    def test_unknown_type(self, project_with_baseline, monkeypatch):
        monkeypatch.chdir(project_with_baseline)
        result = runner.invoke(app, [
            "diff", "foobar/thing", "--project-path", str(project_with_baseline)
        ])
        assert result.exit_code == 1

    def test_nonexistent_node(self, project_with_baseline, monkeypatch):
        monkeypatch.chdir(project_with_baseline)
        result = runner.invoke(app, [
            "diff", "sources/nonexistent", "--project-path", str(project_with_baseline)
        ])
        assert result.exit_code == 1


class TestDiffCommandTypeFilter:
    def test_filter_sources(self, project_with_baseline, modified_source_yaml, monkeypatch):
        monkeypatch.chdir(project_with_baseline)
        (project_with_baseline / "seeknal" / "sources" / "orders.yml").write_text(
            modified_source_yaml
        )
        result = runner.invoke(app, [
            "diff", "--type", "sources", "--project-path", str(project_with_baseline)
        ])
        assert result.exit_code == 0
        assert "orders" in result.output

    def test_filter_no_matches(self, project_with_baseline, modified_source_yaml, monkeypatch):
        monkeypatch.chdir(project_with_baseline)
        (project_with_baseline / "seeknal" / "sources" / "orders.yml").write_text(
            modified_source_yaml
        )
        result = runner.invoke(app, [
            "diff", "--type", "transforms", "--project-path", str(project_with_baseline)
        ])
        assert result.exit_code == 0
        assert "No changes detected" in result.output


class TestDiffCommandStat:
    def test_stat_output(self, project_with_baseline, modified_source_yaml, monkeypatch):
        monkeypatch.chdir(project_with_baseline)
        (project_with_baseline / "seeknal" / "sources" / "orders.yml").write_text(
            modified_source_yaml
        )
        result = runner.invoke(app, [
            "diff", "--stat", "--project-path", str(project_with_baseline)
        ])
        assert result.exit_code == 0
        assert "file(s) changed" in result.output

    def test_stat_no_changes(self, project_with_baseline, monkeypatch):
        monkeypatch.chdir(project_with_baseline)
        result = runner.invoke(app, [
            "diff", "--stat", "--project-path", str(project_with_baseline)
        ])
        assert result.exit_code == 0
        assert "No changes detected" in result.output


class TestDiffCommandEmpty:
    def test_no_seeknal_directory(self, tmp_path, monkeypatch):
        """Project without seeknal/ directory."""
        project = tmp_path / "empty_project"
        project.mkdir()
        monkeypatch.chdir(project)
        result = runner.invoke(app, ["diff", "--project-path", str(project)])
        assert result.exit_code == 0
        assert "No changes detected" in result.output
