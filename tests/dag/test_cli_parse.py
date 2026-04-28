"""Tests for the seeknal parse CLI command."""
import pytest
import tempfile
import json
from pathlib import Path
from typer.testing import CliRunner
from seeknal.cli.main import app


runner = CliRunner()


def _write_source(project_dir: str | Path, name: str, table: str) -> None:
    """Write a minimal project-layout source node for parse tests."""
    sources_dir = Path(project_dir) / "seeknal" / "sources"
    sources_dir.mkdir(parents=True, exist_ok=True)
    (sources_dir / f"{name}.yml").write_text(
        f"""
kind: source
name: {name}
description: Source {name}
source: hive
table: {table}
"""
    )


class TestParseCommand:
    """Test the parse CLI command."""

    def test_parse_help(self):
        """parse command shows help."""
        result = runner.invoke(app, ["parse", "--help"])
        assert result.exit_code == 0
        assert "Parse" in result.output or "parse" in result.output

    def test_parse_generates_manifest(self):
        """parse command generates manifest.json."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_source(tmpdir, "traffic_day", "db.traffic")
            result = runner.invoke(app, [
                "parse",
                "--project", "test_project",
                "--path", tmpdir
            ])

            assert result.exit_code == 0
            assert "manifest.json" in result.output or "Manifest" in result.output

            # Check manifest was created
            manifest_path = Path(tmpdir) / "target" / "manifest.json"
            assert manifest_path.exists()

    def test_parse_validates_dag(self):
        """parse command validates the DAG."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_source(tmpdir, "traffic_day", "db.traffic")
            result = runner.invoke(app, [
                "parse",
                "--project", "test_project",
                "--path", tmpdir
            ])

            assert result.exit_code == 0
            # No validation errors expected

    def test_parse_outputs_json_format(self):
        """parse command can output JSON format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_source(tmpdir, "traffic_day", "db.traffic")
            result = runner.invoke(app, [
                "parse",
                "--project", "test_project",
                "--path", tmpdir,
                "--format", "json"
            ])

            assert result.exit_code == 0

    def test_parse_shows_node_count(self):
        """parse command shows the node count."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_source(tmpdir, "traffic_day", "db.traffic")
            _write_source(tmpdir, "user_events", "db.events")
            result = runner.invoke(app, [
                "parse",
                "--project", "test_project",
                "--path", tmpdir
            ])

            assert result.exit_code == 0
            # Should mention the number of nodes
            assert "2" in result.output or "node" in result.output.lower()

    def test_parse_shows_diff_when_manifest_exists(self):
        """parse command shows diff when previous manifest exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_source(tmpdir, "traffic_day", "db.traffic")
            # First parse
            result1 = runner.invoke(app, [
                "parse",
                "--project", "test_project",
                "--path", tmpdir
            ])
            assert result1.exit_code == 0

            # Add another source for the second parse
            _write_source(tmpdir, "user_events", "db.events")
            # Second parse - should show diff
            result2 = runner.invoke(app, [
                "parse",
                "--project", "test_project",
                "--path", tmpdir
            ])

            assert result2.exit_code == 0
            # Should indicate changes
            assert "added" in result2.output.lower() or "change" in result2.output.lower()

    def test_parse_no_diff_option_skips_comparison(self):
        """parse command with --no-diff skips manifest comparison."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_source(tmpdir, "traffic_day", "db.traffic")
            # First parse
            runner.invoke(app, [
                "parse",
                "--project", "test_project",
                "--path", tmpdir
            ])

            # Update and parse with --no-diff
            _write_source(tmpdir, "user_events", "db.events")
            result = runner.invoke(app, [
                "parse",
                "--project", "test_project",
                "--path", tmpdir,
                "--no-diff"
            ])

            assert result.exit_code == 0
            # Should NOT mention changes when --no-diff is used
            assert "added" not in result.output.lower() or "no changes" in result.output.lower()
