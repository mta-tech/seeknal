"""Tests for the seeknal lineage CLI command."""
import tempfile
from pathlib import Path
from unittest.mock import patch
from typer.testing import CliRunner
from seeknal.cli.main import app


runner = CliRunner()


def _create_sample_project(tmpdir: str) -> None:
    """Create a minimal seeknal project with YAML node files."""
    seeknal_dir = Path(tmpdir) / "seeknal"
    seeknal_dir.mkdir(parents=True, exist_ok=True)

    # Source node
    (seeknal_dir / "traffic_day.yml").write_text(
        "kind: source\n"
        "name: traffic_day\n"
        "description: Daily traffic data\n"
        "source: hive\n"
        "table: db.traffic\n"
    )

    # Transform node with SQL and dependency
    (seeknal_dir / "clean_traffic.yml").write_text(
        "kind: transform\n"
        "name: clean_traffic\n"
        "description: Cleaned traffic data\n"
        "sql: \"SELECT user_id, page_views FROM traffic_day\"\n"
        "inputs:\n"
        "  - source.traffic_day\n"
    )


class TestLineageCommand:
    """Test the lineage CLI command."""

    def test_lineage_help(self):
        """lineage command shows help."""
        result = runner.invoke(app, ["lineage", "--help"])
        assert result.exit_code == 0
        assert "lineage" in result.output.lower()
        assert "--column" in result.output
        assert "--output" in result.output
        assert "--no-open" in result.output

    @patch("seeknal.dag.visualize.webbrowser")
    def test_lineage_generates_html(self, mock_browser):
        """lineage command generates HTML file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _create_sample_project(tmpdir)

            output_path = Path(tmpdir) / "output" / "lineage.html"
            result = runner.invoke(app, [
                "lineage",
                "--project", "test_project",
                "--path", tmpdir,
                "--no-open",
                "--output", str(output_path),
            ])

            assert result.exit_code == 0, f"Output: {result.output}"
            assert output_path.exists()
            content = output_path.read_text()
            assert "LINEAGE_DATA" in content

    @patch("seeknal.dag.visualize.webbrowser")
    def test_lineage_with_focus_node(self, mock_browser):
        """lineage command with a valid focus node."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _create_sample_project(tmpdir)

            output_path = Path(tmpdir) / "output" / "lineage.html"
            result = runner.invoke(app, [
                "lineage",
                "source.traffic_day",
                "--project", "test_project",
                "--path", tmpdir,
                "--no-open",
                "--output", str(output_path),
            ])

            assert result.exit_code == 0, f"Output: {result.output}"
            assert output_path.exists()

    def test_lineage_nonexistent_node(self):
        """lineage command with nonexistent node shows error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _create_sample_project(tmpdir)

            output_path = Path(tmpdir) / "output" / "lineage.html"
            result = runner.invoke(app, [
                "lineage",
                "nonexistent.node",
                "--project", "test_project",
                "--path", tmpdir,
                "--no-open",
                "--output", str(output_path),
            ])

            assert result.exit_code == 1
            assert "not found" in result.output.lower()

    def test_lineage_insecure_output(self):
        """lineage command rejects insecure output path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _create_sample_project(tmpdir)

            result = runner.invoke(app, [
                "lineage",
                "--project", "test_project",
                "--path", tmpdir,
                "--no-open",
                "--output", "/tmp/bad-lineage.html",
            ])

            assert result.exit_code == 1
            assert "insecure" in result.output.lower()

    def test_lineage_empty_project(self):
        """lineage command with empty project shows error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # No seeknal/ directory at all -> DAGBuilder raises ValueError
            result = runner.invoke(app, [
                "lineage",
                "--project", "test_project",
                "--path", tmpdir,
                "--no-open",
            ])

            assert result.exit_code == 1

    @patch("seeknal.dag.visualize.webbrowser")
    def test_lineage_no_open(self, mock_browser):
        """lineage command with --no-open does not open browser."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _create_sample_project(tmpdir)

            output_path = Path(tmpdir) / "output" / "lineage.html"
            result = runner.invoke(app, [
                "lineage",
                "--project", "test_project",
                "--path", tmpdir,
                "--no-open",
                "--output", str(output_path),
            ])

            assert result.exit_code == 0, f"Output: {result.output}"
            mock_browser.open.assert_not_called()

    @patch("seeknal.dag.visualize.webbrowser")
    def test_lineage_default_output_path(self, mock_browser):
        """lineage command uses target/lineage.html as default output."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _create_sample_project(tmpdir)

            result = runner.invoke(app, [
                "lineage",
                "--project", "test_project",
                "--path", tmpdir,
                "--no-open",
            ])

            assert result.exit_code == 0, f"Output: {result.output}"
            default_output = Path(tmpdir) / "target" / "lineage.html"
            assert default_output.exists()


def _create_tagged_project(tmpdir: str) -> None:
    """Create a project with tagged nodes for filtering tests."""
    seeknal_dir = Path(tmpdir) / "seeknal"
    sources_dir = seeknal_dir / "sources"
    transforms_dir = seeknal_dir / "transforms"
    sources_dir.mkdir(parents=True, exist_ok=True)
    transforms_dir.mkdir(parents=True, exist_ok=True)

    # Source (no tags)
    (sources_dir / "raw_data.yml").write_text(
        "kind: source\n"
        "name: raw_data\n"
        "description: Raw data\n"
        "source: csv\n"
        "params:\n"
        "  path: data/raw.csv\n"
    )

    # Tagged transform
    (transforms_dir / "tagged.yml").write_text(
        "kind: transform\n"
        "name: tagged_transform\n"
        "description: Tagged transform\n"
        "tags:\n"
        "  - churn_pipeline\n"
        "transform: SELECT * FROM source.raw_data\n"
        "inputs:\n"
        "  - ref: source.raw_data\n"
    )

    # Untagged transform
    (transforms_dir / "untagged.yml").write_text(
        "kind: transform\n"
        "name: untagged_transform\n"
        "description: Untagged transform\n"
        "transform: SELECT 1\n"
    )


class TestLineageTagFiltering:
    """Tests for --tags and --exclude-tags on lineage command."""

    def test_lineage_ascii_tags_filter(self):
        """--tags filters ASCII lineage to matching nodes + upstream."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _create_tagged_project(tmpdir)

            result = runner.invoke(app, [
                "lineage",
                "--path", tmpdir,
                "--ascii",
                "--tags", "churn_pipeline",
            ])

            assert result.exit_code == 0, f"Output: {result.output}"
            assert "tagged_transform" in result.output
            assert "raw_data" in result.output
            assert "untagged_transform" not in result.output

    def test_lineage_ascii_exclude_tags(self):
        """--exclude-tags hides matching nodes from ASCII lineage."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _create_tagged_project(tmpdir)

            result = runner.invoke(app, [
                "lineage",
                "--path", tmpdir,
                "--ascii",
                "--exclude-tags", "churn_pipeline",
            ])

            assert result.exit_code == 0, f"Output: {result.output}"
            # The tagged node should be excluded (check full ID not substring)
            assert "transform.tagged_transform" not in result.output
            assert "untagged_transform" in result.output

    def test_lineage_tags_and_node_id_conflict(self):
        """--tags + node_id should error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _create_tagged_project(tmpdir)

            result = runner.invoke(app, [
                "lineage",
                "source.raw_data",
                "--path", tmpdir,
                "--ascii",
                "--tags", "churn_pipeline",
            ])

            assert result.exit_code == 1
            assert "Cannot use --tags and node_id together" in result.output

    def test_lineage_help_shows_tags(self):
        """--help shows --tags and --exclude-tags options."""
        result = runner.invoke(app, ["lineage", "--help"])

        assert result.exit_code == 0
        assert "--tags" in result.output
        assert "--exclude-tags" in result.output
