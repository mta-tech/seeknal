"""
Tests for seeknal plan command tag filtering.

Tests the --tags and --exclude-tags options on the plan command
(production diff mode only).
"""

import pytest
from typer.testing import CliRunner
from seeknal.cli.main import app

runner = CliRunner()


@pytest.fixture
def temp_project_path(tmp_path):
    """Create a temporary project path with YAML files."""
    project_path = tmp_path / "test_project"
    project_path.mkdir()

    # Create seeknal directory structure
    (project_path / "seeknal" / "sources").mkdir(parents=True)
    (project_path / "seeknal" / "transforms").mkdir(parents=True)

    return project_path


class TestSeeknalPlanTagFiltering:
    """Test --tags and --exclude-tags filtering on plan command."""

    def test_plan_tags_filter(self, temp_project_path, monkeypatch):
        """Test --tags shows only matching nodes + upstream."""
        # Create source (no tags)
        source_yaml = """\
kind: source
name: raw_data
description: Raw data source
source: csv
params:
  path: data/raw.csv
"""
        (temp_project_path / "seeknal" / "sources" / "raw_data.yml").write_text(
            source_yaml
        )

        # Create tagged transform
        tagged_yaml = """\
kind: transform
name: tagged_transform
description: Tagged transform
tags:
  - churn_pipeline
transform: SELECT * FROM source.raw_data
inputs:
  - ref: source.raw_data
"""
        (temp_project_path / "seeknal" / "transforms" / "tagged.yml").write_text(
            tagged_yaml
        )

        # Create untagged transform
        untagged_yaml = """\
kind: transform
name: untagged_transform
description: Untagged transform
transform: SELECT 1
"""
        (temp_project_path / "seeknal" / "transforms" / "untagged.yml").write_text(
            untagged_yaml
        )

        monkeypatch.chdir(temp_project_path)

        result = runner.invoke(
            app, ["plan", "--tags", "churn_pipeline"]
        )

        assert result.exit_code == 0
        # Tagged transform should be shown
        assert "tagged_transform" in result.stdout
        # Upstream source should be included
        assert "raw_data" in result.stdout
        # Untagged transform should NOT be shown
        assert "untagged_transform" not in result.stdout
        # Summary shows filter info
        assert "filtered by" in result.stdout

    def test_plan_tags_no_match(self, temp_project_path, monkeypatch):
        """Test --tags with no matching nodes shows warning but full plan."""
        source_yaml = """\
kind: source
name: basic_source
description: Basic source
source: csv
params:
  path: data/basic.csv
"""
        (temp_project_path / "seeknal" / "sources" / "basic.yml").write_text(
            source_yaml
        )

        monkeypatch.chdir(temp_project_path)

        result = runner.invoke(
            app, ["plan", "--tags", "nonexistent"]
        )

        assert result.exit_code == 0
        assert "No nodes found with tags" in result.stdout

    def test_plan_exclude_tags(self, temp_project_path, monkeypatch):
        """Test --exclude-tags hides matching nodes from plan."""
        # Create tagged source
        tagged_yaml = """\
kind: source
name: legacy_source
description: Legacy source
tags:
  - legacy
source: csv
params:
  path: data/old.csv
"""
        (temp_project_path / "seeknal" / "sources" / "legacy.yml").write_text(
            tagged_yaml
        )

        # Create untagged source
        normal_yaml = """\
kind: source
name: normal_source
description: Normal source
source: csv
params:
  path: data/normal.csv
"""
        (temp_project_path / "seeknal" / "sources" / "normal.yml").write_text(
            normal_yaml
        )

        monkeypatch.chdir(temp_project_path)

        result = runner.invoke(
            app, ["plan", "--exclude-tags", "legacy"]
        )

        assert result.exit_code == 0
        # Legacy source should NOT be shown
        assert "legacy_source" not in result.stdout
        # Normal source should be shown
        assert "normal_source" in result.stdout
        # Summary shows filter info
        assert "filtered by" in result.stdout

    def test_plan_help_shows_tags(self):
        """Test --help shows --tags and --exclude-tags options."""
        result = runner.invoke(app, ["plan", "--help"])

        assert result.exit_code == 0
        assert "--tags" in result.stdout
        assert "--exclude-tags" in result.stdout
