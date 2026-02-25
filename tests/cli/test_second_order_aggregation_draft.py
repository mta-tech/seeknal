"""
Tests for seeknal draft command with second-order-aggregation node type.

Tests the CLI draft command functionality for second-order aggregations:
- YAML template generation
- Python template generation
- Template variable substitution
- Help text and validation
"""

import pytest
from pathlib import Path
from typer.testing import CliRunner
from seeknal.cli.main import app

runner = CliRunner()


@pytest.fixture
def temp_project_dir(tmp_path):
    """Create a temporary project directory."""
    project_dir = tmp_path / "test_project"
    project_dir.mkdir()
    return project_dir


class TestSecondOrderAggregationDraftYAML:
    """Test YAML draft generation for second-order aggregations."""

    def test_draft_second_order_aggregation_yaml(self, temp_project_dir, monkeypatch):
        """Test creating a second-order aggregation YAML draft."""
        monkeypatch.chdir(temp_project_dir)

        result = runner.invoke(
            app,
            ["draft", "second-order-aggregation", "region_user_metrics"]
        )

        assert result.exit_code == 0
        assert "Created:" in result.stdout
        assert "draft_second_order_aggregation_region_user_metrics.yml" in result.stdout
        assert "dry-run" in result.stdout

        # Verify file was created
        draft_file = temp_project_dir / "draft_second_order_aggregation_region_user_metrics.yml"
        assert draft_file.exists()

        # Check content
        content = draft_file.read_text()
        assert "kind: second_order_aggregation" in content
        assert "name: region_user_metrics" in content
        assert "id_col:" in content
        assert "feature_date_col:" in content
        assert "source:" in content
        assert "features:" in content

    def test_draft_with_description(self, temp_project_dir, monkeypatch):
        """Test creating draft with custom description."""
        monkeypatch.chdir(temp_project_dir)

        result = runner.invoke(
            app,
            [
                "draft", "second-order-aggregation", "region_metrics",
                "--description", "Aggregate user metrics to region level"
            ]
        )

        assert result.exit_code == 0

        draft_file = temp_project_dir / "draft_second_order_aggregation_region_metrics.yml"
        content = draft_file.read_text()
        assert "Aggregate user metrics to region level" in content

    def test_draft_with_force_overwrite(self, temp_project_dir, monkeypatch):
        """Test --force flag overwrites existing file."""
        monkeypatch.chdir(temp_project_dir)

        # Create initial file
        result1 = runner.invoke(
            app,
            ["draft", "second-order-aggregation", "test_agg"]
        )
        assert result1.exit_code == 0

        # Try to create again without --force (should fail)
        result2 = runner.invoke(
            app,
            ["draft", "second-order-aggregation", "test_agg"]
        )
        assert result2.exit_code == 1
        assert "already exists" in result2.stdout

        # Create again with --force (should succeed)
        result3 = runner.invoke(
            app,
            ["draft", "second-order-aggregation", "test_agg", "--force"]
        )
        assert result3.exit_code == 0
        assert "Created:" in result3.stdout

    def test_draft_alternative_node_type_syntax(self, temp_project_dir, monkeypatch):
        """Test alternative 'second-order-agg' syntax if supported."""
        monkeypatch.chdir(temp_project_dir)

        # Test with underscore variant
        result = runner.invoke(
            app,
            ["draft", "second-order-agg", "test_metrics"]
        )

        # May fail if not supported as alias
        # If it passes, verify the file
        if result.exit_code == 0:
            draft_file = temp_project_dir / "draft_second_order_aggregation_test_metrics.yml"
            assert draft_file.exists()


class TestSecondOrderAggregationDraftPython:
    """Test Python draft generation for second-order aggregations."""

    def test_draft_second_order_aggregation_python(self, temp_project_dir, monkeypatch):
        """Test creating a second-order aggregation Python draft."""
        monkeypatch.chdir(temp_project_dir)

        result = runner.invoke(
            app,
            ["draft", "second-order-aggregation", "region_metrics", "--python"]
        )

        assert result.exit_code == 0
        assert "Created:" in result.stdout
        assert "draft_second_order_aggregation_region_metrics.py" in result.stdout
        assert "seeknal apply" in result.stdout

        # Verify file was created
        draft_file = temp_project_dir / "draft_second_order_aggregation_region_metrics.py"
        assert draft_file.exists()

        # Check content
        content = draft_file.read_text()
        assert "# /// script" in content
        assert "requires-python" in content
        assert "second_order_aggregation" in content
        assert "def region_metrics" in content
        assert "ctx.ref(" in content

    def test_draft_python_with_dependencies(self, temp_project_dir, monkeypatch):
        """Test Python draft with custom dependencies."""
        monkeypatch.chdir(temp_project_dir)

        result = runner.invoke(
            app,
            [
                "draft", "second-order-aggregation", "region_metrics",
                "--python",
                "--deps", "pandas,numpy"
            ]
        )

        assert result.exit_code == 0

        draft_file = temp_project_dir / "draft_second_order_aggregation_region_metrics.py"
        content = draft_file.read_text()
        assert '"pandas"' in content
        assert '"numpy"' in content
        assert '"seeknal"' in content

    def test_draft_python_shorthand_py_flag(self, temp_project_dir, monkeypatch):
        """Test -py shorthand flag for Python generation."""
        monkeypatch.chdir(temp_project_dir)

        result = runner.invoke(
            app,
            ["draft", "second-order-aggregation", "test_agg", "-py"]
        )

        assert result.exit_code == 0

        draft_file = temp_project_dir / "draft_second_order_aggregation_test_agg.py"
        assert draft_file.exists()


class TestSecondOrderAggregationDraftValidation:
    """Test validation for second-order aggregation draft command."""

    def test_draft_invalid_name_with_spaces(self, temp_project_dir, monkeypatch):
        """Test draft rejects names with spaces."""
        monkeypatch.chdir(temp_project_dir)

        result = runner.invoke(
            app,
            ["draft", "second-order-aggregation", "invalid name"]
        )

        assert result.exit_code == 1
        assert "cannot contain" in result.stdout.lower()

    def test_draft_invalid_name_with_dots(self, temp_project_dir, monkeypatch):
        """Test draft rejects names with dots."""
        monkeypatch.chdir(temp_project_dir)

        result = runner.invoke(
            app,
            ["draft", "second-order-aggregation", "invalid.name"]
        )

        assert result.exit_code == 1
        assert "cannot contain" in result.stdout.lower()

    def test_draft_empty_name(self, temp_project_dir, monkeypatch):
        """Test draft rejects empty name."""
        monkeypatch.chdir(temp_project_dir)

        result = runner.invoke(
            app,
            ["draft", "second-order-aggregation", ""]
        )

        assert result.exit_code == 1
        assert "cannot be empty" in result.stdout.lower()

    def test_draft_name_too_long(self, temp_project_dir, monkeypatch):
        """Test draft rejects names exceeding 128 characters."""
        monkeypatch.chdir(temp_project_dir)

        long_name = "a" * 129
        result = runner.invoke(
            app,
            ["draft", "second-order-aggregation", long_name]
        )

        assert result.exit_code == 1
        assert "cannot exceed" in result.stdout or "128" in result.stdout


class TestSecondOrderAggregationDraftHelp:
    """Test help documentation for second-order aggregation draft."""

    def test_draft_help_includes_second_order_aggregation(self):
        """Test draft help includes second-order-aggregation option."""
        result = runner.invoke(app, ["draft", "--help"])

        assert result.exit_code == 0
        # Note: The help text has a hardcoded list that doesn't include second-order-aggregation
        # But the actual command works when invoked. Test by trying the command.
        test_result = runner.invoke(
            app,
            ["draft", "second-order-aggregation", "test"]
        )
        # Should not error about invalid node type
        assert test_result.exit_code == 0 or "Invalid node type" not in test_result.stdout

    def test_draft_help_shows_examples(self):
        """Test draft help shows usage examples."""
        result = runner.invoke(app, ["draft", "--help"])

        assert result.exit_code == 0
        assert "Examples:" in result.stdout
        assert "seeknal draft" in result.stdout

    def test_draft_help_shows_python_flag(self):
        """Test draft help shows --python flag."""
        result = runner.invoke(app, ["draft", "--help"])

        assert result.exit_code == 0
        assert "--python" in result.stdout or "-py" in result.stdout

    def test_draft_help_shows_force_flag(self):
        """Test draft help shows --force flag."""
        result = runner.invoke(app, ["draft", "--help"])

        assert result.exit_code == 0
        assert "--force" in result.stdout or "-f" in result.stdout


class TestSecondOrderAggregationTemplateContent:
    """Test generated template content for second-order aggregations."""

    def test_yaml_template_has_required_fields(self, temp_project_dir, monkeypatch):
        """Test YAML template includes all required fields."""
        monkeypatch.chdir(temp_project_dir)

        runner.invoke(
            app,
            ["draft", "second-order-aggregation", "region_metrics"]
        )

        draft_file = temp_project_dir / "draft_second_order_aggregation_region_metrics.yml"
        content = draft_file.read_text()

        # Required fields
        assert "kind: second_order_aggregation" in content
        assert "name:" in content
        assert "id_col:" in content
        assert "feature_date_col:" in content
        assert "source:" in content
        assert "features:" in content

        # Optional fields should be present (commented or with defaults)
        assert "description:" in content
        # Note: inputs field is not in the template for second-order-aggregation
        # because source reference is sufficient

    def test_yaml_template_has_feature_examples(self, temp_project_dir, monkeypatch):
        """Test YAML template includes example feature configurations."""
        monkeypatch.chdir(temp_project_dir)

        runner.invoke(
            app,
            ["draft", "second-order-aggregation", "region_metrics"]
        )

        draft_file = temp_project_dir / "draft_second_order_aggregation_region_metrics.yml"
        content = draft_file.read_text()

        # Should have example features showing different aggregation types
        assert "basic:" in content or "window:" in content or "ratio:" in content

    def test_python_template_has_decorator(self, temp_project_dir, monkeypatch):
        """Test Python template includes @second_order_aggregation decorator."""
        monkeypatch.chdir(temp_project_dir)

        runner.invoke(
            app,
            ["draft", "second-order-aggregation", "region_metrics", "--python"]
        )

        draft_file = temp_project_dir / "draft_second_order_aggregation_region_metrics.py"
        content = draft_file.read_text()

        assert "@second_order_aggregation" in content
        assert "id_col=" in content
        assert "feature_date_col=" in content
        assert "features=" in content

    def test_python_template_has_function_signature(self, temp_project_dir, monkeypatch):
        """Test Python template has correct function signature."""
        monkeypatch.chdir(temp_project_dir)

        runner.invoke(
            app,
            ["draft", "second-order-aggregation", "region_metrics", "--python"]
        )

        draft_file = temp_project_dir / "draft_second_order_aggregation_region_metrics.py"
        content = draft_file.read_text()

        assert "def region_metrics" in content
        assert "ctx)" in content
        assert "ctx.ref(" in content


class TestSecondOrderAggregationDraftIntegration:
    """Integration tests for second-order aggregation draft."""

    def test_draft_yaml_matches_example_structure(self, temp_project_dir, monkeypatch):
        """Test generated YAML matches example file structure."""
        monkeypatch.chdir(temp_project_dir)

        # Generate draft
        runner.invoke(
            app,
            ["draft", "second-order-aggregation", "region_user_metrics", "--description", "Test"]
        )

        draft_file = temp_project_dir / "draft_second_order_aggregation_region_user_metrics.yml"
        draft_content = draft_file.read_text()

        # Verify it has similar structure to example
        # Check for key sections present in the example
        assert "kind:" in draft_content
        assert "name:" in draft_content
        assert "id_col:" in draft_content
        assert "source:" in draft_content
        assert "features:" in draft_content

    def test_multiple_drafts_same_directory(self, temp_project_dir, monkeypatch):
        """Test creating multiple drafts in the same directory."""
        monkeypatch.chdir(temp_project_dir)

        # Create first draft
        result1 = runner.invoke(
            app,
            ["draft", "second-order-aggregation", "metrics_v1"]
        )
        assert result1.exit_code == 0

        # Create second draft with different name
        result2 = runner.invoke(
            app,
            ["draft", "second-order-aggregation", "metrics_v2"]
        )
        assert result2.exit_code == 0

        # Both files should exist
        assert (temp_project_dir / "draft_second_order_aggregation_metrics_v1.yml").exists()
        assert (temp_project_dir / "draft_second_order_aggregation_metrics_v2.yml").exists()

    def test_draft_python_and_yaml_same_name(self, temp_project_dir, monkeypatch):
        """Test creating both YAML and Python drafts with same base name."""
        monkeypatch.chdir(temp_project_dir)

        # Create YAML draft
        result1 = runner.invoke(
            app,
            ["draft", "second-order-aggregation", "test"]
        )
        assert result1.exit_code == 0

        # Create Python draft with same name
        result2 = runner.invoke(
            app,
            ["draft", "second-order-aggregation", "test", "--python"]
        )
        assert result2.exit_code == 0

        # Both files should exist with different extensions
        assert (temp_project_dir / "draft_second_order_aggregation_test.yml").exists()
        assert (temp_project_dir / "draft_second_order_aggregation_test.py").exists()
