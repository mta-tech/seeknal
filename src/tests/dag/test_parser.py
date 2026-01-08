"""Tests for the project parser."""
import pytest
import tempfile
import os
from pathlib import Path
from seeknal.dag.parser import ProjectParser
from seeknal.dag.manifest import Manifest, NodeType


class TestProjectParser:
    """Test the ProjectParser class."""

    def test_parser_creation(self):
        """Can create a parser."""
        parser = ProjectParser(project_name="test_project")
        assert parser.project_name == "test_project"

    def test_parse_empty_project(self):
        """Parser handles empty project."""
        parser = ProjectParser(project_name="test_project")
        manifest = parser.parse()

        assert manifest.metadata.project == "test_project"
        assert len(manifest.nodes) == 0

    def test_parse_creates_manifest(self):
        """Parsing creates a manifest."""
        parser = ProjectParser(project_name="test_project")
        manifest = parser.parse()

        assert isinstance(manifest, Manifest)

    def test_parser_discovers_sources_from_common_config(self):
        """Parser discovers sources from common config."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create common.yml with sources
            common_yml = Path(tmpdir) / "common.yml"
            common_yml.write_text("""
sources:
  - id: traffic_day
    source: hive
    table: db.traffic
    description: Daily traffic data
""")
            parser = ProjectParser(
                project_name="test_project",
                project_path=tmpdir
            )
            manifest = parser.parse()

            assert "source.traffic_day" in manifest.nodes
            assert manifest.nodes["source.traffic_day"].node_type == NodeType.SOURCE

    def test_parser_discovers_transforms_from_common_config(self):
        """Parser discovers transforms from common config."""
        with tempfile.TemporaryDirectory() as tmpdir:
            common_yml = Path(tmpdir) / "common.yml"
            common_yml.write_text("""
transformations:
  - id: rename_cols
    className: RenameTransformer
    description: Rename columns
""")
            parser = ProjectParser(
                project_name="test_project",
                project_path=tmpdir
            )
            manifest = parser.parse()

            assert "transform.rename_cols" in manifest.nodes

    def test_parser_discovers_rules_from_common_config(self):
        """Parser discovers rules from common config."""
        with tempfile.TemporaryDirectory() as tmpdir:
            common_yml = Path(tmpdir) / "common.yml"
            common_yml.write_text("""
rules:
  - id: callExpression
    rule:
      value: "service_type = 'Voice'"
""")
            parser = ProjectParser(
                project_name="test_project",
                project_path=tmpdir
            )
            manifest = parser.parse()

            assert "rule.callExpression" in manifest.nodes

    def test_parser_validates_references(self):
        """Parser validates that all references exist."""
        parser = ProjectParser(project_name="test_project")

        # Add a node that references a non-existent source
        parser._pending_references = [
            ("feature_group.test", "source.missing", "ref")
        ]

        errors = parser.validate()
        assert len(errors) > 0
        assert "source.missing" in errors[0]
