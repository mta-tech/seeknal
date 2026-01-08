"""
Project parser for Seeknal 2.0.

Parses the project structure, common config, and Python files
to build the complete DAG manifest.
"""
import os
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from seeknal.dag.manifest import Manifest, Node, NodeType, Edge


class ProjectParser:
    """
    Parses a Seeknal project to build the DAG manifest.

    The parser:
    1. Reads common.yml for sources, transforms, rules
    2. Discovers Python files for feature groups, models
    3. Validates all references
    4. Builds the complete manifest
    """

    def __init__(
        self,
        project_name: str,
        project_path: Optional[str] = None,
        seeknal_version: str = "2.0.0"
    ):
        self.project_name = project_name
        self.project_path = Path(project_path) if project_path else Path.cwd()
        self.seeknal_version = seeknal_version
        self.manifest = Manifest(
            project=project_name,
            seeknal_version=seeknal_version
        )
        self._pending_references: List[Tuple[str, str, str]] = []
        self._errors: List[str] = []
        self._warnings: List[str] = []

    def parse(self) -> Manifest:
        """
        Parse the project and return the manifest.

        Returns:
            The built Manifest
        """
        # Parse common config
        self._parse_common_config()

        # TODO: Parse Python files for feature groups, models
        # self._parse_python_files()

        # Build edges from registry
        # self._build_edges_from_registry()

        return self.manifest

    def _parse_common_config(self) -> None:
        """Parse common.yml for sources, transforms, rules."""
        common_path = self.project_path / "common.yml"
        if not common_path.exists():
            return

        with open(common_path, "r") as f:
            config = yaml.safe_load(f) or {}

        # Parse sources
        for source_config in config.get("sources", []):
            node = Node(
                id=f"source.{source_config['id']}",
                name=source_config["id"],
                node_type=NodeType.SOURCE,
                description=source_config.get("description"),
                config={
                    "source": source_config.get("source"),
                    "table": source_config.get("table"),
                    "params": source_config.get("params", {}),
                }
            )
            self.manifest.add_node(node)

        # Parse transformations
        for transform_config in config.get("transformations", []):
            node = Node(
                id=f"transform.{transform_config['id']}",
                name=transform_config["id"],
                node_type=NodeType.TRANSFORM,
                description=transform_config.get("description"),
                config={
                    "className": transform_config.get("className"),
                    "params": transform_config.get("params", {}),
                }
            )
            self.manifest.add_node(node)

        # Parse rules
        for rule_config in config.get("rules", []):
            node = Node(
                id=f"rule.{rule_config['id']}",
                name=rule_config["id"],
                node_type=NodeType.RULE,
                description=rule_config.get("description"),
                config={
                    "rule": rule_config.get("rule", {}),
                }
            )
            self.manifest.add_node(node)

        # Parse aggregations (if present)
        for agg_config in config.get("aggregations", []):
            node = Node(
                id=f"aggregation.{agg_config['id']}",
                name=agg_config["id"],
                node_type=NodeType.AGGREGATION,
                description=agg_config.get("description"),
                config=agg_config,
            )
            self.manifest.add_node(node)

    def validate(self) -> List[str]:
        """
        Validate the parsed manifest.

        Returns:
            List of validation errors
        """
        errors = []

        # Check for missing references
        for node_id, ref_id, ref_type in self._pending_references:
            if ref_id not in self.manifest.nodes:
                errors.append(
                    f"Node '{node_id}' references '{ref_id}' which does not exist"
                )

        # Check for cycles
        has_cycle, cycle_path = self.manifest.detect_cycles()
        if has_cycle:
            errors.append(f"Cycle detected in DAG: {' -> '.join(cycle_path)}")

        return errors

    def get_warnings(self) -> List[str]:
        """Get parsing warnings."""
        return self._warnings.copy()

    def get_errors(self) -> List[str]:
        """Get parsing errors."""
        return self._errors.copy()
