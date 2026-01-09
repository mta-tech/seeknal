"""
Project parser for Seeknal 2.0.

Parses the project structure, common config, and Python files
to build the complete DAG manifest.
"""
import yaml
from pathlib import Path
from typing import Any, Optional

from seeknal.dag.manifest import Manifest, Node, NodeType


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
        """Initialize the parser.

        Args:
            project_name: Name of the project.
            project_path: Path to the project directory.
            seeknal_version: Version of Seeknal.

        Raises:
            ValueError: If the project path contains path traversal sequences.
        """
        self.project_name = project_name

        # Validate and resolve project path
        if project_path:
            path = Path(project_path)
            if ".." in path.parts:
                raise ValueError("Invalid project path: path traversal not allowed")
            self.project_path = path.resolve()
        else:
            self.project_path = Path.cwd()

        self.seeknal_version = seeknal_version
        self.manifest = Manifest(
            project=project_name,
            seeknal_version=seeknal_version
        )
        self._pending_references: list[tuple[str, str, str]] = []
        self._errors: list[str] = []
        self._warnings: list[str] = []

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

        with open(common_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

        # Parse sources
        for idx, source_config in enumerate(config.get("sources", [])):
            source_id = source_config.get("id")
            if not source_id:
                self._errors.append(f"Source at index {idx} is missing required 'id' field")
                continue
            node = Node(
                id=f"source.{source_id}",
                name=source_id,
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
        for idx, transform_config in enumerate(config.get("transformations", [])):
            transform_id = transform_config.get("id")
            if not transform_id:
                self._errors.append(f"Transform at index {idx} is missing required 'id' field")
                continue
            node = Node(
                id=f"transform.{transform_id}",
                name=transform_id,
                node_type=NodeType.TRANSFORM,
                description=transform_config.get("description"),
                config={
                    "className": transform_config.get("className"),
                    "params": transform_config.get("params", {}),
                }
            )
            self.manifest.add_node(node)

        # Parse rules
        for idx, rule_config in enumerate(config.get("rules", [])):
            rule_id = rule_config.get("id")
            if not rule_id:
                self._errors.append(f"Rule at index {idx} is missing required 'id' field")
                continue
            node = Node(
                id=f"rule.{rule_id}",
                name=rule_id,
                node_type=NodeType.RULE,
                description=rule_config.get("description"),
                config={
                    "rule": rule_config.get("rule", {}),
                }
            )
            self.manifest.add_node(node)

        # Parse aggregations (if present)
        for idx, agg_config in enumerate(config.get("aggregations", [])):
            agg_id = agg_config.get("id")
            if not agg_id:
                self._errors.append(f"Aggregation at index {idx} is missing required 'id' field")
                continue
            node = Node(
                id=f"aggregation.{agg_id}",
                name=agg_id,
                node_type=NodeType.AGGREGATION,
                description=agg_config.get("description"),
                config=agg_config,
            )
            self.manifest.add_node(node)

    def validate(self) -> list[str]:
        """
        Validate the parsed manifest.

        Returns:
            List of validation errors
        """
        errors: list[str] = []

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

    def get_warnings(self) -> list[str]:
        """Get parsing warnings."""
        return self._warnings.copy()

    def get_errors(self) -> list[str]:
        """Get parsing errors."""
        return self._errors.copy()
