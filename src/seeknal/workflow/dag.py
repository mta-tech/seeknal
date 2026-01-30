"""
DAG building functionality for Seeknal workflow.

This module provides comprehensive DAG (Directed Acyclic Graph) building capabilities:
- Node discovery from YAML files in the seeknal/ directory
- Dependency extraction from YAML inputs
- Topological sorting for execution order
- Cycle detection with clear error reporting

The DAG builder scans for all *.yml files, parses them to extract nodes and edges,
and builds a complete dependency graph that can be used for validation and execution.
"""

from __future__ import annotations

import yaml
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from seeknal.dag.manifest import NodeType
from seeknal.utils.path_security import warn_if_insecure_path


@dataclass(slots=True)
class DAGNode:
    """A node in the DAG.

    Attributes:
        qualified_name: Fully qualified name (e.g., "source.raw_customers")
        kind: Node type from the NodeType enum
        name: Simple name (e.g., "raw_customers")
        file_path: Absolute path to the YAML file
        yaml_data: Raw parsed YAML data
        description: Optional description from YAML
        owner: Optional owner from YAML
        tags: Optional tags from YAML
    """

    qualified_name: str
    kind: NodeType
    name: str
    file_path: str
    yaml_data: dict[str, Any]
    description: Optional[str] = None
    owner: Optional[str] = None
    tags: list[str] = field(default_factory=list)

    def __hash__(self) -> int:
        """Make node hashable for use in sets and dicts."""
        return hash(self.qualified_name)

    def __eq__(self, other: object) -> bool:
        """Equality comparison based on qualified name."""
        if not isinstance(other, DAGNode):
            return NotImplemented
        return self.qualified_name == other.qualified_name

    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"DAGNode({self.qualified_name}, kind={self.kind.value})"

    # Properties for compatibility with executors
    @property
    def id(self) -> str:
        """Get node ID (alias for qualified_name)."""
        return self.qualified_name

    @property
    def config(self) -> dict[str, Any]:
        """Get node config (alias for yaml_data)."""
        return self.yaml_data

    @property
    def node_type(self) -> NodeType:
        """Get node type (alias for kind)."""
        return self.kind


@dataclass(slots=True)
class DAGEdge:
    """A directed edge in the DAG.

    Represents a dependency relationship where 'to_node' depends on 'from_node'.

    Attributes:
        from_node: Qualified name of the source node
        to_node: Qualified name of the destination node
    """

    from_node: str
    to_node: str

    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"DAGEdge({self.from_node} -> {self.to_node})"


class CycleDetectedError(Exception):
    """
    Exception raised when a cycle is detected in the DAG.

    Attributes:
        cycle_path: List of node names forming the cycle
    """

    def __init__(self, cycle_path: list[str], message: str | None = None) -> None:
        """
        Initialize the cycle detection error.

        Args:
            cycle_path: List of node names forming the cycle
            message: Optional custom error message
        """
        self.cycle_path = cycle_path
        if message is None:
            cycle_str = " -> ".join(cycle_path)
            message = f"Cycle detected in DAG: {cycle_str}"
        super().__init__(message)
        self.message = message


class MissingDependencyError(Exception):
    """
    Exception raised when a node references a dependency that doesn't exist.

    Attributes:
        node_name: Name of the node with the missing dependency
        dependency_name: Name of the missing dependency
    """

    def __init__(self, node_name: str, dependency_name: str) -> None:
        """
        Initialize the missing dependency error.

        Args:
            node_name: Name of the node with the missing dependency
            dependency_name: Name of the missing dependency
        """
        self.node_name = node_name
        self.dependency_name = dependency_name
        message = (
            f"Node '{node_name}' depends on '{dependency_name}' "
            f"which does not exist in the DAG"
        )
        super().__init__(message)
        self.message = message


class DAGBuilder:
    """
    Builds and manages a Directed Acyclic Graph (DAG) from YAML files.

    The DAG builder performs the following operations:
    1. Discovers all *.yml files in the seeknal/ directory
    2. Parses each YAML file to extract nodes (kind, name, metadata)
    3. Extracts dependencies from the 'inputs' field
    4. Builds an adjacency list representation
    5. Validates the graph (no cycles, all dependencies exist)
    6. Provides topological sort for execution order

    Example:
        Build a DAG and get execution order::

            builder = DAGBuilder(project_path="/path/to/project")
            builder.build()

            # Get nodes in topological order
            execution_order = builder.topological_sort()

            # Detect cycles
            has_cycle = builder.has_cycle()

            # Get dependencies for a specific node
            upstream = builder.get_upstream("source.raw_data")
            downstream = builder.get_downstream("source.raw_data")
    """

    # Supported node types from YAML 'kind' field
    KIND_MAP: dict[str, NodeType] = {
        "source": NodeType.SOURCE,
        "transform": NodeType.TRANSFORM,
        "feature_group": NodeType.FEATURE_GROUP,
        "model": NodeType.MODEL,
        "aggregation": NodeType.AGGREGATION,
        "second_order_aggregation": NodeType.SECOND_ORDER_AGGREGATION,
        "rule": NodeType.RULE,
        "exposure": NodeType.EXPOSURE,
    }

    def __init__(self, project_path: str | Path = ".") -> None:
        """
        Initialize the DAG builder.

        Args:
            project_path: Path to the project root directory.
                         The seeknal/ directory will be searched relative to this path.

        Raises:
            ValueError: If the project path contains path traversal sequences
        """
        self.project_path = Path(project_path).resolve()

        # Validate path to prevent path traversal attacks
        if ".." in Path(project_path).parts:
            raise ValueError("Invalid project path: path traversal not allowed")

        # Warn about insecure paths
        warn_if_insecure_path(
            str(self.project_path), context="DAG builder project path"
        )

        self.seeknal_dir = self.project_path / "seeknal"

        # Node storage: qualified_name -> DAGNode
        self.nodes: dict[str, DAGNode] = {}

        # Edge storage
        self.edges: list[DAGEdge] = []

        # Adjacency lists for efficient graph traversal
        self._upstream: dict[str, set[str]] = defaultdict(set)  # node -> dependencies
        self._downstream: dict[str, set[str]] = defaultdict(set)  # node -> dependents

        # Errors collected during parsing
        self._parse_errors: list[str] = []

        # Profile config (for default materialization settings)
        self.profile: dict[str, Any] = {}

    def _merge_materialization_config(
        self,
        decorator_config: Optional[dict],
        yaml_config: Optional[dict],
        profile_config: Optional[dict] = None,
    ) -> dict:
        """
        Merge materialization configuration with priority:
        decorator > yaml > profile > defaults

        Args:
            decorator_config: Materialization config from Python decorator
            yaml_config: Materialization config from YAML override file
            profile_config: Materialization config from profile defaults

        Returns:
            Merged materialization configuration dict

        Note:
            Only non-None decorator values override lower-priority configs.
            This allows selective override (e.g., decorator sets table, inherits mode).
        """
        # Start with system defaults
        result = {
            "enabled": False,  # Default: opt-in
            "mode": "append",
            "create_table": True,
        }

        # Merge profile config (if exists)
        if profile_config:
            result.update(profile_config)

        # Merge YAML config (overrides profile)
        if yaml_config:
            result.update(yaml_config)

        # Merge decorator config (highest priority)
        if decorator_config:
            # CRITICAL: Only override non-None values
            for key, value in decorator_config.items():
                if value is not None:
                    result[key] = value

        return result

    def _load_yaml_override_for_node(self, qualified_name: str) -> Optional[dict]:
        """
        Load YAML override file for a specific node.

        Checks for a YAML file matching the node name in seeknal/ subdirectories.
        For example: 'transform.sales_forecast' -> seeknal/transforms/sales_forecast.yml

        Args:
            qualified_name: Fully qualified node name (e.g., "transform.sales_forecast")

        Returns:
            Parsed YAML data dict, or None if no override file exists
        """
        # Parse qualified name: "kind.name" -> kind, name
        parts = qualified_name.split(".", 1)
        if len(parts) != 2:
            return None

        kind, name = parts
        kind_dir = kind + "s"  # "transform" -> "transforms"

        # Look for YAML file in seeknal/{kind_dir}/{name}.yml
        yaml_path = self.seeknal_dir / kind_dir / f"{name}.yml"

        if not yaml_path.exists():
            return None

        try:
            import yaml
            with open(yaml_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)

            if isinstance(data, dict) and "materialization" in data:
                return data.get("materialization")

            return None

        except Exception:
            # If loading fails, just return None (don't fail on override)
            return None

    def build(self) -> None:
        """
        Build the DAG by scanning and parsing all YAML and Python files.

        This method:
        1. Discovers all *.yml files in seeknal/ subdirectories
        2. Discovers all *.py files in seeknal/pipelines/
        3. Parses each file to extract node metadata
        4. Extracts dependencies from inputs (YAML) or ctx.ref() calls (Python)
        5. Builds the adjacency list representation

        Raises:
            ValueError: If no YAML or Python files are found
        """
        # Discover and parse YAML files
        yaml_files = self._discover_yaml_files()

        # Discover and parse Python files
        python_files = self._discover_python_files()

        if not yaml_files and not python_files:
            raise ValueError(
                f"No YAML or Python files found in {self.seeknal_dir}. "
                "Create at least one YAML file in seeknal/ or Python file in seeknal/pipelines/ "
                "to define nodes."
            )

        # Parse YAML files
        for yaml_file in yaml_files:
            self._parse_yaml_file(yaml_file)

        # Parse Python files
        for py_file in python_files:
            self._parse_python_file(py_file)

        # Extract dependencies and build edges
        for node in self.nodes.values():
            self._extract_dependencies(node)

        # Build adjacency lists from edges
        self._build_adjacency_lists()

    def _discover_yaml_files(self) -> list[Path]:
        """
        Discover all *.yml files in the seeknal/ directory.

        Returns:
            List of paths to YAML files
        """
        if not self.seeknal_dir.exists():
            return []

        yaml_files = list(self.seeknal_dir.glob("**/*.yml"))
        return yaml_files

    def _discover_python_files(self) -> list[Path]:
        """
        Discover all *.py files in the seeknal/pipelines/ directory.

        Returns:
            List of paths to Python files
        """
        pipelines_dir = self.seeknal_dir / "pipelines"
        if not pipelines_dir.exists():
            return []

        python_files = list(pipelines_dir.glob("*.py"))
        # Exclude private files (starting with _)
        return [f for f in python_files if not f.name.startswith("_")]

    def _parse_yaml_file(self, file_path: Path) -> None:
        """
        Parse a YAML file and extract node information.

        Args:
            file_path: Path to the YAML file
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)

            if not data or not isinstance(data, dict):
                self._parse_errors.append(
                    f"Invalid YAML in {file_path}: must be a dictionary"
                )
                return

            # Extract required fields
            kind_str = data.get("kind")
            name = data.get("name")

            if not kind_str:
                self._parse_errors.append(f"Missing 'kind' field in {file_path}")
                return

            if not name:
                self._parse_errors.append(f"Missing 'name' field in {file_path}")
                return

            # Map kind string to NodeType enum
            if kind_str not in self.KIND_MAP:
                self._parse_errors.append(
                    f"Invalid kind '{kind_str}' in {file_path}. "
                    f"Valid kinds: {', '.join(self.KIND_MAP.keys())}"
                )
                return

            kind = self.KIND_MAP[kind_str]

            # Build qualified name (e.g., "source.raw_customers")
            qualified_name = f"{kind_str}.{name}"

            # Check for duplicate nodes
            if qualified_name in self.nodes:
                self._parse_errors.append(
                    f"Duplicate node '{qualified_name}' found in {file_path} "
                    f"(already defined in {self.nodes[qualified_name].file_path})"
                )
                return

            # Create node
            node = DAGNode(
                qualified_name=qualified_name,
                kind=kind,
                name=name,
                file_path=str(file_path.resolve()),
                yaml_data=data,
                description=data.get("description"),
                owner=data.get("owner"),
                tags=data.get("tags", []),
            )

            self.nodes[qualified_name] = node

        except yaml.YAMLError as e:
            self._parse_errors.append(f"Failed to parse YAML in {file_path}: {e}")
        except Exception as e:
            self._parse_errors.append(f"Error processing {file_path}: {e}")

    def _parse_python_file(self, file_path: Path) -> None:
        """
        Parse a Python file and extract decorated nodes.

        Uses the PythonPipelineDiscoverer to import the file and extract
        nodes decorated with @source, @transform, or @feature_group.

        Args:
            file_path: Path to the Python file
        """
        from seeknal.pipeline.discoverer import PythonPipelineDiscoverer

        discoverer = PythonPipelineDiscoverer(self.project_path)

        try:
            # Parse the file to extract decorated nodes
            nodes = discoverer.parse_file(file_path)

            # Extract dependencies via AST
            deps_map = discoverer.extract_dependencies_ast(file_path)

            for node_meta in nodes:
                kind_str = node_meta["kind"]
                name = node_meta["name"]

                # Map kind string to NodeType enum
                if kind_str not in self.KIND_MAP:
                    self._parse_errors.append(
                        f"Invalid kind '{kind_str}' in {file_path}. "
                        f"Valid kinds: {', '.join(self.KIND_MAP.keys())}"
                    )
                    continue

                kind = self.KIND_MAP[kind_str]

                # Build qualified name
                qualified_name = f"{kind_str}.{name}"

                # Check for duplicate nodes
                if qualified_name in self.nodes:
                    self._parse_errors.append(
                        f"Duplicate node '{qualified_name}' found in {file_path} "
                        f"(already defined in {self.nodes[qualified_name].file_path})"
                    )
                    continue

                # Create yaml_data from node metadata
                yaml_data = {
                    "kind": kind_str,
                    "name": name,
                    "description": node_meta.get("description"),
                    "tags": node_meta.get("tags", []),
                }

                # Add kind-specific fields
                if kind_str == "source":
                    yaml_data["source"] = node_meta.get("source", "unknown")
                    yaml_data["table"] = node_meta.get("table", "")
                    yaml_data["columns"] = node_meta.get("columns", {})
                elif kind_str in ("transform", "feature_group"):
                    # Include inputs from metadata
                    if "inputs" in node_meta:
                        yaml_data["inputs"] = node_meta["inputs"]
                    # Also include AST-extracted dependencies
                    if name in deps_map:
                        # Merge inputs from decorator and AST
                        existing_inputs = yaml_data.get("inputs", [])
                        ast_deps = [{"ref": d} for d in deps_map[name]]
                        # Combine, avoiding duplicates
                        all_refs = {inp["ref"]: inp for inp in existing_inputs + ast_deps}
                        yaml_data["inputs"] = list(all_refs.values())
                    elif name in deps_map:
                        # Only AST dependencies found
                        yaml_data["inputs"] = [{"ref": d} for d in deps_map[name]]

                if kind_str == "feature_group":
                    yaml_data["entity"] = node_meta.get("entity")
                    yaml_data["features"] = node_meta.get("features", {})

                # Handle materialization for all node types (source, transform, feature_group)
                decorator_mat = node_meta.get("materialization")
                if decorator_mat is not None:
                    # Load YAML override for this node
                    yaml_mat = self._load_yaml_override_for_node(qualified_name)
                    # Get profile defaults
                    profile_mat = self.profile.get("materialization", {})
                    # Merge configs: decorator > yaml > profile > defaults
                    merged_mat = self._merge_materialization_config(
                        decorator_mat, yaml_mat, profile_mat
                    )
                    yaml_data["materialization"] = merged_mat

                # Create node
                node = DAGNode(
                    qualified_name=qualified_name,
                    kind=kind,
                    name=name,
                    file_path=str(file_path.resolve()),
                    yaml_data=yaml_data,
                    description=node_meta.get("description"),
                    owner=node_meta.get("owner"),
                    tags=node_meta.get("tags", []),
                )

                self.nodes[qualified_name] = node

        except SyntaxError as e:
            self._parse_errors.append(f"Syntax error in {file_path}: {e}")
        except ImportError as e:
            self._parse_errors.append(f"Import error in {file_path}: {e}")
        except Exception as e:
            self._parse_errors.append(f"Error processing {file_path}: {e}")

    def _extract_dependencies(self, node: DAGNode) -> None:
        """
        Extract dependencies from a node's YAML data.

        Dependencies are specified in the 'inputs' field as a list of
        objects with 'ref' fields, e.g.:

        inputs:
          - ref: source.raw_data
          - ref: transform.clean_data

        Args:
            node: Node to extract dependencies from
        """
        inputs = node.yaml_data.get("inputs", [])

        if not inputs:
            return

        if not isinstance(inputs, list):
            self._parse_errors.append(
                f"Invalid 'inputs' field in {node.file_path}: must be a list"
            )
            return

        for input_spec in inputs:
            if not isinstance(input_spec, dict):
                continue

            dep_ref = input_spec.get("ref")

            if not dep_ref:
                continue

            # Add edge: dep_ref -> node
            edge = DAGEdge(from_node=dep_ref, to_node=node.qualified_name)
            self.edges.append(edge)

    def _build_adjacency_lists(self) -> None:
        """Build upstream and downstream adjacency lists from edges."""
        # Initialize all nodes in adjacency lists
        for node_name in self.nodes:
            self._upstream[node_name] = set()
            self._downstream[node_name] = set()

        # Populate from edges
        for edge in self.edges:
            if edge.to_node in self._upstream:
                self._upstream[edge.to_node].add(edge.from_node)
            if edge.from_node in self._downstream:
                self._downstream[edge.from_node].add(edge.to_node)

    def get_upstream(self, node_name: str) -> set[str]:
        """
        Get immediate upstream dependencies for a node.

        Args:
            node_name: Qualified name of the node

        Returns:
            Set of qualified names that this node depends on
        """
        return self._upstream.get(node_name, set()).copy()

    def get_downstream(self, node_name: str) -> set[str]:
        """
        Get immediate downstream dependents for a node.

        Args:
            node_name: Qualified name of the node

        Returns:
            Set of qualified names that depend on this node
        """
        return self._downstream.get(node_name, set()).copy()

    def get_all_upstream(self, node_name: str) -> set[str]:
        """
        Get all upstream dependencies (transitive closure).

        Args:
            node_name: Qualified name of the node

        Returns:
            Set of all qualified names that this node transitively depends on
        """
        all_upstream: set[str] = set()
        to_visit: list[str] = [node_name]
        visited: set[str] = set()

        while to_visit:
            current = to_visit.pop()
            if current in visited:
                continue
            visited.add(current)

            for dep in self.get_upstream(current):
                if dep not in visited:
                    all_upstream.add(dep)
                    to_visit.append(dep)

        return all_upstream

    def get_all_downstream(self, node_name: str) -> set[str]:
        """
        Get all downstream dependents (transitive closure).

        Args:
            node_name: Qualified name of the node

        Returns:
            Set of all qualified names that transitively depend on this node
        """
        all_downstream: set[str] = set()
        to_visit: list[str] = [node_name]
        visited: set[str] = set()

        while to_visit:
            current = to_visit.pop()
            if current in visited:
                continue
            visited.add(current)

            for dependent in self.get_downstream(current):
                if dependent not in visited:
                    all_downstream.add(dependent)
                    to_visit.append(dependent)

        return all_downstream

    def has_cycle(self) -> bool:
        """
        Check if the DAG contains a cycle.

        Returns:
            True if a cycle exists, False otherwise
        """
        has_cycle, _ = self._detect_cycle()
        return has_cycle

    def detect_cycle(self) -> tuple[bool, list[str]]:
        """
        Detect if the DAG contains a cycle.

        Returns:
            Tuple of (has_cycle, cycle_path) where cycle_path is a list
            of node names forming the cycle (empty if no cycle)
        """
        return self._detect_cycle()

    def _detect_cycle(self) -> tuple[bool, list[str]]:
        """
        Detect cycles using DFS-based approach.

        Returns:
            Tuple of (has_cycle, cycle_path)
        """
        visited: set[str] = set()
        rec_stack: set[str] = set()
        cycle_path: list[str] = []
        path_positions: dict[str, int] = {}

        def dfs(node: str, path: list[str]) -> bool:
            """DFS traversal with cycle detection."""
            visited.add(node)
            rec_stack.add(node)
            path_positions[node] = len(path)
            path.append(node)

            # Check all downstream nodes
            for neighbor in self.get_downstream(node):
                if neighbor not in visited:
                    if dfs(neighbor, path):
                        return True
                elif neighbor in rec_stack:
                    # Found a cycle - extract cycle path
                    cycle_start = path_positions[neighbor]
                    cycle_path.extend(path[cycle_start:])
                    cycle_path.append(neighbor)
                    return True

            # Backtrack
            path.pop()
            if node in path_positions:
                del path_positions[node]
            rec_stack.remove(node)
            return False

        # Start DFS from each unvisited node
        for node_name in self.nodes:
            if node_name not in visited:
                if dfs(node_name, []):
                    return True, cycle_path

        return False, []

    def topological_sort(self) -> list[str]:
        """
        Perform topological sort using Kahn's algorithm.

        Returns:
            List of node qualified names in topological order (dependencies before dependents)

        Raises:
            CycleDetectedError: If the DAG contains a cycle
        """
        # Check for cycles first
        has_cycle, cycle_path = self._detect_cycle()
        if has_cycle:
            raise CycleDetectedError(cycle_path)

        # Kahn's algorithm
        in_degree: dict[str, int] = {node: 0 for node in self.nodes}

        # Calculate in-degrees
        for edge in self.edges:
            if edge.to_node in in_degree:
                in_degree[edge.to_node] += 1

        # Initialize queue with nodes that have no dependencies
        queue: list[str] = [node for node, degree in in_degree.items() if degree == 0]
        result: list[str] = []

        while queue:
            # Process nodes in lexicographic order for deterministic output
            queue.sort()
            node = queue.pop(0)
            result.append(node)

            # Reduce in-degree for downstream nodes
            for dependent in self.get_downstream(node):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        # Verify all nodes were processed (should not happen if cycle check passed)
        if len(result) != len(self.nodes):
            raise CycleDetectedError([], "Topological sort failed: cycle detected")

        return result

    def validate_dependencies(self) -> list[str]:
        """
        Validate that all referenced dependencies exist.

        Returns:
            List of validation error messages (empty if all valid)

        Raises:
            MissingDependencyError: If any dependency is missing
        """
        errors: list[str] = []

        for edge in self.edges:
            if edge.from_node not in self.nodes:
                error_msg = (
                    f"Node '{edge.to_node}' depends on '{edge.from_node}' "
                    f"which does not exist"
                )
                errors.append(error_msg)

        return errors

    def get_parse_errors(self) -> list[str]:
        """
        Get errors that occurred during YAML parsing.

        Returns:
            List of error messages
        """
        return self._parse_errors.copy()

    def get_node(self, qualified_name: str) -> DAGNode | None:
        """
        Get a node by its qualified name.

        Args:
            qualified_name: Fully qualified node name (e.g., "source.raw_customers")

        Returns:
            DAGNode if found, None otherwise
        """
        return self.nodes.get(qualified_name)

    def get_nodes_by_kind(self, kind: NodeType) -> list[DAGNode]:
        """
        Get all nodes of a specific kind.

        Args:
            kind: NodeType enum value

        Returns:
            List of nodes with the specified kind
        """
        return [node for node in self.nodes.values() if node.kind == kind]

    def get_node_count(self) -> int:
        """
        Get the total number of nodes in the DAG.

        Returns:
            Number of nodes
        """
        return len(self.nodes)

    def get_edge_count(self) -> int:
        """
        Get the total number of edges in the DAG.

        Returns:
            Number of edges
        """
        return len(self.edges)

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"DAGBuilder(nodes={len(self.nodes)}, edges={len(self.edges)}, "
            f"path={self.seeknal_dir})"
        )


def build_dag(project_path: str | Path = ".") -> DAGBuilder:
    """
    Convenience function to build a DAG from a project.

    Args:
        project_path: Path to the project root

    Returns:
        DAGBuilder instance with built DAG

    Raises:
        ValueError: If no YAML files are found or project path is invalid
        CycleDetectedError: If the DAG contains a cycle
        MissingDependencyError: If any dependency is missing
    """
    builder = DAGBuilder(project_path)
    builder.build()

    # Validate dependencies
    errors = builder.validate_dependencies()
    if errors:
        # Extract node name and dependency name from first error message
        first_error = errors[0]
        # Error format: "Node '{node_name}' depends on '{dep_name}' which does not exist"
        import re

        match = re.search(r"Node '([^']+)' depends on '([^']+)'", first_error)
        if match:
            node_name, dep_name = match.groups()
            raise MissingDependencyError(node_name, dep_name)
        else:
            raise MissingDependencyError("unknown", "unknown")

    # Check for cycles
    has_cycle, cycle_path = builder.detect_cycle()
    if has_cycle:
        raise CycleDetectedError(cycle_path)

    return builder
