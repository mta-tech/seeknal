"""
Dependency Registry for DAG tracking.

This module provides a singleton registry that tracks dependencies
between nodes in the Seeknal DAG. When ref(), source(), use_transform(),
or use_rule() are called, they register their dependencies here.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional
from threading import Lock


class DependencyType(Enum):
    """Type of dependency relationship."""
    SOURCE = "source"
    REF = "ref"
    TRANSFORM = "transform"
    RULE = "rule"
    AGGREGATION = "aggregation"


@dataclass
class Dependency:
    """A single dependency relationship."""
    node_id: str
    dependency_id: str
    dependency_type: DependencyType
    params: Dict = field(default_factory=dict)


class DependencyRegistry:
    """
    Singleton registry for tracking DAG dependencies.

    This registry is populated during code execution when ref(), source(),
    use_transform(), and use_rule() functions are called. It maintains
    the dependency graph that will be serialized to manifest.json.

    Thread-safe for concurrent access.
    """

    _instance: Optional["DependencyRegistry"] = None
    _lock: Lock = Lock()

    def __init__(self):
        self._dependencies: List[Dependency] = []
        self._current_node: Optional[str] = None
        self._node_dependencies: Dict[str, List[Dependency]] = {}

    @classmethod
    def get_instance(cls) -> "DependencyRegistry":
        """Get the singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def reset(self) -> None:
        """Reset the registry. Useful for testing."""
        with self._lock:
            self._dependencies = []
            self._current_node = None
            self._node_dependencies = {}

    def set_current_node(self, node_id: str) -> None:
        """Set the current node context for dependency registration."""
        self._current_node = node_id
        if node_id not in self._node_dependencies:
            self._node_dependencies[node_id] = []

    def get_current_node(self) -> Optional[str]:
        """Get the current node context."""
        return self._current_node

    def clear_current_node(self) -> None:
        """Clear the current node context."""
        self._current_node = None

    def register_dependency(
        self,
        node_id: str,
        dependency_id: str,
        dependency_type: DependencyType,
        params: Optional[Dict] = None
    ) -> Dependency:
        """
        Register a dependency.

        Args:
            node_id: The node that has the dependency
            dependency_id: The ID of the dependency (source name, node name, etc.)
            dependency_type: Type of dependency (SOURCE, REF, TRANSFORM, RULE)
            params: Optional parameters passed to the dependency

        Returns:
            The created Dependency object
        """
        dep = Dependency(
            node_id=node_id,
            dependency_id=dependency_id,
            dependency_type=dependency_type,
            params=params or {}
        )

        with self._lock:
            self._dependencies.append(dep)
            if node_id not in self._node_dependencies:
                self._node_dependencies[node_id] = []
            self._node_dependencies[node_id].append(dep)

        return dep

    def get_dependencies_for_node(self, node_id: str) -> List[Dependency]:
        """Get all dependencies for a specific node."""
        return self._node_dependencies.get(node_id, [])

    def get_all_dependencies(self) -> List[Dependency]:
        """Get all registered dependencies."""
        return self._dependencies.copy()

    def get_dependency_graph(self) -> Dict[str, List[str]]:
        """
        Get the dependency graph as adjacency list.

        Returns:
            Dict mapping node_id to list of dependency_ids
        """
        graph = {}
        for node_id, deps in self._node_dependencies.items():
            graph[node_id] = [d.dependency_id for d in deps]
        return graph
