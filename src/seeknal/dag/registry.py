"""
Dependency Registry for DAG tracking.

This module provides a singleton registry that tracks dependencies
between nodes in the Seeknal DAG. When ref(), source(), use_transform(),
or use_rule() are called, they register their dependencies here.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional
from threading import Lock


class DependencyType(Enum):
    """Type of dependency relationship."""
    SOURCE = "source"
    REF = "ref"
    TRANSFORM = "transform"
    RULE = "rule"
    AGGREGATION = "aggregation"


@dataclass(slots=True)
class Dependency:
    """A single dependency relationship."""
    node_id: str
    dependency_id: str
    dependency_type: DependencyType
    params: dict[str, Any] = field(default_factory=dict)


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

    def __init__(self) -> None:
        self._dependencies: list[Dependency] = []
        self._current_node: Optional[str] = None
        self._node_dependencies: dict[str, list[Dependency]] = {}

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
        with self._lock:
            self._current_node = node_id
            if node_id not in self._node_dependencies:
                self._node_dependencies[node_id] = []

    def get_current_node(self) -> Optional[str]:
        """Get the current node context."""
        with self._lock:
            return self._current_node

    def clear_current_node(self) -> None:
        """Clear the current node context."""
        with self._lock:
            self._current_node = None

    def register_dependency(
        self,
        node_id: str,
        dependency_id: str,
        dependency_type: DependencyType,
        params: Optional[dict[str, Any]] = None
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

    def get_dependencies_for_node(self, node_id: str) -> list[Dependency]:
        """Get all dependencies for a specific node."""
        with self._lock:
            deps = self._node_dependencies.get(node_id, [])
            return deps.copy()

    def get_all_dependencies(self) -> list[Dependency]:
        """Get all registered dependencies."""
        with self._lock:
            return self._dependencies.copy()
