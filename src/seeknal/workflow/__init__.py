"""
Workflow module for Seeknal 2.0 dbt-inspired YAML development.

This module provides three-phase workflow:
1. draft - Generate YAML templates from Jinja2
2. dry-run - Validate and preview execution
3. apply - Move file to production and update manifest

Additionally, this module provides DAG building functionality for
dependency tracking and topological sorting of YAML-defined nodes.
"""

from seeknal.workflow.dag import (
    DAGBuilder,
    DAGNode,
    DAGEdge,
    NodeType,
    CycleDetectedError,
    MissingDependencyError,
    build_dag,
)

__all__ = [
    # DAG building
    "DAGBuilder",
    "DAGNode",
    "DAGEdge",
    "NodeType",
    "CycleDetectedError",
    "MissingDependencyError",
    "build_dag",
]

