"""Seeknal Serving: SQL routing core."""

from __future__ import annotations

from seeknal.serving.adapters import (
    CubeAdapter,
    CubeSecurityConfig,
    DBAPIAdapter,
    DuckDBAdapter,
    EngineAdapter,
    EngineCancelled,
    EngineConfigError,
    EngineError,
    EngineTimeout,
    QueryResult,
    WorkloadRoutedAdapter,
    build_adapter,
    build_workload_adapters,
    make_cube_context_token,
    normalize_column_type,
)
from seeknal.serving.discovery import (
    DiscoveryConfig,
    PlacementRefresher,
    discover,
)
from seeknal.serving.enforce import EnforcementError, Grant, enforce
from seeknal.serving.features import QueryFeatures, extract_features
from seeknal.serving.grant_token import (
    GrantTokenError,
    sign_cancel_token,
    sign_grant_token,
    verify_cancel_token,
    verify_grant_token,
)
from seeknal.serving.http import create_app
from seeknal.serving.placement import (
    CubeEntry,
    Engine,
    Placement,
    PlacementError,
    Policy,
    TableEntry,
    TableServe,
)
from seeknal.serving.proving import (
    DiffCase,
    DiffResult,
    LoadReport,
    RuleStats,
    default_cases,
    default_normalise,
    run_differential,
    run_load,
    setup_synthetic_claims_table,
)
from seeknal.serving.router import Router, RoutingDecision, RoutingError
from seeknal.serving.service import PreparedQuery, ServingPipeline

__all__ = [
    "CubeAdapter",
    "CubeEntry",
    "CubeSecurityConfig",
    "DBAPIAdapter",
    "DiffCase",
    "DiffResult",
    "DiscoveryConfig",
    "DuckDBAdapter",
    "EnforcementError",
    "Engine",
    "EngineAdapter",
    "EngineCancelled",
    "EngineConfigError",
    "EngineError",
    "EngineTimeout",
    "Grant",
    "GrantTokenError",
    "LoadReport",
    "Placement",
    "PlacementError",
    "PlacementRefresher",
    "Policy",
    "PreparedQuery",
    "QueryFeatures",
    "QueryResult",
    "Router",
    "RoutingDecision",
    "RoutingError",
    "RuleStats",
    "ServingPipeline",
    "TableEntry",
    "TableServe",
    "WorkloadRoutedAdapter",
    "build_adapter",
    "build_workload_adapters",
    "create_app",
    "default_cases",
    "default_normalise",
    "discover",
    "enforce",
    "extract_features",
    "make_cube_context_token",
    "normalize_column_type",
    "run_differential",
    "run_load",
    "setup_synthetic_claims_table",
    "sign_cancel_token",
    "sign_grant_token",
    "verify_cancel_token",
    "verify_grant_token",
]
