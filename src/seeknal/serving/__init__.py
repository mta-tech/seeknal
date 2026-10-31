"""Seeknal Serving: SQL routing core."""

from __future__ import annotations

from seeknal.serving.adapters import (
    DBAPIAdapter,
    DuckDBAdapter,
    EngineAdapter,
    EngineError,
    EngineTimeout,
    QueryResult,
    build_adapter,
)
from seeknal.serving.enforce import EnforcementError, Grant, enforce
from seeknal.serving.features import QueryFeatures, extract_features
from seeknal.serving.grant_token import (
    GrantTokenError,
    sign_grant_token,
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
from seeknal.serving.router import Router, RoutingDecision, RoutingError
from seeknal.serving.service import PreparedQuery, ServingPipeline

__all__ = [
    "CubeEntry",
    "DBAPIAdapter",
    "DuckDBAdapter",
    "EnforcementError",
    "Engine",
    "EngineAdapter",
    "EngineError",
    "EngineTimeout",
    "Grant",
    "GrantTokenError",
    "Placement",
    "PlacementError",
    "Policy",
    "PreparedQuery",
    "QueryFeatures",
    "QueryResult",
    "Router",
    "RoutingDecision",
    "RoutingError",
    "ServingPipeline",
    "TableEntry",
    "TableServe",
    "build_adapter",
    "create_app",
    "enforce",
    "extract_features",
    "sign_grant_token",
    "verify_grant_token",
]
