"""Multi-target materialization dispatcher.

Routes materialization targets to the correct backend (Iceberg or PostgreSQL)
based on the ``type:`` field in each target config. Supports best-effort
execution where individual target failures do not block other targets.

Usage::

    dispatcher = MaterializationDispatcher()
    result = dispatcher.dispatch(
        con=duckdb_con,
        view_name="my_view",
        targets=[
            {"type": "postgresql", "connection": "pg1", "table": "public.orders", "mode": "full"},
            {"type": "iceberg", "table": "warehouse.ns.orders", "mode": "append"},
        ],
        node_id="transform.orders",
    )
    assert result.all_succeeded
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from seeknal.workflow.materialization.operations import WriteResult  # ty: ignore[unresolved-import]

logger = logging.getLogger(__name__)


@dataclass
class DispatchResult:
    """Result of dispatching materialization to all targets.

    Attributes:
        total: Number of targets attempted.
        succeeded: Number of targets that succeeded.
        failed: Number of targets that failed.
        results: Per-target result dicts with keys ``target``, ``type``,
            ``success``, and either ``write_result`` or ``error``.
    """

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    results: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def all_succeeded(self) -> bool:
        """Return True when every target succeeded (and at least one was attempted)."""
        return self.failed == 0 and self.total > 0


class MaterializationDispatcher:
    """Route materialization targets to the correct backend.

    Args:
        profile_loader: Optional ``ProfileLoader`` instance used to resolve
            connection profiles and Iceberg catalog config.  When *None*, a
            default ``ProfileLoader`` is created lazily on first use.
    """

    def __init__(self, profile_loader: Any = None) -> None:
        self._profile_loader = profile_loader

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def dispatch(
        self,
        con: Any,
        view_name: str,
        targets: List[Dict[str, Any]],
        node_id: str = "",
    ) -> DispatchResult:
        """Materialize to all *targets*. Best-effort: continues on failure.

        Args:
            con: DuckDB connection.
            view_name: Name of the DuckDB view/table containing data.
            targets: List of materialization target configs.  Each dict must
                contain at least a ``type`` key (``"iceberg"`` or
                ``"postgresql"``).  If omitted, defaults to ``"iceberg"``.
            node_id: Node identifier used in log messages.

        Returns:
            ``DispatchResult`` with per-target outcomes.
        """
        result = DispatchResult(total=len(targets))

        for i, target in enumerate(targets):
            target_type = target.get("type", "iceberg")
            target_label = (
                f"{node_id}[{i}]:{target_type}" if node_id else f"[{i}]:{target_type}"
            )

            try:
                logger.info("Materializing %s", target_label)

                if target_type == "postgresql":
                    write_result = self._materialize_postgresql(con, view_name, target)
                elif target_type == "iceberg":
                    write_result = self._materialize_iceberg(con, view_name, target)
                else:
                    raise ValueError(f"Unknown materialization type: {target_type}")

                result.succeeded += 1
                result.results.append({
                    "target": target_label,
                    "type": target_type,
                    "success": True,
                    "write_result": write_result,
                })
                logger.info(
                    "Materialized %s: %d rows", target_label, write_result.row_count
                )

            except Exception as exc:
                result.failed += 1
                result.results.append({
                    "target": target_label,
                    "type": target_type,
                    "success": False,
                    "error": str(exc),
                })
                logger.error("Failed to materialize %s: %s", target_label, exc)

        return result

    # ------------------------------------------------------------------
    # Backend-specific routing
    # ------------------------------------------------------------------

    def _materialize_postgresql(
        self, con: Any, view_name: str, target_config: Dict[str, Any]
    ) -> WriteResult:
        """Route to PostgreSQL materialization helper."""
        # Lazy imports to avoid circular dependencies
        from seeknal.connections.postgresql import parse_postgresql_config  # ty: ignore[unresolved-import]
        from seeknal.workflow.materialization.pg_config import PostgresMaterializationConfig  # ty: ignore[unresolved-import]
        from seeknal.workflow.materialization.postgresql import PostgresMaterializationHelper  # ty: ignore[unresolved-import]

        # Parse and validate materialization config
        mat_config = PostgresMaterializationConfig.from_dict(target_config)
        mat_config.validate()

        # Load connection profile
        connection_name = mat_config.connection
        if self._profile_loader is not None:
            conn_dict = self._profile_loader.load_connection_profile(connection_name)
        else:
            from seeknal.workflow.materialization.profile_loader import ProfileLoader  # ty: ignore[unresolved-import]

            conn_dict = ProfileLoader().load_connection_profile(connection_name)

        pg_config = parse_postgresql_config(conn_dict)

        # Create helper and materialize
        helper = PostgresMaterializationHelper(pg_config, mat_config)
        return helper.materialize(con, view_name)

    def _materialize_iceberg(
        self, con: Any, view_name: str, target_config: Dict[str, Any]
    ) -> WriteResult:
        """Route to Iceberg materialization via operations module."""
        import os

        from seeknal.workflow.materialization.operations import (  # ty: ignore[unresolved-import]
            DuckDBIcebergExtension,
            write_to_iceberg,
        )
        from seeknal.workflow.materialization.profile_loader import ProfileLoader  # ty: ignore[unresolved-import]

        # Load Iceberg profile config
        loader = self._profile_loader if self._profile_loader is not None else ProfileLoader()
        profile_config = loader.load_profile()

        # Setup DuckDB extensions (httpfs + iceberg)
        DuckDBIcebergExtension.load_extension(con)

        # Configure S3/MinIO credentials from env
        DuckDBIcebergExtension.configure_s3(con)

        # Get OAuth2 token from Keycloak
        catalog = profile_config.catalog.interpolate_env_vars()
        token = catalog.bearer_token
        if not token:
            token = DuckDBIcebergExtension.get_oauth2_token()

        # Resolve catalog URI and warehouse with fallback chain:
        # 1. materialization.catalog section (from profile_config above)
        # 2. target_config overrides (per-materialization YAML)
        # 3. source_defaults.iceberg section
        # 4. environment variables (LAKEKEEPER_URI, LAKEKEEPER_WAREHOUSE)
        uri = catalog.uri
        warehouse_path = catalog.warehouse

        # Target-level overrides
        if not uri:
            uri = target_config.get("catalog_uri", "")
        if not warehouse_path:
            warehouse_path = target_config.get("warehouse", "")

        # Fall back to source_defaults.iceberg
        if not uri or not warehouse_path:
            iceberg_defaults = loader.load_source_defaults("iceberg")
            if not uri:
                uri = iceberg_defaults.get("catalog_uri", "")
            if not warehouse_path:
                warehouse_path = iceberg_defaults.get("warehouse", "")

        # Fall back to environment variables
        if not uri:
            uri = os.environ.get("LAKEKEEPER_URI", "")
        if not warehouse_path:
            warehouse_path = os.environ.get("LAKEKEEPER_WAREHOUSE", "")

        # Attach REST catalog using DuckDB ATTACH syntax
        catalog_name = "iceberg_catalog"
        DuckDBIcebergExtension.attach_rest_catalog(
            con=con,
            catalog_name=catalog_name,
            uri=uri,
            warehouse_path=warehouse_path,
            bearer_token=token,
        )

        table_name = target_config.get("table", "")
        mode = target_config.get("mode", "append")

        return write_to_iceberg(
            con=con,
            catalog_name=catalog_name,
            table_name=table_name,
            view_name=view_name,
            mode=mode,
        )
