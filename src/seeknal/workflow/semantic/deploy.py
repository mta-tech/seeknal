"""
StarRocks materialized view deployment for semantic layer metrics.

Generates and executes CREATE MATERIALIZED VIEW DDL from metric definitions,
enabling BI-layer pre-aggregation of metric queries.
"""
from dataclasses import dataclass, field
from typing import Any, Optional

from seeknal.workflow.semantic.compiler import MetricCompiler
from seeknal.workflow.semantic.models import (
    Metric,
    MetricQuery,
)


@dataclass(slots=True)
class DeployResult:
    """Result of deploying a single metric as a StarRocks MV."""
    metric_name: str
    mv_name: str
    ddl: str
    success: bool = True
    error: Optional[str] = None


@dataclass(slots=True)
class DeployConfig:
    """Configuration for metric deployment."""
    refresh_interval: str = "1 DAY"
    properties: dict[str, str] = field(default_factory=dict)
    force_external_table_query_rewrite: bool = False
    drop_existing: bool = False


class MetricDeployer:
    """
    Deploys semantic layer metrics as StarRocks materialized views.

    Each metric becomes a pre-aggregated MV that can serve BI dashboards
    with low-latency queries.
    """

    def __init__(
        self,
        compiler: MetricCompiler,
        connection_config: dict[str, Any],
    ):
        self.compiler = compiler
        self.connection_config = connection_config

    def generate_mv_ddl(
        self,
        metric: Metric,
        config: Optional[DeployConfig] = None,
        dimensions: Optional[list[str]] = None,
    ) -> str:
        """
        Generate CREATE MATERIALIZED VIEW DDL for a metric.

        Args:
            metric: The metric to deploy.
            config: Deployment configuration.
            dimensions: Dimensions to include in the MV.

        Returns:
            SQL DDL string.
        """
        if config is None:
            config = DeployConfig()

        mv_name = f"mv_{metric.name}"

        # Compile the metric query to SQL
        query = MetricQuery(
            metrics=[metric.name],
            dimensions=dimensions or [],
        )
        inner_sql = self.compiler.compile(query)

        # Build DDL parts
        parts = []

        if config.drop_existing:
            parts.append(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name};")

        refresh = f"ASYNC EVERY(INTERVAL {config.refresh_interval})"

        props_items = dict(config.properties)
        if config.force_external_table_query_rewrite:
            props_items["force_external_table_query_rewrite"] = "true"

        props_str = ""
        if props_items:
            items = ", ".join(f'"{k}" = "{v}"' for k, v in props_items.items())
            props_str = f"\nPROPERTIES ({items})"

        parts.append(
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name}\n"
            f"REFRESH {refresh}{props_str}\n"
            f"AS\n{inner_sql}"
        )

        return "\n\n".join(parts)

    def deploy(
        self,
        metrics: list[Metric],
        config: Optional[DeployConfig] = None,
        dimensions: Optional[list[str]] = None,
        dry_run: bool = False,
    ) -> list[DeployResult]:
        """
        Deploy metrics as StarRocks materialized views.

        Args:
            metrics: Metrics to deploy.
            config: Deployment configuration.
            dimensions: Dimensions to include in each MV.
            dry_run: If True, generate DDL without executing.

        Returns:
            List of deployment results.
        """
        results: list[DeployResult] = []

        for metric in metrics:
            mv_name = f"mv_{metric.name}"
            ddl = ""
            try:
                ddl = self.generate_mv_ddl(metric, config, dimensions)

                if dry_run:
                    results.append(DeployResult(
                        metric_name=metric.name,
                        mv_name=mv_name,
                        ddl=ddl,
                        success=True,
                    ))
                    continue

                # Execute DDL on StarRocks
                self._execute_ddl(ddl)
                results.append(DeployResult(
                    metric_name=metric.name,
                    mv_name=mv_name,
                    ddl=ddl,
                    success=True,
                ))

            except Exception as e:
                results.append(DeployResult(
                    metric_name=metric.name,
                    mv_name=mv_name,
                    ddl=ddl,
                    success=False,
                    error=str(e),
                ))

        return results

    def _execute_ddl(self, ddl: str) -> None:
        """Execute DDL statements on StarRocks."""
        from seeknal.connections.starrocks import create_starrocks_connection

        conn = create_starrocks_connection(self.connection_config)
        try:
            cursor = conn.cursor()
            # Execute each statement separately (DROP and CREATE)
            for stmt in ddl.split(";"):
                stmt = stmt.strip()
                if stmt:
                    cursor.execute(stmt)
            cursor.close()
        finally:
            conn.close()
