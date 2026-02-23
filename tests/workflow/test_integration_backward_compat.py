"""Integration and backward compatibility tests for PostgreSQL features (B-10).

Tests cover end-to-end flows combining multiple components:
- YAML materializations: (plural) → DAG builder → dispatcher routing
- Python @materialize decorators → discoverer → DAG builder
- @materialize + materialization= combined into single list
- Backward compat: singular materialization: (no type) → Iceberg default
- Profile loader → connection resolution → PostgreSQL config
- Source with connection profile and pushdown query params
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest  # ty: ignore[unresolved-import]

from seeknal.pipeline.decorators import (  # ty: ignore[unresolved-import]
    materialize,
    source,
    transform,
    feature_group,
    get_registered_nodes,
    clear_registry,
)
from seeknal.pipeline.discoverer import PythonPipelineDiscoverer  # ty: ignore[unresolved-import]
from seeknal.workflow.dag import DAGBuilder  # ty: ignore[unresolved-import]
from seeknal.workflow.materialization.dispatcher import (  # ty: ignore[unresolved-import]
    DispatchResult,
    MaterializationDispatcher,
)
from seeknal.workflow.materialization.operations import WriteResult  # ty: ignore[unresolved-import]
from seeknal.workflow.materialization.profile_loader import (  # ty: ignore[unresolved-import]
    ProfileLoader,
    interpolate_env_vars,
    interpolate_env_vars_in_dict,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    """Clear decorator registry before/after each test."""
    clear_registry()
    yield
    clear_registry()


# =============================================================================
# Backward Compatibility: YAML singular materialization:
# =============================================================================


class TestYAMLBackwardCompat:
    """Singular materialization: YAML key still works for Iceberg."""

    def test_singular_materialization_without_type_becomes_iceberg(self, tmp_path):
        """Existing materialization: dict without type: gets type: iceberg."""
        seeknal_dir = tmp_path / "seeknal" / "transforms"
        seeknal_dir.mkdir(parents=True)
        (seeknal_dir / "node.yml").write_text("""\
kind: transform
name: legacy_node
transform: SELECT * FROM input_0
inputs:
  - ref: source.raw
materialization:
  enabled: true
  table: atlas.ns.legacy
  mode: append
""")

        sources_dir = tmp_path / "seeknal" / "sources"
        sources_dir.mkdir(parents=True)
        (sources_dir / "raw.yml").write_text("""\
kind: source
name: raw
source: csv
table: data.csv
""")

        builder = DAGBuilder(project_path=tmp_path)
        builder.build()

        node = builder.get_node("transform.legacy_node")
        assert node is not None
        mats = node.yaml_data["materializations"]
        assert isinstance(mats, list)
        assert len(mats) == 1
        assert mats[0]["type"] == "iceberg"
        assert mats[0]["enabled"] is True
        assert mats[0]["table"] == "atlas.ns.legacy"

    def test_singular_with_explicit_iceberg_type(self, tmp_path):
        """Singular materialization: with type: iceberg preserved."""
        seeknal_dir = tmp_path / "seeknal" / "transforms"
        seeknal_dir.mkdir(parents=True)
        (seeknal_dir / "node.yml").write_text("""\
kind: transform
name: explicit_ice
transform: SELECT * FROM input_0
inputs:
  - ref: source.raw
materialization:
  type: iceberg
  table: atlas.ns.table
""")

        sources_dir = tmp_path / "seeknal" / "sources"
        sources_dir.mkdir(parents=True)
        (sources_dir / "raw.yml").write_text("""\
kind: source
name: raw
source: csv
table: data.csv
""")

        builder = DAGBuilder(project_path=tmp_path)
        builder.build()

        node = builder.get_node("transform.explicit_ice")
        assert node is not None
        assert node.yaml_data["materializations"][0]["type"] == "iceberg"

    def test_no_materialization_key_is_ok(self, tmp_path):
        """Nodes without materialization config have no materializations key."""
        seeknal_dir = tmp_path / "seeknal" / "sources"
        seeknal_dir.mkdir(parents=True)
        (seeknal_dir / "plain.yml").write_text("""\
kind: source
name: plain
source: csv
table: data.csv
""")

        builder = DAGBuilder(project_path=tmp_path)
        builder.build()

        node = builder.get_node("source.plain")
        assert node is not None
        assert "materializations" not in node.yaml_data


# =============================================================================
# YAML plural materializations: → DAG → dispatcher
# =============================================================================


class TestYAMLPluralMaterializations:
    """Plural materializations: YAML key flows through DAG to dispatcher."""

    def test_plural_yaml_multi_target(self, tmp_path):
        """YAML with plural materializations: list parsed correctly."""
        seeknal_dir = tmp_path / "seeknal" / "transforms"
        seeknal_dir.mkdir(parents=True)
        (seeknal_dir / "multi.yml").write_text("""\
kind: transform
name: multi_target
transform: SELECT * FROM input_0
inputs:
  - ref: source.raw
materializations:
  - type: iceberg
    table: atlas.ns.multi
  - type: postgresql
    connection: my_pg
    table: public.multi
    mode: full
""")

        sources_dir = tmp_path / "seeknal" / "sources"
        sources_dir.mkdir(parents=True)
        (sources_dir / "raw.yml").write_text("""\
kind: source
name: raw
source: csv
table: data.csv
""")

        builder = DAGBuilder(project_path=tmp_path)
        builder.build()

        node = builder.get_node("transform.multi_target")
        mats = node.yaml_data["materializations"]
        assert len(mats) == 2
        assert mats[0]["type"] == "iceberg"
        assert mats[1]["type"] == "postgresql"
        assert mats[1]["connection"] == "my_pg"

    def test_dispatcher_routes_multi_target_correctly(self):
        """Dispatcher correctly routes Iceberg and PostgreSQL targets."""
        pg_wr = WriteResult(success=True, row_count=100, duration_seconds=0.1)
        ice_wr = WriteResult(success=True, row_count=100, duration_seconds=0.2)

        dispatcher = MaterializationDispatcher(profile_loader=MagicMock())

        targets = [
            {"type": "iceberg", "table": "atlas.ns.t"},
            {"type": "postgresql", "connection": "pg", "table": "public.t", "mode": "full"},
        ]

        with (
            patch.object(dispatcher, "_materialize_iceberg", return_value=ice_wr) as m_ice,
            patch.object(dispatcher, "_materialize_postgresql", return_value=pg_wr) as m_pg,
        ):
            result = dispatcher.dispatch(MagicMock(), "view", targets, node_id="transform.t")

        assert result.all_succeeded
        assert result.total == 2
        m_ice.assert_called_once()
        m_pg.assert_called_once()


# =============================================================================
# Python @materialize → discoverer → DAG builder
# =============================================================================


class TestPythonMaterializeIntegration:
    """@materialize decorators flow through discoverer to DAG builder."""

    def test_stacked_materialize_flows_to_dag(self, tmp_path):
        """Stacked @materialize on Python node produces materializations in DAG."""
        pipelines_dir = tmp_path / "seeknal" / "pipelines"
        pipelines_dir.mkdir(parents=True)

        py_content = '''\
from seeknal.pipeline.decorators import transform, materialize

@transform(name="enriched", inputs=["source.raw"])
@materialize(type="postgresql", connection="pg", table="public.enriched", mode="full")
@materialize(type="iceberg", table="atlas.ns.enriched")
def enriched(ctx):
    return ctx.ref("source.raw")
'''
        (pipelines_dir / "pipeline.py").write_text(py_content)

        sources_dir = tmp_path / "seeknal" / "sources"
        sources_dir.mkdir(parents=True)
        (sources_dir / "raw.yml").write_text("""\
kind: source
name: raw
source: csv
table: data.csv
""")

        builder = DAGBuilder(project_path=tmp_path)
        builder.build()

        node = builder.get_node("transform.enriched")
        assert node is not None
        mats = node.yaml_data.get("materializations", [])
        assert len(mats) == 2
        # Bottom decorator first (iceberg), then postgresql
        types = [m["type"] for m in mats]
        assert "iceberg" in types
        assert "postgresql" in types

    def test_materialize_combined_with_materialization_param(self, tmp_path):
        """@materialize + materialization= param combined into single list."""
        pipelines_dir = tmp_path / "seeknal" / "pipelines"
        pipelines_dir.mkdir(parents=True)

        py_content = '''\
from seeknal.pipeline.decorators import transform, materialize

@transform(
    name="combined",
    inputs=["source.raw"],
    materialization={"enabled": True, "table": "atlas.ns.combined"},
)
@materialize(type="postgresql", connection="pg", table="public.combined", mode="full")
def combined(ctx):
    return ctx.ref("source.raw")
'''
        (pipelines_dir / "pipeline.py").write_text(py_content)

        sources_dir = tmp_path / "seeknal" / "sources"
        sources_dir.mkdir(parents=True)
        (sources_dir / "raw.yml").write_text("""\
kind: source
name: raw
source: csv
table: data.csv
""")

        builder = DAGBuilder(project_path=tmp_path)
        builder.build()

        node = builder.get_node("transform.combined")
        assert node is not None
        mats = node.yaml_data.get("materializations", [])
        # Should have both: the materialization= (normalized to iceberg) + @materialize pg
        assert len(mats) >= 2
        types = [m["type"] for m in mats]
        assert "iceberg" in types
        assert "postgresql" in types


# =============================================================================
# Profile loader → connection resolution end-to-end
# =============================================================================


class TestProfileConnectionResolution:
    """Profile loader resolves connection profiles with env var interpolation."""

    def test_profile_to_connection_config(self, tmp_path, monkeypatch):
        """Full flow: profiles.yml → load_connection_profile → env var resolution."""
        monkeypatch.setenv("PG_PASSWORD", "secret123")
        profile_file = tmp_path / "profiles.yml"
        profile_file.write_text("""\
connections:
  my_pg:
    type: postgresql
    host: ${PG_HOST:localhost}
    port: 5432
    user: ${PG_USER:admin}
    password: ${PG_PASSWORD}
    database: analytics
""")

        loader = ProfileLoader(profile_path=profile_file)
        profile = loader.load_connection_profile("my_pg")

        assert profile["host"] == "localhost"  # default used
        assert profile["user"] == "admin"  # default used
        assert profile["password"] == "secret123"  # env var resolved
        assert profile["database"] == "analytics"
        assert profile["port"] == 5432

    def test_clear_credentials_after_use(self, tmp_path, monkeypatch):
        """Credentials can be cleared after connection is established."""
        monkeypatch.setenv("PG_PASS", "secret")
        profile_file = tmp_path / "profiles.yml"
        profile_file.write_text("""\
connections:
  dev:
    type: postgresql
    host: localhost
    password: ${PG_PASS}
    token: bearer_xyz
""")

        loader = ProfileLoader(profile_path=profile_file)
        profile = loader.load_connection_profile("dev")

        assert profile["password"] == "secret"
        assert profile["token"] == "bearer_xyz"

        ProfileLoader.clear_connection_credentials(profile)
        assert "password" not in profile
        assert "token" not in profile
        assert profile["host"] == "localhost"


# =============================================================================
# Source with connection and query params
# =============================================================================


class TestSourceConnectionAndQuery:
    """Source decorator with connection and query kwargs."""

    def test_source_query_connection_in_params(self):
        """@source(query=..., connection=...) stored in params."""

        @source(
            name="pg_orders",
            source="postgres",
            connection="my_pg",
            query="SELECT * FROM orders WHERE status = 'active'",
        )
        def pg_orders():
            pass

        nodes = get_registered_nodes()
        meta = nodes["source.pg_orders"]
        assert meta["params"]["query"] == "SELECT * FROM orders WHERE status = 'active'"
        assert meta["params"]["connection"] == "my_pg"
        assert meta["source"] == "postgres"

    def test_source_with_connection_in_dag(self, tmp_path):
        """Python source with connection param flows to DAG node."""
        pipelines_dir = tmp_path / "seeknal" / "pipelines"
        pipelines_dir.mkdir(parents=True)

        py_content = '''\
from seeknal.pipeline.decorators import source

@source(
    name="pg_data",
    source="postgres",
    connection="local_pg",
    query="SELECT id, name FROM users",
)
def pg_data():
    pass
'''
        (pipelines_dir / "pipeline.py").write_text(py_content)

        builder = DAGBuilder(project_path=tmp_path)
        builder.build()

        node = builder.get_node("source.pg_data")
        assert node is not None
        assert node.yaml_data["params"]["connection"] == "local_pg"
        assert node.yaml_data["params"]["query"] == "SELECT id, name FROM users"


# =============================================================================
# Dispatcher + Profile Loader combined flow
# =============================================================================


class TestDispatcherProfileIntegration:
    """Dispatcher uses profile loader to resolve connection for PostgreSQL targets."""

    def test_dispatcher_resolves_profile_for_postgresql(self, tmp_path, monkeypatch):
        """Dispatcher calls profile_loader.load_connection_profile for pg targets."""
        monkeypatch.setenv("PG_PASS", "s3cret")
        profile_file = tmp_path / "profiles.yml"
        profile_file.write_text("""\
connections:
  test_pg:
    type: postgresql
    host: localhost
    port: 5432
    user: tester
    password: ${PG_PASS}
    database: test_db
""")

        loader = ProfileLoader(profile_path=profile_file)
        dispatcher = MaterializationDispatcher(profile_loader=loader)

        # Mock the PostgreSQL helper to avoid real DB calls
        mock_helper_cls = MagicMock()
        mock_helper_cls.return_value.materialize.return_value = WriteResult(
            success=True, row_count=50, duration_seconds=0.1
        )

        with (
            patch(
                "seeknal.workflow.materialization.postgresql.PostgresMaterializationHelper",
                mock_helper_cls,
            ),
            patch(
                "seeknal.connections.postgresql.parse_postgresql_config",
                return_value=MagicMock(),
            ) as mock_parse,
        ):
            result = dispatcher.dispatch(
                MagicMock(),
                "my_view",
                [{"type": "postgresql", "connection": "test_pg", "table": "public.out", "mode": "full"}],
            )

        assert result.succeeded == 1
        # Verify profile was loaded and parsed
        mock_parse.assert_called_once()
        call_args = mock_parse.call_args[0][0]
        assert call_args["host"] == "localhost"
        assert call_args["password"] == "s3cret"


# =============================================================================
# Full end-to-end: YAML → DAG → dispatcher
# =============================================================================


class TestFullEndToEnd:
    """Full pipeline: YAML definition → DAG build → dispatcher routing."""

    def test_yaml_multi_target_to_dispatcher(self, tmp_path):
        """Multi-target YAML materializations flow to dispatcher correctly."""
        seeknal_dir = tmp_path / "seeknal" / "transforms"
        seeknal_dir.mkdir(parents=True)
        (seeknal_dir / "orders.yml").write_text("""\
kind: transform
name: orders
transform: SELECT * FROM input_0
inputs:
  - ref: source.raw
materializations:
  - type: iceberg
    table: atlas.ns.orders
    mode: append
  - type: postgresql
    connection: my_pg
    table: public.orders
    mode: full
""")

        sources_dir = tmp_path / "seeknal" / "sources"
        sources_dir.mkdir(parents=True)
        (sources_dir / "raw.yml").write_text("""\
kind: source
name: raw
source: csv
table: data.csv
""")

        builder = DAGBuilder(project_path=tmp_path)
        builder.build()

        node = builder.get_node("transform.orders")
        targets = node.yaml_data["materializations"]

        # Feed directly to dispatcher
        dispatcher = MaterializationDispatcher(profile_loader=MagicMock())
        ice_wr = WriteResult(success=True, row_count=100, duration_seconds=0.1)
        pg_wr = WriteResult(success=True, row_count=100, duration_seconds=0.2)

        with (
            patch.object(dispatcher, "_materialize_iceberg", return_value=ice_wr),
            patch.object(dispatcher, "_materialize_postgresql", return_value=pg_wr),
        ):
            result = dispatcher.dispatch(
                MagicMock(), "orders_view", targets, node_id="transform.orders"
            )

        assert result.all_succeeded
        assert result.total == 2

    def test_existing_tests_still_pass_marker(self):
        """Marker test: confirms this file loads without import errors.

        The actual regression check is running ALL existing tests before
        adding B-10. This test simply proves the integration test module
        itself loads successfully with all dependencies.
        """
        assert True
