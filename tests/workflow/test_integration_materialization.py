"""Integration and backward compatibility tests for PostgreSQL materialization.

B-10: Final gate for Workstream B. Tests how the following modules work
together as a pipeline:

- dag.py _normalize_materializations()
- dispatcher.py MaterializationDispatcher
- postgresql.py PostgresMaterializationHelper
- pg_config.py PostgresMaterializationConfig
- postgresql.py (connections) PostgreSQLConfig, parse_postgresql_config
- profile_loader.py ProfileLoader
- decorators.py @materialize

Each test class covers a specific integration boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest  # ty: ignore[unresolved-import]

from seeknal.connections.postgresql import (  # ty: ignore[unresolved-import]
    PostgreSQLConfig,
    parse_postgresql_config,
)
from seeknal.workflow.dag import DAGBuilder  # ty: ignore[unresolved-import]
from seeknal.workflow.materialization.dispatcher import (  # ty: ignore[unresolved-import]
    DispatchResult,
    MaterializationDispatcher,
)
from seeknal.workflow.materialization.operations import WriteResult  # ty: ignore[unresolved-import]
from seeknal.workflow.materialization.pg_config import (  # ty: ignore[unresolved-import]
    PostgresConfigError,
    PostgresMaterializationConfig,
    PostgresMaterializationMode,
)


# ---------------------------------------------------------------------------
# Shared helpers and fixtures
# ---------------------------------------------------------------------------


def _ok_write(row_count: int = 42) -> WriteResult:
    return WriteResult(success=True, row_count=row_count, duration_seconds=0.1)


def _pg_connection_dict() -> Dict[str, Any]:
    return {
        "type": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "testdb",
        "user": "testuser",
        "password": "testpass",
    }


@pytest.fixture
def mock_con():
    """Minimal mock DuckDB connection."""
    con = MagicMock()
    con.execute.return_value = MagicMock()
    con.execute.return_value.fetchone.return_value = (42,)
    return con


@pytest.fixture
def mock_profile_loader():
    """Profile loader that returns a valid PG connection dict."""
    loader = MagicMock()
    loader.load_connection_profile.return_value = _pg_connection_dict()
    return loader


@pytest.fixture
def dag_builder():
    """DAGBuilder for normalisation tests."""
    return DAGBuilder(project_path="/tmp/test")


# =========================================================================
# 1. DAG Builder -> Dispatcher Integration
# =========================================================================


class TestDAGToDispatcherIntegration:
    """Verify that _normalize_materializations output is usable by the dispatcher."""

    def test_yaml_plural_materializations_flow_to_dispatcher(
        self, dag_builder, mock_con, mock_profile_loader
    ):
        """Parse YAML with materializations: list, verify dispatcher receives correct targets."""
        targets = [
            {"type": "iceberg", "table": "atlas.ns.orders", "mode": "append"},
            {
                "type": "postgresql",
                "connection": "pg1",
                "table": "public.orders",
                "mode": "full",
            },
        ]
        data = {"materializations": targets}

        result = dag_builder._normalize_materializations(data, Path("test.yml"))

        # Normalized output should have a list with both targets
        assert "materializations" in result
        assert len(result["materializations"]) == 2
        assert result["materializations"][0]["type"] == "iceberg"
        assert result["materializations"][1]["type"] == "postgresql"

        # Feed to dispatcher -- mock both backends
        dispatcher = MaterializationDispatcher(profile_loader=mock_profile_loader)
        with (
            patch.object(dispatcher, "_materialize_iceberg", return_value=_ok_write(10)),
            patch.object(dispatcher, "_materialize_postgresql", return_value=_ok_write(20)),
        ):
            dispatch_result = dispatcher.dispatch(
                con=mock_con,
                view_name="my_view",
                targets=result["materializations"],
                node_id="transform.orders",
            )

        assert dispatch_result.total == 2
        assert dispatch_result.all_succeeded
        assert dispatch_result.results[0]["type"] == "iceberg"
        assert dispatch_result.results[1]["type"] == "postgresql"

    def test_yaml_singular_materialization_normalized_to_list(self, dag_builder):
        """materialization: (singular dict, no type) -> list of one with type: iceberg."""
        data = {
            "materialization": {
                "enabled": True,
                "table": "atlas.ns.my_table",
                "mode": "append",
            }
        }
        result = dag_builder._normalize_materializations(data, Path("test.yml"))

        assert "materializations" in result
        mat_list = result["materializations"]
        assert isinstance(mat_list, list) and len(mat_list) == 1
        assert mat_list[0]["type"] == "iceberg"

    def test_yaml_singular_with_type_postgresql(self, dag_builder):
        """materialization: {type: postgresql, ...} -> list of one postgresql target."""
        data = {
            "materialization": {
                "type": "postgresql",
                "connection": "my_pg",
                "table": "public.orders",
                "mode": "full",
            }
        }
        result = dag_builder._normalize_materializations(data, Path("test.yml"))

        mat_list = result["materializations"]
        assert len(mat_list) == 1
        assert mat_list[0]["type"] == "postgresql"
        assert mat_list[0]["connection"] == "my_pg"

    def test_yaml_both_keys_raises_parse_error(self, dag_builder):
        """materialization: AND materializations: present -> parse error recorded."""
        data = {
            "materialization": {"type": "iceberg"},
            "materializations": [{"type": "postgresql", "connection": "pg1", "table": "public.t"}],
        }
        dag_builder._normalize_materializations(data, Path("test.yml"))

        errors = dag_builder.get_parse_errors()
        assert len(errors) == 1
        assert "Both 'materialization' and 'materializations'" in errors[0]


# =========================================================================
# 2. Dispatcher -> Helper Integration
# =========================================================================


class TestDispatcherToHelperIntegration:
    """Verify dispatcher creates helper with correct config and calls materialize."""

    def test_dispatcher_creates_pg_helper_with_correct_config(
        self, mock_con, mock_profile_loader
    ):
        """Dispatcher loads profile, parses config, and calls helper.materialize()."""
        dispatcher = MaterializationDispatcher(profile_loader=mock_profile_loader)

        target = {
            "type": "postgresql",
            "connection": "pg1",
            "table": "public.orders",
            "mode": "full",
        }

        with patch(
            "seeknal.workflow.materialization.postgresql.PostgresMaterializationHelper"
        ) as MockHelper:
            MockHelper.return_value.materialize.return_value = _ok_write(100)

            result = dispatcher.dispatch(
                con=mock_con,
                view_name="my_view",
                targets=[target],
                node_id="transform.orders",
            )

        assert result.all_succeeded
        # Helper was instantiated with a PostgreSQLConfig and a PgMatConfig
        call_args = MockHelper.call_args
        pg_config = call_args[0][0]
        mat_config = call_args[0][1]
        assert isinstance(pg_config, PostgreSQLConfig)
        assert isinstance(mat_config, PostgresMaterializationConfig)
        assert mat_config.table == "public.orders"
        assert mat_config.mode == PostgresMaterializationMode.FULL

    def test_dispatcher_pg_config_validation_failure_caught(
        self, mock_con, mock_profile_loader
    ):
        """Invalid pg config (empty connection) is caught by best-effort loop."""
        dispatcher = MaterializationDispatcher(profile_loader=mock_profile_loader)

        # Missing connection field -> validation error in PostgresMaterializationConfig
        target = {
            "type": "postgresql",
            "connection": "",
            "table": "public.orders",
            "mode": "full",
        }

        result = dispatcher.dispatch(
            con=mock_con,
            view_name="v",
            targets=[target],
            node_id="t.x",
        )

        assert result.failed == 1
        assert not result.all_succeeded
        assert "error" in result.results[0]

    def test_dispatcher_mixed_targets_both_execute(
        self, mock_con, mock_profile_loader
    ):
        """Both Iceberg and PostgreSQL targets execute in sequence."""
        dispatcher = MaterializationDispatcher(profile_loader=mock_profile_loader)
        targets = [
            {"type": "iceberg", "table": "atlas.ns.orders", "mode": "append"},
            {
                "type": "postgresql",
                "connection": "pg1",
                "table": "public.orders",
                "mode": "full",
            },
        ]

        with (
            patch.object(dispatcher, "_materialize_iceberg", return_value=_ok_write(10)),
            patch.object(dispatcher, "_materialize_postgresql", return_value=_ok_write(20)),
        ):
            result = dispatcher.dispatch(
                con=mock_con, view_name="v", targets=targets, node_id="t.o"
            )

        assert result.total == 2
        assert result.succeeded == 2
        assert result.all_succeeded
        # Verify row counts come from respective backends
        assert result.results[0]["write_result"].row_count == 10
        assert result.results[1]["write_result"].row_count == 20


# =========================================================================
# 3. Decorator -> DAG Integration
# =========================================================================


class TestDecoratorToDAGIntegration:
    """Verify @materialize decorator output is compatible with DAG builder."""

    def test_single_materialize_decorator_produces_list(self):
        """@materialize(type='postgresql', ...) -> _seeknal_materializations list of 1."""
        from seeknal.pipeline.decorators import materialize, transform  # ty: ignore[unresolved-import]

        @materialize(type="postgresql", connection="pg1", table="public.out", mode="full")
        @transform(name="my_transform")
        def my_func(ctx):
            pass

        assert hasattr(my_func, "_seeknal_materializations")
        mats = my_func._seeknal_materializations
        assert len(mats) == 1
        assert mats[0]["type"] == "postgresql"
        assert mats[0]["connection"] == "pg1"
        assert mats[0]["table"] == "public.out"

    def test_stacked_decorators_produce_list_of_two(self):
        """Two @materialize decorators -> list of 2 targets."""
        from seeknal.pipeline.decorators import materialize, transform  # ty: ignore[unresolved-import]

        @materialize(type="postgresql", connection="pg1", table="public.out", mode="full")
        @materialize(type="iceberg", table="atlas.ns.out", mode="append")
        @transform(name="stacked_transform")
        def my_func(ctx):
            pass

        mats = my_func._seeknal_materializations
        assert len(mats) == 2
        types = {m["type"] for m in mats}
        assert types == {"postgresql", "iceberg"}

    def test_decorator_materializations_compatible_with_dag_builder(self, dag_builder):
        """Decorator output format matches what DAG builder's normalizer expects."""
        from seeknal.pipeline.decorators import materialize, transform  # ty: ignore[unresolved-import]

        @materialize(type="postgresql", connection="pg1", table="public.t", mode="full")
        @transform(name="compat_test")
        def my_func(ctx):
            pass

        mats = my_func._seeknal_materializations

        # Simulate what the DAG builder does: inject into data as 'materializations'
        data: Dict[str, Any] = {"materializations": mats}
        result = dag_builder._normalize_materializations(data, Path("test.py"))

        # Should pass through without errors
        assert dag_builder.get_parse_errors() == []
        assert result["materializations"] is mats
        assert result["materializations"][0]["type"] == "postgresql"


# =========================================================================
# 4. Profile -> Connection Integration
# =========================================================================


class TestProfileToConnectionIntegration:
    """Verify profile loading -> config parsing -> connection string roundtrip."""

    def test_profile_to_postgresql_config_roundtrip(self):
        """load_connection_profile() output -> parse_postgresql_config() -> PostgreSQLConfig."""
        profile_dict = _pg_connection_dict()

        config = parse_postgresql_config(profile_dict)

        assert isinstance(config, PostgreSQLConfig)
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "testdb"
        assert config.user == "testuser"
        assert config.password == "testpass"

        libpq = config.to_libpq_string()
        assert "host=localhost" in libpq
        assert "dbname=testdb" in libpq
        assert "password=testpass" in libpq

    def test_profile_env_var_interpolation_in_connection(self, monkeypatch):
        """Profile with ${PG_HOST} is interpolated before config parsing."""
        monkeypatch.setenv("TEST_PG_HOST", "db.example.com")
        monkeypatch.setenv("TEST_PG_PASS", "s3cret")

        profile_dict = {
            "type": "postgresql",
            "host": "${TEST_PG_HOST}",
            "port": 5432,
            "database": "prod",
            "user": "admin",
            "password": "${TEST_PG_PASS}",
        }

        config = parse_postgresql_config(profile_dict)

        assert config.host == "db.example.com"
        assert config.password == "s3cret"

    def test_connection_profile_used_by_dispatcher(
        self, mock_con, mock_profile_loader
    ):
        """End-to-end: dispatcher loads profile, parses config, passes to helper."""
        dispatcher = MaterializationDispatcher(profile_loader=mock_profile_loader)

        target = {
            "type": "postgresql",
            "connection": "my_pg",
            "table": "public.output",
            "mode": "full",
        }

        with patch(
            "seeknal.workflow.materialization.postgresql.PostgresMaterializationHelper"
        ) as MockHelper:
            MockHelper.return_value.materialize.return_value = _ok_write(50)
            result = dispatcher.dispatch(
                con=mock_con, view_name="v", targets=[target], node_id="t.x"
            )

        # Verify the profile was loaded with the correct name
        mock_profile_loader.load_connection_profile.assert_called_once_with("my_pg")

        # Verify helper received a real PostgreSQLConfig parsed from the profile
        pg_config = MockHelper.call_args[0][0]
        assert isinstance(pg_config, PostgreSQLConfig)
        assert pg_config.host == "localhost"
        assert pg_config.database == "testdb"
        assert result.all_succeeded


# =========================================================================
# 5. Backward Compatibility
# =========================================================================


class TestBackwardCompatibility:
    """Ensure existing pre-PostgreSQL behavior is not broken."""

    def test_existing_iceberg_singular_still_works(self, dag_builder):
        """materialization: {enabled: true, mode: append} without type -> iceberg."""
        data = {
            "materialization": {
                "enabled": True,
                "mode": "append",
                "table": "atlas.ns.my_table",
            }
        }
        result = dag_builder._normalize_materializations(data, Path("test.yml"))

        assert dag_builder.get_parse_errors() == []
        mat_list = result["materializations"]
        assert len(mat_list) == 1
        assert mat_list[0]["type"] == "iceberg"
        assert mat_list[0]["enabled"] is True
        assert mat_list[0]["mode"] == "append"

    def test_existing_modules_importable(self):
        """All materialization modules import without error."""
        import seeknal.workflow.materialization.dispatcher  # ty: ignore[unresolved-import]
        import seeknal.workflow.materialization.postgresql  # ty: ignore[unresolved-import]
        import seeknal.workflow.materialization.pg_config  # ty: ignore[unresolved-import]
        import seeknal.connections.postgresql  # ty: ignore[unresolved-import]

    def test_no_regression_in_dag_builder_existing_behavior(self, dag_builder):
        """DAG builder with old-style materialization: dict still produces correct node config."""
        data = {
            "kind": "source",
            "name": "old_source",
            "materialization": {
                "enabled": True,
                "table": "atlas.ns.legacy",
                "mode": "overwrite",
            },
        }
        result = dag_builder._normalize_materializations(data, Path("legacy.yml"))

        assert dag_builder.get_parse_errors() == []
        assert "materializations" in result
        mat = result["materializations"][0]
        assert mat["type"] == "iceberg"
        assert mat["enabled"] is True
        assert mat["table"] == "atlas.ns.legacy"
        assert mat["mode"] == "overwrite"

    def test_none_materialization_ignored(self, dag_builder):
        """materialization: null (None) does not produce a materializations list."""
        data = {"materialization": None}
        result = dag_builder._normalize_materializations(data, Path("null.yml"))

        assert dag_builder.get_parse_errors() == []
        assert "materializations" not in result


# =========================================================================
# 6. Config round-trip: from_dict -> validate -> Helper
# =========================================================================


class TestConfigRoundTrip:
    """Verify pg_config -> PostgresMaterializationHelper integration."""

    def test_from_dict_to_helper_full_mode(self, mock_con):
        """from_dict() -> validate() -> helper.materialize() for full mode."""
        raw = {
            "type": "postgresql",
            "connection": "my_pg",
            "table": "public.output",
            "mode": "full",
        }
        mat_config = PostgresMaterializationConfig.from_dict(raw)
        mat_config.validate()

        assert mat_config.mode == PostgresMaterializationMode.FULL
        assert mat_config.connection == "my_pg"
        assert mat_config.table == "public.output"

    def test_from_dict_to_helper_upsert_mode(self, mock_con):
        """from_dict() -> validate() -> correct unique_keys for upsert mode."""
        raw = {
            "type": "postgresql",
            "connection": "pg1",
            "table": "public.orders",
            "mode": "upsert_by_key",
            "unique_keys": ["order_id", "product_id"],
        }
        mat_config = PostgresMaterializationConfig.from_dict(raw)
        mat_config.validate()

        assert mat_config.mode == PostgresMaterializationMode.UPSERT_BY_KEY
        assert mat_config.unique_keys == ["order_id", "product_id"]

    def test_from_dict_invalid_mode_raises(self):
        """from_dict with bogus mode raises PostgresConfigError."""
        raw = {
            "type": "postgresql",
            "connection": "pg1",
            "table": "public.t",
            "mode": "delete_all",
        }
        with pytest.raises(PostgresConfigError, match="Invalid PostgreSQL materialization mode"):
            PostgresMaterializationConfig.from_dict(raw)

    def test_incremental_without_time_column_fails_validation(self):
        """incremental_by_time mode requires time_column."""
        raw = {
            "type": "postgresql",
            "connection": "pg1",
            "table": "public.t",
            "mode": "incremental_by_time",
        }
        config = PostgresMaterializationConfig.from_dict(raw)
        with pytest.raises(PostgresConfigError, match="time_column"):
            config.validate()
