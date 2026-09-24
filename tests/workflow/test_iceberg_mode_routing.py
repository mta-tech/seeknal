"""Behavioral routing tests for advanced Iceberg materialization modes."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from seeknal.dag.manifest import Node, NodeType
from seeknal.featurestore.featurestore import (
    IcebergStoreOutput,
    OfflineStore,
    OfflineStoreEnum,
)
from seeknal.workflow.materialization.config import (
    CatalogConfig,
    ConfigurationError,
    MaterializationConfig,
    MaterializationMode,
)
from seeknal.workflow.materialization.decorator import MaterializationMixin
from seeknal.workflow.materialization.dispatcher import (
    DispatchResult,
    MaterializationDispatcher,
)
from seeknal.workflow.materialization.operations import WriteResult
from seeknal.workflow.materialization.yaml_integration import (
    IcebergMaterializationError,
    IcebergMaterializationHelper,
)
from seeknal.workflow.executors.base import (
    ExecutionContext,
    ExecutorResult,
    ExecutionStatus,
)
from seeknal.workflow.executors.feature_group_executor import FeatureGroupExecutor
from seeknal.workflow.executors.source_executor import SourceExecutor
from seeknal.workflow.executors.transform_executor import TransformExecutor


@pytest.mark.parametrize("mode", ["upsert", "insert_overwrite"])
@pytest.mark.parametrize("invalid", [{}, {"table": "lake.ns.target", "create_table": "false"}])
@pytest.mark.parametrize(
    ("executor_class", "node_type"),
    [
        (SourceExecutor, NodeType.SOURCE),
        (TransformExecutor, NodeType.TRANSFORM),
        (FeatureGroupExecutor, NodeType.FEATURE_GROUP),
    ],
)
def test_inherited_advanced_preflight_failure_marks_runner_failed(
    tmp_path, executor_class, node_type, mode, invalid
):
    from seeknal.dag.manifest import Manifest
    from seeknal.workflow.runner import DAGRunner

    profile_path = tmp_path / "selected.yml"
    profile_path.write_text(
        "materialization:\n"
        f"  default_mode: {mode}\n"
        "  unique_keys: [id]\n"
        "  partition_by: [business_date]\n"
    )
    node = Node(
        id=f"{node_type.value}.invalid_target",
        name="invalid_target",
        node_type=node_type,
        config={"materialization": {"enabled": True, **invalid}},
    )
    context = ExecutionContext(
        project_name="inherited-mode",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
        profile_path=profile_path,
        duckdb_connection=MagicMock(),
    )
    executor = executor_class(node, context)
    computed = ExecutorResult(node_id=node.id, status=ExecutionStatus.SUCCESS)
    manifest = Manifest(project="inherited-mode")
    manifest.add_node(node)
    runner = DAGRunner(manifest, target_path=context.target_path, exec_context=context)
    with (
        patch.object(executor, "validate"),
        patch.object(executor, "pre_execute"),
        patch.object(executor, "execute", return_value=computed),
        patch("seeknal.workflow.executors.get_executor", return_value=executor),
        patch("seeknal.workflow.materialization.operations.write_to_iceberg") as write,
    ):
        summary = runner.run(full=True)

    assert summary.failed_nodes == 1
    state = runner.run_state.nodes[node.id]
    assert state.status == "failed"
    assert state.metadata["materialization"]["required_failed"] is True
    assert state.metadata["materialization"]["failure_category"] == "preflight_failed"
    write.assert_not_called()


def _profile(
    *,
    uri: str = "http://profile:8181",
    warehouse: str = "s3://profile-bucket/warehouse",
    unique_keys: list[str] | None = None,
    partition_by: list[str] | None = None,
    create_table: bool = True,
    max_batch_bytes: int = 268435456,
) -> MaterializationConfig:
    return MaterializationConfig(
        enabled=True,
        catalog=CatalogConfig(
            uri=uri,
            warehouse=warehouse,
            bearer_token="profile-token",
        ),
        unique_keys=unique_keys or [],
        partition_by=partition_by or [],
        create_table=create_table,
        max_batch_bytes=max_batch_bytes,
    )


def _successful_write(rows: int = 2) -> WriteResult:
    return WriteResult(
        success=True,
        snapshot_id="snapshot-1",
        row_count=rows,
        mode="upsert",
        input_row_count=rows,
        outcome="committed",
        snapshot_verified=True,
    )


@pytest.mark.parametrize(
    ("profile_uri", "profile_warehouse", "defaults", "target", "expected"),
    [
        (
            "http://profile:8181",
            "s3://profile-bucket/warehouse",
            {"catalog_uri": "http://defaults:8181", "warehouse": "defaults-wh"},
            {"catalog_uri": "http://target:8181", "warehouse": "target-wh"},
            ("http://target:8181", "target-wh"),
        ),
        (
            "http://profile:8181",
            "s3://profile-bucket/warehouse",
            {"catalog_uri": "http://defaults:8181", "warehouse": "defaults-wh"},
            {},
            ("http://profile:8181", "s3://profile-bucket/warehouse"),
        ),
        (
            "",
            "",
            {"catalog_uri": "http://defaults:8181", "warehouse": "defaults-wh"},
            {},
            ("http://defaults:8181", "defaults-wh"),
        ),
        ("", "", {}, {}, ("http://env:8181", "env-wh")),
    ],
)
def test_advanced_dispatch_resolves_catalog_precedence_without_duckdb_setup(
    monkeypatch,
    profile_uri,
    profile_warehouse,
    defaults,
    target,
    expected,
):
    monkeypatch.setenv("LAKEKEEPER_URI", "http://env:8181")
    monkeypatch.setenv("LAKEKEEPER_WAREHOUSE", "env-wh")
    loader = MagicMock()
    loader.load_profile.return_value = _profile(
        uri=profile_uri,
        warehouse=profile_warehouse,
        unique_keys=["customer_id"],
    )
    loader.load_source_defaults.return_value = defaults
    dispatcher = MaterializationDispatcher(loader)
    write_result = _successful_write()

    with (
        patch(
            "seeknal.workflow.materialization.operations.write_to_iceberg",
            return_value=write_result,
        ) as write,
        patch(
            "seeknal.workflow.materialization.operations.DuckDBIcebergExtension.load_extension"
        ) as load_extension,
        patch(
            "seeknal.workflow.materialization.operations.DuckDBIcebergExtension.attach_rest_catalog"
        ) as attach_catalog,
    ):
        actual = dispatcher._materialize_iceberg(
            MagicMock(),
            "transform.customers",
            {
                "table": "lake.analytics.customers",
                "mode": "upsert",
                **target,
            },
        )

    assert actual is write_result
    catalog = write.call_args.kwargs["catalog_config"]
    assert (catalog.uri, catalog.warehouse) == expected
    assert write.call_args.kwargs["unique_keys"] == ["customer_id"]
    load_extension.assert_not_called()
    attach_catalog.assert_not_called()


def test_advanced_dispatch_preserves_inherited_and_explicit_empty_options():
    loader = MagicMock()
    loader.load_profile.return_value = _profile(
        unique_keys=["profile_id"],
        partition_by=["event_date"],
        create_table=True,
        max_batch_bytes=8192,
    )
    dispatcher = MaterializationDispatcher(loader)

    with patch(
        "seeknal.workflow.materialization.operations.write_to_iceberg",
        side_effect=[_successful_write(), _successful_write()],
    ) as write:
        dispatcher._materialize_iceberg(
            MagicMock(),
            "transform.upserted",
            {
                "table": "lake.analytics.upserted",
                "mode": "upsert",
                "partition_by": [],
                "create_table": False,
            },
        )
        dispatcher._materialize_iceberg(
            MagicMock(),
            "transform.refreshed",
            {
                "table": "lake.analytics.refreshed",
                "mode": "insert_overwrite",
                "unique_keys": [],
                "max_batch_bytes": 1024,
            },
        )

    upsert = write.call_args_list[0].kwargs
    assert upsert["unique_keys"] == ["profile_id"]
    assert upsert["partition_by"] == []
    assert upsert["create_table"] is False
    assert upsert["max_batch_bytes"] == 8192

    insert_overwrite = write.call_args_list[1].kwargs
    assert insert_overwrite["unique_keys"] == []
    assert insert_overwrite["partition_by"] == ["event_date"]
    assert insert_overwrite["create_table"] is True
    assert insert_overwrite["max_batch_bytes"] == 1024


def test_advanced_dispatch_rejects_invalid_resolved_config_before_write():
    loader = MagicMock()
    loader.load_profile.return_value = _profile(unique_keys=[])
    dispatcher = MaterializationDispatcher(loader)

    with patch(
        "seeknal.workflow.materialization.operations.write_to_iceberg"
    ) as write:
        with pytest.raises(ConfigurationError, match="unique_keys"):
            dispatcher._materialize_iceberg(
                MagicMock(),
                "transform.customers",
                {"table": "lake.analytics.customers", "mode": "upsert"},
            )

    write.assert_not_called()


def test_yaml_singular_materialization_routes_advanced_fields_through_dispatcher():
    node = SimpleNamespace(
        id="transform.daily_sales",
        name="daily_sales",
        node_type=SimpleNamespace(value="transform"),
        config={
            "materialization": {
                "enabled": True,
                "table": "lake.marts.daily_sales",
                "mode": "insert_overwrite",
                "partition_by": ["sales_date"],
                "create_table": False,
                "max_batch_bytes": 4096,
                "catalog_uri": "http://target:8181",
                "warehouse": "s3://target/warehouse",
            }
        },
    )

    with patch.object(
        MaterializationDispatcher,
        "_materialize_iceberg",
        return_value=_successful_write(),
    ) as route:
        result = IcebergMaterializationHelper.materialize_node(
            node,
            source_con=MagicMock(),
        )

    assert result["success"] is True
    assert result["iceberg_table"] == "lake.marts.daily_sales"
    assert route.call_args.args[1] == "transform.daily_sales"
    assert route.call_args.args[2] == {
        "table": "lake.marts.daily_sales",
        "mode": "insert_overwrite",
        "catalog_uri": "http://target:8181",
        "warehouse": "s3://target/warehouse",
        "partition_by": ["sales_date"],
        "create_table": False,
        "max_batch_bytes": 4096,
    }


def test_yaml_singular_uses_explicit_profile_for_inherited_advanced_options(
    tmp_path,
):
    custom_profile = tmp_path / "custom-profiles.yml"
    custom_profile.write_text("""\
materialization:
  enabled: false
  default_mode: upsert
  unique_keys: [custom_id]
  create_table: false
  max_batch_bytes: 1234
  catalog:
    uri: http://custom-profile:8181
    warehouse: s3://custom-profile/warehouse
    bearer_token: custom-token
""")
    ambient_profile = tmp_path / "ambient-profiles.yml"
    ambient_profile.write_text("""\
materialization:
  enabled: false
  default_mode: insert_overwrite
  partition_by: [ambient_date]
  catalog:
    uri: http://ambient-profile:8181
    warehouse: s3://ambient-profile/warehouse
""")
    node = SimpleNamespace(
        id="transform.customers",
        name="customers",
        node_type=SimpleNamespace(value="transform"),
        config={
            "materialization": {
                "enabled": True,
                "table": "lake.analytics.customers",
            }
        },
    )

    with (
        patch(
            "seeknal.workflow.materialization.profile_loader.ProfileLoader.DEFAULT_PROFILE_PATH",
            ambient_profile,
        ),
        patch(
            "seeknal.workflow.materialization.operations.write_to_iceberg",
            return_value=_successful_write(),
        ) as write,
    ):
        result = IcebergMaterializationHelper.materialize_node(
            node,
            source_con=MagicMock(),
            profile_path=custom_profile,
        )

    call = write.call_args.kwargs
    assert call["mode"] == "upsert"
    assert call["unique_keys"] == ["custom_id"]
    assert call["create_table"] is False
    assert call["max_batch_bytes"] == 1234
    assert call["catalog_config"].uri == "http://custom-profile:8181"
    assert result["mode"] == "upsert"


def test_yaml_advanced_config_error_is_marked_required():
    with pytest.raises(IcebergMaterializationError) as raised:
        IcebergMaterializationHelper.extract_materialization_config({
            "materialization": {
                "enabled": True,
                "table": "lake.analytics.customers",
                "mode": "upsert",
                "unique_keys": "customer_id",
            }
        })

    assert raised.value.required_materialization is True
    assert raised.value.failure_category == "preflight_failed"


@pytest.mark.parametrize("as_dict", [False, True])
def test_featurestore_typed_and_dict_configs_forward_identical_advanced_options(
    as_dict,
):
    output = IcebergStoreOutput(
        table="customers",
        namespace="analytics",
        warehouse="s3://target/warehouse",
        mode="upsert",
        unique_keys=["customer_id"],
        create_table=False,
        max_batch_bytes=2048,
    )
    value = output.to_dict() if as_dict else output
    store = OfflineStore(kind=OfflineStoreEnum.ICEBERG, value=value)
    profile = _profile()

    with (
        patch(
            "seeknal.workflow.materialization.profile_loader.ProfileLoader.load_profile",
            return_value=profile,
        ),
        patch("duckdb.connect") as connect,
        patch(
            "seeknal.workflow.materialization.operations.write_to_iceberg",
            return_value=_successful_write(),
        ) as write,
        patch(
            "seeknal.workflow.materialization.operations.DuckDBIcebergExtension.load_extension"
        ) as load_extension,
        patch(
            "seeknal.workflow.materialization.operations.DuckDBIcebergExtension.attach_rest_catalog"
        ) as create_catalog,
    ):
        result = store._write_to_iceberg(MagicMock())

    call = write.call_args.kwargs
    assert call["mode"] == "upsert"
    assert call["unique_keys"] == ["customer_id"]
    assert call["partition_by"] == []
    assert call["create_table"] is False
    assert call["max_batch_bytes"] == 2048
    assert call["catalog_config"].warehouse == "s3://target/warehouse"
    assert result["write_result"]["success"] is True
    connect.return_value.register.assert_called_once()
    connect.return_value.close.assert_called_once()
    load_extension.assert_not_called()
    create_catalog.assert_not_called()


def test_featurestore_advanced_mode_does_not_hide_profile_failure():
    store = OfflineStore(
        kind=OfflineStoreEnum.ICEBERG,
        value=IcebergStoreOutput(
            table="customers",
            mode="upsert",
            unique_keys=["customer_id"],
        ),
    )

    with (
        patch(
            "seeknal.workflow.materialization.profile_loader.ProfileLoader.load_profile",
            side_effect=ConfigurationError("broken profile"),
        ),
        patch("duckdb.connect") as connect,
        patch(
            "seeknal.workflow.materialization.operations.write_to_iceberg"
        ) as write,
    ):
        with pytest.raises(ConfigurationError, match="broken profile"):
            store._write_to_iceberg(MagicMock())

    connect.assert_not_called()
    write.assert_not_called()


def test_materialization_mixin_advanced_mode_bypasses_eager_create_path():
    mixin = object.__new__(MaterializationMixin)
    mixin._materialization_node = SimpleNamespace(id="transform.customers")
    mixin._materialization_context = SimpleNamespace(
        get_duckdb_connection=MagicMock(return_value=MagicMock())
    )
    mixin._materialization_config = _profile(unique_keys=["customer_id"])
    mixin._materialization_config.default_mode = MaterializationMode.UPSERT
    mixin._materialization_config.table = "lake.analytics.customers"
    mixin._auditor = None
    mixin._profile_loader = None

    with (
        patch(
            "seeknal.workflow.materialization.decorator.write_to_iceberg",
            return_value=_successful_write(),
        ) as write,
        patch.object(mixin, "_setup_catalog") as setup_catalog,
        patch.object(mixin, "_get_or_create_table") as get_or_create,
        patch(
            "seeknal.workflow.materialization.decorator.DuckDBIcebergExtension.load_extension"
        ) as load_extension,
    ):
        result = mixin._perform_materialization()

    assert result.metadata["materialization"] == "success"
    assert write.call_args.kwargs["unique_keys"] == ["customer_id"]
    setup_catalog.assert_not_called()
    get_or_create.assert_not_called()
    load_extension.assert_not_called()


def test_materialization_mixin_preserves_advanced_failure_category():
    mixin = object.__new__(MaterializationMixin)
    mixin._materialization_node = SimpleNamespace(id="transform.customers")
    mixin._materialization_context = SimpleNamespace(
        get_duckdb_connection=MagicMock(return_value=MagicMock())
    )
    mixin._materialization_config = _profile(unique_keys=["customer_id"])
    mixin._materialization_config.default_mode = MaterializationMode.UPSERT
    mixin._auditor = None
    failure = RuntimeError("commit state unknown")
    failure.failure_category = "commit_unknown"

    with patch(
        "seeknal.workflow.materialization.decorator.write_to_iceberg",
        side_effect=failure,
    ):
        result = mixin._perform_materialization()

    assert result.metadata["materialization"] == "failed"
    assert result.metadata["required_materialization_failed"] is True
    assert result.metadata["failure_category"] == "commit_unknown"


def test_dispatch_result_serializes_typed_result_and_rejects_failed_write_result():
    typed = DispatchResult(
        total=1,
        succeeded=1,
        results=[{
            "target": "transform.customers[0]:iceberg",
            "type": "iceberg",
            "success": True,
            "write_result": _successful_write(),
        }],
    )
    encoded = json.dumps(typed.serializable_results)
    assert json.loads(encoded)[0]["write_result"]["snapshot_id"] == "snapshot-1"

    dispatcher = MaterializationDispatcher()
    with patch.object(
        dispatcher,
        "_materialize_iceberg",
        return_value=WriteResult(success=False, error_message="commit rejected"),
    ):
        failed = dispatcher.dispatch(
            MagicMock(),
            "transform.customers",
            [{"type": "iceberg", "table": "lake.analytics.customers"}],
        )

    assert failed.succeeded == 0
    assert failed.failed == 1
    assert failed.results[0]["success"] is False
    assert failed.results[0]["error"] == "commit rejected"


def test_dispatch_skips_explicitly_disabled_target():
    dispatcher = MaterializationDispatcher()
    with patch.object(
        dispatcher, "_materialize_iceberg", return_value=_successful_write()
    ) as materialize:
        result = dispatcher.dispatch(
            MagicMock(),
            "transform.customers",
            [
                {
                    "type": "iceberg",
                    "table": "lake.analytics.disabled",
                    "enabled": False,
                },
                {
                    "type": "iceberg",
                    "table": "lake.analytics.enabled",
                    "mode": "append",
                },
            ],
        )

    assert (result.total, result.succeeded, result.failed) == (1, 1, 0)
    materialize.assert_called_once()
    assert materialize.call_args.args[2]["table"] == "lake.analytics.enabled"


def test_mixin_node_enabled_flag_overrides_profile_default():
    mixin = object.__new__(MaterializationMixin)
    profile = _profile(unique_keys=["customer_id"])
    profile.enabled = False
    loader = MagicMock()
    loader.load_profile.return_value = profile
    mixin._profile_loader = loader
    mixin._materialization_node = SimpleNamespace(
        id="transform.customers",
        config={
            "materialization": {
                "enabled": True,
                "mode": "upsert",
                "table": "lake.analytics.customers",
            }
        },
    )
    mixin._materialization_context = MagicMock()
    mixin._materialization_config = None

    enabled = mixin._load_materialization_config()

    assert enabled is not None
    assert enabled.enabled is True
    assert enabled.default_mode is MaterializationMode.UPSERT
    assert enabled.unique_keys == ["customer_id"]

    profile.enabled = True
    mixin._materialization_node.config["materialization"]["enabled"] = False
    mixin._materialization_config = None
    assert mixin._load_materialization_config() is None


def test_mixin_loads_profile_from_execution_context(tmp_path):
    profile_path = tmp_path / "profiles.yml"
    mixin = object.__new__(MaterializationMixin)
    mixin._profile_loader = None
    mixin._materialization_node = SimpleNamespace(
        id="transform.customers",
        config={"materialization": {"enabled": False}},
    )
    mixin._materialization_context = SimpleNamespace(profile_path=profile_path)
    mixin._materialization_config = None
    loader = MagicMock()
    loader.load_profile.return_value = _profile()

    with patch(
        "seeknal.workflow.materialization.decorator.ProfileLoader",
        return_value=loader,
    ) as loader_class:
        assert mixin._load_materialization_config() is None

    loader_class.assert_called_once_with(profile_path=profile_path)


@pytest.mark.parametrize(
    ("executor_class", "node_type", "config", "patch_target"),
    [
        (
            SourceExecutor,
            NodeType.SOURCE,
            {"source": "csv"},
            "seeknal.workflow.executors.source_executor.materialize_node_if_enabled",
        ),
        (
            TransformExecutor,
            NodeType.TRANSFORM,
            {"transform": "SELECT 1"},
            "seeknal.workflow.executors.transform_executor.materialize_node_if_enabled",
        ),
        (
            FeatureGroupExecutor,
            NodeType.FEATURE_GROUP,
            {
                "entity": {"name": "customer", "join_keys": ["customer_id"]},
            },
            "seeknal.workflow.executors.feature_group_executor.materialize_node_if_enabled",
        ),
    ],
)
def test_legacy_executor_threads_profile_and_preserves_failure_category(
    tmp_path,
    executor_class,
    node_type,
    config,
    patch_target,
):
    profile_path = tmp_path / "custom-profile.yml"
    node = Node(
        id=f"{node_type.value}.customers",
        name="customers",
        node_type=node_type,
        config={
            **config,
            "materialization": {
                "enabled": True,
                "table": "lake.analytics.customers",
                "mode": "upsert",
                "unique_keys": ["customer_id"],
            },
        },
    )
    context = ExecutionContext(
        project_name="routing-test",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
        duckdb_connection=MagicMock(),
        profile_path=profile_path,
    )
    executor = executor_class(node, context)
    execution_result = ExecutorResult(
        node_id=node.id,
        status=ExecutionStatus.SUCCESS,
    )
    failure = IcebergMaterializationError("commit state unknown")
    failure.required_materialization = True
    failure.failure_category = "commit_unknown"

    with patch(patch_target, side_effect=failure) as materialize:
        result = executor.post_execute(execution_result)

    assert materialize.call_args.kwargs["profile_path"] == profile_path
    assert result.metadata["materialization"]["success"] is False
    assert result.metadata["materialization"]["required_failed"] is True
    assert result.metadata["materialization"]["failure_category"] == (
        "commit_unknown"
    )


def test_feature_group_post_execute_dispatches_normalized_table_targets(tmp_path):
    target = {
        "type": "iceberg",
        "table": "lake.analytics.customer_features",
        "mode": "upsert",
        "unique_keys": ["customer_id"],
        "create_table": False,
    }
    node = Node(
        id="feature_group.customer_features",
        name="customer_features",
        node_type=NodeType.FEATURE_GROUP,
        config={
            "entity": {"name": "customer", "join_keys": ["customer_id"]},
            "materialization": {"offline": True, "online": False},
            "materializations": [target],
        },
    )
    context = ExecutionContext(
        project_name="routing-test",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
        duckdb_connection=MagicMock(),
    )
    executor = FeatureGroupExecutor(node, context)
    write_result = _successful_write(3)
    dispatched = DispatchResult(
        total=1,
        succeeded=1,
        results=[{
            "target": "feature_group.customer_features[0]:iceberg",
            "type": "iceberg",
            "success": True,
            "write_result": write_result,
        }],
    )
    execution_result = ExecutorResult(
        node_id=node.id,
        status=ExecutionStatus.SUCCESS,
        row_count=3,
    )

    with patch.object(
        MaterializationDispatcher, "dispatch", return_value=dispatched
    ) as dispatch:
        result = executor.post_execute(execution_result)

    assert dispatch.call_args.kwargs["view_name"] == (
        "feature_group.customer_features"
    )
    assert dispatch.call_args.kwargs["targets"] == [target]
    assert result.metadata["materialization"] == {
        "enabled": True,
        "success": True,
        "total": 1,
        "succeeded": 1,
        "failed": 0,
        "required_failed": False,
        "results": dispatched.serializable_results,
    }
    json.dumps(result.metadata)
