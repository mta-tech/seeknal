"""Tests for the @materialize stackable decorator.

Tests cover:
- Single @materialize producing one-element list
- Multiple (stacked) @materialize producing ordered list
- @materialize with @source, @transform, @feature_group
- Combination of @materialize + materialization= parameter
- Discoverer propagation of _seeknal_materializations to node metadata
- DAG builder integration with plural materializations from Python
"""

import pytest  # ty: ignore[unresolved-import]

from seeknal.pipeline.decorators import (  # ty: ignore[unresolved-import]
    materialize,
    source,
    transform,
    feature_group,
    get_registered_nodes,
    clear_registry,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    """Clear the registry before and after each test."""
    clear_registry()
    yield
    clear_registry()


# =============================================================================
# @materialize decorator unit tests
# =============================================================================


class TestMaterializeDecorator:
    """Tests for the @materialize decorator itself."""

    def test_single_materialize_creates_list(self):
        """Single @materialize stores a one-element list on func."""

        @materialize(type="iceberg", table="atlas.ns.t1")
        def my_func(ctx):
            pass

        assert hasattr(my_func, "_seeknal_materializations")
        assert len(my_func._seeknal_materializations) == 1
        assert my_func._seeknal_materializations[0]["type"] == "iceberg"
        assert my_func._seeknal_materializations[0]["table"] == "atlas.ns.t1"

    def test_stacked_materialize_produces_ordered_list(self):
        """Multiple @materialize decorators produce a list (bottom-up order)."""

        @materialize(type="postgresql", connection="pg1", table="public.t1")
        @materialize(type="iceberg", table="atlas.ns.t1")
        def my_func(ctx):
            pass

        mats = my_func._seeknal_materializations
        assert len(mats) == 2
        # Bottom decorator runs first
        assert mats[0]["type"] == "iceberg"
        assert mats[1]["type"] == "postgresql"
        assert mats[1]["connection"] == "pg1"

    def test_materialize_default_values(self):
        """@materialize with no args uses sensible defaults."""

        @materialize()
        def my_func(ctx):
            pass

        mat = my_func._seeknal_materializations[0]
        assert mat["type"] == "iceberg"
        assert mat["table"] == ""
        assert mat["mode"] == "full"
        assert "connection" not in mat
        assert "time_column" not in mat
        assert "lookback" not in mat
        assert "unique_keys" not in mat

    def test_materialize_all_params(self):
        """@materialize passes through all optional parameters."""

        @materialize(
            type="postgresql",
            connection="my_pg",
            table="analytics.orders",
            mode="upsert_by_key",
            time_column="updated_at",
            lookback="7d",
            unique_keys=["order_id"],
            custom_param="extra",
        )
        def my_func(ctx):
            pass

        mat = my_func._seeknal_materializations[0]
        assert mat["type"] == "postgresql"
        assert mat["connection"] == "my_pg"
        assert mat["table"] == "analytics.orders"
        assert mat["mode"] == "upsert_by_key"
        assert mat["time_column"] == "updated_at"
        assert mat["lookback"] == "7d"
        assert mat["unique_keys"] == ["order_id"]
        assert mat["custom_param"] == "extra"

    def test_materialize_does_not_wrap(self):
        """@materialize returns the original function, not a wrapper."""

        def original(ctx):
            return "hello"

        decorated = materialize(type="iceberg", table="atlas.ns.t1")(original)
        assert decorated is original

    def test_three_stacked_materializations(self):
        """Three stacked @materialize decorators all collected."""

        @materialize(type="postgresql", connection="pg1", table="public.t1")
        @materialize(type="postgresql", connection="pg2", table="public.t2")
        @materialize(type="iceberg", table="atlas.ns.t1")
        def my_func(ctx):
            pass

        assert len(my_func._seeknal_materializations) == 3


# =============================================================================
# @materialize + node decorator integration
# =============================================================================


class TestMaterializeWithNodeDecorators:
    """Tests for @materialize combined with @source, @transform, etc."""

    def test_materialize_with_transform(self):
        """@materialize + @transform: materializations visible in registry."""

        @transform(name="clean_data", inputs=["source.raw"])
        @materialize(type="postgresql", connection="pg", table="public.clean")
        @materialize(type="iceberg", table="atlas.ns.clean")
        def clean_data(ctx):
            pass

        nodes = get_registered_nodes()
        assert "transform.clean_data" in nodes
        # The func stored in metadata should have _seeknal_materializations
        func = nodes["transform.clean_data"]["func"]
        assert hasattr(func, "_seeknal_materializations")
        assert len(func._seeknal_materializations) == 2

    def test_materialize_with_source(self):
        """@materialize + @source: materializations visible in registry."""

        @source(name="raw_data", source="csv", table="data.csv")
        @materialize(type="iceberg", table="atlas.ns.raw")
        def raw_data():
            pass

        nodes = get_registered_nodes()
        func = nodes["source.raw_data"]["func"]
        assert hasattr(func, "_seeknal_materializations")
        assert func._seeknal_materializations[0]["type"] == "iceberg"

    def test_materialize_with_feature_group(self):
        """@materialize + @feature_group: materializations visible."""

        @feature_group(name="user_feats", entity="user")
        @materialize(type="postgresql", connection="pg", table="public.feats")
        def user_feats(ctx):
            pass

        nodes = get_registered_nodes()
        func = nodes["feature_group.user_feats"]["func"]
        assert len(func._seeknal_materializations) == 1
        assert func._seeknal_materializations[0]["connection"] == "pg"

    def test_materialize_combined_with_materialization_param(self):
        """@materialize + materialization= kwarg both present on same node."""

        @transform(
            name="orders",
            materialization={"enabled": True, "table": "atlas.ns.orders"},
        )
        @materialize(type="postgresql", connection="pg", table="public.orders")
        def orders(ctx):
            pass

        nodes = get_registered_nodes()
        meta = nodes["transform.orders"]
        # The materialization= param is in "materialization" key
        assert meta["materialization"]["table"] == "atlas.ns.orders"
        # The @materialize list is on func
        func = meta["func"]
        assert len(func._seeknal_materializations) == 1
        assert func._seeknal_materializations[0]["type"] == "postgresql"

    def test_source_query_and_connection_params(self):
        """@source explicit query and connection kwargs pass through params."""

        @source(
            name="pg_orders",
            source="postgres",
            connection="my_pg",
            query="SELECT * FROM orders WHERE active",
        )
        def pg_orders():
            pass

        nodes = get_registered_nodes()
        meta = nodes["source.pg_orders"]
        assert meta["params"]["query"] == "SELECT * FROM orders WHERE active"
        assert meta["params"]["connection"] == "my_pg"


# =============================================================================
# Discoverer integration tests
# =============================================================================


class TestDiscovererMaterializeIntegration:
    """Tests for discoverer picking up _seeknal_materializations."""

    def test_discoverer_propagates_materializations(self, tmp_path):
        """Discoverer parse_file adds materializations to node metadata."""
        from seeknal.pipeline.discoverer import PythonPipelineDiscoverer  # ty: ignore[unresolved-import]

        pipelines_dir = tmp_path / "seeknal" / "pipelines"
        pipelines_dir.mkdir(parents=True)

        py_content = '''\
from seeknal.pipeline.decorators import transform, materialize

@transform(name="my_node", inputs=["source.raw"])
@materialize(type="postgresql", connection="pg", table="public.out", mode="full")
@materialize(type="iceberg", table="atlas.ns.out")
def my_node(ctx):
    return ctx.ref("source.raw")
'''
        (pipelines_dir / "pipeline.py").write_text(py_content)

        discoverer = PythonPipelineDiscoverer(project_path=tmp_path)
        nodes = discoverer.parse_file(pipelines_dir / "pipeline.py")

        assert len(nodes) == 1
        node = nodes[0]
        assert "materializations" in node
        assert len(node["materializations"]) == 2
        assert node["materializations"][0]["type"] == "iceberg"
        assert node["materializations"][1]["type"] == "postgresql"
        assert node["materializations"][1]["connection"] == "pg"

    def test_discoverer_no_materialize_no_key(self, tmp_path):
        """Discoverer doesn't add materializations key if no @materialize used."""
        from seeknal.pipeline.discoverer import PythonPipelineDiscoverer  # ty: ignore[unresolved-import]

        pipelines_dir = tmp_path / "seeknal" / "pipelines"
        pipelines_dir.mkdir(parents=True)

        py_content = '''\
from seeknal.pipeline.decorators import source

@source(name="raw", source="csv", table="data.csv")
def raw():
    pass
'''
        (pipelines_dir / "pipeline.py").write_text(py_content)

        discoverer = PythonPipelineDiscoverer(project_path=tmp_path)
        nodes = discoverer.parse_file(pipelines_dir / "pipeline.py")

        assert len(nodes) == 1
        assert "materializations" not in nodes[0]
