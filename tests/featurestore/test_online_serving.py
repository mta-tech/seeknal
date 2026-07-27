"""Tests for online-serving declaration and terminality validation."""

import pytest

from seeknal.featurestore.online_serving import (
    FEATURE_PRODUCING_KINDS,
    GenerationMode,
    OnlineServingConfigError,
    OnlineServingTarget,
    TerminalityError,
    assert_terminal,
    is_terminal,
    parse_online_targets,
    resolve_upstream_nodes,
)


def pg_target(**over):
    entry = {
        "type": "postgresql",
        "connection": "feature_online_pg",
        "table": "feature_online.fg_retail__customer_30d",
        "mode": "upsert_by_key",
        "unique_keys": ["customer_id"],
        "serve_online": True,
    }
    entry.update(over)
    return entry


class FakeNode:
    def __init__(self, kind):
        self.kind = kind


class FakeDag:
    """Minimal graph: {node: (downstream set, upstream set)} plus kinds."""

    def __init__(self, downstream=None, upstream=None, kinds=None):
        self._down = downstream or {}
        self._up = upstream or {}
        self._kinds = kinds or {}

    def get_all_downstream(self, node_name):
        return set(self._down.get(node_name, set()))

    def get_all_upstream(self, node_name):
        return set(self._up.get(node_name, set()))

    def get_node(self, qualified_name):
        kind = self._kinds.get(qualified_name)
        return FakeNode(kind) if kind else None


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class TestParsing:
    def test_publication_is_opt_in(self):
        """Silence must never publish."""
        entry = pg_target()
        entry.pop("serve_online")
        assert parse_online_targets([entry]) == []

    def test_serve_online_false_does_not_publish(self):
        assert parse_online_targets([pg_target(serve_online=False)]) == []

    def test_no_materializations_is_safe(self):
        assert parse_online_targets(None) == []
        assert parse_online_targets([]) == []

    def test_opted_in_target_is_parsed(self):
        (t,) = parse_online_targets([pg_target()])
        assert t.table == "feature_online.fg_retail__customer_30d"
        assert t.unique_keys == ("customer_id",)
        assert t.generation_mode is GenerationMode.STAGED

    def test_only_postgresql_can_serve_online(self):
        with pytest.raises(OnlineServingConfigError, match="only supported for type"):
            parse_online_targets([pg_target(type="iceberg")])

    def test_offline_iceberg_target_is_ignored_not_rejected(self):
        """An Iceberg target that never opted in is simply offline."""
        assert parse_online_targets([{"type": "iceberg", "table": "a.b"}]) == []

    def test_unknown_generation_mode_rejected(self):
        with pytest.raises(OnlineServingConfigError, match="unknown generation_mode"):
            parse_online_targets([pg_target(generation_mode="teleport")])

    def test_mixed_list_parses_only_opted_in_entries(self):
        targets = parse_online_targets(
            [{"type": "iceberg", "table": "a.b"}, pg_target()]
        )
        assert len(targets) == 1


class TestServingTtl:
    """serving_ttl_days was documented on FeatureGroup for a long time with a
    stated default of 1 day, but no implementation existed anywhere."""

    def test_defaults_to_no_expiry(self):
        """Adopting the documented default of 1 would silently retire nearly
        every row in an existing online store on first run after upgrade."""
        (t,) = parse_online_targets([pg_target()])
        assert t.serving_ttl_days is None

    def test_explicit_value_is_parsed(self):
        (t,) = parse_online_targets([pg_target(serving_ttl_days=7)])
        assert t.serving_ttl_days == 7

    def test_numeric_string_is_accepted(self):
        (t,) = parse_online_targets([pg_target(serving_ttl_days="30")])
        assert t.serving_ttl_days == 30

    def test_non_numeric_rejected(self):
        with pytest.raises(OnlineServingConfigError, match="integer number of days"):
            parse_online_targets([pg_target(serving_ttl_days="a week")])

    @pytest.mark.parametrize("bad", [0, -1])
    def test_non_positive_rejected_with_guidance(self, bad):
        with pytest.raises(OnlineServingConfigError, match="omit it for no expiry"):
            parse_online_targets([pg_target(serving_ttl_days=bad)])


class TestTargetValidation:
    def test_invented_upsert_mode_rejected_with_pointer(self):
        """An earlier design used 'upsert', which is not in the real enum."""
        with pytest.raises(OnlineServingConfigError, match="upsert_by_key"):
            parse_online_targets([pg_target(mode="upsert")])

    def test_unique_keys_required(self):
        with pytest.raises(OnlineServingConfigError, match="unique_keys"):
            parse_online_targets([pg_target(unique_keys=[])])

    def test_table_must_be_schema_qualified(self):
        with pytest.raises(OnlineServingConfigError, match="schema-qualified"):
            parse_online_targets([pg_target(table="customer_30d")])

    def test_connection_required(self):
        with pytest.raises(OnlineServingConfigError, match="connection"):
            parse_online_targets([pg_target(connection="")])

    def test_schema_and_table_name_split(self):
        t = OnlineServingTarget(
            connection="c", table="feature_online.fg_x", unique_keys=("id",)
        )
        assert (t.schema, t.table_name) == ("feature_online", "fg_x")


# ---------------------------------------------------------------------------
# Terminality
# ---------------------------------------------------------------------------


class TestTerminality:
    """serve_online declares intent; the graph decides terminality. A caller
    cannot assert it about itself."""

    def test_node_with_no_downstream_is_terminal(self):
        dag = FakeDag(downstream={"fg.a": set()})
        assert is_terminal(dag, "fg.a")

    def test_downstream_feature_group_makes_node_intermediate(self):
        dag = FakeDag(
            downstream={"agg.a": {"fg.b"}}, kinds={"fg.b": "feature_group"}
        )
        assert not is_terminal(dag, "agg.a")

    def test_downstream_second_order_aggregation_makes_node_intermediate(self):
        dag = FakeDag(
            downstream={"agg.a": {"soa.b"}},
            kinds={"soa.b": "second_order_aggregation"},
        )
        assert not is_terminal(dag, "agg.a")

    @pytest.mark.parametrize("kind", ["model", "exposure", "metric", "semantic_model"])
    def test_downstream_consumers_do_not_block_terminality(self, kind):
        """Something that reads features is a consumer, not a producer."""
        dag = FakeDag(downstream={"fg.a": {"x.b"}}, kinds={"x.b": kind})
        assert is_terminal(dag, "fg.a")

    def test_all_feature_producing_kinds_block_terminality(self):
        for kind in FEATURE_PRODUCING_KINDS:
            dag = FakeDag(downstream={"n.a": {"n.b"}}, kinds={"n.b": kind})
            assert not is_terminal(dag, "n.a"), kind

    def test_unknown_downstream_node_is_ignored(self):
        dag = FakeDag(downstream={"fg.a": {"ghost"}}, kinds={})
        assert is_terminal(dag, "fg.a")

    def test_enum_style_kind_is_handled(self):
        """DAGNode.kind is a NodeType enum in production, a string in tests."""

        class EnumKind:
            value = "feature_group"

        dag = FakeDag(downstream={"agg.a": {"fg.b"}})
        dag._kinds = {"fg.b": EnumKind()}
        assert not is_terminal(dag, "agg.a")


class TestAssertTerminal:
    def test_terminal_node_passes(self):
        assert_terminal(FakeDag(downstream={"fg.a": set()}), "fg.a")

    def test_non_terminal_node_raises_naming_the_offenders(self):
        dag = FakeDag(
            downstream={"agg.a": {"fg.b", "model.c"}},
            kinds={"fg.b": "feature_group", "model.c": "model"},
        )
        with pytest.raises(TerminalityError) as exc:
            assert_terminal(dag, "agg.a")
        assert "fg.b" in str(exc.value)
        assert "model.c" not in str(exc.value)  # consumers are not offenders


class TestUpstreamResolution:
    def test_uses_transitive_closure(self):
        dag = FakeDag(upstream={"fg.a": {"src.x", "agg.y"}})
        assert resolve_upstream_nodes(dag, "fg.a") == ["agg.y", "src.x"]

    def test_no_upstream_returns_empty(self):
        assert resolve_upstream_nodes(FakeDag(upstream={"fg.a": set()}), "fg.a") == []

    def test_result_is_deterministic(self):
        dag = FakeDag(upstream={"fg.a": {"c", "a", "b"}})
        assert resolve_upstream_nodes(dag, "fg.a") == ["a", "b", "c"]
