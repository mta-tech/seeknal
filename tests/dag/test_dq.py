"""Tests for data quality visualization module."""

import json
import os
import tempfile
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import pytest  # ty: ignore[unresolved-import]

from seeknal.dag.manifest import Manifest, Node, NodeType  # ty: ignore[unresolved-import]
from seeknal.dag.dq import (  # ty: ignore[unresolved-import]
    ColumnStats,
    NodeSummary,
    ProfileSummary,
    RuleResult,
    DQMetadata,
    DQData,
    DQDataBuilder,
    DQVisualizationError,
    render_dq_ascii,
    generate_dq_html,
)
from seeknal.workflow.state import RunState, NodeState, NodeStatus  # ty: ignore[unresolved-import]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_manifest_with_profile_and_rules() -> Manifest:
    """Create a manifest with profile and rule nodes."""
    m = Manifest(project="test_project")
    m.add_node(Node(
        id="source.orders",
        name="orders",
        node_type=NodeType.SOURCE,
    ))
    m.add_node(Node(
        id="profile.orders_stats",
        name="orders_stats",
        node_type=NodeType.PROFILE,
    ))
    m.add_node(Node(
        id="rule.check_orders",
        name="check_orders",
        node_type=NodeType.RULE,
    ))
    m.add_node(Node(
        id="rule.check_nulls",
        name="check_nulls",
        node_type=NodeType.RULE,
    ))
    m.add_edge("source.orders", "profile.orders_stats")
    m.add_edge("source.orders", "rule.check_orders")
    m.add_edge("source.orders", "rule.check_nulls")
    return m


def _make_run_state_with_rules() -> RunState:
    """Create a RunState with rule node results."""
    state = RunState()
    state.nodes["rule.check_orders"] = NodeState(
        hash="abc123",
        status=NodeStatus.SUCCESS.value,
        duration_ms=150,
        last_run="2026-02-24T17:08:09",
        metadata={
            "rule_type": "sql_assertion",
            "passed": True,
            "checks": [{"name": "order_id_positive", "passed": True}],
            "violations": 0,
        },
    )
    state.nodes["rule.check_nulls"] = NodeState(
        hash="def456",
        status=NodeStatus.FAILED.value,
        duration_ms=80,
        last_run="2026-02-24T17:08:10",
        metadata={
            "rule_type": "not_null",
            "passed": False,
            "checks": [{"name": "email_not_null", "passed": False, "violations": 3}],
            "violations": 3,
        },
    )
    return state


def _make_profile_parquet(tmp_path: Path, profile_name: str) -> None:
    """Create a mock profile parquet file using DuckDB."""
    import duckdb  # ty: ignore[unresolved-import]

    intermediate = tmp_path / "intermediate"
    intermediate.mkdir(parents=True, exist_ok=True)
    parquet_path = intermediate / f"profile_{profile_name}.parquet"

    con = duckdb.connect()
    con.execute("""
        CREATE TABLE profile_data (
            column_name VARCHAR,
            metric VARCHAR,
            value VARCHAR,
            detail VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO profile_data VALUES
        ('_table_', 'row_count', '100', NULL),
        ('price', 'null_percent', '0.0', NULL),
        ('price', 'distinct_count', '50', NULL),
        ('price', 'min', '9.99', NULL),
        ('price', 'avg', '85.58', NULL),
        ('price', 'max', '249.99', NULL),
        ('name', 'null_percent', '5.0', NULL),
        ('name', 'distinct_count', '95', NULL),
        ('name', 'min', NULL, NULL),
        ('name', 'avg', NULL, NULL),
        ('name', 'max', NULL, NULL)
    """)
    con.execute(f"COPY profile_data TO '{parquet_path}' (FORMAT PARQUET)")
    con.close()


def _make_dq_data(
    node_summaries: Optional[list] = None,
    profiles: Optional[list] = None,
    rules: Optional[list] = None,
    trends: Optional[dict] = None,
) -> DQData:
    """Create a DQData instance with defaults."""
    if node_summaries is None:
        node_summaries = []
    if profiles is None:
        profiles = []
    if rules is None:
        rules = []
    if trends is None:
        trends = {}

    passed = sum(1 for r in rules if r.status == "pass")
    warned = sum(1 for r in rules if r.status == "warn")
    failed = sum(1 for r in rules if r.status == "fail")
    total = len(rules)
    health = ((passed + warned) / total * 100) if total > 0 else 100.0

    total_nodes = len(node_summaries)
    nodes_succeeded = sum(1 for n in node_summaries if n.status == "success")
    nodes_failed = sum(1 for n in node_summaries if n.status == "failed")

    return DQData(
        node_summaries=node_summaries,
        profiles=profiles,
        rules=rules,
        trends=trends,
        metadata=DQMetadata(
            project="test_project",
            run_id="20260224_170809",
            timestamp="2026-02-24T17:08:09",
            total_nodes=total_nodes,
            nodes_succeeded=nodes_succeeded,
            nodes_failed=nodes_failed,
            total_profiles=len(profiles),
            total_rules=total,
            rules_passed=passed,
            rules_warned=warned,
            rules_failed=failed,
            health_score=round(health, 1),
        ),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def manifest_with_dq():
    """Manifest with profile and rule nodes."""
    return _make_manifest_with_profile_and_rules()


@pytest.fixture
def run_state_with_rules():
    """RunState with rule results."""
    return _make_run_state_with_rules()


@pytest.fixture
def sample_profile_summary():
    """A ProfileSummary with two columns."""
    return ProfileSummary(
        node_id="profile.orders_stats",
        name="orders_stats",
        row_count=100,
        column_count=2,
        columns=[
            ColumnStats(
                name="price",
                metrics={"null_percent": "0.0", "distinct_count": "50",
                         "min": "9.99", "avg": "85.58", "max": "249.99"},
                detail={},
            ),
            ColumnStats(
                name="name",
                metrics={"null_percent": "5.0", "distinct_count": "95",
                         "min": None, "avg": None, "max": None},
                detail={},
            ),
        ],
    )


@pytest.fixture
def sample_rules():
    """List of RuleResult with pass and fail."""
    return [
        RuleResult(
            node_id="rule.check_orders",
            name="check_orders",
            rule_type="sql_assertion",
            status="pass",
            checks=[{"name": "order_id_positive", "passed": True}],
            message="All checks passed",
            duration_ms=150,
        ),
        RuleResult(
            node_id="rule.check_nulls",
            name="check_nulls",
            rule_type="not_null",
            status="fail",
            checks=[{"name": "email_not_null", "passed": False, "violations": 3}],
            message="3 violation(s)",
            duration_ms=80,
        ),
    ]


# ---------------------------------------------------------------------------
# TestDQDataBuilder
# ---------------------------------------------------------------------------


class TestDQDataBuilder:
    """Tests for DQDataBuilder class."""

    def test_build_returns_dq_data(self, manifest_with_dq, run_state_with_rules):
        """Build produces DQData instance."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(run_state_with_rules, manifest_with_dq, target)
            data = builder.build()

            assert isinstance(data, DQData)

    def test_build_extracts_rules(self, manifest_with_dq, run_state_with_rules):
        """Build extracts rule results from RunState."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(run_state_with_rules, manifest_with_dq, target)
            data = builder.build()

            assert len(data.rules) == 2
            rule_names = {r.name for r in data.rules}
            assert "check_orders" in rule_names
            assert "check_nulls" in rule_names

    def test_build_rule_status_pass(self, manifest_with_dq, run_state_with_rules):
        """Passed rule has status 'pass'."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(run_state_with_rules, manifest_with_dq, target)
            data = builder.build()

            rule = next(r for r in data.rules if r.name == "check_orders")
            assert rule.status == "pass"
            assert rule.rule_type == "sql_assertion"

    def test_build_rule_status_fail(self, manifest_with_dq, run_state_with_rules):
        """Failed rule has status 'fail'."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(run_state_with_rules, manifest_with_dq, target)
            data = builder.build()

            rule = next(r for r in data.rules if r.name == "check_nulls")
            assert rule.status == "fail"
            assert rule.message == "3 violation(s)"

    def test_build_rule_status_warn(self, manifest_with_dq):
        """Rule with warned > 0 gets status 'warn'."""
        state = RunState()
        state.nodes["rule.check_orders"] = NodeState(
            hash="abc123",
            status=NodeStatus.SUCCESS.value,
            duration_ms=100,
            last_run="2026-02-24T17:08:09",
            metadata={
                "rule_type": "profile_check",
                "passed": True,
                "warned": 2,
                "checks": [],
            },
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(state, manifest_with_dq, target)
            data = builder.build()

            rule = next(r for r in data.rules if r.name == "check_orders")
            assert rule.status == "warn"
            assert "2 warning(s)" in rule.message

    def test_build_profiles_from_parquet(self, manifest_with_dq):
        """Build reads profile parquet and produces ProfileSummary."""
        state = RunState()
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            _make_profile_parquet(target, "orders_stats")

            builder = DQDataBuilder(state, manifest_with_dq, target)
            data = builder.build()

            assert len(data.profiles) == 1
            profile = data.profiles[0]
            assert profile.name == "orders_stats"
            assert profile.row_count == 100
            assert profile.column_count == 2  # price + name, not _table_

    def test_build_profiles_column_metrics(self, manifest_with_dq):
        """Profile columns have correct metrics from parquet."""
        state = RunState()
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            _make_profile_parquet(target, "orders_stats")

            builder = DQDataBuilder(state, manifest_with_dq, target)
            data = builder.build()

            profile = data.profiles[0]
            price_col = next(c for c in profile.columns if c.name == "price")
            assert price_col.metrics["null_percent"] == "0.0"
            assert price_col.metrics["avg"] == "85.58"
            assert price_col.metrics["max"] == "249.99"

    def test_build_profiles_no_parquet(self, manifest_with_dq, run_state_with_rules):
        """Profile node without parquet is skipped."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(run_state_with_rules, manifest_with_dq, target)
            data = builder.build()

            assert len(data.profiles) == 0

    def test_build_no_rules(self):
        """Build with no rule nodes produces empty rules list."""
        m = Manifest(project="test")
        m.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))

        state = RunState()
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(state, m, target)
            data = builder.build()

            assert len(data.rules) == 0
            assert data.metadata.total_rules == 0

    def test_build_empty_state(self, manifest_with_dq):
        """Build with empty RunState produces no rules (no state entries)."""
        state = RunState()
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(state, manifest_with_dq, target)
            data = builder.build()

            assert len(data.rules) == 0

    def test_build_focus_node_rule(self, manifest_with_dq, run_state_with_rules):
        """Focus on a specific rule node filters results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(run_state_with_rules, manifest_with_dq, target)
            data = builder.build(focus_node="rule.check_orders")

            assert len(data.rules) == 1
            assert data.rules[0].name == "check_orders"

    def test_build_focus_node_profile(self, manifest_with_dq):
        """Focus on a profile node filters to just that profile."""
        state = RunState()
        # Add a second profile node
        manifest_with_dq.add_node(Node(
            id="profile.customers_stats",
            name="customers_stats",
            node_type=NodeType.PROFILE,
        ))
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            _make_profile_parquet(target, "orders_stats")

            builder = DQDataBuilder(state, manifest_with_dq, target)
            data = builder.build(focus_node="profile.orders_stats")

            assert len(data.profiles) == 1
            assert data.profiles[0].name == "orders_stats"

    def test_build_focus_node_not_found(self, manifest_with_dq, run_state_with_rules):
        """Focus on nonexistent node raises DQVisualizationError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(run_state_with_rules, manifest_with_dq, target)

            with pytest.raises(DQVisualizationError, match="not found"):
                builder.build(focus_node="rule.nonexistent")

    def test_build_metadata_health_score(self, manifest_with_dq, run_state_with_rules):
        """Health score computed correctly: (passed + warned) / total * 100."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(run_state_with_rules, manifest_with_dq, target)
            data = builder.build()

            # 1 passed, 0 warned, 1 failed -> (1+0)/2 * 100 = 50.0
            assert data.metadata.health_score == 50.0

    def test_build_metadata_all_pass(self, manifest_with_dq):
        """Health score is 100 when all rules pass."""
        state = RunState()
        state.nodes["rule.check_orders"] = NodeState(
            hash="abc123",
            status=NodeStatus.SUCCESS.value,
            duration_ms=100,
            last_run="2026-02-24T17:08:09",
            metadata={"rule_type": "sql_assertion", "passed": True, "checks": []},
        )
        state.nodes["rule.check_nulls"] = NodeState(
            hash="def456",
            status=NodeStatus.SUCCESS.value,
            duration_ms=50,
            last_run="2026-02-24T17:08:09",
            metadata={"rule_type": "not_null", "passed": True, "checks": []},
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(state, manifest_with_dq, target)
            data = builder.build()

            assert data.metadata.health_score == 100.0

    def test_build_metadata_no_rules(self):
        """Health score is 100 when there are no rules."""
        m = Manifest(project="test")
        m.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))
        state = RunState()
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(state, m, target)
            data = builder.build()

            assert data.metadata.health_score == 100.0

    def test_build_trends_no_history(self, manifest_with_dq, run_state_with_rules):
        """No dq_history.parquet returns empty trends."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(run_state_with_rules, manifest_with_dq, target)
            data = builder.build()

            assert data.trends == {}

    def test_build_trends_from_history(self, manifest_with_dq, run_state_with_rules):
        """Reads trend data from dq_history.parquet."""
        import duckdb  # ty: ignore[unresolved-import]

        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            history_path = target / "dq_history.parquet"

            con = duckdb.connect()
            con.execute("""
                CREATE TABLE history (
                    node_id VARCHAR,
                    column_name VARCHAR,
                    metric VARCHAR,
                    run_id VARCHAR,
                    timestamp VARCHAR,
                    value DOUBLE
                )
            """)
            con.execute("""
                INSERT INTO history VALUES
                ('rule.check_orders', '_table_', 'passed', 'run_1', '2026-02-24T10:00:00', 1.0),
                ('rule.check_orders', '_table_', 'passed', 'run_2', '2026-02-24T11:00:00', 1.0),
                ('rule.check_orders', '_table_', 'passed', 'run_3', '2026-02-24T12:00:00', 0.0)
            """)
            con.execute(f"COPY history TO '{history_path}' (FORMAT PARQUET)")
            con.close()

            builder = DQDataBuilder(run_state_with_rules, manifest_with_dq, target)
            data = builder.build()

            key = "rule.check_orders._table_.passed"
            assert key in data.trends
            assert len(data.trends[key]) == 3

    def test_frozen_dataclass(self):
        """DQData is immutable."""
        data = _make_dq_data()
        with pytest.raises(AttributeError):
            data.profiles = []  # type: ignore[misc]

    def test_json_serializable(self, sample_profile_summary, sample_rules):
        """DQData is JSON-serializable via dataclasses.asdict."""
        import dataclasses

        data = _make_dq_data(
            profiles=[sample_profile_summary],
            rules=sample_rules,
        )
        result = json.dumps(dataclasses.asdict(data))
        parsed = json.loads(result)

        assert parsed["metadata"]["project"] == "test_project"
        assert len(parsed["profiles"]) == 1
        assert len(parsed["rules"]) == 2

    def test_build_node_summaries_all_nodes(self, manifest_with_dq, run_state_with_rules):
        """Node summaries include ALL manifest nodes, not just profile/rule."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(run_state_with_rules, manifest_with_dq, target)
            data = builder.build()

            # Manifest has: source.orders, profile.orders_stats,
            # rule.check_orders, rule.check_nulls = 4 nodes
            assert len(data.node_summaries) == 4
            types = {n.node_type for n in data.node_summaries}
            assert "source" in types
            assert "profile" in types
            assert "rule" in types

    def test_build_node_summaries_with_state(self, manifest_with_dq, run_state_with_rules):
        """Node summaries pick up status/duration from RunState."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(run_state_with_rules, manifest_with_dq, target)
            data = builder.build()

            rule_summary = next(
                n for n in data.node_summaries if n.node_id == "rule.check_orders"
            )
            assert rule_summary.status == "success"
            assert rule_summary.duration_ms == 150

    def test_build_node_summaries_no_state(self, manifest_with_dq):
        """Nodes without RunState entries show as pending."""
        state = RunState()
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(state, manifest_with_dq, target)
            data = builder.build()

            for ns in data.node_summaries:
                assert ns.status == "pending"
                assert ns.duration_ms == 0

    def test_build_node_summaries_focus_node(self, manifest_with_dq, run_state_with_rules):
        """Focus node filters node summaries to single node."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(run_state_with_rules, manifest_with_dq, target)
            data = builder.build(focus_node="source.orders")

            assert len(data.node_summaries) == 1
            assert data.node_summaries[0].node_id == "source.orders"

    def test_metadata_node_counts(self, manifest_with_dq, run_state_with_rules):
        """Metadata includes correct node success/failure counts."""
        # Add source state so we have: 1 source success + 1 rule success + 1 rule failed
        run_state_with_rules.nodes["source.orders"] = NodeState(
            hash="src1",
            status=NodeStatus.SUCCESS.value,
            duration_ms=200,
            last_run="2026-02-24T17:08:08",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir)
            builder = DQDataBuilder(run_state_with_rules, manifest_with_dq, target)
            data = builder.build()

            assert data.metadata.total_nodes == 4
            assert data.metadata.nodes_succeeded == 2  # source + check_orders
            assert data.metadata.nodes_failed == 1     # check_nulls


# ---------------------------------------------------------------------------
# TestRenderDQAscii
# ---------------------------------------------------------------------------


class TestRenderDQAscii:
    """Tests for render_dq_ascii() function."""

    def test_empty_data(self):
        """Empty DQData shows 'no data' message."""
        data = _make_dq_data()
        output = render_dq_ascii(data)

        assert "No data quality data found" in output
        assert "seeknal run" in output

    def test_rules_only(self, sample_rules):
        """Rules without profiles renders rule section."""
        data = _make_dq_data(rules=sample_rules)
        output = render_dq_ascii(data)

        assert "Rules" in output
        assert "check_orders" in output
        assert "PASS" in output
        assert "check_nulls" in output
        assert "FAIL" in output

    def test_profiles_only(self, sample_profile_summary):
        """Profile without rules renders profile section."""
        data = _make_dq_data(profiles=[sample_profile_summary])
        output = render_dq_ascii(data)

        assert "Profile: orders_stats" in output
        assert "100 rows" in output
        assert "2 cols" in output
        assert "price" in output
        assert "name" in output

    def test_profile_column_metrics(self, sample_profile_summary):
        """Profile columns show metric values."""
        data = _make_dq_data(profiles=[sample_profile_summary])
        output = render_dq_ascii(data)

        assert "0.0%" in output  # null_percent for price
        assert "85.58" in output  # avg for price
        assert "249.99" in output  # max for price

    def test_profile_null_metrics(self, sample_profile_summary):
        """None metric values show as '--'."""
        data = _make_dq_data(profiles=[sample_profile_summary])
        output = render_dq_ascii(data)

        # name column has None for min, avg, max
        assert "--" in output

    def test_footer_summary(self, sample_rules):
        """Footer shows rule count summary."""
        data = _make_dq_data(rules=sample_rules)
        output = render_dq_ascii(data)

        assert "2 rules" in output
        assert "1 passed" in output
        assert "1 failed" in output

    def test_footer_no_rules(self):
        """Footer with no rules omits rule summary line."""
        data = _make_dq_data(profiles=[ProfileSummary(
            node_id="p.x", name="x", row_count=10, column_count=1,
            columns=[ColumnStats(name="a", metrics={"null_percent": "0"}, detail={})],
        )])
        output = render_dq_ascii(data)

        assert "rules:" not in output

    def test_warn_status_icon(self):
        """Warning rule shows warning icon."""
        rules = [RuleResult(
            node_id="rule.w",
            name="warn_rule",
            rule_type="profile_check",
            status="warn",
            checks=[],
            message="1 warning(s)",
            duration_ms=50,
        )]
        data = _make_dq_data(rules=rules)
        output = render_dq_ascii(data)

        assert "WARN" in output
        assert "1 warning(s)" in output

    def test_run_id_in_header(self, sample_rules):
        """Report header shows run_id."""
        data = _make_dq_data(rules=sample_rules)
        output = render_dq_ascii(data)

        assert "20260224_170809" in output

    def test_large_numeric_formatting(self):
        """Large numbers (>= 1000) formatted without decimals."""
        profiles = [ProfileSummary(
            node_id="p.x", name="x", row_count=10, column_count=1,
            columns=[ColumnStats(
                name="big_col",
                metrics={"null_percent": "0", "distinct_count": "5",
                         "min": "1000", "avg": "5000.5", "max": "10000"},
                detail={},
            )],
        )]
        data = _make_dq_data(profiles=profiles)
        output = render_dq_ascii(data)

        assert "5001" in output or "5000" in output  # rounded large avg
        assert "10000" in output

    def test_pipeline_overview_section(self):
        """Node summaries render a Pipeline Overview table."""
        summaries = [
            NodeSummary(
                node_id="source.orders", name="orders", node_type="source",
                status="success", row_count=1000, duration_ms=250, last_run="",
            ),
            NodeSummary(
                node_id="transform.clean", name="clean", node_type="transform",
                status="failed", row_count=0, duration_ms=50, last_run="",
            ),
        ]
        data = _make_dq_data(node_summaries=summaries)
        output = render_dq_ascii(data)

        assert "Pipeline Overview" in output
        assert "orders" in output
        assert "source" in output
        assert "SUCCESS" in output
        assert "FAILED" in output
        assert "2 nodes" in output

    def test_pipeline_overview_duration_formatting(self):
        """Duration >= 1000ms formatted as seconds."""
        summaries = [
            NodeSummary(
                node_id="source.a", name="slow_source", node_type="source",
                status="success", row_count=500, duration_ms=2500, last_run="",
            ),
        ]
        data = _make_dq_data(node_summaries=summaries)
        output = render_dq_ascii(data)

        assert "2.5s" in output

    def test_node_footer_counts(self):
        """Footer shows node success/failure counts."""
        summaries = [
            NodeSummary(
                node_id="source.a", name="a", node_type="source",
                status="success", row_count=100, duration_ms=50, last_run="",
            ),
            NodeSummary(
                node_id="source.b", name="b", node_type="source",
                status="success", row_count=200, duration_ms=80, last_run="",
            ),
            NodeSummary(
                node_id="transform.c", name="c", node_type="transform",
                status="failed", row_count=0, duration_ms=10, last_run="",
            ),
        ]
        data = _make_dq_data(node_summaries=summaries)
        output = render_dq_ascii(data)

        assert "3 nodes" in output
        assert "2 succeeded" in output
        assert "1 failed" in output


# ---------------------------------------------------------------------------
# TestGenerateDQHtml
# ---------------------------------------------------------------------------


class TestGenerateDQHtml:
    """Tests for generate_dq_html() function."""

    def test_produces_html_file(self, sample_profile_summary, sample_rules):
        """generate_dq_html creates an HTML file."""
        data = _make_dq_data(
            profiles=[sample_profile_summary],
            rules=sample_rules,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "dq_report.html"
            result = generate_dq_html(data, output, open_browser=False)

            assert result == output
            assert output.exists()
            content = output.read_text()
            assert "<!DOCTYPE html>" in content or "<html" in content

    def test_embeds_json_data(self, sample_profile_summary, sample_rules):
        """HTML contains embedded JSON with profile and rule data."""
        data = _make_dq_data(
            profiles=[sample_profile_summary],
            rules=sample_rules,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "dq_report.html"
            generate_dq_html(data, output, open_browser=False)

            content = output.read_text()
            assert "test_project" in content
            assert "orders_stats" in content

    def test_sanitizes_script_tags(self):
        """JSON with </script> gets escaped."""
        rules = [RuleResult(
            node_id="rule.xss",
            name='</script><script>alert("xss")</script>',
            rule_type="sql",
            status="pass",
            checks=[],
            message="test",
            duration_ms=0,
        )]
        data = _make_dq_data(rules=rules)

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "dq_report.html"
            generate_dq_html(data, output, open_browser=False)

            content = output.read_text()
            # The JSON portion should not have raw </script>
            # Search for the DATA assignment
            assert "<\\/script>" in content or "alert" not in content.split("</script>")[0]

    def test_rejects_insecure_path(self, sample_rules):
        """Raises error for insecure output paths."""
        data = _make_dq_data(rules=sample_rules)

        with pytest.raises(DQVisualizationError, match="Insecure output path"):
            generate_dq_html(data, Path("/tmp/dq_report.html"), open_browser=False)

    def test_creates_parent_dirs(self, sample_rules):
        """Creates parent directories if they don't exist."""
        data = _make_dq_data(rules=sample_rules)

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "nested" / "dir" / "dq_report.html"
            generate_dq_html(data, output, open_browser=False)

            assert output.exists()

    def test_no_browser_when_disabled(self, sample_rules):
        """Browser does not open when open_browser=False."""
        data = _make_dq_data(rules=sample_rules)

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "dq_report.html"
            with patch("seeknal.dag.dq.webbrowser.open") as mock_open:
                generate_dq_html(data, output, open_browser=False)
                mock_open.assert_not_called()

    def test_ssh_skips_browser(self, sample_rules):
        """SSH session detection skips browser open."""
        data = _make_dq_data(rules=sample_rules)

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "dq_report.html"
            with patch.dict(os.environ, {"SSH_CONNECTION": "1.2.3.4 5678 9.10.11.12 22"}):
                with patch("seeknal.dag.dq.webbrowser.open") as mock_open:
                    generate_dq_html(data, output, open_browser=True)
                    mock_open.assert_not_called()

    def test_opens_browser_when_not_ssh(self, sample_rules):
        """Browser opens when not in SSH session and open_browser=True."""
        data = _make_dq_data(rules=sample_rules)

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "dq_report.html"
            env = os.environ.copy()
            env.pop("SSH_CONNECTION", None)
            with patch.dict(os.environ, env, clear=True):
                with patch("seeknal.dag.dq.webbrowser.open") as mock_open:
                    generate_dq_html(data, output, open_browser=True)
                    mock_open.assert_called_once()

    def test_includes_chart_js(self, sample_rules):
        """HTML includes Chart.js CDN reference."""
        data = _make_dq_data(rules=sample_rules)

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "dq_report.html"
            generate_dq_html(data, output, open_browser=False)

            content = output.read_text()
            assert "chart.js" in content.lower() or "Chart" in content

    def test_empty_data_html(self):
        """HTML generated even with empty DQData."""
        data = _make_dq_data()

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "dq_report.html"
            result = generate_dq_html(data, output, open_browser=False)
            assert result.exists()
