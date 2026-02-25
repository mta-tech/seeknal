"""Tests for sql_assertion rule type."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pandas as pd
import pytest

from seeknal.dag.manifest import Node, NodeType
from seeknal.workflow.executors.rule_executor import RuleExecutor
from seeknal.workflow.executors.base import (
    ExecutionContext,
    ExecutionStatus,
    ExecutorValidationError,
    ExecutorExecutionError,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_target(tmp_path):
    """Create target directory with intermediate parquets."""
    intermediate = tmp_path / "intermediate"
    intermediate.mkdir()
    return tmp_path


@pytest.fixture
def users_parquet(tmp_target):
    """Create a users parquet with some duplicate user_ids."""
    df = pd.DataFrame({
        "user_id": [1, 2, 3, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Charlie-dup", "Diana"],
        "age": [25, 30, 35, 35, 28],
    })
    path = tmp_target / "intermediate" / "source_users.parquet"
    df.to_parquet(path, index=False)
    return tmp_target


@pytest.fixture
def orders_parquet(tmp_target):
    """Create an orders parquet."""
    df = pd.DataFrame({
        "order_id": [1, 2, 3],
        "user_id": [1, 2, 99],  # user_id 99 has no matching user
        "amount": [10.0, 20.0, 30.0],
    })
    path = tmp_target / "intermediate" / "source_orders.parquet"
    df.to_parquet(path, index=False)
    return tmp_target


@pytest.fixture
def both_parquets(tmp_target):
    """Create both users and orders parquets for cross-table tests."""
    intermediate = tmp_target / "intermediate"

    users = pd.DataFrame({
        "user_id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
    })
    users.to_parquet(intermediate / "source_users.parquet", index=False)

    orders = pd.DataFrame({
        "order_id": [1, 2, 3],
        "user_id": [1, 2, 99],  # 99 = orphan
        "amount": [10.0, 20.0, 30.0],
    })
    orders.to_parquet(intermediate / "source_orders.parquet", index=False)
    return tmp_target


def make_context(target_path, dry_run=False):
    return ExecutionContext(
        project_name="test",
        workspace_path=target_path.parent,
        target_path=target_path,
        dry_run=dry_run,
    )


def make_node(name, rule_config, inputs):
    return Node(
        id=f"rule.{name}",
        name=name,
        node_type=NodeType.RULE,
        config={
            "rule": rule_config,
            "inputs": inputs,
        },
    )


# ---------------------------------------------------------------------------
# Validation Tests
# ---------------------------------------------------------------------------


class TestSqlAssertionValidation:
    """Tests for sql_assertion YAML validation."""

    def test_valid_config(self):
        node = make_node("dup_check", {
            "type": "sql_assertion",
            "sql": "SELECT user_id FROM input_data GROUP BY user_id HAVING COUNT(*) > 1",
        }, [{"ref": "source.users"}])
        ctx = make_context(Path("/tmp/fake"))
        executor = RuleExecutor(node, ctx)
        executor.validate()  # Should not raise

    def test_missing_sql_raises(self):
        node = make_node("bad", {
            "type": "sql_assertion",
        }, [{"ref": "source.users"}])
        ctx = make_context(Path("/tmp/fake"))
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorValidationError, match="requires a 'sql' string"):
            executor.validate()

    def test_empty_sql_raises(self):
        node = make_node("bad", {
            "type": "sql_assertion",
            "sql": "",
        }, [{"ref": "source.users"}])
        ctx = make_context(Path("/tmp/fake"))
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorValidationError, match="requires a 'sql' string"):
            executor.validate()

    def test_non_string_sql_raises(self):
        node = make_node("bad", {
            "type": "sql_assertion",
            "sql": 123,
        }, [{"ref": "source.users"}])
        ctx = make_context(Path("/tmp/fake"))
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorValidationError, match="requires a 'sql' string"):
            executor.validate()

    def test_sql_assertion_in_multi_check(self):
        """sql_assertion works inside a multi-check rule list."""
        node = make_node("multi", [
            {"type": "null", "columns": ["name"]},
            {"type": "sql_assertion", "sql": "SELECT 1 WHERE 1=0"},
        ], [{"ref": "source.users"}])
        ctx = make_context(Path("/tmp/fake"))
        executor = RuleExecutor(node, ctx)
        executor.validate()  # Should not raise


# ---------------------------------------------------------------------------
# Execution Tests - Single Input
# ---------------------------------------------------------------------------


class TestSqlAssertionExecution:
    """Tests for sql_assertion execution with single input."""

    def test_passing_assertion(self, users_parquet):
        """Query returning 0 rows = pass."""
        node = make_node("age_check", {
            "type": "sql_assertion",
            "sql": "SELECT * FROM input_data WHERE age < 0",
        }, [{"ref": "source.users"}])
        ctx = make_context(users_parquet)
        executor = RuleExecutor(node, ctx)
        executor.validate()
        result = executor.run()
        assert result.status == ExecutionStatus.SUCCESS
        assert result.metadata["passed"] is True
        assert result.metadata["violations"] == 0

    def test_failing_assertion_raises(self, users_parquet):
        """Query returning rows = fail (severity=error raises)."""
        node = make_node("dup_users", {
            "type": "sql_assertion",
            "sql": "SELECT user_id, COUNT(*) as cnt FROM input_data GROUP BY user_id HAVING cnt > 1",
        }, [{"ref": "source.users"}])
        ctx = make_context(users_parquet)
        executor = RuleExecutor(node, ctx)
        executor.validate()
        with pytest.raises(ExecutorExecutionError, match="SQL assertion failed"):
            executor.run()

    def test_failing_assertion_warn_severity(self, users_parquet):
        """With severity=warn, failing assertion returns SUCCESS with metadata."""
        node = Node(
            id="rule.dup_warn",
            name="dup_warn",
            node_type=NodeType.RULE,
            config={
                "rule": {
                    "type": "sql_assertion",
                    "sql": "SELECT user_id, COUNT(*) as cnt FROM input_data GROUP BY user_id HAVING cnt > 1",
                },
                "inputs": [{"ref": "source.users"}],
                "params": {"severity": "warn"},
            },
        )
        ctx = make_context(users_parquet)
        executor = RuleExecutor(node, ctx)
        executor.validate()
        result = executor.run()
        assert result.status == ExecutionStatus.SUCCESS
        assert result.metadata["passed"] is False
        assert result.metadata["violations"] > 0

    def test_named_view_matches_ref(self, users_parquet):
        """Input ref 'source.users' creates 'source_users' view."""
        node = make_node("named_view", {
            "type": "sql_assertion",
            "sql": 'SELECT * FROM source_users WHERE age < 0',
        }, [{"ref": "source.users"}])
        ctx = make_context(users_parquet)
        executor = RuleExecutor(node, ctx)
        result = executor.run()
        assert result.status == ExecutionStatus.SUCCESS
        assert result.metadata["passed"] is True

    def test_dry_run_skips_execution(self, users_parquet):
        """Dry run returns without executing the SQL."""
        node = make_node("dry", {
            "type": "sql_assertion",
            "sql": "SELECT 1",
        }, [{"ref": "source.users"}])
        ctx = make_context(users_parquet, dry_run=True)
        executor = RuleExecutor(node, ctx)
        result = executor.run()
        assert result.is_dry_run is True

    def test_invalid_sql_raises(self, users_parquet):
        """Bad SQL raises ExecutorExecutionError."""
        node = make_node("bad_sql", {
            "type": "sql_assertion",
            "sql": "SELECT * FROM nonexistent_table",
        }, [{"ref": "source.users"}])
        ctx = make_context(users_parquet)
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorExecutionError, match="sql_assertion query failed"):
            executor.run()

    def test_custom_error_message(self, users_parquet):
        """Custom error_message in params overrides default."""
        node = Node(
            id="rule.custom_msg",
            name="custom_msg",
            node_type=NodeType.RULE,
            config={
                "rule": {
                    "type": "sql_assertion",
                    "sql": "SELECT user_id, COUNT(*) as cnt FROM input_data GROUP BY user_id HAVING cnt > 1",
                },
                "inputs": [{"ref": "source.users"}],
                "params": {"error_message": "Duplicate users found!"},
            },
        )
        ctx = make_context(users_parquet)
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorExecutionError, match="Duplicate users found!"):
            executor.run()


# ---------------------------------------------------------------------------
# Execution Tests - Multiple Inputs (Cross-Table)
# ---------------------------------------------------------------------------


class TestSqlAssertionMultiInput:
    """Tests for sql_assertion with multiple input refs."""

    def test_cross_table_assertion_passing(self, both_parquets):
        """Cross-table check with all FKs valid passes when filtered to valid."""
        node = make_node("fk_valid", {
            "type": "sql_assertion",
            "sql": """
                SELECT o.order_id, o.user_id
                FROM source_orders o
                LEFT JOIN source_users u ON o.user_id = u.user_id
                WHERE u.user_id IS NULL
                  AND o.user_id IN (SELECT user_id FROM source_users)
            """,
        }, [{"ref": "source.orders"}, {"ref": "source.users"}])
        ctx = make_context(both_parquets)
        executor = RuleExecutor(node, ctx)
        result = executor.run()
        assert result.status == ExecutionStatus.SUCCESS

    def test_cross_table_orphan_detection(self, both_parquets):
        """Detect orphan order rows where user_id has no match."""
        node = make_node("fk_check", {
            "type": "sql_assertion",
            "sql": """
                SELECT o.order_id, o.user_id
                FROM source_orders o
                LEFT JOIN source_users u ON o.user_id = u.user_id
                WHERE u.user_id IS NULL
            """,
        }, [{"ref": "source.orders"}, {"ref": "source.users"}])
        ctx = make_context(both_parquets)
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorExecutionError, match="SQL assertion failed"):
            executor.run()

    def test_first_input_also_aliased_as_input_data(self, both_parquets):
        """First input is available as both 'source_orders' and 'input_data'."""
        node = make_node("alias_check", {
            "type": "sql_assertion",
            "sql": "SELECT * FROM input_data WHERE amount < 0",
        }, [{"ref": "source.orders"}, {"ref": "source.users"}])
        ctx = make_context(both_parquets)
        executor = RuleExecutor(node, ctx)
        result = executor.run()
        assert result.status == ExecutionStatus.SUCCESS

    def test_metadata_contains_sql(self, both_parquets):
        """Result metadata includes the original SQL for debugging."""
        sql = "SELECT * FROM source_orders WHERE amount < 0"
        node = make_node("meta_check", {
            "type": "sql_assertion",
            "sql": sql,
        }, [{"ref": "source.orders"}, {"ref": "source.users"}])
        ctx = make_context(both_parquets)
        executor = RuleExecutor(node, ctx)
        result = executor.run()
        assert result.metadata["sql"] == sql
        assert result.metadata["rule_type"] == "sql_assertion"
