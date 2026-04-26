"""Tests for Ask table discovery across attached source catalogs."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.describe_table import describe_table
from seeknal.ask.agents.tools.list_tables import list_tables


class _ReplWithAttached:
    attached = {"warehouse"}

    def execute_oneshot(self, sql: str, limit=None):
        normalized = " ".join(sql.split()).lower()
        if normalized == "show tables":
            return ["name"], [("local_customers",)]
        if '"warehouse".information_schema.tables' in normalized:
            return ["table_schema", "table_name", "table_type"], [
                ("analytics", "monthly_revenue", "VIEW"),
                ("mart", "orders", "BASE TABLE"),
            ]
        raise AssertionError(f"unexpected SQL: {sql}")


class _DescribeRepl:
    attached = {"wh"}

    def execute_oneshot(self, sql: str, limit=None):
        normalized = " ".join(sql.split()).lower()
        if '"wh".information_schema.tables' in normalized:
            return ["table_schema", "table_name"], [("analytics", "monthly_revenue")]
        assert sql == "DESCRIBE wh.analytics.monthly_revenue"
        return ["column_name", "column_type", "null"], [
            ("month", "DATE", "YES"),
            ("revenue", "DOUBLE", "YES"),
        ]


def test_list_tables_includes_attached_catalog_names(tmp_path: Path):
    set_tool_context(
        ToolContext(
            repl=_ReplWithAttached(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
        )
    )

    out = list_tables()

    assert "local_customers (project)" in out
    assert "warehouse.analytics.monthly_revenue (VIEW)" in out
    assert "warehouse.mart.orders (BASE TABLE)" in out


def test_list_tables_accepts_optional_glob_filter(tmp_path: Path):
    set_tool_context(
        ToolContext(
            repl=_ReplWithAttached(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
        )
    )

    out = list_tables(query="warehouse.analytics.*")

    assert "warehouse.analytics.monthly_revenue (VIEW)" in out
    assert "warehouse.mart.orders" not in out
    assert "local_customers" not in out


def test_describe_table_repairs_missing_catalog_separator(tmp_path: Path):
    set_tool_context(
        ToolContext(
            repl=_DescribeRepl(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
        )
    )

    out = describe_table("whanalytics.monthly_revenue")

    assert "Schema for `wh.analytics.monthly_revenue`" in out
    assert "`revenue` (DOUBLE)" in out


def test_describe_table_resolves_unique_unqualified_attached_table(tmp_path: Path):
    set_tool_context(
        ToolContext(
            repl=_DescribeRepl(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
        )
    )

    out = describe_table("monthly_revenue")

    assert "Schema for `wh.analytics.monthly_revenue`" in out
