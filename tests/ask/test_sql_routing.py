"""Tests for :mod:`seeknal.ask.sql_routing`.

The module answers one question — may this statement run on a remote source
instead of the local engine — from a parsed AST. These tests pin the two
properties that make the answer trustworthy: it never routes a statement that
could touch a local view, and a routed statement keeps its local meaning.
"""

from __future__ import annotations

import pytest

from seeknal.ask.sql_routing import analyze

REMOTE = {"warehouse"}


def _plan(sql: str, namespaces=REMOTE):
    return analyze(sql, namespaces, target_dialect="postgres")


# ---------------------------------------------------------------------------
# Routable: every table reference resolves to the one remote namespace
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT a FROM warehouse.public.t WHERE x = 1",
        "SELECT a, count(*) AS n FROM warehouse.public.t GROUP BY a",
        # A name the statement defines for itself is not a local table.
        "WITH c AS (SELECT a FROM warehouse.public.t) SELECT * FROM c",
        "WITH a AS (SELECT n FROM warehouse.public.t),"
        " b AS (SELECT n FROM warehouse.public.u)"
        " SELECT * FROM a UNION ALL SELECT * FROM b",
        # Two-part reference: namespace + table.
        "SELECT a FROM warehouse.t",
    ],
)
def test_routes_statements_confined_to_one_remote_namespace(sql):
    assert _plan(sql).namespace == "warehouse"


def test_routed_sql_drops_the_namespace_qualifier():
    """The remote engine has no such catalog, so it must not be addressed."""
    plan = _plan("SELECT a FROM warehouse.public.t")
    assert plan.is_routable
    assert "warehouse." not in plan.sql
    assert "public.t" in plan.sql


def test_routed_sql_preserves_null_ordering():
    """DuckDB sorts NULLs last on DESC; PostgreSQL sorts them first.

    Left implicit, an ordered query would come back in a different order than
    the local engine produces. Transpiling must make the local default explicit.
    """
    plan = _plan("SELECT a FROM warehouse.public.t ORDER BY a DESC")
    assert "NULLS LAST" in plan.sql.upper()


# ---------------------------------------------------------------------------
# Declined: anything that might not mean the same thing remotely
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql,expected_reason",
    [
        # A bare name may be a local parquet/Iceberg view.
        ("SELECT a FROM t", "unqualified table reference"),
        (
            "SELECT a FROM warehouse.public.t JOIN v ON v.k = t.k",
            "unqualified table reference",
        ),
        # A CTE does not excuse a sibling reference that is not self-defined.
        (
            "WITH c AS (SELECT a FROM warehouse.public.t) SELECT * FROM c JOIN v ON 1=1",
            "unqualified table reference",
        ),
        ("SELECT 1", "no qualified table reference"),
        # Engines invent different names for a projection the statement does
        # not alias: DuckDB says count_star(), PostgreSQL says count.
        ("SELECT COUNT(*) FROM warehouse.public.t", "unaliased projection"),
        (
            "SELECT date_trunc('year', d) FROM warehouse.public.t",
            "unaliased projection",
        ),
    ],
)
def test_declines_when_a_reference_may_be_local(sql, expected_reason):
    plan = _plan(sql)
    assert not plan.is_routable
    assert expected_reason in plan.reason


def test_declines_when_statement_spans_namespaces():
    plan = analyze(
        "SELECT * FROM ns_a.public.t JOIN ns_b.public.u ON 1=1",
        {"ns_a", "ns_b"},
        target_dialect="postgres",
    )
    assert not plan.is_routable
    assert "multiple namespaces" in plan.reason


def test_declines_unknown_namespace():
    plan = _plan("SELECT a FROM other.public.t")
    assert not plan.is_routable
    assert "not routable" in plan.reason


def test_declines_when_no_remote_source_configured():
    plan = analyze("SELECT a FROM warehouse.public.t", set(), target_dialect="postgres")
    assert not plan.is_routable
    assert "no remote namespace" in plan.reason


def test_declines_unparsable_sql_without_raising():
    plan = _plan("SELECT FROM WHERE ((((")
    assert not plan.is_routable
    assert plan.reason


def test_reason_is_empty_when_routable():
    """Callers log ``reason`` only for declines; a routed plan carries none."""
    assert _plan("SELECT a FROM warehouse.public.t").reason == ""
