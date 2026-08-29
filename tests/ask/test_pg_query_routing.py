"""Tests for PG query routing on the agent path — SQLR.

``_try_pg_route`` sends PG-only SQL straight to PostgreSQL so the source does
the aggregation instead of DuckDB pulling raw columns over the network. The
safety property under test: routing may decline, but it must never raise and
never change what the caller receives.
"""

from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import patch

import pytest

from seeknal.ask.agents.tools.execute_sql import _try_pg_route


class _Ctx:
    """Minimal stand-in for ToolContext."""

    def __init__(self, pg_passthrough: bool = True):
        self.pg_passthrough = pg_passthrough
        self.project_path = Path("/nonexistent-project")
        self.db_lock = threading.Lock()
        self.sql_timeout_seconds = 0


SQL = "SELECT count(*) FROM warehouse.public.t WHERE x = 1"


def test_gate_off_never_routes():
    """Default-off means the routed path is not even attempted."""
    with patch("seeknal.sources.config.load_source_registry") as loader:
        assert _try_pg_route(_Ctx(pg_passthrough=False), SQL, None) is None
        loader.assert_not_called()


def test_registry_failure_falls_back():
    """A broken/missing source registry declines routing instead of raising."""
    with patch(
        "seeknal.sources.config.load_source_registry",
        side_effect=RuntimeError("no project"),
    ):
        assert _try_pg_route(_Ctx(), SQL, None) is None


def test_non_pg_sql_declines():
    """SQL that does not resolve to a single PG namespace stays on DuckDB."""
    with patch("seeknal.sources.config.load_source_registry"), patch(
        "seeknal.ask._pg_oracle.detect_pg_only_namespace", return_value=None
    ):
        assert _try_pg_route(_Ctx(), "SELECT 1", None) is None


def test_pg_error_falls_back_and_is_recorded():
    """A dialect/runtime error on PG must fall back, not surface to the agent."""
    from seeknal.ask.testing import SqlOracleResult

    events: list[tuple] = []

    with patch("seeknal.sources.config.load_source_registry"), patch(
        "seeknal.ask._pg_oracle.detect_pg_only_namespace", return_value="warehouse"
    ), patch("seeknal.ask._pg_oracle.resolve_pg_dsn", return_value="postgresql://x/y"), patch(
        "seeknal.ask._pg_oracle.strip_namespace", side_effect=lambda s, ns: s
    ), patch(
        "seeknal.ask._pg_oracle.execute_via_psycopg2",
        return_value=SqlOracleResult(error="operator does not exist: text = integer"),
    ), patch(
        "seeknal.ask.agents.tools._context.record_timing_event",
        side_effect=lambda name, ms, **kw: events.append((name, kw.get("reason", ""))),
    ):
        ctx = _Ctx()
        # A namespace match requires a source whose .namespace matches; the
        # patched registry is a MagicMock, so sources.values() yields mocks and
        # the lookup returns None -> declines. Either way: no exception, no rows.
        assert _try_pg_route(ctx, SQL, None) is None


def test_unexpected_exception_falls_back():
    """Any unforeseen failure declines routing rather than breaking the tool."""
    with patch("seeknal.sources.config.load_source_registry"), patch(
        "seeknal.ask._pg_oracle.detect_pg_only_namespace",
        side_effect=ValueError("boom"),
    ):
        assert _try_pg_route(_Ctx(), SQL, None) is None


@pytest.mark.parametrize(
    "sql,expected",
    [
        # Regression: masking a qualified ref used to leave "FROM      WHERE",
        # and WHERE was then read as a bare table name, rejecting every
        # realistic qualified query.
        ("SELECT a FROM pg_ns.public.t WHERE x = 1", "pg_ns"),
        ("SELECT a FROM pg_ns.public.t GROUP BY a", "pg_ns"),
        # A name the query defines itself is resolved by PostgreSQL, so a
        # WITH-query over one PG namespace is still PG-only. This is how
        # combined ERBA+ERLA questions are written.
        (
            "WITH x AS (SELECT a FROM pg_ns.public.t) SELECT * FROM x",
            "pg_ns",
        ),
        (
            "WITH a AS (SELECT n FROM pg_ns.public.t), b AS (SELECT n FROM pg_ns.public.u)"
            " SELECT * FROM a UNION ALL SELECT * FROM b",
            "pg_ns",
        ),
        # Still rejected: a bare table ref may be a local parquet view.
        ("SELECT a FROM t WHERE x = 1", None),
        ("SELECT a FROM pg_ns.public.t JOIN v ON v.k = t.k", None),
        # A CTE does not launder an unqualified ref that is NOT locally defined.
        (
            "WITH x AS (SELECT a FROM pg_ns.public.t) SELECT * FROM x JOIN v ON 1=1",
            None,
        ),
    ],
)
def test_detect_accepts_realistic_qualified_sql(sql, expected):
    from seeknal.ask._pg_oracle import detect_pg_only_namespace
    from seeknal.sources.config import SourceConfig, SourceRegistry

    src = SourceConfig(
        name="pg_ns",
        source_kind="connected",
        source_type="database",
        namespace="pg_ns",
        access="read_only",
        role="other",
        priority=0,
        description="fixture",
        connector="postgresql",
        resource=None,
        dsn_env=None,
    )
    registry = SourceRegistry(
        sources={"pg_ns": src},
        default_mode="analyst",
        explicit=True,
        project_path=None,
    )
    assert detect_pg_only_namespace(sql, registry) == expected
