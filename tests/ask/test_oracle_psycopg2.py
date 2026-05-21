"""Tests for the psycopg2 oracle path — issue #64.

The PG-direct path bypasses DuckDB's postgres_scanner for SQL that
references only one connected PG namespace. This file covers:

- Phase C (helpers): ``detect_pg_only_namespace``, ``resolve_pg_dsn``,
  ``strip_namespace``, ``execute_via_psycopg2``.
- Phase D (integration): ``execute_expected_sql`` branching.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _make_source(
    name: str,
    namespace: str,
    *,
    source_kind: str = "connected",
    source_type: str = "database",
    connector: str = "postgresql",
    is_read_only: bool = True,
    dsn_env: str | None = None,
) -> Any:
    """Construct a minimal SourceConfig fixture without exercising YAML I/O."""
    from seeknal.sources.config import SourceConfig

    src = SourceConfig(
        name=name,
        source_kind=source_kind,
        source_type=source_type,
        namespace=namespace,
        access="read_only" if is_read_only else "read_write",
        role="other",
        priority=0,
        description="fixture",
        connector=connector,
        resource=None,
        dsn_env=dsn_env,
    )
    return src


def _make_registry(sources_list: list[Any]) -> Any:
    from seeknal.sources.config import SourceRegistry

    return SourceRegistry(
        sources={src.name: src for src in sources_list},
        default_mode="analyst",
        explicit=True,
        project_path=None,
    )


# ---------------------------------------------------------------------------
# AC-C1: detect — single connected-PG namespace -> namespace returned
# ---------------------------------------------------------------------------


def test_ac_c1_detect_single_pg_namespace():
    from seeknal.ask._pg_oracle import detect_pg_only_namespace

    registry = _make_registry([_make_source("pg_src", "pg_ns")])
    ns = detect_pg_only_namespace(
        "SELECT * FROM pg_ns.public.t WHERE pg_ns.public.t.x = 1",
        registry,
    )
    assert ns == "pg_ns"


# ---------------------------------------------------------------------------
# AC-C2: detect — mixed PG + non-PG namespace -> None
# ---------------------------------------------------------------------------


def test_ac_c2_detect_mixed_pg_and_other_returns_none():
    from seeknal.ask._pg_oracle import detect_pg_only_namespace

    registry = _make_registry([_make_source("pg_src", "pg_ns")])
    ns = detect_pg_only_namespace(
        "SELECT * FROM pg_ns.public.t JOIN other.x.y ON t.k = y.k",
        registry,
    )
    assert ns is None


# ---------------------------------------------------------------------------
# AC-C3: detect — two distinct PG namespaces -> None
# ---------------------------------------------------------------------------


def test_ac_c3_detect_two_pg_namespaces_returns_none():
    from seeknal.ask._pg_oracle import detect_pg_only_namespace

    registry = _make_registry(
        [_make_source("a_src", "ns_a"), _make_source("b_src", "ns_b")]
    )
    ns = detect_pg_only_namespace(
        "SELECT * FROM ns_a.public.t JOIN ns_b.public.u ON t.k = u.k",
        registry,
    )
    assert ns is None


# ---------------------------------------------------------------------------
# AC-C4: detect — unqualified FROM forces None (force DuckDB fallback)
# ---------------------------------------------------------------------------


def test_ac_c4_detect_unqualified_from_returns_none():
    from seeknal.ask._pg_oracle import detect_pg_only_namespace

    registry = _make_registry([_make_source("pg_src", "pg_ns")])
    ns = detect_pg_only_namespace("SELECT * FROM t WHERE x = 1", registry)
    assert ns is None


# ---------------------------------------------------------------------------
# AC-C5: detect — mixed qualified-PG + unqualified parquet view -> None
# ---------------------------------------------------------------------------


def test_ac_c5_detect_mixed_qualified_and_unqualified_returns_none():
    from seeknal.ask._pg_oracle import detect_pg_only_namespace

    registry = _make_registry([_make_source("pg_src", "pg_ns")])
    ns = detect_pg_only_namespace(
        "SELECT * FROM pg_ns.public.t JOIN parquet_view v ON v.x = t.y",
        registry,
    )
    assert ns is None


# ---------------------------------------------------------------------------
# AC-C6: DSN — env var set returns URL as-is
# ---------------------------------------------------------------------------


def test_ac_c6_dsn_env_var_returns_url(monkeypatch):
    from seeknal.ask._pg_oracle import resolve_pg_dsn

    monkeypatch.setenv("MY_PG_DSN", "postgresql://u:p@h:5432/d")
    src = _make_source("pg_src", "pg_ns", dsn_env="MY_PG_DSN")
    dsn = resolve_pg_dsn(src, {"connections": {}})
    assert dsn == "postgresql://u:p@h:5432/d"


# ---------------------------------------------------------------------------
# AC-C7: DSN — env var unset; connections[source.name] composes URL
# ---------------------------------------------------------------------------


def test_ac_c7_dsn_via_connections_by_name(monkeypatch):
    from seeknal.ask._pg_oracle import resolve_pg_dsn

    monkeypatch.delenv("MY_PG_DSN", raising=False)
    src = _make_source("pg_src", "pg_ns", dsn_env="MY_PG_DSN")
    dsn = resolve_pg_dsn(
        src,
        {
            "connections": {
                "pg_src": {
                    "type": "postgresql",
                    "host": "h1",
                    "port": 5432,
                    "user": "u1",
                    "password": "p1",
                    "database": "db1",
                }
            }
        },
    )
    assert "h1" in dsn
    assert "u1" in dsn
    assert "p1" in dsn
    assert "db1" in dsn
    assert dsn.startswith("postgresql://")


# ---------------------------------------------------------------------------
# AC-C8: DSN — env unset, only connections[source.namespace] -> URL composed
# ---------------------------------------------------------------------------


def test_ac_c8_dsn_via_connections_by_namespace(monkeypatch):
    from seeknal.ask._pg_oracle import resolve_pg_dsn

    monkeypatch.delenv("MY_PG_DSN", raising=False)
    src = _make_source("pg_src", "pg_ns", dsn_env="MY_PG_DSN")
    dsn = resolve_pg_dsn(
        src,
        {
            "connections": {
                "pg_ns": {
                    "type": "postgresql",
                    "host": "h2",
                    "port": 5432,
                    "user": "u2",
                    "password": "p2",
                    "database": "db2",
                }
            }
        },
    )
    assert "h2" in dsn
    assert "db2" in dsn


# ---------------------------------------------------------------------------
# AC-C9: DSN — both blocks present, name takes precedence
# ---------------------------------------------------------------------------


def test_ac_c9_dsn_name_wins_over_namespace(monkeypatch):
    from seeknal.ask._pg_oracle import resolve_pg_dsn

    monkeypatch.delenv("MY_PG_DSN", raising=False)
    src = _make_source("pg_src", "pg_ns", dsn_env="MY_PG_DSN")
    dsn = resolve_pg_dsn(
        src,
        {
            "connections": {
                "pg_src": {
                    "type": "postgresql",
                    "host": "by_name",
                    "port": 5432,
                    "user": "u",
                    "password": "p",
                    "database": "db_name",
                },
                "pg_ns": {
                    "type": "postgresql",
                    "host": "by_namespace",
                    "port": 5432,
                    "user": "u",
                    "password": "p",
                    "database": "db_namespace",
                },
            }
        },
    )
    assert "by_name" in dsn
    assert "by_namespace" not in dsn


# ---------------------------------------------------------------------------
# AC-C10: DSN — env unset + no profile block -> RuntimeError with env-var hint
# ---------------------------------------------------------------------------


def test_ac_c10_dsn_raises_runtime_error_with_hint(monkeypatch):
    from seeknal.ask._pg_oracle import resolve_pg_dsn

    monkeypatch.delenv("MY_PG_DSN", raising=False)
    src = _make_source("pg_src", "pg_ns", dsn_env="MY_PG_DSN")
    with pytest.raises(RuntimeError) as excinfo:
        resolve_pg_dsn(src, {"connections": {}})
    message = str(excinfo.value)
    assert "MY_PG_DSN" in message
    assert "export" in message


# ---------------------------------------------------------------------------
# AC-C11: strip_namespace strips 3-part and 2-part refs
# ---------------------------------------------------------------------------


def test_ac_c11_strip_namespace_basic():
    from seeknal.ask._pg_oracle import strip_namespace

    out = strip_namespace(
        "SELECT * FROM pg_ns.public.t WHERE pg_ns.public.t.x = 1", "pg_ns"
    )
    assert out == "SELECT * FROM public.t WHERE public.t.x = 1"


# ---------------------------------------------------------------------------
# AC-C12: strip_namespace does NOT touch other namespaces
# ---------------------------------------------------------------------------


def test_ac_c12_strip_namespace_leaves_other_namespace():
    from seeknal.ask._pg_oracle import strip_namespace

    out = strip_namespace(
        "SELECT * FROM pg_ns.public.t JOIN other_ns.public.t ON 1=1",
        "pg_ns",
    )
    assert "other_ns.public.t" in out
    assert "pg_ns.public.t" not in out


# ---------------------------------------------------------------------------
# AC-C13: strip_namespace handles namespace == schema name collision
# ---------------------------------------------------------------------------


def test_ac_c13_strip_namespace_public_collision():
    from seeknal.ask._pg_oracle import strip_namespace

    out = strip_namespace("SELECT * FROM public.t", "public")
    assert out == "SELECT * FROM t"


# ---------------------------------------------------------------------------
# AC-C14: execute_via_psycopg2 returns rows with _jsonable cells
# ---------------------------------------------------------------------------


def test_ac_c14_execute_via_psycopg2_returns_rows():
    from seeknal.ask._pg_oracle import execute_via_psycopg2

    mock_psycopg2 = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    mock_cursor.description = [("col_a",), ("col_b",)]
    mock_cursor.fetchall.return_value = [(1, "x"), (2, "y")]
    mock_conn.cursor.return_value = mock_cursor
    mock_psycopg2.connect.return_value = mock_conn

    with patch("seeknal.ask._pg_oracle.psycopg2", mock_psycopg2):
        # Provide a tool context so record_timing_event has somewhere to write.
        _install_tool_context()
        result = execute_via_psycopg2("postgresql://x/y", "SELECT * FROM t")

    assert result.columns == ["col_a", "col_b"]
    assert result.rows == [[1, "x"], [2, "y"]]


# ---------------------------------------------------------------------------
# AC-C15: non-SELECT SQL rejected, psycopg2 never called
# ---------------------------------------------------------------------------


def test_ac_c15_non_select_sql_rejected():
    from seeknal.ask._pg_oracle import execute_via_psycopg2

    mock_psycopg2 = MagicMock()

    with patch("seeknal.ask._pg_oracle.psycopg2", mock_psycopg2):
        result = execute_via_psycopg2("postgresql://x/y", "UPDATE t SET x = 1")

    assert result.error is not None
    assert "SELECT" in result.error or "read" in result.error.lower()
    mock_psycopg2.connect.assert_not_called()


# ---------------------------------------------------------------------------
# AC-C16: lifecycle assertions — set_session BEFORE cursor()
# ---------------------------------------------------------------------------


def test_ac_c16_lifecycle_set_session_before_cursor():
    from seeknal.ask._pg_oracle import execute_via_psycopg2

    mock_psycopg2 = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    mock_cursor.description = [("n",)]
    mock_cursor.fetchall.return_value = [(1,)]
    mock_conn.cursor.return_value = mock_cursor
    mock_psycopg2.connect.return_value = mock_conn

    with patch("seeknal.ask._pg_oracle.psycopg2", mock_psycopg2):
        _install_tool_context()
        execute_via_psycopg2("postgresql://x/y", "SELECT 1")

    # autocommit was set
    assert mock_conn.autocommit is True
    # set_session was called with readonly=True
    mock_conn.set_session.assert_called_once_with(readonly=True)
    # set_session must precede cursor()
    methods = [c[0] for c in mock_conn.method_calls]
    set_session_pos = methods.index("set_session")
    cursor_pos = methods.index("cursor")
    assert set_session_pos < cursor_pos, (
        f"set_session (idx {set_session_pos}) must precede cursor() "
        f"(idx {cursor_pos}); ordering: {methods}"
    )


# ---------------------------------------------------------------------------
# AC-C17: distinct timing label
# ---------------------------------------------------------------------------


def test_ac_c17_timing_label_is_distinct():
    from seeknal.ask._pg_oracle import execute_via_psycopg2

    mock_psycopg2 = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    mock_cursor.description = [("n",)]
    mock_cursor.fetchall.return_value = [(1,)]
    mock_conn.cursor.return_value = mock_cursor
    mock_psycopg2.connect.return_value = mock_conn

    with patch("seeknal.ask._pg_oracle.psycopg2", mock_psycopg2), patch(
        "seeknal.ask._pg_oracle.record_timing_event"
    ) as mock_timing:
        _install_tool_context()
        execute_via_psycopg2("postgresql://x/y", "SELECT 1")
        # Must be called with the dedicated label.
        call_names = [c.args[0] for c in mock_timing.call_args_list]
        assert "execute_sql_pg_direct" in call_names
        assert "execute_sql" not in call_names


# ---------------------------------------------------------------------------
# Phase D — execute_expected_sql branching
# ---------------------------------------------------------------------------


def _write_agent_yml(tmp_path: Path, body: str) -> None:
    (tmp_path / "seeknal_agent.yml").write_text(body)


def test_ac_d1_no_pg_sources_uses_repl_path(tmp_path):
    """AC-D1: when no connected PG sources exist, REPL path is used."""
    from seeknal.ask.testing import execute_expected_sql

    # No seeknal_agent.yml -> implicit registry (no connected PG sources).
    # Use a SQL that DuckDB in-memory can run.
    with patch("seeknal.cli.repl.REPL") as mock_repl_cls:
        mock_repl = MagicMock()
        mock_repl.execute_oneshot.return_value = (["n"], [(1,)])
        mock_repl_cls.return_value = mock_repl

        result = execute_expected_sql(tmp_path, "SELECT 1 AS n")

    assert result.error is None
    assert result.columns == ["n"]
    assert result.rows == [[1]]
    mock_repl_cls.assert_called_once()


def test_ac_d2_missing_dsn_env_propagates_runtime_error(tmp_path, monkeypatch):
    """AC-D2: missing dsn_env env var + no profile block -> RuntimeError.

    The error must propagate out (no silent DuckDB fallback) and contain
    the env-var name.
    """
    from seeknal.ask.testing import execute_expected_sql

    monkeypatch.delenv("MY_PG_DSN_AC_D2", raising=False)
    _write_agent_yml(
        tmp_path,
        """
sources:
  pg_src:
    source_kind: connected
    source_type: database
    namespace: pg_ns
    access: read_only
    role: other
    description: fixture
    connector: postgresql
    dsn_env: MY_PG_DSN_AC_D2
""".strip(),
    )

    with pytest.raises(RuntimeError) as excinfo:
        execute_expected_sql(tmp_path, "SELECT * FROM pg_ns.public.t")
    assert "MY_PG_DSN_AC_D2" in str(excinfo.value)


def test_ac_d3_psycopg2_operational_error_surfaces_no_repl(tmp_path, monkeypatch):
    """AC-D3: psycopg2.OperationalError surfaces in SqlOracleResult.error.

    REPL must NEVER be instantiated when routing succeeded.
    """
    from seeknal.ask.testing import execute_expected_sql

    monkeypatch.setenv("MY_PG_DSN_D3", "postgresql://u:p@h/db")
    _write_agent_yml(
        tmp_path,
        """
sources:
  pg_src:
    source_kind: connected
    source_type: database
    namespace: pg_ns
    access: read_only
    role: other
    description: fixture
    connector: postgresql
    dsn_env: MY_PG_DSN_D3
""".strip(),
    )

    # Build the operational error class on the mocked module
    mock_psycopg2 = MagicMock()
    mock_psycopg2.OperationalError = type("OperationalError", (Exception,), {})
    mock_psycopg2.ProgrammingError = type("ProgrammingError", (Exception,), {})
    mock_psycopg2.connect.side_effect = mock_psycopg2.OperationalError(
        "connection refused"
    )

    with patch("seeknal.ask._pg_oracle.psycopg2", mock_psycopg2), patch(
        "seeknal.cli.repl.REPL"
    ) as mock_repl_cls:
        result = execute_expected_sql(
            tmp_path, "SELECT * FROM pg_ns.public.t"
        )

    assert result.error is not None
    assert "connection refused" in result.error
    mock_repl_cls.assert_not_called()


def test_ac_d4_divergence_smoke_jsonable_parity(tmp_path, monkeypatch):
    """AC-D4: trivial SELECT 1 AS n yields byte-identical result via both paths.

    Engine divergence on _jsonable layout would surface here.
    """
    from seeknal.ask.testing import execute_expected_sql

    # ---- psycopg2 path ----
    monkeypatch.setenv("MY_PG_DSN_D4", "postgresql://u:p@h/db")
    _write_agent_yml(
        tmp_path,
        """
sources:
  pg_src:
    source_kind: connected
    source_type: database
    namespace: pg_ns
    access: read_only
    role: other
    description: fixture
    connector: postgresql
    dsn_env: MY_PG_DSN_D4
""".strip(),
    )

    mock_psycopg2 = MagicMock()
    mock_psycopg2.OperationalError = type("OperationalError", (Exception,), {})
    mock_psycopg2.ProgrammingError = type("ProgrammingError", (Exception,), {})
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    mock_cursor.description = [("n",)]
    mock_cursor.fetchall.return_value = [(1,)]
    mock_conn.cursor.return_value = mock_cursor
    mock_psycopg2.connect.return_value = mock_conn

    with patch("seeknal.ask._pg_oracle.psycopg2", mock_psycopg2):
        pg_result = execute_expected_sql(
            tmp_path, "SELECT n FROM pg_ns.public.t"
        )

    # ---- DuckDB path (no agent yml, so no PG routing) ----
    other_tmp = tmp_path.parent / "duck_only"
    other_tmp.mkdir()
    with patch("seeknal.cli.repl.REPL") as mock_repl_cls:
        mock_repl = MagicMock()
        mock_repl.execute_oneshot.return_value = (["n"], [(1,)])
        mock_repl_cls.return_value = mock_repl
        duck_result = execute_expected_sql(other_tmp, "SELECT 1 AS n")

    # Byte-identical columns + rows
    assert pg_result.columns == duck_result.columns == ["n"]
    assert pg_result.rows == duck_result.rows == [[1]]


def test_ac_d5_source_config_error_falls_back_silently(tmp_path):
    """AC-D5: SourceConfigError during detection -> silent REPL fallback."""
    from seeknal.ask.testing import execute_expected_sql
    from seeknal.sources.config import SourceConfigError

    with patch(
        "seeknal.ask._pg_oracle.detect_pg_only_namespace",
        side_effect=SourceConfigError("bad yaml"),
    ), patch("seeknal.cli.repl.REPL") as mock_repl_cls:
        mock_repl = MagicMock()
        mock_repl.execute_oneshot.return_value = (["n"], [(1,)])
        mock_repl_cls.return_value = mock_repl

        result = execute_expected_sql(tmp_path, "SELECT 1 AS n")

    assert result.error is None
    assert result.rows == [[1]]


def test_ac_d6_profile_loader_private_api_exists():
    """AC-D6: regression test for ProfileLoader._load_profile_data coupling.

    If this attribute is renamed, the assertion failure points back to
    the plan ADR Consequences entry that documents the coupling.
    """
    from seeknal.workflow.materialization.profile_loader import ProfileLoader

    assert hasattr(ProfileLoader, "_load_profile_data"), (
        "ProfileLoader._load_profile_data was renamed; update "
        "execute_expected_sql in seeknal/ask/testing.py and remove this "
        "guard. See ralplan-pg-extract-pushdown-issue-64.md ADR "
        "Consequences (private-API coupling, MAJOR #2)."
    )
    assert callable(ProfileLoader._load_profile_data)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _install_tool_context() -> None:
    """Install a minimal ToolContext so record_timing_event has a sink."""
    from seeknal.ask.agents.tools._context import ToolContext, set_tool_context

    set_tool_context(
        ToolContext(
            repl=MagicMock(),
            artifact_discovery=MagicMock(),
            project_path=Path("/tmp"),
        )
    )
