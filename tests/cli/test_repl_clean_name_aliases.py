"""F2 regression tests: REPL registers clean (prefix-stripped) aliases for nodes.

The Ask agent / manifest surface friendly node names (``channel_profitability``)
but, before this fix, only the kind-prefixed view (``transform_channel_profitability``)
was queryable — so the agent hit "table does not exist". These tests lock in that
both the prefixed name AND the bare alias resolve, that sources are not clobbered,
and that base-name collisions are handled deterministically.
"""
from __future__ import annotations

from pathlib import Path

import duckdb


def _write_parquet(path: Path, sql: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    safe = str(path).replace("'", "''")
    con.execute(f"COPY ({sql}) TO '{safe}' (FORMAT PARQUET)")
    con.close()


def test_clean_name_aliases_registered(tmp_path: Path):
    from seeknal.cli.repl import REPL

    inter = tmp_path / "target" / "intermediate"
    _write_parquet(inter / "transform_foo.parquet", "SELECT 1 AS a UNION ALL SELECT 2")
    _write_parquet(inter / "source_bar.parquet", "SELECT 1 AS b UNION ALL SELECT 2 UNION ALL SELECT 3")

    repl = REPL(project_path=tmp_path, skip_history=True)
    try:
        # Prefixed views still resolve (unchanged behavior).
        assert repl.conn.execute("SELECT count(*) FROM transform_foo").fetchone()[0] == 2
        assert repl.conn.execute("SELECT count(*) FROM source_bar").fetchone()[0] == 3
        # F2: the clean prefix-stripped aliases now also resolve...
        assert repl.conn.execute("SELECT count(*) FROM foo").fetchone()[0] == 2
        assert repl.conn.execute("SELECT count(*) FROM bar").fetchone()[0] == 3
        # ...and point at the same data as the prefixed view.
        assert (
            repl.conn.execute("SELECT count(*) FROM foo").fetchone()[0]
            == repl.conn.execute("SELECT count(*) FROM transform_foo").fetchone()[0]
        )
    finally:
        repl.conn.close()


def test_clean_alias_collision_is_deterministic_and_non_clobbering(tmp_path: Path):
    from seeknal.cli.repl import REPL

    inter = tmp_path / "target" / "intermediate"
    # Same base name "baz" for a source and a transform — a genuine collision.
    _write_parquet(inter / "source_baz.parquet", "SELECT 'src' AS k")
    _write_parquet(inter / "transform_baz.parquet", "SELECT 'xfm' AS k UNION ALL SELECT 'xfm2'")

    repl = REPL(project_path=tmp_path, skip_history=True)
    try:
        # Both prefixed views remain available to disambiguate.
        assert repl.conn.execute("SELECT count(*) FROM source_baz").fetchone()[0] == 1
        assert repl.conn.execute("SELECT count(*) FROM transform_baz").fetchone()[0] == 2
        # The bare alias resolves deterministically to the alphabetically-first
        # kind (source < transform) — never ambiguous, never a clobber error.
        assert repl.conn.execute("SELECT count(*) FROM baz").fetchone()[0] == 1
        assert repl.conn.execute("SELECT k FROM baz").fetchone()[0] == "src"
    finally:
        repl.conn.close()


def test_clean_alias_does_not_shadow_legacy_cache_node(tmp_path: Path):
    """A bare alias must never pre-empt a real, distinct legacy cache node."""
    from seeknal.cli.repl import REPL

    inter = tmp_path / "target" / "intermediate"
    cache = tmp_path / "target" / "cache"
    _write_parquet(inter / "transform_orders.parquet", "SELECT 1 AS a UNION ALL SELECT 2 UNION ALL SELECT 3")
    _write_parquet(cache / "orders.parquet", "SELECT 99 AS a")  # distinct legacy node

    repl = REPL(project_path=tmp_path, skip_history=True)
    try:
        assert repl.conn.execute("SELECT count(*) FROM transform_orders").fetchone()[0] == 3
        # `orders` resolves to the REAL cache node, not the transform alias.
        assert repl.conn.execute("SELECT a FROM orders").fetchone()[0] == 99
        assert repl.conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 1
    finally:
        repl.conn.close()


def test_clean_alias_does_not_shadow_consolidated_entity(tmp_path: Path):
    """A `transform_entity_*` node must not shadow the consolidated `entity_*` view."""
    from seeknal.cli.repl import REPL

    inter = tmp_path / "target" / "intermediate"
    fs = tmp_path / "target" / "feature_store" / "customer"
    _write_parquet(inter / "transform_entity_customer.parquet", "SELECT 1 AS a UNION ALL SELECT 2")
    _write_parquet(fs / "features.parquet", "SELECT 7 AS customer_id")  # canonical entity

    repl = REPL(project_path=tmp_path, skip_history=True)
    try:
        # `entity_customer` resolves to the consolidated entity, not the transform.
        assert repl.conn.execute("SELECT customer_id FROM entity_customer").fetchone()[0] == 7
        assert repl.conn.execute("SELECT count(*) FROM transform_entity_customer").fetchone()[0] == 2
    finally:
        repl.conn.close()
