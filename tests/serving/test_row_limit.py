"""End-to-end tests for row-limit semantics and DuckDBAdapter truncation probe."""

from __future__ import annotations

import hashlib
import time

import duckdb
import pytest

from seeknal.serving.adapters import DuckDBAdapter, QueryResult
from seeknal.serving.enforce import EnforcementError, Grant, enforce
from seeknal.serving.service import PreparedQuery


@pytest.fixture
def duckdb_conn():
    """In-memory DuckDB connection preloaded with a 500-row items table."""
    con = duckdb.connect()
    con.execute("CREATE TABLE items (id INT, name VARCHAR)")
    con.execute("INSERT INTO items SELECT i, 'item_' || i FROM range(500) t(i)")
    try:
        yield con
    finally:
        con.close()


def _make_grant(
    sql: str,
    *,
    max_rows: int = 50,
) -> tuple[Grant, dict[str, list[str]]]:
    """Helper to construct Grant and schemas for test queries."""
    claims = {
        "sub": "user:test",
        "ws": "ws-test",
        "jti": "jti-001",
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": max_rows,
        "exp": time.time() + 3600.0,
        "tables": {
            "items": {
                "columns": ["id", "name"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }
    schemas = {"items": ["id", "name"]}
    return Grant.from_claims(claims), schemas


def _run_enforced(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    *,
    grant_max_rows: int = 50,
    request_max_rows: int | None = None,
    dialect: str = "postgres",
) -> tuple[QueryResult, int | None, str]:
    """Enforce SQL and execute via DuckDBAdapter."""
    grant, schemas = _make_grant(sql, max_rows=grant_max_rows)
    enforced = enforce(
        sql,
        grant,
        schemas=schemas,
        target_dialect="duckdb",
        target_kind="duckdb",
        dialect=dialect,
        max_rows=request_max_rows,
    )
    adapter = DuckDBAdapter(lambda: con)
    prep = PreparedQuery(
        engine="duck",
        engine_kind="duckdb",
        sql=enforced.sql,
        params=(),
        paramstyle="qmark",
        rule="test",
        reason="test",
        row_limit=enforced.row_limit,
        masked_columns=enforced.masked_columns,
        redacted_sql=enforced.redacted_sql,
        subject=grant.subject,
        workspace=grant.workspace,
        audit_id=grant.audit_id,
        cube_security_context=enforced.cube_security_context,
    )
    result = adapter.execute(prep, timeout_seconds=10.0)
    return result, enforced.row_limit, enforced.sql


@pytest.mark.parametrize("limit", [1, 3, 5])
def test_user_limit_small_returns_exact_rows_untruncated(duckdb_conn, limit: int):
    """LIMIT 1, 3, 5 returns exactly that many rows with truncated=False."""
    sql = f"SELECT id, name FROM items ORDER BY id LIMIT {limit}"
    result, row_limit, enforced_sql = _run_enforced(duckdb_conn, sql, grant_max_rows=50)

    assert row_limit == 51
    assert f"LIMIT {limit}" in enforced_sql
    assert len(result.rows) == limit
    assert result.truncated is False
    assert [r[0] for r in result.rows] == list(range(limit))


def test_user_limit_zero_returns_zero_rows_untruncated(duckdb_conn):
    """LIMIT 0 returns 0 rows with truncated=False."""
    sql = "SELECT id, name FROM items ORDER BY id LIMIT 0"
    result, row_limit, enforced_sql = _run_enforced(duckdb_conn, sql, grant_max_rows=50)

    assert row_limit == 51
    assert "LIMIT 0" in enforced_sql
    assert len(result.rows) == 0
    assert result.truncated is False


def test_user_limit_equals_max_rows_untruncated(duckdb_conn):
    """LIMIT == max_rows returns exactly max_rows with truncated=False."""
    sql = "SELECT id, name FROM items ORDER BY id LIMIT 50"
    result, row_limit, enforced_sql = _run_enforced(duckdb_conn, sql, grant_max_rows=50)

    assert row_limit == 51
    assert "LIMIT 50" in enforced_sql
    assert len(result.rows) == 50
    assert result.truncated is False
    assert [r[0] for r in result.rows] == list(range(50))


def test_user_limit_greater_than_max_rows_truncated(duckdb_conn):
    """LIMIT > max_rows returns max_rows with truncated=True."""
    sql = "SELECT id, name FROM items ORDER BY id LIMIT 100"
    result, row_limit, enforced_sql = _run_enforced(duckdb_conn, sql, grant_max_rows=50)

    assert row_limit == 51
    assert "LIMIT 51" in enforced_sql
    assert len(result.rows) == 50
    assert result.truncated is True
    assert [r[0] for r in result.rows] == list(range(50))


def test_no_user_limit_truncated_at_max_rows(duckdb_conn):
    """No LIMIT over 500 rows returns max_rows with truncated=True."""
    sql = "SELECT id, name FROM items ORDER BY id"
    result, row_limit, enforced_sql = _run_enforced(duckdb_conn, sql, grant_max_rows=50)

    assert row_limit == 51
    assert "LIMIT 51" in enforced_sql
    assert len(result.rows) == 50
    assert result.truncated is True
    assert [r[0] for r in result.rows] == list(range(50))


def test_request_max_rows_lower_than_grant(duckdb_conn):
    """Request max_rows lower than the grant caps row_limit to request max_rows + 1."""
    # 1. Smaller user LIMIT within request max_rows (LIMIT 10, req_max=20, grant_max=50)
    sql_small = "SELECT id, name FROM items ORDER BY id LIMIT 10"
    res_small, row_lim_small, enforced_sql_small = _run_enforced(
        duckdb_conn,
        sql_small,
        grant_max_rows=50,
        request_max_rows=20,
    )
    assert row_lim_small == 21
    assert "LIMIT 10" in enforced_sql_small
    assert len(res_small.rows) == 10
    assert res_small.truncated is False

    # 2. No LIMIT: truncated at request max_rows (20)
    sql_none = "SELECT id, name FROM items ORDER BY id"
    res_none, row_lim_none, enforced_sql_none = _run_enforced(
        duckdb_conn,
        sql_none,
        grant_max_rows=50,
        request_max_rows=20,
    )
    assert row_lim_none == 21
    assert "LIMIT 21" in enforced_sql_none
    assert len(res_none.rows) == 20
    assert res_none.truncated is True

    # 3. User LIMIT larger than request max_rows (LIMIT 30, req_max=20)
    sql_large = "SELECT id, name FROM items ORDER BY id LIMIT 30"
    res_large, row_lim_large, enforced_sql_large = _run_enforced(
        duckdb_conn,
        sql_large,
        grant_max_rows=50,
        request_max_rows=20,
    )
    assert row_lim_large == 21
    assert "LIMIT 21" in enforced_sql_large
    assert len(res_large.rows) == 20
    assert res_large.truncated is True


def test_limit_with_offset(duckdb_conn):
    """LIMIT n OFFSET m preserves OFFSET and returns n rows untruncated."""
    sql = "SELECT id, name FROM items ORDER BY id LIMIT 5 OFFSET 10"
    result, row_limit, enforced_sql = _run_enforced(duckdb_conn, sql, grant_max_rows=50)

    assert row_limit == 51
    assert "LIMIT 5" in enforced_sql
    assert "OFFSET 10" in enforced_sql
    assert len(result.rows) == 5
    assert result.truncated is False
    assert [r[0] for r in result.rows] == [10, 11, 12, 13, 14]


def test_union_all_with_top_level_limit(duckdb_conn):
    """UNION ALL with top-level LIMIT preserves UNION and rewrites outer limit."""
    sql = (
        "SELECT id, name FROM items WHERE id < 10 "
        "UNION ALL "
        "SELECT id, name FROM items WHERE id >= 100 AND id < 110 "
        "ORDER BY id LIMIT 5"
    )
    result, row_limit, enforced_sql = _run_enforced(duckdb_conn, sql, grant_max_rows=50)

    assert row_limit == 51
    assert "LIMIT 5" in enforced_sql
    assert len(result.rows) == 5
    assert result.truncated is False
    assert [r[0] for r in result.rows] == [0, 1, 2, 3, 4]


def test_subquery_with_own_limit(duckdb_conn):
    """Subquery limits are untouched while top-level query gets cap/limit."""
    # Outer query without explicit limit gets cap (51), subquery LIMIT 3 is untouched
    sql = "SELECT sub.id, sub.name FROM (SELECT id, name FROM items ORDER BY id LIMIT 3) AS sub"
    result, row_limit, enforced_sql = _run_enforced(duckdb_conn, sql, grant_max_rows=50)

    assert row_limit == 51
    assert "LIMIT 3" in enforced_sql
    assert "LIMIT 51" in enforced_sql
    assert len(result.rows) == 3
    assert result.truncated is False
    assert [r[0] for r in result.rows] == [0, 1, 2]

    # Outer query with its own LIMIT 2 over subquery with LIMIT 10
    sql_outer_limit = (
        "SELECT sub.id, sub.name FROM (SELECT id, name FROM items ORDER BY id LIMIT 10) AS sub "
        "ORDER BY sub.id LIMIT 2"
    )
    res_outer, row_lim_outer, enforced_sql_outer = _run_enforced(
        duckdb_conn, sql_outer_limit, grant_max_rows=50
    )
    assert row_lim_outer == 51
    assert "LIMIT 10" in enforced_sql_outer
    assert "LIMIT 2" in enforced_sql_outer
    assert len(res_outer.rows) == 2
    assert res_outer.truncated is False
    assert [r[0] for r in res_outer.rows] == [0, 1]


def test_fetch_first_n_rows_only(duckdb_conn):
    """FETCH FIRST n ROWS ONLY parsed with dialect='postgres' rewrites to LIMIT min(n, cap)."""
    # 1. FETCH FIRST 5 ROWS ONLY (n <= max_rows)
    sql_5 = "SELECT id, name FROM items ORDER BY id FETCH FIRST 5 ROWS ONLY"
    res_5, row_lim_5, enforced_sql_5 = _run_enforced(
        duckdb_conn, sql_5, grant_max_rows=50, dialect="postgres"
    )
    assert row_lim_5 == 51
    assert "LIMIT 5" in enforced_sql_5
    assert len(res_5.rows) == 5
    assert res_5.truncated is False

    # 2. FETCH NEXT 3 ROWS ONLY
    sql_next = "SELECT id, name FROM items ORDER BY id FETCH NEXT 3 ROWS ONLY"
    res_next, row_lim_next, enforced_sql_next = _run_enforced(
        duckdb_conn, sql_next, grant_max_rows=50, dialect="postgres"
    )
    assert row_lim_next == 51
    assert "LIMIT 3" in enforced_sql_next
    assert len(res_next.rows) == 3
    assert res_next.truncated is False

    # 3. FETCH FIRST 100 ROWS ONLY (n > max_rows) -> capped to LIMIT 51, truncated
    sql_100 = "SELECT id, name FROM items ORDER BY id FETCH FIRST 100 ROWS ONLY"
    res_100, row_lim_100, enforced_sql_100 = _run_enforced(
        duckdb_conn, sql_100, grant_max_rows=50, dialect="postgres"
    )
    assert row_lim_100 == 51
    assert "LIMIT 51" in enforced_sql_100
    assert len(res_100.rows) == 50
    assert res_100.truncated is True

    # 4. FETCH ... PERCENT raises EnforcementError
    sql_percent = (
        "SELECT id, name FROM items ORDER BY id FETCH FIRST 10 PERCENT ROWS ONLY"
    )
    with pytest.raises(
        EnforcementError, match="FETCH ... PERCENT or WITH TIES is not supported"
    ):
        _run_enforced(duckdb_conn, sql_percent, grant_max_rows=50, dialect="postgres")

    # 5. FETCH ... WITH TIES raises EnforcementError
    sql_ties = "SELECT id, name FROM items ORDER BY id FETCH FIRST 10 ROWS WITH TIES"
    with pytest.raises(
        EnforcementError, match="FETCH ... PERCENT or WITH TIES is not supported"
    ):
        _run_enforced(duckdb_conn, sql_ties, grant_max_rows=50, dialect="postgres")


@pytest.mark.parametrize(
    "invalid_sql",
    [
        "SELECT id FROM items LIMIT 1 + 1",
        "SELECT id FROM items LIMIT (SELECT 1)",
        "SELECT id FROM items LIMIT ?",
        "SELECT id FROM items LIMIT :param",
        "SELECT id FROM items LIMIT $1",
        "SELECT id FROM items LIMIT '10'",
        "SELECT id FROM items LIMIT name",
        "SELECT id FROM items LIMIT -1",
        "SELECT id FROM items LIMIT 10.5",
        "SELECT id FROM items LIMIT NULL",
        "SELECT id FROM items FETCH FIRST '5' ROWS ONLY",
        "SELECT id FROM items FETCH FIRST ROW ONLY",
    ],
)
def test_non_literal_limit_refused(duckdb_conn, invalid_sql: str):
    """Top-level limit that is not a non-negative integer literal raises EnforcementError."""
    with pytest.raises(
        EnforcementError, match="LIMIT must be a non-negative integer literal"
    ):
        _run_enforced(duckdb_conn, invalid_sql, grant_max_rows=50, dialect="postgres")
