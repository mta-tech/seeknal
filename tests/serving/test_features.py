"""Tests for seeknal.serving.features."""

from __future__ import annotations

import pytest
import sqlglot

from seeknal.serving.features import extract_features


def _parse_and_extract(sql: str, dialect: str = "postgres"):
    statement = sqlglot.parse_one(sql, read=dialect)
    return extract_features(statement)


def test_statement_kinds():
    assert (
        _parse_and_extract("SELECT 1").kind == "select"
    )
    assert (
        _parse_and_extract("SELECT 1 UNION ALL SELECT 2").kind == "select"
    )
    assert (
        _parse_and_extract("INSERT INTO t VALUES (1)").kind == "insert"
    )
    assert (
        _parse_and_extract("UPDATE t SET a = 1 WHERE b = 2").kind == "update"
    )
    assert (
        _parse_and_extract("DELETE FROM t WHERE b = 2").kind == "delete"
    )
    assert (
        _parse_and_extract(
            "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET a = 1"
        ).kind
        == "merge"
    )
    assert (
        _parse_and_extract("CREATE TABLE t (a INT)").kind == "ddl"
    )
    assert (
        _parse_and_extract("DROP TABLE t").kind == "ddl"
    )
    assert (
        _parse_and_extract("ALTER TABLE t ADD COLUMN b INT").kind == "ddl"
    )
    assert (
        _parse_and_extract("TRUNCATE TABLE t").kind == "ddl"
    )
    assert (
        _parse_and_extract("SET search_path TO public").kind == "other"
    )


def test_extract_tables_and_cte_exclusion():
    # Simple table
    f1 = _parse_and_extract("SELECT * FROM claims.claim")
    assert f1.tables == ("claims.claim",)

    # Three-part table
    f2 = _parse_and_extract("SELECT * FROM lakekeeper.claims.claim")
    assert f2.tables == ("lakekeeper.claims.claim",)

    # Dotted references in first-seen order, deduplicated
    f3 = _parse_and_extract(
        "SELECT * FROM claims.claim c1 JOIN ref.province p ON 1=1 JOIN claims.claim c2 ON 1=1"
    )
    assert f3.tables == ("claims.claim", "ref.province")

    # CTE names must be excluded
    f4 = _parse_and_extract(
        "WITH cte1 AS (SELECT * FROM raw.data), cte2 AS (SELECT * FROM cte1) SELECT * FROM cte2"
    )
    assert f4.tables == ("raw.data",)

    # Bare vs qualified table references
    f5 = _parse_and_extract("SELECT * FROM province")
    assert f5.tables == ("province",)


def test_query_flags():
    # Aggregate
    assert _parse_and_extract("SELECT count(*) FROM t").has_aggregate
    assert _parse_and_extract("SELECT sum(amount) FROM t").has_aggregate
    assert _parse_and_extract("SELECT avg(amount) FROM t").has_aggregate
    assert _parse_and_extract("SELECT max(amount) FROM t").has_aggregate
    assert not _parse_and_extract("SELECT amount FROM t").has_aggregate

    # Group by
    assert _parse_and_extract("SELECT a FROM t GROUP BY a").has_group_by
    assert not _parse_and_extract("SELECT a FROM t").has_group_by

    # Window
    assert _parse_and_extract("SELECT row_number() OVER () FROM t").has_window
    assert not _parse_and_extract("SELECT a FROM t").has_window

    # Join
    assert _parse_and_extract("SELECT * FROM t JOIN u ON t.id = u.id").has_join
    assert not _parse_and_extract("SELECT * FROM t").has_join

    # Distinct
    assert _parse_and_extract("SELECT DISTINCT a FROM t").has_distinct
    assert not _parse_and_extract("SELECT a FROM t").has_distinct

    # Subquery vs CTE
    assert _parse_and_extract("SELECT * FROM (SELECT 1 FROM t) sub").has_subquery
    assert _parse_and_extract("SELECT * FROM t WHERE a IN (SELECT b FROM u)").has_subquery
    assert _parse_and_extract("SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u)").has_subquery
    assert not _parse_and_extract("WITH c AS (SELECT * FROM t) SELECT * FROM c").has_subquery


def test_limit():
    assert _parse_and_extract("SELECT * FROM t LIMIT 10").limit == 10
    assert _parse_and_extract("SELECT * FROM t LIMIT 100 OFFSET 20").limit == 100
    assert _parse_and_extract("SELECT * FROM t").limit is None
    # Subquery limit does not set outermost limit
    assert _parse_and_extract("SELECT * FROM (SELECT * FROM t LIMIT 5) sub").limit is None
    assert (
        _parse_and_extract(
            "SELECT * FROM (SELECT * FROM t LIMIT 5) sub LIMIT 25"
        ).limit
        == 25
    )


def test_key_equalities():
    # Plain equality col = literal
    f1 = _parse_and_extract("SELECT * FROM claims.claim WHERE claim_id = 123")
    assert f1.key_equalities == {"claim_id": 1}

    # Reversed: literal = col
    f2 = _parse_and_extract("SELECT * FROM claims.claim WHERE 123 = claim_id")
    assert f2.key_equalities == {"claim_id": 1}

    # Table qualifier dropped and name lower-cased
    f3 = _parse_and_extract(
        "SELECT * FROM claims.claim WHERE claims.claim.CLAIM_ID = 'ABC'"
    )
    assert f3.key_equalities == {"claim_id": 1}

    # Parameters: $1 and ?
    f4 = _parse_and_extract("SELECT * FROM claims.claim WHERE claim_id = $1")
    assert f4.key_equalities == {"claim_id": 1}

    f5 = _parse_and_extract("SELECT * FROM claims.claim WHERE claim_id = ?")
    assert f5.key_equalities == {"claim_id": 1}

    # IN list with literals and parameters
    f6 = _parse_and_extract(
        "SELECT * FROM claims.claim WHERE claim_id IN (1, 2, $1, 4)"
    )
    assert f6.key_equalities == {"claim_id": 4}

    # Multiple top-level AND conjuncts
    f7 = _parse_and_extract(
        "SELECT * FROM claims.claim WHERE claim_id = 10 AND status = 'active' AND code IN ('A', 'B')"
    )
    assert f7.key_equalities == {"claim_id": 1, "status": 1, "code": 2}

    # Inside OR must not count
    f8 = _parse_and_extract(
        "SELECT * FROM claims.claim WHERE (claim_id = 1 OR claim_id = 2)"
    )
    assert f8.key_equalities == {}

    # Inside NOT must not count
    f9 = _parse_and_extract("SELECT * FROM claims.claim WHERE NOT (claim_id = 1)")
    assert f9.key_equalities == {}

    # IN subquery must not count
    f10 = _parse_and_extract(
        "SELECT * FROM claims.claim WHERE claim_id IN (SELECT id FROM other)"
    )
    assert f10.key_equalities == {}

    # Expression on column must not count
    f11 = _parse_and_extract(
        "SELECT * FROM claims.claim WHERE lower(claim_id) = 'abc'"
    )
    assert f11.key_equalities == {}
