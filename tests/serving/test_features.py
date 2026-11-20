"""Tests for seeknal.serving.features."""

from __future__ import annotations

import sqlglot

from seeknal.serving.features import OrderByItem, PredicateFeature, extract_features


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


def test_sargable_predicates_extraction():
    # eq, in, range, between, prefix_like, and single-column OR
    f = _parse_and_extract(
        "SELECT * FROM rpt.txn t "
        "WHERE t.customer_id = $1 "
        "AND branch_code IN ('B1', 'B2', 'B3') "
        "AND t.as_of_date >= '2026-10-01' "
        "AND amount BETWEEN 100 AND 500 "
        "AND merchant_code LIKE 'MRC\\%01%' "
        "AND (status = 'OK' OR status IN ('SETTLED', 'DONE'))"
    )
    assert f.predicates == (
        PredicateFeature(
            column="customer_id", op="eq", value_count=1, table_qualifier="t"
        ),
        PredicateFeature(
            column="branch_code", op="in", value_count=3, table_qualifier=None
        ),
        PredicateFeature(
            column="as_of_date", op="range", value_count=1, table_qualifier="t"
        ),
        PredicateFeature(
            column="amount", op="between", value_count=2, table_qualifier=None
        ),
        PredicateFeature(
            column="merchant_code",
            op="prefix_like",
            value_count=1,
            table_qualifier=None,
        ),
        PredicateFeature(
            column="status", op="or_eq", value_count=3, table_qualifier=None
        ),
    )


def test_disqualified_non_sargable_predicates():
    # Wrapped columns (CAST, ::text, lower), IS [NOT] NULL, = NULL, bad LIKE, mixed OR
    for sql in (
        "SELECT * FROM t WHERE CAST(id AS text) = '1'",
        "SELECT * FROM t WHERE id::text = '1'",
        "SELECT * FROM t WHERE lower(id) = 'x'",
        "SELECT * FROM t WHERE id IS NULL",
        "SELECT * FROM t WHERE id IS NOT NULL",
        "SELECT * FROM t WHERE id = NULL",
        "SELECT * FROM t WHERE id = 1 OR true",
        "SELECT * FROM t WHERE id = 1 OR other = 2",
        "SELECT * FROM t WHERE name LIKE '%abc'",
        "SELECT * FROM t WHERE name LIKE 'a_b%'",
        "SELECT * FROM t WHERE name LIKE '%'",
        "SELECT * FROM t WHERE name ILIKE 'abc%'",
    ):
        f = _parse_and_extract(sql)
        assert f.predicates == (), f"Expected empty predicates for: {sql}"


def test_offset_and_order_by_extraction():
    f1 = _parse_and_extract(
        "SELECT * FROM t ORDER BY t.as_of_date DESC, txn_id ASC, lower(name), 1 LIMIT 50 OFFSET 250"
    )
    assert f1.offset == 250
    assert f1.order_by == (
        OrderByItem(
            column="as_of_date",
            direction="desc",
            table_qualifier="t",
            is_bare_column=True,
        ),
        OrderByItem(
            column="txn_id",
            direction="asc",
            table_qualifier=None,
            is_bare_column=True,
        ),
        OrderByItem(
            column=None,
            direction="asc",
            table_qualifier=None,
            is_bare_column=False,
        ),
        OrderByItem(
            column=None,
            direction="asc",
            table_qualifier=None,
            is_bare_column=False,
        ),
    )

    f_neg = _parse_and_extract("SELECT * FROM t OFFSET -5")
    assert f_neg.offset == -5


def test_parameterised_prefix_like_extraction():
    stmt = sqlglot.parse_one(
        "SELECT * FROM rpt.txn WHERE merchant_code LIKE $2 AND customer_id = $1",
        read="postgres",
    )
    # Without params -> LIKE $2 is not resolved, only eq is counted
    f_no_params = extract_features(stmt)
    assert f_no_params.predicates == (
        PredicateFeature(column="customer_id", op="eq", value_count=1),
    )

    # Valid prefix pattern in params[1] -> counted as prefix_like
    f_ok = extract_features(stmt, params=["C-1", "MRC01%"])
    assert f_ok.predicates == (
        PredicateFeature(column="merchant_code", op="prefix_like", value_count=1),
        PredicateFeature(column="customer_id", op="eq", value_count=1),
    )

    # Escaped underscore / percent before trailing % -> valid prefix_like
    f_esc = extract_features(stmt, params=["C-1", r"MRC\_01\%A%"])
    assert f_esc.predicates[0] == PredicateFeature(
        column="merchant_code", op="prefix_like", value_count=1
    )

    # Leading wildcard, interior wildcard, bare %, non-string, NOT LIKE -> rejected
    for bad_pat in ("%MRC", "M_C%", "%", "MRC%01%", 123, True):
        f_bad = extract_features(stmt, params=["C-1", bad_pat])
        assert all(p.op != "prefix_like" for p in f_bad.predicates)

    stmt_not = sqlglot.parse_one(
        "SELECT * FROM rpt.txn WHERE NOT (merchant_code LIKE $1)",
        read="postgres",
    )
    assert extract_features(stmt_not, params=["MRC01%"]).predicates == ()


