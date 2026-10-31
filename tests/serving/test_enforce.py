"""Tests for query policy enforcement in Seeknal Serving."""

from __future__ import annotations

import hashlib
import time
from typing import Any

import duckdb
import pytest
import sqlglot

from seeknal.serving.enforce import (
    MASK_TEMPLATES,
    VALID_MASKS,
    EnforcedQuery,
    EnforcementError,
    Grant,
    RowFilter,
    TableGrant,
    enforce,
)


def _claims_fixture(
    sql: str,
    *,
    max_rows: int = 1000,
    exp: float | None = None,
    actions: tuple[str, ...] = ("select",),
    row_filters: list[dict[str, Any]] | None = None,
    masks: dict[str, str] | None = None,
    columns: list[str] | None = None,
) -> tuple[Grant, dict[str, list[str]]]:
    """Helper to build a valid Grant and schemas for test queries."""
    if exp is None:
        exp = time.time() + 3600.0
    if columns is None:
        columns = ["claim_id", "province", "idrg_code", "amount", "admit_date"]
    if masks is None:
        masks = {"patient_nik": "hash", "patient_name": "redact", "birth_date": "year"}
    if row_filters is None:
        row_filters = [{"column": "province", "op": "in", "values": ["JB"]}]

    claims = {
        "sub": "user:7f3c",
        "ws": "claims-jabar",
        "jti": "aud-0192",
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": max_rows,
        "exp": exp,
        "tables": {
            "claims.claim": {
                "columns": columns,
                "masks": masks,
                "row_filters": row_filters,
                "actions": list(actions),
            }
        },
    }
    schemas = {
        "claims.claim": [
            "claim_id",
            "patient_nik",
            "patient_name",
            "province",
            "idrg_code",
            "amount",
            "admit_date",
            "birth_date",
            "secret_note",
        ]
    }
    return Grant.from_claims(claims), schemas


# ==============================================================================
# Rule 1: Integrity
# ==============================================================================

def test_integrity_sha256_mismatch():
    sql = "SELECT claim_id FROM claims.claim"
    grant, schemas = _claims_fixture(sql)
    tampered_sql = "SELECT claim_id FROM claims.claim WHERE 1=1"
    with pytest.raises(EnforcementError, match="SQL SHA256 mismatch"):
        enforce(tampered_sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


def test_integrity_expired_grant():
    sql = "SELECT claim_id FROM claims.claim"
    grant, schemas = _claims_fixture(sql, exp=1000.0)
    with pytest.raises(EnforcementError, match="grant expired"):
        enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb", now=2000.0)


def test_integrity_unparseable_sql():
    sql = "SELECT FROM WHERE"
    # Create grant with sha matching the unparseable sql
    grant, schemas = _claims_fixture(sql)
    with pytest.raises(EnforcementError, match="SQL parse error"):
        enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


def test_integrity_multi_statement_refused():
    sql = "SELECT claim_id FROM claims.claim; SELECT claim_id FROM claims.claim"
    grant, schemas = _claims_fixture(sql)
    with pytest.raises(EnforcementError, match="expected exactly one statement"):
        enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


# ==============================================================================
# Rule 2: Tables
# ==============================================================================

def test_table_not_granted_refused():
    sql = "SELECT * FROM secret_table"
    grant, schemas = _claims_fixture(sql)
    with pytest.raises(EnforcementError, match="table 'secret_table' is not granted"):
        enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


def test_table_not_in_schemas_refused():
    sql = "SELECT * FROM claims.claim"
    grant, _ = _claims_fixture(sql)
    # Empty schemas map
    with pytest.raises(EnforcementError, match="table 'claims.claim' is not in schemas"):
        enforce(sql, grant, schemas={}, target_dialect="duckdb", target_kind="duckdb")


def test_table_case_insensitive_matching():
    sql = "SELECT claim_id FROM CLAIMS.CLAIM"
    grant, schemas = _claims_fixture(sql)
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    assert res.tables == ("claims.claim",)


def test_cte_excluded_from_table_grant_check():
    sql = "WITH my_cte AS (SELECT 1 AS n) SELECT n FROM my_cte"
    grant, schemas = _claims_fixture(sql)
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    assert res.tables == ()


# ==============================================================================
# Rule 3: Actions
# ==============================================================================

def test_action_select_required_for_select():
    sql = "SELECT claim_id FROM claims.claim"
    grant, schemas = _claims_fixture(sql, actions=("insert",))
    with pytest.raises(EnforcementError, match="action 'select' is not granted on table 'claims.claim'"):
        enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


def test_action_insert_required_for_insert():
    sql = "INSERT INTO claims.claim (claim_id) VALUES (1)"
    grant, schemas = _claims_fixture(sql, actions=("select",), row_filters=[])
    with pytest.raises(EnforcementError, match="action 'insert' is not granted on table 'claims.claim'"):
        enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


def test_action_update_required_for_update():
    sql = "UPDATE claims.claim SET amount = 100 WHERE claim_id = 1"
    grant, schemas = _claims_fixture(sql, actions=("select",), row_filters=[])
    with pytest.raises(EnforcementError, match="action 'update' is not granted on table 'claims.claim'"):
        enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


def test_action_delete_required_for_delete():
    sql = "DELETE FROM claims.claim WHERE claim_id = 1"
    grant, schemas = _claims_fixture(sql, actions=("select",), row_filters=[])
    with pytest.raises(EnforcementError, match="action 'delete' is not granted on table 'claims.claim'"):
        enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


def test_action_unsupported_statement_kinds_refused():
    unsupported = [
        "CREATE TABLE claims.claim (id INT)",
        "DROP TABLE claims.claim",
        "SET search_path = public",
    ]
    for sql in unsupported:
        grant, schemas = _claims_fixture(sql)
        with pytest.raises(EnforcementError, match="statement kind .* is not permitted"):
            enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


# ==============================================================================
# Rule 4: Columns & Star Expansion
# ==============================================================================

def test_column_ungranted_in_schema_refused():
    sql = "SELECT secret_note FROM claims.claim"
    grant, schemas = _claims_fixture(sql)
    with pytest.raises(EnforcementError, match="column 'secret_note' in table 'claims.claim' is neither visible nor masked"):
        enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


def test_column_not_in_schema_refused():
    sql = "SELECT totally_fake FROM claims.claim"
    grant, schemas = _claims_fixture(sql)
    with pytest.raises(EnforcementError, match="column 'totally_fake' does not resolve"):
        enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


def test_column_ambiguous_unqualified_refused():
    sql = "SELECT id FROM claims.claim AS c JOIN claims.other AS o ON c.id = o.id"
    claims = {
        "sub": "u", "ws": "w", "jti": "j",
        "sql_sha256": hashlib.sha256(sql.encode()).hexdigest(),
        "max_rows": 100, "exp": time.time() + 1000,
        "tables": {
            "claims.claim": {"columns": ["id"], "masks": {}, "actions": ["select"]},
            "claims.other": {"columns": ["id"], "masks": {}, "actions": ["select"]},
        },
    }
    schemas = {"claims.claim": ["id"], "claims.other": ["id"]}
    with pytest.raises(EnforcementError, match="ambiguous column reference 'id'"):
        enforce(sql, Grant.from_claims(claims), schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


def test_column_unknown_qualifier_refused():
    sql = "SELECT bad_alias.claim_id FROM claims.claim AS c"
    grant, schemas = _claims_fixture(sql)
    with pytest.raises(EnforcementError, match="unknown table or alias qualifier 'bad_alias'"):
        enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


def test_columns_produced_by_projection_alias_allowed():
    sql = "SELECT amount * 2 AS double_amount FROM claims.claim ORDER BY double_amount"
    grant, schemas = _claims_fixture(sql)
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    assert "ORDER BY double_amount" in res.sql


def test_columns_produced_by_cte_allowed():
    sql = "WITH summary AS (SELECT province, count(*) AS cnt FROM claims.claim GROUP BY province) SELECT cnt FROM summary"
    grant, schemas = _claims_fixture(sql)
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    assert "SELECT cnt FROM summary" in res.sql


def test_star_expansion_omits_ungranted_columns():
    sql = "SELECT * FROM claims.claim"
    grant, schemas = _claims_fixture(sql)
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    # Outer query must list all granted columns in schema order, and NOT secret_note
    assert "secret_note" not in res.sql
    assert "claim_id, patient_nik, patient_name, province, idrg_code, amount, admit_date, birth_date" in res.sql


def test_table_star_expansion():
    sql = "SELECT c.* FROM claims.claim AS c"
    grant, schemas = _claims_fixture(sql)
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    assert "secret_note" not in res.sql
    assert "c.claim_id, c.patient_nik, c.patient_name, c.province, c.idrg_code, c.amount, c.admit_date, c.birth_date" in res.sql


# ==============================================================================
# Rule 5: Secure-View Rewrite
# ==============================================================================

def test_secure_view_rewrite_unaliased_table():
    sql = "SELECT claim_id, province FROM claims.claim WHERE amount > 50"
    grant, schemas = _claims_fixture(sql)
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    # Derived table alias should be 'claim' (last name segment)
    assert "FROM claims.claim WHERE province IN ('JB')) AS claim" in res.sql


def test_secure_view_rewrite_aliased_table():
    sql = "SELECT c.claim_id FROM claims.claim AS c WHERE c.amount > 50"
    grant, schemas = _claims_fixture(sql)
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    # Derived table alias should be 'c'
    assert "FROM claims.claim WHERE province IN ('JB')) AS c" in res.sql


# ==============================================================================
# Rule 6: Writes
# ==============================================================================

def test_update_row_filters_injected():
    sql = "UPDATE claims.claim SET amount = 200 WHERE claim_id = 1"
    grant, schemas = _claims_fixture(sql, actions=("select", "update"))
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    assert "WHERE claim_id = 1 AND province IN ('JB')" in res.sql


def test_delete_row_filters_injected():
    sql = "DELETE FROM claims.claim WHERE claim_id = 1"
    grant, schemas = _claims_fixture(sql, actions=("select", "delete"))
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    assert "WHERE claim_id = 1 AND province IN ('JB')" in res.sql


def test_update_setting_masked_column_refused():
    sql = "UPDATE claims.claim SET patient_name = 'Alice' WHERE claim_id = 1"
    grant, schemas = _claims_fixture(sql, actions=("select", "update"))
    with pytest.raises(EnforcementError, match="UPDATE cannot set masked column 'patient_name'"):
        enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


def test_update_setting_ungranted_column_refused():
    sql = "UPDATE claims.claim SET secret_note = 'Confidential' WHERE claim_id = 1"
    grant, schemas = _claims_fixture(sql, actions=("select", "update"))
    with pytest.raises(EnforcementError, match="UPDATE cannot set column 'secret_note' outside granted columns"):
        enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


def test_insert_into_table_with_row_filters_refused():
    sql = "INSERT INTO claims.claim (claim_id) VALUES (1)"
    grant, schemas = _claims_fixture(sql, actions=("select", "insert"), row_filters=[{"column": "province", "op": "eq", "values": ["JB"]}])
    with pytest.raises(EnforcementError, match="INSERT into table 'claims.claim' with row filters is not permitted"):
        enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


def test_insert_naming_masked_column_refused():
    sql = "INSERT INTO claims.claim (claim_id, patient_nik) VALUES (1, '3201')"
    grant, schemas = _claims_fixture(sql, actions=("select", "insert"), row_filters=[])
    with pytest.raises(EnforcementError, match="INSERT naming masked column 'patient_nik'"):
        enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")


# ==============================================================================
# Rule 7: Row Cap
# ==============================================================================

def test_row_cap_applied_when_limit_absent():
    sql = "SELECT claim_id FROM claims.claim"
    grant, schemas = _claims_fixture(sql, max_rows=50)
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    assert res.row_limit == 51
    assert "LIMIT 51" in res.sql


def test_row_cap_applied_when_existing_limit_larger():
    sql = "SELECT claim_id FROM claims.claim LIMIT 100"
    grant, schemas = _claims_fixture(sql, max_rows=50)
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    assert res.row_limit == 51
    assert "LIMIT 51" in res.sql


def test_row_cap_keeps_smaller_existing_limit():
    sql = "SELECT claim_id FROM claims.claim LIMIT 10"
    grant, schemas = _claims_fixture(sql, max_rows=50)
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    assert res.row_limit == 10
    assert "LIMIT 10" in res.sql


def test_row_cap_none_for_writes():
    sql = "UPDATE claims.claim SET amount = 50 WHERE claim_id = 1"
    grant, schemas = _claims_fixture(sql, actions=("select", "update"), row_filters=[])
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    assert res.row_limit is None


# ==============================================================================
# Rule 8: Cube Target Kind
# ==============================================================================

def test_cube_path_skips_table_rewrite_and_builds_security_context():
    sql = "SELECT claim_id, province FROM claims.claim WHERE amount > 10"
    grant, schemas = _claims_fixture(sql)
    res = enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube")
    # Cube does not rewrite tables to derived tables
    assert "FROM claims.claim WHERE" in res.sql
    assert "AS claim" not in res.sql
    assert res.cube_security_context is not None
    assert res.cube_security_context["subject"] == "user:7f3c"
    assert "claims.claim" in res.cube_security_context["masks"]
    assert res.cube_security_context["masks"]["claims.claim"]["patient_nik"] == "hash"
    assert len(res.cube_security_context["row_filters"]["claims.claim"]) == 1


# ==============================================================================
# Rule 9: Mask Expressions Round-Trip
# ==============================================================================

def test_mask_templates_round_trip():
    """Verify that each mask template parses and renders unchanged in meaning."""
    for method, dialects in MASK_TEMPLATES.items():
        for dialect, template in dialects.items():
            parsed = sqlglot.parse_one(template, read=dialect)
            rendered = parsed.sql(dialect=dialect)
            parsed_again = sqlglot.parse_one(rendered, read=dialect)
            assert parsed_again is not None, f"Failed re-parsing for {method} on {dialect}"
            # Rendered SQL must call the exact function name specified
            if method == "hash":
                if dialect == "postgres":
                    assert "ENCODE" in rendered.upper() and "SHA256" in rendered.upper()
                elif dialect == "starrocks":
                    assert "SHA2" in rendered.upper()
                elif dialect == "duckdb":
                    assert "SHA256" in rendered.upper()
            elif method == "last4":
                assert "RIGHT" in rendered.upper()
            elif method == "year":
                assert "DATE_TRUNC" in rendered.upper()


# ==============================================================================
# Rule 10: Row Filter Literals
# ==============================================================================

def test_row_filter_ops_and_types():
    sql = "SELECT claim_id FROM claims.claim"
    rf_list = [
        RowFilter("province", "eq", ("JB",)),
        RowFilter("amount", "gte", (100,)),
        RowFilter("amount", "lte", (500.5,)),
        RowFilter("is_active", "eq", (True,)),
        RowFilter("archived_at", "eq", (None,)),
        RowFilter("idrg_code", "in", ("A-1", "B-2")),
    ]
    tg = TableGrant(
        table="claims.claim",
        columns=("claim_id", "province", "amount", "is_active", "archived_at", "idrg_code"),
        masks={},
        row_filters=tuple(rf_list),
        actions=("select",),
    )
    grant = Grant(
        subject="sub", workspace="ws", audit_id="j",
        sql_sha256=hashlib.sha256(sql.encode()).hexdigest(),
        max_rows=10, expires_at=time.time() + 1000,
        tables={"claims.claim": tg},
    )
    schemas = {"claims.claim": ["claim_id", "province", "amount", "is_active", "archived_at", "idrg_code"]}
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    assert "province = 'JB'" in res.sql
    assert "amount >= 100" in res.sql
    assert "amount <= 500.5" in res.sql
    assert "archived_at IS NULL" in res.sql
    assert "idrg_code IN ('A-1', 'B-2')" in res.sql


# ==============================================================================
# Rule 11: Redacted SQL
# ==============================================================================

def test_redacted_sql_replaces_literals():
    sql = "SELECT * FROM claims.claim WHERE patient_nik = '320123' AND amount > 500 AND is_valid = TRUE"
    grant, schemas = _claims_fixture(
        sql,
        columns=["claim_id", "province", "idrg_code", "amount", "admit_date", "is_valid"],
    )
    schemas["claims.claim"].append("is_valid")
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    assert res.redacted_sql == "SELECT * FROM claims.claim WHERE patient_nik = ? AND amount > ? AND is_valid = ?"


# ==============================================================================
# Claims & Dataclass Validation
# ==============================================================================

def test_claims_validation_unknown_mask_method():
    claims = {
        "sub": "u", "ws": "w", "jti": "j", "sql_sha256": "x", "max_rows": 10, "exp": 1000,
        "tables": {"t": {"columns": ["a"], "masks": {"b": "caesar_cipher"}, "actions": ["select"]}},
    }
    with pytest.raises(EnforcementError, match="unknown mask method 'caesar_cipher'"):
        Grant.from_claims(claims)


def test_claims_validation_column_both_visible_and_masked():
    claims = {
        "sub": "u", "ws": "w", "jti": "j", "sql_sha256": "x", "max_rows": 10, "exp": 1000,
        "tables": {"t": {"columns": ["patient_nik"], "masks": {"PATIENT_NIK": "hash"}, "actions": ["select"]}},
    }
    with pytest.raises(EnforcementError, match="column 'patient_nik' is both visible and masked"):
        Grant.from_claims(claims)


def test_claims_validation_non_positive_max_rows():
    claims = {
        "sub": "u", "ws": "w", "jti": "j", "sql_sha256": "x", "max_rows": 0, "exp": 1000,
        "tables": {"t": {"columns": ["a"], "masks": {}, "actions": ["select"]}},
    }
    with pytest.raises(EnforcementError, match="max_rows must be positive"):
        Grant.from_claims(claims)


def test_claims_validation_empty_table_list():
    claims = {
        "sub": "u", "ws": "w", "jti": "j", "sql_sha256": "x", "max_rows": 10, "exp": 1000,
        "tables": {},
    }
    with pytest.raises(EnforcementError, match="empty table list"):
        Grant.from_claims(claims)


def test_row_filter_validation():
    with pytest.raises(EnforcementError, match="unknown row filter op 'like'"):
        RowFilter("col", "like", ("val",))
    with pytest.raises(EnforcementError, match="requires exactly 1 value"):
        RowFilter("col", "eq", ("v1", "v2"))
    with pytest.raises(EnforcementError, match="requires at least 1 value"):
        RowFilter("col", "in", ())
    with pytest.raises(EnforcementError, match="None value only permitted with 'eq' op"):
        RowFilter("col", "gte", (None,))


# ==============================================================================
# In-Memory DuckDB Execution Tests
# ==============================================================================

@pytest.fixture
def duckdb_con():
    """Create an in-memory DuckDB database populated with test claims."""
    con = duckdb.connect()
    con.execute("CREATE SCHEMA claims")
    con.execute("""
        CREATE TABLE claims.claim (
            claim_id INT,
            patient_nik VARCHAR,
            patient_name VARCHAR,
            province VARCHAR,
            idrg_code VARCHAR,
            amount INT,
            admit_date DATE,
            birth_date DATE,
            secret_note VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO claims.claim VALUES
            (1, '3201234567890001', 'Budi Santoso', 'JB', 'A-1', 1000, '2024-01-01', '1980-05-15', 'secret-1'),
            (2, '3201234567890002', 'Dewi Lestari', 'JB', 'A-1', 2000, '2024-01-05', '1985-08-20', 'secret-2'),
            (3, '3171234567890003', 'Andi Pratama', 'DKI', 'B-1', 3000, '2024-02-01', '1990-12-10', 'secret-3'),
            (4, '3201234567890004', 'Eko Prasetyo', 'JB', 'A-2', 4000, '2024-02-15', '1975-03-30', 'secret-4'),
            (5, '3201234567890005', 'Rina Wijaya', 'JB', 'A-2', 5000, '2024-03-01', '1995-07-25', 'secret-5')
    """)
    yield con
    con.close()


def test_duckdb_execution_masks_and_filters(duckdb_con):
    """Prove in DuckDB: masked values are masked, filtered rows never appear."""
    sql = "SELECT claim_id, patient_nik, patient_name, birth_date, province FROM claims.claim"
    grant, schemas = _claims_fixture(sql)
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")

    cursor = duckdb_con.execute(res.sql)
    rows = cursor.fetchall()

    # Only rows with province='JB' should appear (rows 1, 2, 4, 5; row 3 is DKI)
    assert len(rows) == 4
    for claim_id, nik, name, bdate, prov in rows:
        assert prov == "JB"
        assert name == "****"
        # NIK is hashed (64 hex characters for SHA256)
        assert len(nik) == 64
        # Birth date is truncated to year (month and day are 1)
        assert bdate.month == 1
        assert bdate.day == 1


def test_duckdb_execution_sql_injection_defense(duckdb_con):
    """Prove in DuckDB: an injected quote in a filter value does not break out."""
    malicious_val = "JB' OR '1'='1"
    sql = "SELECT claim_id, province FROM claims.claim"
    grant, schemas = _claims_fixture(sql, row_filters=[{"column": "province", "op": "eq", "values": [malicious_val]}])
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")

    cursor = duckdb_con.execute(res.sql)
    rows = cursor.fetchall()
    # No rows match the literal province "JB' OR '1'='1"
    assert len(rows) == 0


def test_duckdb_execution_star_expansion_omits_unauthorized(duckdb_con):
    """Prove in DuckDB: star expansion returns only granted columns."""
    sql = "SELECT * FROM claims.claim"
    grant, schemas = _claims_fixture(sql)
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")

    cursor = duckdb_con.execute(res.sql)
    returned_col_names = [desc[0] for desc in cursor.description]
    assert "secret_note" not in returned_col_names
    assert returned_col_names == [
        "claim_id", "patient_nik", "patient_name", "province",
        "idrg_code", "amount", "admit_date", "birth_date"
    ]


def test_duckdb_execution_limit_capping(duckdb_con):
    """Prove in DuckDB: LIMIT is max_rows + 1."""
    sql = "SELECT claim_id FROM claims.claim"
    # max_rows is 2, so limit must be 3
    grant, schemas = _claims_fixture(sql, max_rows=2)
    res = enforce(sql, grant, schemas=schemas, target_dialect="duckdb", target_kind="duckdb")
    assert res.row_limit == 3

    cursor = duckdb_con.execute(res.sql)
    rows = cursor.fetchall()
    assert len(rows) == 3
