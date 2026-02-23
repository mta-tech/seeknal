"""Tests for audit engine: parsing, execution, all 5 built-in types."""

import pytest
import duckdb

from seeknal.workflow.audits import (
    AuditConfig,
    AuditResult,
    AuditRunner,
    AuditSeverity,
    AuditType,
    parse_audits,
)


class TestParseAudits:
    """Test YAML audit section parsing and validation."""

    def test_parse_not_null(self):
        config = {"audits": [{"type": "not_null", "columns": ["id", "name"]}]}
        audits = parse_audits(config)
        assert len(audits) == 1
        assert audits[0].audit_type == "not_null"
        assert audits[0].columns == ["id", "name"]

    def test_parse_unique(self):
        config = {"audits": [{"type": "unique", "columns": ["email"]}]}
        audits = parse_audits(config)
        assert audits[0].audit_type == "unique"

    def test_parse_accepted_values(self):
        config = {"audits": [{"type": "accepted_values", "columns": ["status"], "values": ["active", "inactive"]}]}
        audits = parse_audits(config)
        assert audits[0].values == ["active", "inactive"]

    def test_parse_row_count(self):
        config = {"audits": [{"type": "row_count", "min": 100, "max": 10000}]}
        audits = parse_audits(config)
        assert audits[0].min_count == 100
        assert audits[0].max_count == 10000

    def test_parse_custom_sql(self):
        config = {"audits": [{"type": "custom_sql", "sql": "SELECT * FROM __THIS__ WHERE amount < 0"}]}
        audits = parse_audits(config)
        assert "__THIS__" in audits[0].sql

    def test_parse_severity_warn(self):
        config = {"audits": [{"type": "not_null", "columns": ["id"], "severity": "warn"}]}
        audits = parse_audits(config)
        assert audits[0].severity == "warn"

    def test_parse_empty_audits(self):
        assert parse_audits({}) == []
        assert parse_audits({"audits": []}) == []

    def test_parse_invalid_type(self):
        with pytest.raises(ValueError, match="invalid type"):
            parse_audits({"audits": [{"type": "invalid"}]})

    def test_parse_missing_type(self):
        with pytest.raises(ValueError, match="missing required 'type'"):
            parse_audits({"audits": [{"columns": ["id"]}]})

    def test_parse_not_null_missing_columns(self):
        with pytest.raises(ValueError, match="requires 'columns'"):
            parse_audits({"audits": [{"type": "not_null"}]})

    def test_parse_accepted_values_missing_values(self):
        with pytest.raises(ValueError, match="requires 'values'"):
            parse_audits({"audits": [{"type": "accepted_values", "columns": ["status"]}]})

    def test_parse_row_count_missing_bounds(self):
        with pytest.raises(ValueError, match="requires 'min' and/or 'max'"):
            parse_audits({"audits": [{"type": "row_count"}]})

    def test_parse_custom_sql_missing_sql(self):
        with pytest.raises(ValueError, match="requires 'sql'"):
            parse_audits({"audits": [{"type": "custom_sql"}]})

    def test_parse_invalid_severity(self):
        with pytest.raises(ValueError, match="invalid severity"):
            parse_audits({"audits": [{"type": "not_null", "columns": ["id"], "severity": "info"}]})

    def test_parse_multiple_audits(self):
        config = {"audits": [
            {"type": "not_null", "columns": ["id"]},
            {"type": "unique", "columns": ["email"]},
            {"type": "row_count", "min": 1},
        ]}
        audits = parse_audits(config)
        assert len(audits) == 3


class TestAuditResult:
    """Test AuditResult serialization."""

    def test_roundtrip(self):
        r = AuditResult(
            audit_type="not_null",
            passed=True,
            failing_rows=0,
            severity="error",
            message="ok",
            columns=["id"],
            duration_ms=10,
        )
        d = r.to_dict()
        restored = AuditResult.from_dict(d)
        assert restored.audit_type == "not_null"
        assert restored.passed is True
        assert restored.columns == ["id"]


@pytest.fixture
def duckdb_conn():
    """Create a DuckDB connection with test data."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE test_data AS SELECT * FROM (
            VALUES
                (1, 'alice', 'active', 100.0),
                (2, 'bob', 'active', 200.0),
                (3, NULL, 'inactive', 50.0),
                (4, 'dave', 'pending', -10.0),
                (2, 'bob_dup', 'active', 150.0)
        ) AS t(id, name, status, amount)
    """)
    return conn


class TestAuditNotNull:
    """Test not_null audit type."""

    def test_pass(self, duckdb_conn):
        audit = AuditConfig(audit_type="not_null", columns=["id"])
        runner = AuditRunner(duckdb_conn)
        results = runner.run_audits([audit], "test_data")
        assert results[0].passed is True

    def test_fail(self, duckdb_conn):
        audit = AuditConfig(audit_type="not_null", columns=["name"])
        runner = AuditRunner(duckdb_conn)
        results = runner.run_audits([audit], "test_data")
        assert results[0].passed is False
        assert results[0].failing_rows == 1


class TestAuditUnique:
    """Test unique audit type."""

    def test_pass(self, duckdb_conn):
        audit = AuditConfig(audit_type="unique", columns=["name"])
        runner = AuditRunner(duckdb_conn)
        results = runner.run_audits([audit], "test_data")
        # name column has NULL which is unique-ish, but no exact duplicates
        assert results[0].passed is True

    def test_fail(self, duckdb_conn):
        audit = AuditConfig(audit_type="unique", columns=["id"])
        runner = AuditRunner(duckdb_conn)
        results = runner.run_audits([audit], "test_data")
        # id=2 appears twice
        assert results[0].passed is False
        assert results[0].failing_rows >= 1


class TestAuditAcceptedValues:
    """Test accepted_values audit type."""

    def test_pass(self, duckdb_conn):
        audit = AuditConfig(
            audit_type="accepted_values",
            columns=["status"],
            values=["active", "inactive", "pending"],
        )
        runner = AuditRunner(duckdb_conn)
        results = runner.run_audits([audit], "test_data")
        assert results[0].passed is True

    def test_fail(self, duckdb_conn):
        audit = AuditConfig(
            audit_type="accepted_values",
            columns=["status"],
            values=["active", "inactive"],
        )
        runner = AuditRunner(duckdb_conn)
        results = runner.run_audits([audit], "test_data")
        assert results[0].passed is False
        assert results[0].failing_rows >= 1  # "pending" is not accepted


class TestAuditRowCount:
    """Test row_count audit type."""

    def test_pass_within_bounds(self, duckdb_conn):
        audit = AuditConfig(audit_type="row_count", min_count=1, max_count=100)
        runner = AuditRunner(duckdb_conn)
        results = runner.run_audits([audit], "test_data")
        assert results[0].passed is True

    def test_fail_below_min(self, duckdb_conn):
        audit = AuditConfig(audit_type="row_count", min_count=1000)
        runner = AuditRunner(duckdb_conn)
        results = runner.run_audits([audit], "test_data")
        assert results[0].passed is False

    def test_fail_above_max(self, duckdb_conn):
        audit = AuditConfig(audit_type="row_count", max_count=2)
        runner = AuditRunner(duckdb_conn)
        results = runner.run_audits([audit], "test_data")
        assert results[0].passed is False


class TestAuditCustomSql:
    """Test custom_sql audit type."""

    def test_pass(self, duckdb_conn):
        audit = AuditConfig(
            audit_type="custom_sql",
            sql="SELECT * FROM __THIS__ WHERE amount < -1000",
        )
        runner = AuditRunner(duckdb_conn)
        results = runner.run_audits([audit], "test_data")
        assert results[0].passed is True

    def test_fail(self, duckdb_conn):
        audit = AuditConfig(
            audit_type="custom_sql",
            sql="SELECT * FROM __THIS__ WHERE amount < 0",
        )
        runner = AuditRunner(duckdb_conn)
        results = runner.run_audits([audit], "test_data")
        assert results[0].passed is False
        assert results[0].failing_rows == 1

    def test_this_replacement(self, duckdb_conn):
        """Verify __THIS__ is replaced with the actual table name."""
        audit = AuditConfig(
            audit_type="custom_sql",
            sql="SELECT * FROM __THIS__ LIMIT 0",
        )
        runner = AuditRunner(duckdb_conn)
        results = runner.run_audits([audit], "test_data")
        assert results[0].passed is True


class TestAuditSeverity:
    """Test severity levels."""

    def test_error_severity(self, duckdb_conn):
        audit = AuditConfig(audit_type="not_null", columns=["name"], severity="error")
        runner = AuditRunner(duckdb_conn)
        results = runner.run_audits([audit], "test_data")
        assert results[0].severity == "error"

    def test_warn_severity(self, duckdb_conn):
        audit = AuditConfig(audit_type="not_null", columns=["name"], severity="warn")
        runner = AuditRunner(duckdb_conn)
        results = runner.run_audits([audit], "test_data")
        assert results[0].severity == "warn"


class TestAuditDuration:
    """Test that duration is tracked."""

    def test_duration_recorded(self, duckdb_conn):
        audit = AuditConfig(audit_type="row_count", min_count=0)
        runner = AuditRunner(duckdb_conn)
        results = runner.run_audits([audit], "test_data")
        assert results[0].duration_ms >= 0
