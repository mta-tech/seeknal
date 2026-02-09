"""
Data quality audit engine for Seeknal workflow nodes.

Provides inline audit rules that can be attached to any node via the `audits:`
YAML section. Audits execute DuckDB SQL to validate data quality after node
execution.

Built-in audit types:
- not_null: Check that specified columns have no NULL values
- unique: Check that column combinations are unique
- accepted_values: Check that column values are in an allowed set
- row_count: Check that row count is within min/max bounds
- custom_sql: Execute user-provided SQL that returns failing rows
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from seeknal.validation import validate_column_name


class AuditSeverity(Enum):
    """Severity level for audit failures."""
    ERROR = "error"
    WARN = "warn"


class AuditType(Enum):
    """Built-in audit types."""
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    ACCEPTED_VALUES = "accepted_values"
    ROW_COUNT = "row_count"
    CUSTOM_SQL = "custom_sql"


@dataclass
class AuditResult:
    """Result of running a single audit."""
    audit_type: str
    passed: bool
    failing_rows: int = 0
    severity: str = "error"
    message: str = ""
    columns: List[str] = field(default_factory=list)
    duration_ms: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "audit_type": self.audit_type,
            "passed": self.passed,
            "failing_rows": self.failing_rows,
            "severity": self.severity,
            "message": self.message,
            "columns": self.columns,
            "duration_ms": self.duration_ms,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AuditResult":
        return cls(
            audit_type=data["audit_type"],
            passed=data["passed"],
            failing_rows=data.get("failing_rows", 0),
            severity=data.get("severity", "error"),
            message=data.get("message", ""),
            columns=data.get("columns", []),
            duration_ms=data.get("duration_ms", 0),
        )


@dataclass
class AuditConfig:
    """Parsed configuration for a single audit rule."""
    audit_type: str
    severity: str = "error"
    columns: List[str] = field(default_factory=list)
    values: List[Any] = field(default_factory=list)
    min_count: Optional[int] = None
    max_count: Optional[int] = None
    sql: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AuditConfig":
        return cls(
            audit_type=data["type"],
            severity=data.get("severity", "error"),
            columns=data.get("columns", []),
            values=data.get("values", []),
            min_count=data.get("min"),
            max_count=data.get("max"),
            sql=data.get("sql"),
        )


def parse_audits(config: Dict[str, Any]) -> List[AuditConfig]:
    """
    Parse the audits section from a node's YAML config.

    Args:
        config: Node config dict (may contain 'audits' key)

    Returns:
        List of parsed AuditConfig objects

    Raises:
        ValueError: If audit schema is invalid
    """
    audits_raw = config.get("audits", [])
    if not audits_raw:
        return []

    audits = []
    valid_types = {t.value for t in AuditType}

    for idx, audit_data in enumerate(audits_raw):
        if not isinstance(audit_data, dict):
            raise ValueError(f"Audit at index {idx}: expected dict, got {type(audit_data).__name__}")

        audit_type = audit_data.get("type")
        if not audit_type:
            raise ValueError(f"Audit at index {idx}: missing required 'type' field")
        if audit_type not in valid_types:
            raise ValueError(f"Audit at index {idx}: invalid type '{audit_type}'. Valid: {valid_types}")

        severity = audit_data.get("severity", "error")
        if severity not in ("error", "warn"):
            raise ValueError(f"Audit at index {idx}: invalid severity '{severity}'. Valid: error, warn")

        # Type-specific validation
        if audit_type in ("not_null", "unique"):
            if not audit_data.get("columns"):
                raise ValueError(f"Audit at index {idx}: '{audit_type}' requires 'columns'")
        elif audit_type == "accepted_values":
            if not audit_data.get("columns"):
                raise ValueError(f"Audit at index {idx}: 'accepted_values' requires 'columns'")
            if not audit_data.get("values"):
                raise ValueError(f"Audit at index {idx}: 'accepted_values' requires 'values'")
        elif audit_type == "row_count":
            if audit_data.get("min") is None and audit_data.get("max") is None:
                raise ValueError(f"Audit at index {idx}: 'row_count' requires 'min' and/or 'max'")
        elif audit_type == "custom_sql":
            if not audit_data.get("sql"):
                raise ValueError(f"Audit at index {idx}: 'custom_sql' requires 'sql'")

        # Validate column names for SQL injection prevention
        for col in audit_data.get("columns", []):
            validate_column_name(col)

        audits.append(AuditConfig.from_dict(audit_data))

    return audits


class AuditRunner:
    """
    Executes audit rules against DuckDB tables/views.

    Usage:
        runner = AuditRunner(conn)
        results = runner.run_audits(audit_configs, "source.users")
    """

    def __init__(self, conn):
        """
        Args:
            conn: DuckDB connection
        """
        self.conn = conn

    def run_audits(
        self,
        audits: List[AuditConfig],
        table_name: str,
    ) -> List[AuditResult]:
        """
        Run all audit rules against a table.

        Args:
            audits: List of audit configurations
            table_name: Name of the DuckDB table/view to audit

        Returns:
            List of AuditResult objects
        """
        import time
        results = []
        for audit in audits:
            start = time.time()
            try:
                result = self._run_single_audit(audit, table_name)
            except Exception as e:
                result = AuditResult(
                    audit_type=audit.audit_type,
                    passed=False,
                    severity=audit.severity,
                    message=f"Audit execution error: {e}",
                    columns=audit.columns,
                )
            result.duration_ms = int((time.time() - start) * 1000)
            results.append(result)
        return results

    def _run_single_audit(self, audit: AuditConfig, table_name: str) -> AuditResult:
        """Execute a single audit rule."""
        match audit.audit_type:
            case "not_null":
                return self._audit_not_null(audit, table_name)
            case "unique":
                return self._audit_unique(audit, table_name)
            case "accepted_values":
                return self._audit_accepted_values(audit, table_name)
            case "row_count":
                return self._audit_row_count(audit, table_name)
            case "custom_sql":
                return self._audit_custom_sql(audit, table_name)
            case _:
                return AuditResult(
                    audit_type=audit.audit_type,
                    passed=False,
                    severity=audit.severity,
                    message=f"Unknown audit type: {audit.audit_type}",
                )

    def _audit_not_null(self, audit: AuditConfig, table_name: str) -> AuditResult:
        """Check that columns have no NULL values."""
        conditions = " OR ".join(f'"{col}" IS NULL' for col in audit.columns)
        sql = f'SELECT COUNT(*) FROM "{table_name}" WHERE {conditions}'
        count = self.conn.execute(sql).fetchone()[0]
        return AuditResult(
            audit_type="not_null",
            passed=count == 0,
            failing_rows=count,
            severity=audit.severity,
            message=f"{count} rows with NULL values" if count > 0 else "All values non-null",
            columns=audit.columns,
        )

    def _audit_unique(self, audit: AuditConfig, table_name: str) -> AuditResult:
        """Check that column combinations are unique."""
        cols = ", ".join(f'"{col}"' for col in audit.columns)
        sql = f'SELECT COUNT(*) FROM (SELECT {cols}, COUNT(*) as cnt FROM "{table_name}" GROUP BY {cols} HAVING cnt > 1)'
        count = self.conn.execute(sql).fetchone()[0]
        return AuditResult(
            audit_type="unique",
            passed=count == 0,
            failing_rows=count,
            severity=audit.severity,
            message=f"{count} duplicate groups" if count > 0 else "All values unique",
            columns=audit.columns,
        )

    def _audit_accepted_values(self, audit: AuditConfig, table_name: str) -> AuditResult:
        """Check that column values are in the accepted set."""
        col = audit.columns[0]
        validate_column_name(col)
        placeholders = ", ".join(f"'{v}'" for v in audit.values)
        sql = f'SELECT COUNT(*) FROM "{table_name}" WHERE "{col}" NOT IN ({placeholders})'
        count = self.conn.execute(sql).fetchone()[0]
        return AuditResult(
            audit_type="accepted_values",
            passed=count == 0,
            failing_rows=count,
            severity=audit.severity,
            message=f"{count} rows with invalid values" if count > 0 else "All values accepted",
            columns=audit.columns,
        )

    def _audit_row_count(self, audit: AuditConfig, table_name: str) -> AuditResult:
        """Check that row count is within bounds."""
        sql = f'SELECT COUNT(*) FROM "{table_name}"'
        count = self.conn.execute(sql).fetchone()[0]
        passed = True
        msgs = []
        if audit.min_count is not None and count < audit.min_count:
            passed = False
            msgs.append(f"count {count} < min {audit.min_count}")
        if audit.max_count is not None and count > audit.max_count:
            passed = False
            msgs.append(f"count {count} > max {audit.max_count}")
        return AuditResult(
            audit_type="row_count",
            passed=passed,
            failing_rows=0 if passed else count,
            severity=audit.severity,
            message="; ".join(msgs) if msgs else f"Row count {count} within bounds",
        )

    def _audit_custom_sql(self, audit: AuditConfig, table_name: str) -> AuditResult:
        """Execute custom SQL that returns failing rows."""
        # Replace __THIS__ with the actual table name
        sql = audit.sql.replace("__THIS__", f'"{table_name}"')
        # Wrap to count failing rows
        count_sql = f"SELECT COUNT(*) FROM ({sql})"
        count = self.conn.execute(count_sql).fetchone()[0]
        return AuditResult(
            audit_type="custom_sql",
            passed=count == 0,
            failing_rows=count,
            severity=audit.severity,
            message=f"{count} failing rows" if count > 0 else "Custom audit passed",
        )
