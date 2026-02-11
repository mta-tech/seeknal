"""
SQL diff module using SQLGlot AST comparison.

This module provides SQL diffing capabilities for the Seeknal DAG,
including AST-based comparison, edit type classification, and change
categorization for detecting breaking changes.

Key features:
- AST-based SQL comparison (semantic diffs, not text-based)
- Edit type classification (INSERT/DELETE/UPDATE)
- Change categorization (breaking/non-breaking)
- Column-level and table-level change detection
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Set, Dict, Any, Tuple

from .sql_parser import SQLParser, SQLDialect, SQLParseResult, SQLGLOT_AVAILABLE

logger = logging.getLogger(__name__)


class EditType(Enum):
    """Types of edits detected in SQL diff."""
    INSERT = "insert"  # New element added
    DELETE = "delete"  # Element removed
    UPDATE = "update"  # Element modified
    MOVE = "move"      # Element position changed
    NONE = "none"      # No change
    UNKNOWN = "unknown"  # Cannot determine type


class ChangeCategory(Enum):
    """Categories of changes based on impact."""
    BREAKING = "breaking"           # Breaking change, requires remediation
    NON_BREAKING = "non_breaking"   # Safe change, no action needed
    UNKNOWN = "unknown"             # Cannot determine impact


class ChangeType(Enum):
    """Specific types of SQL changes."""
    # Column changes
    COLUMN_ADDED = "column_added"
    COLUMN_REMOVED = "column_removed"
    COLUMN_RENAMED = "column_renamed"
    COLUMN_TYPE_CHANGED = "column_type_changed"

    # Table changes
    TABLE_ADDED = "table_added"
    TABLE_REMOVED = "table_removed"
    TABLE_RENAMED = "table_renamed"

    # Join changes
    JOIN_ADDED = "join_added"
    JOIN_REMOVED = "join_removed"
    JOIN_CHANGED = "join_changed"

    # Filter changes
    FILTER_ADDED = "filter_added"
    FILTER_REMOVED = "filter_removed"
    FILTER_CHANGED = "filter_changed"

    # Aggregation changes
    AGGREGATION_ADDED = "aggregation_added"
    AGGREGATION_REMOVED = "aggregation_removed"
    AGGREGATION_CHANGED = "aggregation_changed"

    # Structural changes
    STRUCTURAL_CHANGE = "structural_change"

    # Unknown
    UNKNOWN = "unknown"


@dataclass
class SQLEdit:
    """A single edit detected in SQL diff."""
    edit_type: EditType
    change_type: ChangeType
    category: ChangeCategory
    description: str
    location: Optional[str] = None
    old_value: Optional[str] = None
    new_value: Optional[str] = None

    def __str__(self) -> str:
        """String representation of the edit."""
        parts = [f"[{self.edit_type.value.upper()}]"]
        if self.category != ChangeCategory.UNKNOWN:
            parts.append(f"[{self.category.value}]")
        parts.append(self.change_type.value.replace("_", " ").title())
        parts.append(f": {self.description}")
        return " ".join(parts)


@dataclass
class SQLDiffResult:
    """Result of diffing two SQL statements."""
    source_sql: str
    target_sql: str
    has_changes: bool = False
    edits: List[SQLEdit] = field(default_factory=list)
    is_breaking: bool = False

    @property
    def breaking_changes(self) -> List[SQLEdit]:
        """Get only breaking changes."""
        return [e for e in self.edits if e.category == ChangeCategory.BREAKING]

    @property
    def non_breaking_changes(self) -> List[SQLEdit]:
        """Get only non-breaking changes."""
        return [e for e in self.edits if e.category == ChangeCategory.NON_BREAKING]

    @property
    def summary(self) -> Dict[str, int]:
        """Get summary of changes by type."""
        summary = {
            "total": len(self.edits),
            "breaking": len(self.breaking_changes),
            "non_breaking": len(self.non_breaking_changes),
        }
        # Count by change type
        for edit in self.edits:
            change_type = edit.change_type.value
            summary[change_type] = summary.get(change_type, 0) + 1
        return summary


class SQLDiffer:
    """
    SQL differ using SQLGlot for AST-based comparison.

    Compares two SQL statements semantically (not text-based) to
    detect meaningful changes in the query structure.

    Attributes:
        parser: SQLParser instance for parsing SQL
    """

    def __init__(self, dialect: SQLDialect = SQLDialect.GENERIC):
        """
        Initialize the SQL differ.

        Args:
            dialect: The SQL dialect to use for parsing

        Raises:
            ImportError: If sqlglot is not installed
        """
        if not SQLGLOT_AVAILABLE:
            raise ImportError(
                "sqlglot is required for SQL diffing. "
                "Install with: pip install sqlglot~=25.0.0"
            )

        self.parser = SQLParser(dialect=dialect)

    def diff(self, source_sql: str, target_sql: str) -> SQLDiffResult:
        """
        Compare two SQL statements and detect changes.

        Performs AST-based comparison to detect semantic differences,
        not just text differences.

        Args:
            source_sql: The original SQL statement
            target_sql: The modified SQL statement

        Returns:
            SQLDiffResult with detected changes

        Examples:
            >>> differ = SQLDiffer()
            >>> source = "SELECT a, b FROM foo"
            >>> target = "SELECT a, b, c FROM foo"
            >>> result = differ.diff(source, target)
            >>> result.has_changes
            True
            >>> result.breaking_changes
            []
        """
        # Parse both SQL statements
        source_result = self.parser.parse(source_sql)
        target_result = self.parser.parse(target_sql)

        # If either is invalid, return basic comparison
        if not source_result.is_valid or not target_result.is_valid:
            return self._fallback_diff(source_sql, target_sql, source_result, target_result)

        # Check if SQL is identical
        if source_result.normalized_sql == target_result.normalized_sql:
            return SQLDiffResult(
                source_sql=source_sql,
                target_sql=target_sql,
                has_changes=False,
                edits=[]
            )

        # Detect changes
        edits = self._detect_changes(source_result, target_result)

        # Determine if any are breaking
        is_breaking = any(e.category == ChangeCategory.BREAKING for e in edits)

        return SQLDiffResult(
            source_sql=source_sql,
            target_sql=target_sql,
            has_changes=True,
            edits=edits,
            is_breaking=is_breaking
        )

    def _fallback_diff(
        self,
        source_sql: str,
        target_sql: str,
        source_result: SQLParseResult,
        target_result: SQLParseResult
    ) -> SQLDiffResult:
        """Fallback diff when one or both SQL statements are invalid."""
        edits = []

        if not source_result.is_valid and not target_result.is_valid:
            # Both invalid - minimal info
            edits.append(SQLEdit(
                edit_type=EditType.UNKNOWN,
                change_type=ChangeType.UNKNOWN,
                category=ChangeCategory.UNKNOWN,
                description=f"Both SQL statements are invalid: {source_result.error or target_result.error}"
            ))
        elif not source_result.is_valid:
            edits.append(SQLEdit(
                edit_type=EditType.UNKNOWN,
                change_type=ChangeType.UNKNOWN,
                category=ChangeCategory.UNKNOWN,
                description=f"Source SQL is invalid: {source_result.error}"
            ))
        elif not target_result.is_valid:
            edits.append(SQLEdit(
                edit_type=EditType.UNKNOWN,
                change_type=ChangeType.UNKNOWN,
                category=ChangeCategory.UNKNOWN,
                description=f"Target SQL is invalid: {target_result.error}"
            ))
        else:
            # Both valid but normalization failed - text diff
            if source_sql.strip() != target_sql.strip():
                edits.append(SQLEdit(
                    edit_type=EditType.UPDATE,
                    change_type=ChangeType.STRUCTURAL_CHANGE,
                    category=ChangeCategory.UNKNOWN,
                    description="SQL text differs (normalization failed)"
                ))

        return SQLDiffResult(
            source_sql=source_sql,
            target_sql=target_sql,
            has_changes=bool(edits),
            edits=edits
        )

    def _detect_changes(
        self,
        source_result: SQLParseResult,
        target_result: SQLParseResult
    ) -> List[SQLEdit]:
        """Detect changes between two parsed SQL results."""
        edits: List[SQLEdit] = []

        # Column changes
        edits.extend(self._detect_column_changes(source_result, target_result))

        # Table changes
        edits.extend(self._detect_table_changes(source_result, target_result))

        # If no specific changes detected but SQL differs, mark as structural
        if not edits:
            edits.append(SQLEdit(
                edit_type=EditType.UPDATE,
                change_type=ChangeType.STRUCTURAL_CHANGE,
                category=ChangeCategory.NON_BREAKING,
                description="SQL structure changed (whitespace, comments, or formatting)"
            ))

        return edits

    def _detect_column_changes(
        self,
        source_result: SQLParseResult,
        target_result: SQLParseResult
    ) -> List[SQLEdit]:
        """Detect column-related changes."""
        edits: List[SQLEdit] = []

        source_columns = set(source_result.columns)
        target_columns = set(target_result.columns)

        # Added columns (non-breaking)
        added_columns = target_columns - source_columns
        for col in sorted(added_columns):
            edits.append(SQLEdit(
                edit_type=EditType.INSERT,
                change_type=ChangeType.COLUMN_ADDED,
                category=ChangeCategory.NON_BREAKING,
                description=f"Column '{col}' was added",
                old_value=None,
                new_value=col
            ))

        # Removed columns (breaking!)
        removed_columns = source_columns - target_columns
        for col in sorted(removed_columns):
            edits.append(SQLEdit(
                edit_type=EditType.DELETE,
                change_type=ChangeType.COLUMN_REMOVED,
                category=ChangeCategory.BREAKING,
                description=f"Column '{col}' was removed",
                old_value=col,
                new_value=None
            ))

        return edits

    def _detect_table_changes(
        self,
        source_result: SQLParseResult,
        target_result: SQLParseResult
    ) -> List[SQLEdit]:
        """Detect table-related changes."""
        edits: List[SQLEdit] = []

        source_tables = set(source_result.tables)
        target_tables = set(target_result.tables)

        # Added tables
        added_tables = target_tables - source_tables
        for table in sorted(added_tables):
            edits.append(SQLEdit(
                edit_type=EditType.INSERT,
                change_type=ChangeType.TABLE_ADDED,
                category=ChangeCategory.NON_BREAKING,
                description=f"Table '{table}' was added (new JOIN or source)",
                old_value=None,
                new_value=table
            ))

        # Removed tables
        removed_tables = source_tables - target_tables
        for table in sorted(removed_tables):
            edits.append(SQLEdit(
                edit_type=EditType.DELETE,
                change_type=ChangeType.TABLE_REMOVED,
                category=ChangeCategory.BREAKING,
                description=f"Table '{table}' was removed (JOIN removed or source changed)",
                old_value=table,
                new_value=None
            ))

        return edits

    def categorize_change(self, edit: SQLEdit) -> ChangeCategory:
        """
        Categorize a change as breaking or non-breaking.

        Default categorization logic based on change type.
        Can be overridden for custom logic.

        Args:
            edit: The SQLEdit to categorize

        Returns:
            ChangeCategory (BREAKING or NON_BREAKING)
        """
        # Breaking changes
        if edit.change_type in (
            ChangeType.COLUMN_REMOVED,
            ChangeType.TABLE_REMOVED,
            ChangeType.COLUMN_TYPE_CHANGED,
        ):
            return ChangeCategory.BREAKING

        # Non-breaking changes
        if edit.change_type in (
            ChangeType.COLUMN_ADDED,
            ChangeType.TABLE_ADDED,
            ChangeType.COLUMN_RENAMED,
            ChangeType.TABLE_RENAMED,
            ChangeType.JOIN_ADDED,
            ChangeType.FILTER_ADDED,
            ChangeType.AGGREGATION_ADDED,
        ):
            return ChangeCategory.NON_BREAKING

        # Context-dependent changes
        if edit.change_type in (
            ChangeType.JOIN_REMOVED,
            ChangeType.JOIN_CHANGED,
            ChangeType.FILTER_REMOVED,
            ChangeType.FILTER_CHANGED,
            ChangeType.AGGREGATION_REMOVED,
            ChangeType.AGGREGATION_CHANGED,
        ):
            return ChangeCategory.UNKNOWN

        return ChangeCategory.UNKNOWN

    def get_breaking_changes(self, source_sql: str, target_sql: str) -> List[SQLEdit]:
        """
        Get only breaking changes between two SQL statements.

        Args:
            source_sql: The original SQL statement
            target_sql: The modified SQL statement

        Returns:
            List of breaking SQLEdit objects
        """
        result = self.diff(source_sql, target_sql)
        return result.breaking_changes

    def has_breaking_changes(self, source_sql: str, target_sql: str) -> bool:
        """
        Check if there are any breaking changes between two SQL statements.

        Args:
            source_sql: The original SQL statement
            target_sql: The modified SQL statement

        Returns:
            True if there are breaking changes, False otherwise
        """
        result = self.diff(source_sql, target_sql)
        return result.is_breaking


def create_sql_differ(dialect: SQLDialect = SQLDialect.GENERIC) -> SQLDiffer:
    """
    Factory function to create a SQL differ.

    Args:
        dialect: The SQL dialect to use for parsing

    Returns:
        Configured SQLDiffer instance

    Examples:
        >>> differ = create_sql_differ(SQLDialect.DUCKDB)
        >>> result = differ.diff("SELECT a FROM foo", "SELECT a, b FROM foo")
    """
    return SQLDiffer(dialect=dialect)


# Convenience functions

def diff_sql(
    source_sql: str,
    target_sql: str,
    dialect: SQLDialect = SQLDialect.GENERIC
) -> SQLDiffResult:
    """Diff two SQL statements."""
    differ = SQLDiffer(dialect=dialect)
    return differ.diff(source_sql, target_sql)


def has_breaking_changes(
    source_sql: str,
    target_sql: str,
    dialect: SQLDialect = SQLDialect.GENERIC
) -> bool:
    """Check if SQL diff has breaking changes."""
    differ = SQLDiffer(dialect=dialect)
    return differ.has_breaking_changes(source_sql, target_sql)
