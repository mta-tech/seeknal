"""
Tests for SQL diff module using SQLGlot AST comparison.
"""

import pytest

from seeknal.dag.sql_diff import (
    SQLDiffer,
    EditType,
    ChangeType,
    ChangeCategory,
    SQLEdit,
    SQLDiffResult,
    create_sql_differ,
    diff_sql,
    has_breaking_changes,
    SQLGLOT_AVAILABLE,
)


@pytest.mark.skipif(not SQLGLOT_AVAILABLE, reason="sqlglot not installed")
class TestSQLEdit:
    """Tests for the SQLEdit dataclass."""

    def test_create_edit(self):
        """Test creating an SQL edit."""
        edit = SQLEdit(
            edit_type=EditType.INSERT,
            change_type=ChangeType.COLUMN_ADDED,
            category=ChangeCategory.NON_BREAKING,
            description="Column 'x' was added",
            old_value=None,
            new_value="x"
        )
        assert edit.edit_type == EditType.INSERT
        assert edit.change_type == ChangeType.COLUMN_ADDED
        assert edit.category == ChangeCategory.NON_BREAKING

    def test_edit_string_representation(self):
        """Test string representation of an edit."""
        edit = SQLEdit(
            edit_type=EditType.DELETE,
            change_type=ChangeType.COLUMN_REMOVED,
            category=ChangeCategory.BREAKING,
            description="Column 'x' was removed"
        )
        str_repr = str(edit)
        assert "[DELETE]" in str_repr
        assert "breaking" in str_repr
        assert "Column Removed" in str_repr


@pytest.mark.skipif(not SQLGLOT_AVAILABLE, reason="sqlglot not installed")
class TestSQLDiffResult:
    """Tests for the SQLDiffResult dataclass."""

    def test_create_result(self):
        """Test creating a diff result."""
        result = SQLDiffResult(
            source_sql="SELECT a FROM foo",
            target_sql="SELECT a, b FROM foo",
            has_changes=True,
            edits=[]
        )
        assert result.source_sql == "SELECT a FROM foo"
        assert result.has_changes is True
        assert result.is_breaking is False

    def test_breaking_changes_filter(self):
        """Test filtering breaking changes."""
        breaking_edit = SQLEdit(
            edit_type=EditType.DELETE,
            change_type=ChangeType.COLUMN_REMOVED,
            category=ChangeCategory.BREAKING,
            description="Breaking change"
        )
        non_breaking_edit = SQLEdit(
            edit_type=EditType.INSERT,
            change_type=ChangeType.COLUMN_ADDED,
            category=ChangeCategory.NON_BREAKING,
            description="Non-breaking change"
        )

        result = SQLDiffResult(
            source_sql="SELECT a FROM foo",
            target_sql="SELECT b FROM foo",
            has_changes=True,
            edits=[breaking_edit, non_breaking_edit],
            is_breaking=True
        )

        assert len(result.breaking_changes) == 1
        assert len(result.non_breaking_changes) == 1

    def test_summary(self):
        """Test summary of changes."""
        edit1 = SQLEdit(
            edit_type=EditType.INSERT,
            change_type=ChangeType.COLUMN_ADDED,
            category=ChangeCategory.NON_BREAKING,
            description="Column added"
        )
        edit2 = SQLEdit(
            edit_type=EditType.DELETE,
            change_type=ChangeType.COLUMN_REMOVED,
            category=ChangeCategory.BREAKING,
            description="Column removed"
        )

        result = SQLDiffResult(
            source_sql="SELECT a FROM foo",
            target_sql="SELECT b FROM foo",
            has_changes=True,
            edits=[edit1, edit2],
            is_breaking=True
        )

        summary = result.summary
        assert summary["total"] == 2
        assert summary["breaking"] == 1
        assert summary["non_breaking"] == 1
        assert summary["column_added"] == 1
        assert summary["column_removed"] == 1


@pytest.mark.skipif(not SQLGLOT_AVAILABLE, reason="sqlglot not installed")
class TestSQLDiffer:
    """Tests for the SQLDiffer class."""

    def test_init(self):
        """Test creating a SQL differ."""
        differ = SQLDiffer()
        assert differ.parser is not None

    def test_init_with_dialect(self):
        """Test creating a differ with a specific dialect."""
        from seeknal.dag.sql_parser import SQLDialect
        differ = SQLDiffer(dialect=SQLDialect.DUCKDB)
        assert differ.parser is not None

    def test_diff_identical_sql(self):
        """Test diffing identical SQL statements."""
        differ = SQLDiffer()
        sql = "SELECT a, b FROM foo"
        result = differ.diff(sql, sql)

        assert result.has_changes is False
        assert len(result.edits) == 0
        assert result.is_breaking is False

    def test_diff_whitespace_only(self):
        """Test that whitespace-only changes are not detected as changes."""
        differ = SQLDiffer()
        sql1 = "SELECT a, b FROM foo"
        sql2 = "SELECT  a,b  FROM  foo"
        result = differ.diff(sql1, sql2)

        # Normalized SQL should be the same
        assert result.has_changes is False

    def test_diff_case_insensitive(self):
        """Test that case differences are normalized."""
        differ = SQLDiffer()
        sql1 = "SELECT a FROM foo"
        sql2 = "select a from foo"
        result = differ.diff(sql1, sql2)

        # Should normalize to same form
        assert result.has_changes is False

    def test_diff_column_added(self):
        """Test detecting an added column."""
        differ = SQLDiffer()
        source = "SELECT a, b FROM foo"
        target = "SELECT a, b, c FROM foo"
        result = differ.diff(source, target)

        assert result.has_changes is True
        assert len(result.edits) == 1
        assert result.edits[0].change_type == ChangeType.COLUMN_ADDED
        assert result.edits[0].category == ChangeCategory.NON_BREAKING
        assert result.is_breaking is False

    def test_diff_column_removed(self):
        """Test detecting a removed column (breaking change)."""
        differ = SQLDiffer()
        source = "SELECT a, b, c FROM foo"
        target = "SELECT a, b FROM foo"
        result = differ.diff(source, target)

        assert result.has_changes is True
        assert len(result.edits) == 1
        assert result.edits[0].change_type == ChangeType.COLUMN_REMOVED
        assert result.edits[0].category == ChangeCategory.BREAKING
        assert result.is_breaking is True

    def test_diff_multiple_column_changes(self):
        """Test detecting multiple column changes."""
        differ = SQLDiffer()
        source = "SELECT a, b FROM foo"
        target = "SELECT a, c, d FROM foo"
        result = differ.diff(source, target)

        assert result.has_changes is True
        # b removed (breaking), c and d added (non-breaking)
        assert len(result.edits) == 3
        assert any(e.change_type == ChangeType.COLUMN_REMOVED for e in result.edits)
        assert any(e.change_type == ChangeType.COLUMN_ADDED for e in result.edits)

    def test_diff_table_added(self):
        """Test detecting an added table (new JOIN)."""
        differ = SQLDiffer()
        source = "SELECT * FROM users"
        target = "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
        result = differ.diff(source, target)

        assert result.has_changes is True
        assert any(e.change_type == ChangeType.TABLE_ADDED for e in result.edits)

    def test_diff_table_removed(self):
        """Test detecting a removed table (breaking change)."""
        differ = SQLDiffer()
        source = "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
        target = "SELECT * FROM users"
        result = differ.diff(source, target)

        assert result.has_changes is True
        assert any(e.change_type == ChangeType.TABLE_REMOVED for e in result.edits)
        # Removing a table (JOIN) could be breaking
        removed_tables = [e for e in result.edits if e.change_type == ChangeType.TABLE_REMOVED]
        assert len(removed_tables) > 0
        assert removed_tables[0].category == ChangeCategory.BREAKING

    def test_diff_with_filter(self):
        """Test detecting changes in WHERE clause."""
        differ = SQLDiffer()
        source = "SELECT a FROM foo WHERE b > 10"
        target = "SELECT a FROM foo WHERE c > 10"

        # Different columns used in filter
        result = differ.diff(source, target)
        assert result.has_changes is True

    def test_diff_invalid_source_sql(self):
        """Test diffing with invalid source SQL."""
        differ = SQLDiffer()
        source = "SELCET * FROM foo"  # Invalid
        target = "SELECT * FROM foo"
        result = differ.diff(source, target)

        assert result.has_changes is True
        assert any("invalid" in e.description.lower() for e in result.edits)

    def test_diff_invalid_target_sql(self):
        """Test diffing with invalid target SQL."""
        differ = SQLDiffer()
        source = "SELECT * FROM foo"
        target = "SELCET * FROM foo"  # Invalid
        result = differ.diff(source, target)

        assert result.has_changes is True
        assert any("invalid" in e.description.lower() for e in result.edits)

    def test_diff_both_invalid(self):
        """Test diffing when both SQL statements are invalid."""
        differ = SQLDiffer()
        source = "INVALID SQL HERE"
        target = "ALSO INVALID"
        result = differ.diff(source, target)

        assert result.has_changes is True
        assert len(result.edits) > 0

    def test_get_breaking_changes(self):
        """Test getting only breaking changes."""
        differ = SQLDiffer()
        source = "SELECT a, b, c FROM foo"
        target = "SELECT a, b, d FROM foo"
        breaking = differ.get_breaking_changes(source, target)

        # c removed (breaking), d added (non-breaking)
        assert len(breaking) == 1
        assert breaking[0].change_type == ChangeType.COLUMN_REMOVED

    def test_has_breaking_changes_true(self):
        """Test has_breaking_changes returns True when there are breaking changes."""
        differ = SQLDiffer()
        source = "SELECT a, b FROM foo"
        target = "SELECT a FROM foo"  # b removed

        assert differ.has_breaking_changes(source, target) is True

    def test_has_breaking_changes_false(self):
        """Test has_breaking_changes returns False when there are no breaking changes."""
        differ = SQLDiffer()
        source = "SELECT a FROM foo"
        target = "SELECT a, b FROM foo"  # b added

        assert differ.has_breaking_changes(source, target) is False

    def test_categorize_change(self):
        """Test change categorization logic."""
        differ = SQLDiffer()

        # Column removed - breaking
        edit = SQLEdit(
            edit_type=EditType.DELETE,
            change_type=ChangeType.COLUMN_REMOVED,
            category=ChangeCategory.UNKNOWN,
            description="Column removed"
        )
        category = differ.categorize_change(edit)
        assert category == ChangeCategory.BREAKING

        # Column added - non-breaking
        edit2 = SQLEdit(
            edit_type=EditType.INSERT,
            change_type=ChangeType.COLUMN_ADDED,
            category=ChangeCategory.UNKNOWN,
            description="Column added"
        )
        category2 = differ.categorize_change(edit2)
        assert category2 == ChangeCategory.NON_BREAKING


@pytest.mark.skipif(not SQLGLOT_AVAILABLE, reason="sqlglot not installed")
class TestCreateSQLDiffer:
    """Tests for the create_sql_differ factory function."""

    def test_create_default_differ(self):
        """Test creating differ with default dialect."""
        differ = create_sql_differ()
        assert differ.parser is not None

    def test_create_duckdb_differ(self):
        """Test creating DuckDB differ."""
        from seeknal.dag.sql_parser import SQLDialect
        differ = create_sql_differ(SQLDialect.DUCKDB)
        assert differ.parser is not None


@pytest.mark.skipif(not SQLGLOT_AVAILABLE, reason="sqlglot not installed")
class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_diff_sql_function(self):
        """Test diff_sql convenience function."""
        source = "SELECT a FROM foo"
        target = "SELECT a, b FROM foo"
        result = diff_sql(source, target)

        assert result.has_changes is True

    def test_has_breaking_changes_function(self):
        """Test has_breaking_changes convenience function."""
        source = "SELECT a, b FROM foo"
        target = "SELECT a FROM foo"

        assert has_breaking_changes(source, target) is True
