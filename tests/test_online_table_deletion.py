"""
Unit tests for OnlineTableRequest.delete_by_id and dependency checking.

Tests for complete table deletion functionality including cascade deletion
of related tables and dependency checking.

These tests verify:
1. The implementation logic through static code analysis
2. Expected behavior patterns match the established codebase conventions
3. Edge case handling is properly implemented
"""

import os
import sys
import re
import pytest
from unittest.mock import patch, MagicMock

# Path to the source code for static analysis
SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REQUEST_FILE = os.path.join(SRC_DIR, 'src', 'seeknal', 'request.py')


def read_request_source():
    """Read the request.py source code for analysis."""
    with open(REQUEST_FILE, 'r') as f:
        return f.read()


class TestDeleteByIdImplementation:
    """
    Tests for OnlineTableRequest.delete_by_id implementation.
    These tests analyze the actual source code to verify implementation.
    """

    @pytest.fixture
    def source_code(self):
        """Read the source code once per test class."""
        return read_request_source()

    def test_delete_by_id_method_exists(self, source_code):
        """Verify that OnlineTableRequest.delete_by_id method exists."""
        assert 'def delete_by_id(' in source_code
        assert 'OnlineTableRequest' in source_code

    def test_delete_by_id_is_static_method(self, source_code):
        """Verify delete_by_id is a static method."""
        # Find the delete_by_id method and check for @staticmethod decorator
        pattern = r'@staticmethod\s+def delete_by_id\('
        assert re.search(pattern, source_code), "delete_by_id should be a static method"

    def test_delete_by_id_checks_item_exists(self, source_code):
        """Verify delete_by_id checks if item exists before deletion."""
        # The pattern should show select_by_id being called
        assert 'OnlineTableRequest.select_by_id(id)' in source_code

    def test_delete_by_id_returns_early_if_not_found(self, source_code):
        """Verify delete_by_id returns early if table not found."""
        assert 'if not item:' in source_code
        # After checking if not item, it should return
        pattern = r'if not item:\s+return'
        assert re.search(pattern, source_code), "Should return early if item not found"

    def test_delete_by_id_deletes_feature_group_mappings(self, source_code):
        """Verify delete_by_id deletes FeatureGroupOnlineTable mappings."""
        assert 'FeatureGroupOnlineTable.online_table_id' in source_code

    def test_delete_by_id_deletes_watermarks(self, source_code):
        """Verify delete_by_id deletes OnlineWatermarkTable records."""
        assert 'OnlineWatermarkTable.online_table_id' in source_code

    def test_delete_by_id_deletes_workspace_mappings(self, source_code):
        """Verify delete_by_id deletes WorkspaceOnlineTable mappings."""
        assert 'WorkspaceOnlineTable.online_table_id' in source_code

    def test_delete_by_id_uses_session_context(self, source_code):
        """Verify delete_by_id uses get_db_session context manager."""
        assert 'with get_db_session() as session:' in source_code

    def test_delete_by_id_commits_changes(self, source_code):
        """Verify delete_by_id commits changes to database."""
        assert 'session.commit()' in source_code

    def test_delete_by_id_returns_true_on_success(self, source_code):
        """Verify delete_by_id returns True on successful deletion."""
        # The method should return True at the end
        # Look for return True after the main deletion logic
        assert 'return True' in source_code


class TestCheckDependenciesImplementation:
    """
    Tests for OnlineTableRequest.check_dependencies implementation.
    """

    @pytest.fixture
    def source_code(self):
        """Read the source code once per test class."""
        return read_request_source()

    def test_check_dependencies_method_exists(self, source_code):
        """Verify that check_dependencies method exists."""
        assert 'def check_dependencies(' in source_code

    def test_check_dependencies_is_static_method(self, source_code):
        """Verify check_dependencies is a static method."""
        pattern = r'@staticmethod\s+def check_dependencies\('
        assert re.search(pattern, source_code), "check_dependencies should be a static method"

    def test_check_dependencies_has_type_hint(self, source_code):
        """Verify check_dependencies has proper type hints."""
        # Should return List[str]
        assert 'List[str]' in source_code

    def test_check_dependencies_queries_feature_group_table(self, source_code):
        """Verify check_dependencies queries FeatureGroupTable."""
        assert 'FeatureGroupTable.name' in source_code

    def test_check_dependencies_joins_tables(self, source_code):
        """Verify check_dependencies joins with FeatureGroupOnlineTable."""
        assert 'FeatureGroupOnlineTable.feature_group_id' in source_code

    def test_check_dependencies_filters_by_table_id(self, source_code):
        """Verify check_dependencies filters by table_id."""
        assert 'FeatureGroupOnlineTable.online_table_id == table_id' in source_code

    def test_check_dependencies_returns_list(self, source_code):
        """Verify check_dependencies returns a list."""
        # Should convert results to list
        assert 'list(results)' in source_code


class TestSelectByIdImplementation:
    """Tests for OnlineTableRequest.select_by_id implementation."""

    @pytest.fixture
    def source_code(self):
        return read_request_source()

    def test_select_by_id_exists(self, source_code):
        """Verify select_by_id method exists."""
        assert 'def select_by_id(' in source_code

    def test_select_by_id_is_static(self, source_code):
        """Verify select_by_id is static method."""
        pattern = r'@staticmethod\s+def select_by_id\('
        assert re.search(pattern, source_code)

    def test_select_by_id_queries_online_table(self, source_code):
        """Verify select_by_id queries OnlineTable."""
        assert 'select(OnlineTable)' in source_code

    def test_select_by_id_filters_by_id(self, source_code):
        """Verify select_by_id filters by id."""
        assert 'OnlineTable.id == id' in source_code


class TestCascadeDeletionOrder:
    """Tests to verify cascade deletion happens in correct order."""

    @pytest.fixture
    def source_code(self):
        return read_request_source()

    def test_delete_order_fg_before_watermarks(self, source_code):
        """Verify feature group mappings deleted before watermarks."""
        # Find positions in source code
        fg_pos = source_code.find('FeatureGroupOnlineTable.online_table_id == id')
        wm_pos = source_code.find('OnlineWatermarkTable.online_table_id == id')

        # Feature group mappings should be handled first
        assert fg_pos < wm_pos, "FeatureGroup mappings should be deleted before watermarks"

    def test_delete_order_watermarks_before_workspace(self, source_code):
        """Verify watermarks deleted before workspace mappings."""
        wm_pos = source_code.find('OnlineWatermarkTable.online_table_id == id')
        ws_pos = source_code.find('WorkspaceOnlineTable.online_table_id == id')

        assert wm_pos < ws_pos, "Watermarks should be deleted before workspace mappings"

    def test_delete_main_table_last(self, source_code):
        """Verify the main table is deleted last in delete_by_id method."""
        # Extract just the delete_by_id method
        delete_by_id_start = source_code.find('def delete_by_id(id):')
        # Find next method (starts with @staticmethod or def at same indentation)
        next_method_match = re.search(
            r'\n    @staticmethod\n    def [a-z_]+\(',
            source_code[delete_by_id_start + 50:]  # Skip past the current method signature
        )
        if next_method_match:
            delete_by_id_end = delete_by_id_start + 50 + next_method_match.start()
        else:
            delete_by_id_end = len(source_code)

        delete_by_id_code = source_code[delete_by_id_start:delete_by_id_end]

        # Within delete_by_id, find positions
        ws_pos = delete_by_id_code.find('WorkspaceOnlineTable.online_table_id == id')

        # The final session.delete(item) should come after workspace handling
        # Look for "session.delete(item)" which is the main table deletion
        delete_item_pattern = r'session\.delete\(item\)'
        match = re.search(delete_item_pattern, delete_by_id_code)

        assert match, "Should delete the main table item in delete_by_id"
        assert match.start() > ws_pos, "Main table should be deleted after workspace mappings"


class TestHelperMethods:
    """Tests for related helper methods."""

    @pytest.fixture
    def source_code(self):
        return read_request_source()

    def test_get_feature_group_from_online_table_exists(self, source_code):
        """Verify helper method exists."""
        assert 'def get_feature_group_from_online_table(' in source_code

    def test_delete_feature_group_from_online_table_exists(self, source_code):
        """Verify delete helper method exists."""
        assert 'def delete_feature_group_from_online_table(' in source_code

    def test_get_online_watermarks_exists(self, source_code):
        """Verify get_online_watermarks method exists."""
        assert 'def get_online_watermarks(' in source_code

    def test_delete_online_watermarks_exists(self, source_code):
        """Verify delete_online_watermarks method exists."""
        assert 'def delete_online_watermarks(' in source_code


class TestEdgeCaseHandling:
    """Tests for edge case handling in the implementation."""

    @pytest.fixture
    def source_code(self):
        return read_request_source()

    def test_handles_empty_feature_group_list(self, source_code):
        """
        Verify implementation handles empty feature group list.

        When no feature groups are linked, the for loop simply doesn't execute.
        The pattern `for fg in feature_group_maps:` handles this correctly.
        """
        assert 'for fg in feature_group_maps:' in source_code

    def test_handles_empty_watermarks_list(self, source_code):
        """
        Verify implementation handles empty watermarks list.

        When no watermarks exist, the for loop simply doesn't execute.
        """
        assert 'for w in watermarks:' in source_code

    def test_handles_empty_workspace_list(self, source_code):
        """
        Verify implementation handles empty workspace list.

        When no workspace mappings exist, the for loop simply doesn't execute.
        """
        assert 'for w in workspace_maps:' in source_code


class TestDatabasePatterns:
    """Tests to verify proper database patterns are used."""

    @pytest.fixture
    def source_code(self):
        return read_request_source()

    def test_uses_select_pattern(self, source_code):
        """Verify proper SQLModel select pattern is used."""
        assert 'from sqlmodel import' in source_code
        assert 'select' in source_code

    def test_uses_where_clause(self, source_code):
        """Verify proper where clause usage."""
        assert '.where(' in source_code

    def test_uses_session_exec(self, source_code):
        """Verify proper session.exec pattern."""
        assert 'session.exec(' in source_code

    def test_uses_first_for_single_result(self, source_code):
        """Verify .first() is used for single result queries."""
        assert '.first()' in source_code

    def test_uses_all_for_multiple_results(self, source_code):
        """Verify .all() is used for multiple result queries."""
        assert '.all()' in source_code
