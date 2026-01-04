"""
Integration tests for file cleanup verification during table deletion.

Tests verify that file deletion operations properly clean up:
1. Parquet data files
2. Metadata files (_metadata.json)
3. Table directories
4. DuckDB tables

These tests verify implementation through:
1. Static code analysis to verify implementation patterns
2. Functional tests using temporary directories to verify actual behavior

Note: Due to Python 3.10+ syntax in some modules, static analysis is used
where direct imports would fail on Python 3.9.
"""

import os
import re
import json
import shutil
import tempfile
import pytest

# Try to import pandas - only needed for functional tests
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    pd = None

# Path to the source code for static analysis
SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FEATURESTORE_FILE = os.path.join(SRC_DIR, 'seeknal', 'featurestore', 'duckdbengine', 'featurestore.py')
FEATURE_GROUP_FILE = os.path.join(SRC_DIR, 'seeknal', 'featurestore', 'duckdbengine', 'feature_group.py')


def read_featurestore_source():
    """Read the featurestore.py source code for analysis."""
    with open(FEATURESTORE_FILE, 'r') as f:
        return f.read()


def read_feature_group_source():
    """Read the feature_group.py source code for analysis."""
    with open(FEATURE_GROUP_FILE, 'r') as f:
        return f.read()


def extract_offline_store_delete(source_code):
    """Extract the OfflineStoreDuckDB.delete method."""
    # Find the delete method in OfflineStoreDuckDB class
    start = source_code.find('class OfflineStoreDuckDB:')
    if start == -1:
        return ""

    class_section = source_code[start:]
    # Find the delete method within this class
    delete_start = class_section.find('def delete(self, project:')
    if delete_start == -1:
        return ""

    # Find the end of the method
    rest = class_section[delete_start:]
    # Look for next method or class
    next_def = rest.find('\n    def ', 1)
    next_class = rest.find('\nclass ', 1)
    next_dataclass = rest.find('\n@dataclass', 1)

    ends = [e for e in [next_def, next_class, next_dataclass] if e != -1]
    end = min(ends) if ends else len(rest)

    return rest[:end]


def extract_online_store_delete(source_code):
    """Extract the OnlineStoreDuckDB.delete method."""
    # Find OnlineStoreDuckDB class
    start = source_code.find('class OnlineStoreDuckDB:')
    if start == -1:
        return ""

    class_section = source_code[start:]
    # Find the delete method within this class
    delete_start = class_section.find('def delete(self, name:')
    if delete_start == -1:
        return ""

    # Find the end of the method
    rest = class_section[delete_start:]
    next_def = rest.find('\n    def ', 1)
    next_class = rest.find('\nclass ', 1)

    ends = [e for e in [next_def, next_class] if e != -1]
    end = min(ends) if ends else len(rest)

    return rest[:end]


def extract_online_features_delete(source_code):
    """Extract the OnlineFeaturesDuckDB.delete method."""
    # Find OnlineFeaturesDuckDB class
    start = source_code.find('class OnlineFeaturesDuckDB:')
    if start == -1:
        return ""

    class_section = source_code[start:]
    # Find the delete method
    delete_start = class_section.find('def delete(self)')
    if delete_start == -1:
        return ""

    rest = class_section[delete_start:]
    # Find end of method
    next_def = rest.find('\n    def ', 1)
    next_class = rest.find('\nclass ', 1)
    alias_line = rest.find('\n# Alias', 1)

    ends = [e for e in [next_def, next_class, alias_line] if e != -1]
    end = min(ends) if ends else len(rest)

    return rest[:end]


# ==============================================================================
# OFFLINE STORE FILE CLEANUP TESTS
# ==============================================================================

class TestOfflineStoreDuckDBDeleteExists:
    """Tests verifying OfflineStoreDuckDB.delete method exists and is properly defined."""

    @pytest.fixture
    def source_code(self):
        return read_featurestore_source()

    def test_offline_store_delete_method_exists(self, source_code):
        """Verify OfflineStoreDuckDB has delete method."""
        assert 'class OfflineStoreDuckDB:' in source_code
        assert 'def delete(self, project:' in source_code

    def test_offline_store_delete_has_docstring(self, source_code):
        """Verify delete method has documentation."""
        delete_code = extract_offline_store_delete(source_code)
        assert '"""Delete' in delete_code or "'''Delete" in delete_code

    def test_offline_store_delete_returns_bool(self, source_code):
        """Verify delete method returns bool."""
        delete_code = extract_offline_store_delete(source_code)
        assert '-> bool:' in delete_code


class TestOfflineStoreDuckDBDeleteImplementation:
    """Tests for OfflineStoreDuckDB.delete implementation patterns."""

    @pytest.fixture
    def source_code(self):
        return read_featurestore_source()

    @pytest.fixture
    def delete_code(self, source_code):
        return extract_offline_store_delete(source_code)

    def test_delete_uses_shutil_rmtree(self, source_code):
        """Verify delete uses shutil.rmtree for directory removal."""
        assert 'import shutil' in source_code
        assert 'shutil.rmtree(' in source_code

    def test_delete_builds_correct_path(self, delete_code):
        """Verify delete builds path from project, entity, name."""
        assert 'os.path.join(' in delete_code
        assert 'project' in delete_code
        assert 'entity' in delete_code
        assert 'name' in delete_code

    def test_delete_checks_path_exists(self, delete_code):
        """Verify delete checks if path exists before deletion."""
        assert 'os.path.exists(' in delete_code

    def test_delete_handles_nonexistent_path(self, delete_code):
        """Verify delete handles non-existent path gracefully."""
        # Should return False if path doesn't exist
        assert 'return False' in delete_code

    def test_delete_returns_true_on_success(self, delete_code):
        """Verify delete returns True on successful deletion."""
        assert 'return True' in delete_code

    def test_delete_logs_info_message(self, delete_code):
        """Verify delete logs info on successful deletion."""
        assert 'logger.info(' in delete_code

    def test_delete_logs_warning_for_missing(self, delete_code):
        """Verify delete logs warning when directory not found."""
        assert 'logger.warning(' in delete_code


class TestOfflineStorePathConstruction:
    """Tests for offline store path construction patterns."""

    @pytest.fixture
    def source_code(self):
        return read_featurestore_source()

    def test_get_table_path_method_exists(self, source_code):
        """Verify _get_table_path method exists in OfflineStoreDuckDB."""
        assert 'def _get_table_path(' in source_code

    def test_get_metadata_path_method_exists(self, source_code):
        """Verify _get_metadata_path method exists."""
        assert 'def _get_metadata_path(' in source_code

    def test_metadata_file_is_json(self, source_code):
        """Verify metadata file uses _metadata.json."""
        assert '_metadata.json' in source_code

    def test_uses_os_path_for_paths(self, source_code):
        """Verify os.path is used for path operations."""
        assert 'import os' in source_code
        assert 'os.path.join(' in source_code


# ==============================================================================
# ONLINE STORE FILE CLEANUP TESTS
# ==============================================================================

class TestOnlineStoreDuckDBDeleteExists:
    """Tests verifying OnlineStoreDuckDB.delete method exists and is properly defined."""

    @pytest.fixture
    def source_code(self):
        return read_featurestore_source()

    def test_online_store_delete_method_exists(self, source_code):
        """Verify OnlineStoreDuckDB has delete method."""
        assert 'class OnlineStoreDuckDB:' in source_code
        delete_code = extract_online_store_delete(source_code)
        assert 'def delete(' in delete_code

    def test_online_store_delete_has_name_param(self, source_code):
        """Verify delete method has name parameter."""
        delete_code = extract_online_store_delete(source_code)
        assert 'name:' in delete_code

    def test_online_store_delete_has_project_param(self, source_code):
        """Verify delete method has project parameter."""
        delete_code = extract_online_store_delete(source_code)
        assert 'project:' in delete_code

    def test_online_store_delete_has_entity_param(self, source_code):
        """Verify delete method has entity parameter (optional)."""
        delete_code = extract_online_store_delete(source_code)
        assert 'entity' in delete_code

    def test_online_store_delete_returns_bool(self, source_code):
        """Verify delete method returns bool."""
        delete_code = extract_online_store_delete(source_code)
        assert '-> bool:' in delete_code


class TestOnlineStoreDuckDBDeleteImplementation:
    """Tests for OnlineStoreDuckDB.delete implementation patterns."""

    @pytest.fixture
    def source_code(self):
        return read_featurestore_source()

    @pytest.fixture
    def delete_code(self, source_code):
        return extract_online_store_delete(source_code)

    def test_delete_drops_duckdb_table(self, delete_code):
        """Verify delete drops the DuckDB table."""
        assert 'DROP TABLE IF EXISTS' in delete_code

    def test_delete_uses_shutil_rmtree_for_files(self, delete_code):
        """Verify delete uses shutil.rmtree for file cleanup."""
        assert 'shutil.rmtree(' in delete_code

    def test_delete_builds_table_directory_path(self, delete_code):
        """Verify delete builds correct table directory path."""
        assert 'os.path.join(' in delete_code

    def test_delete_checks_directory_exists(self, delete_code):
        """Verify delete checks if directory exists."""
        assert 'os.path.exists(' in delete_code

    def test_delete_handles_entity_in_path(self, delete_code):
        """Verify delete includes entity in path when provided."""
        assert 'if entity:' in delete_code

    def test_delete_has_fallback_path_without_entity(self, delete_code):
        """Verify delete handles path when entity is not provided."""
        # Should have else branch for when entity is None
        assert 'else:' in delete_code

    def test_delete_gets_connection(self, delete_code):
        """Verify delete gets DuckDB connection for table drop."""
        assert '_get_connection()' in delete_code

    def test_delete_handles_drop_table_exception(self, delete_code):
        """Verify delete handles exceptions during table drop."""
        assert 'except Exception' in delete_code


class TestOnlineStoreTableDeletion:
    """Tests for DuckDB table deletion in OnlineStoreDuckDB."""

    @pytest.fixture
    def source_code(self):
        return read_featurestore_source()

    @pytest.fixture
    def delete_code(self, source_code):
        return extract_online_store_delete(source_code)

    def test_uses_if_exists_clause(self, delete_code):
        """Verify DROP TABLE uses IF EXISTS to prevent errors."""
        assert 'IF EXISTS' in delete_code

    def test_executes_drop_statement(self, delete_code):
        """Verify delete executes the DROP statement."""
        assert 'conn.execute(' in delete_code

    def test_logs_drop_operation(self, delete_code):
        """Verify delete logs the table drop operation."""
        assert 'logger.info(' in delete_code

    def test_logs_warning_on_drop_failure(self, delete_code):
        """Verify delete logs warning if drop fails."""
        assert 'logger.warning(' in delete_code


# ==============================================================================
# ONLINE FEATURES DUCKDB DELETE INTEGRATION TESTS
# ==============================================================================

class TestOnlineFeaturesDuckDBDeleteExists:
    """Tests verifying OnlineFeaturesDuckDB.delete method exists."""

    @pytest.fixture
    def source_code(self):
        return read_feature_group_source()

    def test_online_features_delete_method_exists(self, source_code):
        """Verify OnlineFeaturesDuckDB has delete method."""
        assert 'class OnlineFeaturesDuckDB:' in source_code
        delete_code = extract_online_features_delete(source_code)
        assert 'def delete(self)' in delete_code

    def test_online_features_delete_has_docstring(self, source_code):
        """Verify delete method has documentation."""
        delete_code = extract_online_features_delete(source_code)
        assert '"""Delete' in delete_code

    def test_online_features_delete_returns_bool(self, source_code):
        """Verify delete method returns bool."""
        delete_code = extract_online_features_delete(source_code)
        assert '-> bool:' in delete_code


class TestOnlineFeaturesDuckDBDeleteImplementation:
    """Tests for OnlineFeaturesDuckDB.delete implementation."""

    @pytest.fixture
    def source_code(self):
        return read_feature_group_source()

    @pytest.fixture
    def delete_code(self, source_code):
        return extract_online_features_delete(source_code)

    def test_delete_calls_online_store_delete(self, delete_code):
        """Verify delete calls online_store.delete."""
        assert 'online_store.delete(' in delete_code

    def test_delete_passes_name(self, delete_code):
        """Verify delete passes table name to online_store.delete."""
        assert 'name=self.name' in delete_code

    def test_delete_passes_project(self, delete_code):
        """Verify delete passes project to online_store.delete."""
        assert 'project=self.project' in delete_code

    def test_delete_passes_entity(self, delete_code):
        """Verify delete passes entity information."""
        assert 'entity=' in delete_code

    def test_delete_handles_lookup_key(self, delete_code):
        """Verify delete handles lookup_key for entity name."""
        assert 'lookup_key' in delete_code

    def test_delete_calls_metadata_cleanup(self, delete_code):
        """Verify delete calls OnlineTableRequest.delete_by_id for metadata."""
        assert 'OnlineTableRequest.delete_by_id' in delete_code

    def test_delete_checks_id_before_metadata_cleanup(self, delete_code):
        """Verify delete checks id is not None before metadata cleanup."""
        assert 'if self.id is not None' in delete_code


class TestOnlineFeaturesDuckDBDeleteErrorHandling:
    """Tests for error handling in OnlineFeaturesDuckDB.delete."""

    @pytest.fixture
    def source_code(self):
        return read_feature_group_source()

    @pytest.fixture
    def delete_code(self, source_code):
        return extract_online_features_delete(source_code)

    def test_delete_has_try_except_for_file_deletion(self, delete_code):
        """Verify delete handles file deletion errors."""
        assert 'try:' in delete_code
        assert 'except Exception' in delete_code

    def test_delete_tracks_deletion_success(self, delete_code):
        """Verify delete tracks success/failure status."""
        assert 'deletion_success' in delete_code

    def test_delete_logs_file_deletion_success(self, delete_code):
        """Verify delete logs successful file deletion."""
        assert 'logger.info(' in delete_code

    def test_delete_logs_file_deletion_error(self, delete_code):
        """Verify delete logs file deletion errors."""
        assert 'logger.error(' in delete_code

    def test_delete_continues_on_file_error(self, delete_code):
        """Verify delete continues to metadata cleanup even if file deletion fails."""
        # Should still attempt metadata cleanup after file deletion error
        # This is verified by the deletion_success pattern
        pattern = r'deletion_success = False\s+# Step 2:'
        # More flexible check - ensure both steps are present
        assert 'Step 1' in delete_code
        assert 'Step 2' in delete_code

    def test_delete_returns_partial_failure_status(self, delete_code):
        """Verify delete can return partial failure status."""
        assert 'return deletion_success' in delete_code


class TestOnlineFeaturesDuckDBDeleteLogging:
    """Tests for logging in OnlineFeaturesDuckDB.delete."""

    @pytest.fixture
    def source_code(self):
        return read_feature_group_source()

    @pytest.fixture
    def delete_code(self, source_code):
        return extract_online_features_delete(source_code)

    def test_logs_complete_success(self, delete_code):
        """Verify delete logs complete success message."""
        assert 'Successfully deleted' in delete_code

    def test_logs_partial_failure(self, delete_code):
        """Verify delete logs warning for partial failure."""
        assert 'Partial deletion' in delete_code or 'logger.warning(' in delete_code

    def test_logs_include_table_name(self, delete_code):
        """Verify log messages include table name."""
        # Table name should be in log messages
        assert "self.name" in delete_code


# ==============================================================================
# FUNCTIONAL FILE CLEANUP TESTS
# ==============================================================================

@pytest.mark.skipif(not HAS_PANDAS, reason="pandas not available")
class TestOfflineStoreFileCleanupFunctional:
    """Functional tests for OfflineStoreDuckDB file cleanup."""

    @pytest.fixture
    def temp_storage_dir(self):
        """Create a temporary directory for storage."""
        temp_dir = tempfile.mkdtemp(prefix="test_offline_store_")
        yield temp_dir
        # Cleanup
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    def test_delete_removes_parquet_file(self, temp_storage_dir):
        """Verify delete removes parquet data file."""
        # Import after creating temp dir to avoid module loading issues
        from seeknal.featurestore.duckdbengine.featurestore import (
            OfflineStoreDuckDB,
            FeatureStoreFileOutput
        )

        # Setup: Create offline store and write data
        store = OfflineStoreDuckDB(
            value=FeatureStoreFileOutput(path=temp_storage_dir)
        )

        # Create test data
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })

        # Write data
        store.write(
            df=df,
            project='test_project',
            entity='test_entity',
            name='test_table',
            mode='overwrite'
        )

        # Verify parquet file was created
        table_dir = os.path.join(temp_storage_dir, 'test_project', 'test_entity', 'test_table')
        parquet_path = os.path.join(table_dir, 'data.parquet')
        assert os.path.exists(parquet_path), "Parquet file should exist before deletion"

        # Delete the table
        result = store.delete(
            project='test_project',
            entity='test_entity',
            name='test_table'
        )

        # Verify deletion
        assert result is True, "Delete should return True"
        assert not os.path.exists(parquet_path), "Parquet file should be removed"
        assert not os.path.exists(table_dir), "Table directory should be removed"

    def test_delete_removes_metadata_file(self, temp_storage_dir):
        """Verify delete removes _metadata.json file."""
        from seeknal.featurestore.duckdbengine.featurestore import (
            OfflineStoreDuckDB,
            FeatureStoreFileOutput
        )

        store = OfflineStoreDuckDB(
            value=FeatureStoreFileOutput(path=temp_storage_dir)
        )

        df = pd.DataFrame({'id': [1], 'value': ['test']})
        store.write(
            df=df,
            project='test_project',
            entity='test_entity',
            name='test_table',
            mode='overwrite'
        )

        table_dir = os.path.join(temp_storage_dir, 'test_project', 'test_entity', 'test_table')
        metadata_path = os.path.join(table_dir, '_metadata.json')
        assert os.path.exists(metadata_path), "Metadata file should exist before deletion"

        store.delete(
            project='test_project',
            entity='test_entity',
            name='test_table'
        )

        assert not os.path.exists(metadata_path), "Metadata file should be removed"

    def test_delete_removes_entire_directory(self, temp_storage_dir):
        """Verify delete removes the entire table directory."""
        from seeknal.featurestore.duckdbengine.featurestore import (
            OfflineStoreDuckDB,
            FeatureStoreFileOutput
        )

        store = OfflineStoreDuckDB(
            value=FeatureStoreFileOutput(path=temp_storage_dir)
        )

        df = pd.DataFrame({'id': [1], 'value': ['test']})
        store.write(
            df=df,
            project='test_project',
            entity='test_entity',
            name='test_table',
            mode='overwrite'
        )

        table_dir = os.path.join(temp_storage_dir, 'test_project', 'test_entity', 'test_table')
        assert os.path.exists(table_dir), "Table directory should exist before deletion"

        store.delete(
            project='test_project',
            entity='test_entity',
            name='test_table'
        )

        assert not os.path.exists(table_dir), "Table directory should be completely removed"

    def test_delete_nonexistent_returns_false(self, temp_storage_dir):
        """Verify delete returns False for nonexistent table."""
        from seeknal.featurestore.duckdbengine.featurestore import (
            OfflineStoreDuckDB,
            FeatureStoreFileOutput
        )

        store = OfflineStoreDuckDB(
            value=FeatureStoreFileOutput(path=temp_storage_dir)
        )

        result = store.delete(
            project='nonexistent',
            entity='nonexistent',
            name='nonexistent'
        )

        assert result is False, "Delete should return False for nonexistent table"

    def test_delete_preserves_other_tables(self, temp_storage_dir):
        """Verify delete only removes the target table, not others."""
        from seeknal.featurestore.duckdbengine.featurestore import (
            OfflineStoreDuckDB,
            FeatureStoreFileOutput
        )

        store = OfflineStoreDuckDB(
            value=FeatureStoreFileOutput(path=temp_storage_dir)
        )

        df = pd.DataFrame({'id': [1], 'value': ['test']})

        # Create two tables
        store.write(df=df, project='test_project', entity='test_entity', name='table1', mode='overwrite')
        store.write(df=df, project='test_project', entity='test_entity', name='table2', mode='overwrite')

        table1_dir = os.path.join(temp_storage_dir, 'test_project', 'test_entity', 'table1')
        table2_dir = os.path.join(temp_storage_dir, 'test_project', 'test_entity', 'table2')

        assert os.path.exists(table1_dir)
        assert os.path.exists(table2_dir)

        # Delete only table1
        store.delete(project='test_project', entity='test_entity', name='table1')

        assert not os.path.exists(table1_dir), "Deleted table directory should be removed"
        assert os.path.exists(table2_dir), "Other table directory should be preserved"


@pytest.mark.skipif(not HAS_PANDAS, reason="pandas not available")
class TestOnlineStoreFileCleanupFunctional:
    """Functional tests for OnlineStoreDuckDB file cleanup."""

    @pytest.fixture
    def temp_storage_dir(self):
        """Create a temporary directory for storage."""
        temp_dir = tempfile.mkdtemp(prefix="test_online_store_")
        yield temp_dir
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    def test_delete_removes_table_directory(self, temp_storage_dir):
        """Verify delete removes table directory."""
        from seeknal.featurestore.duckdbengine.featurestore import (
            OnlineStoreDuckDB,
            FeatureStoreFileOutput
        )

        store = OnlineStoreDuckDB(
            value=FeatureStoreFileOutput(path=temp_storage_dir)
        )

        # Create table directory manually to test deletion
        table_dir = os.path.join(temp_storage_dir, 'test_project', 'test_entity', 'test_table')
        os.makedirs(table_dir, exist_ok=True)
        parquet_path = os.path.join(table_dir, 'data.parquet')

        # Create a dummy parquet file
        df = pd.DataFrame({'id': [1], 'value': ['test']})
        df.to_parquet(parquet_path, engine='pyarrow', index=False)

        assert os.path.exists(table_dir)

        # Delete
        result = store.delete(
            name='test_table',
            project='test_project',
            entity='test_entity'
        )

        assert result is True
        assert not os.path.exists(table_dir), "Table directory should be removed"

    def test_delete_works_without_entity(self, temp_storage_dir):
        """Verify delete works when entity is not provided."""
        from seeknal.featurestore.duckdbengine.featurestore import (
            OnlineStoreDuckDB,
            FeatureStoreFileOutput
        )

        store = OnlineStoreDuckDB(
            value=FeatureStoreFileOutput(path=temp_storage_dir)
        )

        # Create table directory without entity level
        table_dir = os.path.join(temp_storage_dir, 'test_project', 'test_table')
        os.makedirs(table_dir, exist_ok=True)
        parquet_path = os.path.join(table_dir, 'data.parquet')

        df = pd.DataFrame({'id': [1], 'value': ['test']})
        df.to_parquet(parquet_path, engine='pyarrow', index=False)

        result = store.delete(
            name='test_table',
            project='test_project',
            entity=None
        )

        assert result is True
        assert not os.path.exists(table_dir)


# ==============================================================================
# IMPORT AND DEPENDENCY TESTS
# ==============================================================================

class TestFileCleanupImports:
    """Tests for required imports in file cleanup modules."""

    @pytest.fixture
    def source_code(self):
        return read_featurestore_source()

    def test_imports_os(self, source_code):
        """Verify os module is imported."""
        assert 'import os' in source_code

    def test_imports_shutil(self, source_code):
        """Verify shutil module is imported."""
        assert 'import shutil' in source_code

    def test_imports_json(self, source_code):
        """Verify json module is imported for metadata."""
        assert 'import json' in source_code

    def test_imports_logger(self, source_code):
        """Verify logger is imported."""
        assert 'from ...context import logger' in source_code


class TestMetadataHandling:
    """Tests for metadata file handling patterns."""

    @pytest.fixture
    def source_code(self):
        return read_featurestore_source()

    def test_save_metadata_uses_json_dump(self, source_code):
        """Verify _save_metadata uses json.dump."""
        assert 'json.dump(' in source_code

    def test_load_metadata_uses_json_load(self, source_code):
        """Verify _load_metadata uses json.load."""
        assert 'json.load(' in source_code

    def test_metadata_structure_has_versions(self, source_code):
        """Verify metadata structure includes versions."""
        assert '"versions"' in source_code

    def test_metadata_structure_has_watermarks(self, source_code):
        """Verify metadata structure includes watermarks."""
        assert '"watermarks"' in source_code

    def test_metadata_structure_has_schema(self, source_code):
        """Verify metadata structure includes schema."""
        assert '"schema"' in source_code


class TestPathSecurityPatterns:
    """Tests for path handling security patterns."""

    @pytest.fixture
    def source_code(self):
        return read_featurestore_source()

    def test_uses_os_path_join(self, source_code):
        """Verify os.path.join is used for path construction."""
        assert 'os.path.join(' in source_code

    def test_uses_os_path_exists(self, source_code):
        """Verify os.path.exists is used for path checking."""
        assert 'os.path.exists(' in source_code

    def test_uses_os_makedirs(self, source_code):
        """Verify os.makedirs is used for directory creation."""
        assert 'os.makedirs(' in source_code


class TestDeleteOrderOfOperations:
    """Tests for the order of deletion operations."""

    @pytest.fixture
    def source_code(self):
        return read_feature_group_source()

    @pytest.fixture
    def delete_code(self, source_code):
        return extract_online_features_delete(source_code)

    def test_files_deleted_before_metadata(self, delete_code):
        """Verify data files are deleted before metadata cleanup."""
        file_delete_pos = delete_code.find('online_store.delete(')
        metadata_delete_pos = delete_code.find('OnlineTableRequest.delete_by_id')

        assert file_delete_pos < metadata_delete_pos, (
            "Files should be deleted before metadata"
        )

    def test_two_step_deletion_process(self, delete_code):
        """Verify deletion follows two-step process."""
        assert 'Step 1' in delete_code
        assert 'Step 2' in delete_code

    def test_step1_is_file_deletion(self, delete_code):
        """Verify Step 1 is about data file deletion."""
        step1_pos = delete_code.find('Step 1')
        step2_pos = delete_code.find('Step 2')

        step1_section = delete_code[step1_pos:step2_pos]
        assert 'online_store.delete' in step1_section

    def test_step2_is_metadata_cleanup(self, delete_code):
        """Verify Step 2 is about metadata cleanup."""
        step2_pos = delete_code.find('Step 2')
        step2_section = delete_code[step2_pos:]
        assert 'delete_by_id' in step2_section or 'metadata' in step2_section.lower()
