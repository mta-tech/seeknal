"""Unit tests for OfflineStore.delete() method."""

import os
import shutil
import tempfile
import stat
from unittest.mock import MagicMock, patch

import pytest

from seeknal.featurestore.featurestore import (
    OfflineStore,
    OfflineStoreEnum,
    FeatureStoreFileOutput,
    FeatureStoreHiveTableOutput,
)
from seeknal.exceptions import InvalidIdentifierError


class TestOfflineStoreDeleteFileType:
    """Tests for OfflineStore.delete() with FILE storage type."""

    def test_delete_file_type_when_path_exists(self, secure_temp_dir):
        """Should delete directory when path exists."""
        # Create a test directory with some files
        base_path = secure_temp_dir.mkdir("offline_store")
        table_path = os.path.join(base_path, "fg_test_project__test_entity")
        os.makedirs(table_path)
        # Create a dummy file
        with open(os.path.join(table_path, "data.parquet"), "w") as f:
            f.write("test data")

        # Create OfflineStore with FILE kind
        store = OfflineStore(
            kind=OfflineStoreEnum.FILE,
            value=FeatureStoreFileOutput(path=base_path),
        )

        # Verify path exists before deletion
        assert os.path.exists(table_path)

        # Delete
        result = store.delete(project="test_project", entity="test_entity")

        # Verify deletion
        assert result is True
        assert not os.path.exists(table_path)

    def test_delete_file_type_when_path_does_not_exist(self, secure_temp_dir):
        """Should return True when path doesn't exist (idempotent)."""
        base_path = secure_temp_dir.mkdir("offline_store")

        store = OfflineStore(
            kind=OfflineStoreEnum.FILE,
            value=FeatureStoreFileOutput(path=base_path),
        )

        # Path does not exist
        table_path = os.path.join(base_path, "fg_nonexistent__entity")
        assert not os.path.exists(table_path)

        # Delete should still return True
        result = store.delete(project="nonexistent", entity="entity")

        assert result is True

    def test_delete_file_type_with_dict_value(self, secure_temp_dir):
        """Should handle dict value configuration."""
        base_path = secure_temp_dir.mkdir("offline_store")
        table_path = os.path.join(base_path, "fg_myproject__myentity")
        os.makedirs(table_path)

        store = OfflineStore(
            kind=OfflineStoreEnum.FILE,
            value={"path": base_path, "kind": "delta"},
        )

        result = store.delete(project="myproject", entity="myentity")

        assert result is True
        assert not os.path.exists(table_path)

    def test_delete_file_type_with_none_value_uses_config_base_url(self):
        """Should use CONFIG_BASE_URL when value is None."""
        with patch("seeknal.featurestore.featurestore.CONFIG_BASE_URL", "/mock/base/path"):
            with patch("seeknal.featurestore.featurestore.os.path.exists") as mock_exists:
                with patch("seeknal.featurestore.featurestore.shutil.rmtree") as mock_rmtree:
                    mock_exists.return_value = True

                    store = OfflineStore(kind=OfflineStoreEnum.FILE, value=None)
                    result = store.delete(project="proj", entity="ent")

                    assert result is True
                    expected_path = "/mock/base/path/data/fg_proj__ent"
                    mock_exists.assert_called_once_with(expected_path)
                    mock_rmtree.assert_called_once_with(expected_path)

    def test_delete_file_type_with_string_value(self, secure_temp_dir):
        """Should handle string value as base path."""
        base_path = secure_temp_dir.mkdir("offline_store")
        table_path = os.path.join(base_path, "fg_strproject__strentity")
        os.makedirs(table_path)

        # When value is a string, it's used as the base path directly
        store = OfflineStore(
            kind=OfflineStoreEnum.FILE,
            value=base_path,
        )

        result = store.delete(project="strproject", entity="strentity")

        assert result is True
        assert not os.path.exists(table_path)


class TestOfflineStoreDeleteHiveTableType:
    """Tests for OfflineStore.delete() with HIVE_TABLE storage type."""

    def test_delete_hive_table_when_table_exists(self):
        """Should drop table when it exists."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True

        store = OfflineStore(
            kind=OfflineStoreEnum.HIVE_TABLE,
            value=FeatureStoreHiveTableOutput(database="testdb"),
        )

        result = store.delete(spark=mock_spark, project="hiveproj", entity="hiveent")

        assert result is True
        mock_spark.catalog.tableExists.assert_called_once_with("testdb.fg_hiveproj__hiveent")
        mock_spark.sql.assert_called_once_with("DROP TABLE IF EXISTS testdb.fg_hiveproj__hiveent")

    def test_delete_hive_table_when_table_does_not_exist(self):
        """Should return True without dropping when table doesn't exist (idempotent)."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False

        store = OfflineStore(
            kind=OfflineStoreEnum.HIVE_TABLE,
            value=FeatureStoreHiveTableOutput(database="testdb"),
        )

        result = store.delete(spark=mock_spark, project="missing", entity="table")

        assert result is True
        mock_spark.catalog.tableExists.assert_called_once_with("testdb.fg_missing__table")
        # SQL should not be called when table doesn't exist
        mock_spark.sql.assert_not_called()

    def test_delete_hive_table_with_none_value_uses_default_database(self):
        """Should use 'seeknal' database when value is None."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True

        store = OfflineStore(kind=OfflineStoreEnum.HIVE_TABLE, value=None)

        result = store.delete(spark=mock_spark, project="proj", entity="ent")

        assert result is True
        mock_spark.catalog.tableExists.assert_called_once_with("seeknal.fg_proj__ent")
        mock_spark.sql.assert_called_once_with("DROP TABLE IF EXISTS seeknal.fg_proj__ent")

    def test_delete_hive_table_with_dict_value(self):
        """Should handle dict value configuration."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True

        store = OfflineStore(
            kind=OfflineStoreEnum.HIVE_TABLE,
            value={"database": "custom_db"},
        )

        result = store.delete(spark=mock_spark, project="proj", entity="ent")

        assert result is True
        mock_spark.catalog.tableExists.assert_called_once_with("custom_db.fg_proj__ent")

    def test_delete_hive_table_creates_spark_session_if_not_provided(self):
        """Should create SparkSession if not provided."""
        with patch("seeknal.featurestore.featurestore.SparkSession") as mock_spark_class:
            mock_spark = MagicMock()
            mock_spark.catalog.tableExists.return_value = False
            mock_spark_class.builder.getOrCreate.return_value = mock_spark

            store = OfflineStore(kind=OfflineStoreEnum.HIVE_TABLE, value=None)
            result = store.delete(project="proj", entity="ent")

            assert result is True
            mock_spark_class.builder.getOrCreate.assert_called_once()


class TestOfflineStoreDeleteValidation:
    """Tests for OfflineStore.delete() parameter validation."""

    def test_delete_raises_error_when_project_is_missing(self):
        """Should raise ValueError when project is not provided."""
        store = OfflineStore(kind=OfflineStoreEnum.FILE, value=None)

        with pytest.raises(ValueError) as exc_info:
            store.delete(entity="test_entity")

        assert "project" in str(exc_info.value)
        assert "entity" in str(exc_info.value)

    def test_delete_raises_error_when_entity_is_missing(self):
        """Should raise ValueError when entity is not provided."""
        store = OfflineStore(kind=OfflineStoreEnum.FILE, value=None)

        with pytest.raises(ValueError) as exc_info:
            store.delete(project="test_project")

        assert "project" in str(exc_info.value)
        assert "entity" in str(exc_info.value)

    def test_delete_raises_error_when_both_project_and_entity_missing(self):
        """Should raise ValueError when both project and entity are missing."""
        store = OfflineStore(kind=OfflineStoreEnum.FILE, value=None)

        with pytest.raises(ValueError) as exc_info:
            store.delete()

        assert "project" in str(exc_info.value)
        assert "entity" in str(exc_info.value)

    def test_delete_validates_project_name(self):
        """Should validate project name for invalid characters."""
        store = OfflineStore(kind=OfflineStoreEnum.FILE, value=None)

        # Project name with SQL injection attempt should be rejected
        with pytest.raises(InvalidIdentifierError):
            store.delete(project="test'; DROP TABLE --", entity="valid_entity")

    def test_delete_validates_entity_name(self):
        """Should validate entity name for invalid characters."""
        store = OfflineStore(kind=OfflineStoreEnum.FILE, value=None)

        # Entity name with invalid characters should be rejected
        with pytest.raises(InvalidIdentifierError):
            store.delete(project="valid_project", entity="bad-entity-name")


class TestOfflineStoreDeleteUnknownKind:
    """Tests for OfflineStore.delete() with unknown storage kind."""

    def test_delete_returns_false_for_unknown_kind(self):
        """Should return False and log warning for unknown store kind."""
        store = OfflineStore(kind=OfflineStoreEnum.FILE, value=None)
        # Manually set kind to an unknown value to test the fallback case
        store.kind = "unknown_kind"

        result = store.delete(project="proj", entity="ent")

        assert result is False


class TestOfflineStoreDeleteTableNameFormat:
    """Tests to verify correct table name format: fg_{project}__{entity}."""

    def test_table_name_format_for_file_type(self, secure_temp_dir):
        """Should construct correct table name pattern for FILE type."""
        base_path = secure_temp_dir.mkdir("offline_store")

        # Create directory with expected naming pattern
        expected_table_name = "fg_myproj__myent"
        table_path = os.path.join(base_path, expected_table_name)
        os.makedirs(table_path)

        store = OfflineStore(
            kind=OfflineStoreEnum.FILE,
            value={"path": base_path},
        )

        result = store.delete(project="myproj", entity="myent")

        assert result is True
        # Verify the correctly named directory was deleted
        assert not os.path.exists(table_path)

    def test_table_name_format_for_hive_table_type(self):
        """Should construct correct table name pattern for HIVE_TABLE type."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True

        store = OfflineStore(
            kind=OfflineStoreEnum.HIVE_TABLE,
            value={"database": "mydb"},
        )

        store.delete(spark=mock_spark, project="proj1", entity="ent1")

        # Verify full table name format: database.fg_{project}__{entity}
        expected_full_name = "mydb.fg_proj1__ent1"
        mock_spark.catalog.tableExists.assert_called_once_with(expected_full_name)


class TestOfflineStoreDeleteLogging:
    """Tests to verify logging behavior during deletion."""

    def test_logs_info_when_file_deleted(self, secure_temp_dir, caplog):
        """Should log info message when files are deleted."""
        import logging

        base_path = secure_temp_dir.mkdir("offline_store")
        table_path = os.path.join(base_path, "fg_logproj__logent")
        os.makedirs(table_path)

        store = OfflineStore(
            kind=OfflineStoreEnum.FILE,
            value={"path": base_path},
        )

        with caplog.at_level(logging.INFO):
            store.delete(project="logproj", entity="logent")

        assert "Deleted offline store files at:" in caplog.text

    def test_logs_info_when_file_path_does_not_exist(self, secure_temp_dir, caplog):
        """Should log info message when path doesn't exist."""
        import logging

        base_path = secure_temp_dir.mkdir("offline_store")

        store = OfflineStore(
            kind=OfflineStoreEnum.FILE,
            value={"path": base_path},
        )

        with caplog.at_level(logging.INFO):
            store.delete(project="nonexistent", entity="entity")

        assert "Offline store path does not exist:" in caplog.text

    def test_logs_info_when_hive_table_dropped(self, caplog):
        """Should log info message when Hive table is dropped."""
        import logging

        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True

        store = OfflineStore(
            kind=OfflineStoreEnum.HIVE_TABLE,
            value={"database": "testdb"},
        )

        with caplog.at_level(logging.INFO):
            store.delete(spark=mock_spark, project="proj", entity="ent")

        assert "Dropped Hive table:" in caplog.text

    def test_logs_info_when_hive_table_does_not_exist(self, caplog):
        """Should log info message when Hive table doesn't exist."""
        import logging

        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False

        store = OfflineStore(
            kind=OfflineStoreEnum.HIVE_TABLE,
            value={"database": "testdb"},
        )

        with caplog.at_level(logging.INFO):
            store.delete(spark=mock_spark, project="missing", entity="table")

        assert "Hive table does not exist:" in caplog.text

    def test_logs_warning_for_unknown_kind(self, caplog):
        """Should log warning for unknown storage kind."""
        import logging

        store = OfflineStore(kind=OfflineStoreEnum.FILE, value=None)
        store.kind = "unknown_kind"

        with caplog.at_level(logging.WARNING):
            store.delete(project="proj", entity="ent")

        assert "Unknown offline store kind:" in caplog.text
