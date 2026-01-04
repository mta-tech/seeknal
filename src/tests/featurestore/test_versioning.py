"""Unit tests for feature group versioning methods.

Tests cover:
- list_versions(): Lists all versions for a feature group
- get_version(): Retrieves specific version metadata
- compare_versions(): Detects added/removed/modified features
- compare_schemas(): Correctly parses and compares Avro schemas
"""
import json
from datetime import datetime

import pytest
from seeknal.entity import Entity
from seeknal.featurestore.feature_group import (
    FeatureGroup,
    Materialization,
)
from seeknal.flow import Flow, FlowInput, FlowInputEnum, FlowOutput
from seeknal.project import Project
from seeknal.request import FeatureGroupRequest


@pytest.fixture(scope="module")
def input_data_spark(spark):
    """Load test data into Spark and create a Hive table."""
    comm_day = spark.read.format("parquet").load("tests/data/feateng_comm_day")
    comm_day.write.mode("overwrite").saveAsTable("comm_day_versioning")


@pytest.fixture(scope="module")
def versioned_feature_group(spark, input_data_spark):
    """Create a feature group for versioning tests."""
    Project(name="versioning_test_project").get_or_create()

    flow_input = FlowInput(kind=FlowInputEnum.HIVE_TABLE, value="comm_day_versioning")
    my_flow = Flow(
        input=flow_input,
        tasks=None,
        output=FlowOutput(),
        name="versioning_test_flow"
    )

    entity = Entity(name="msisdn_versioning", join_keys=["msisdn"]).get_or_create()

    my_fg = FeatureGroup(
        name="versioned_feature_group",
        entity=entity,
        materialization=Materialization(event_time_col="day"),
    ).set_flow(my_flow)
    my_fg.set_features()
    my_fg.get_or_create()

    return my_fg


class TestListVersions:
    """Tests for FeatureGroup.list_versions() method."""

    def test_list_versions_returns_list(self, versioned_feature_group):
        """Test that list_versions returns a list."""
        versions = versioned_feature_group.list_versions()
        assert isinstance(versions, list)

    def test_list_versions_contains_version_metadata(self, versioned_feature_group):
        """Test that each version contains expected metadata fields."""
        versions = versioned_feature_group.list_versions()
        assert len(versions) > 0

        for version in versions:
            assert "version" in version
            assert "avro_schema" in version
            assert "created_at" in version
            assert "updated_at" in version
            assert "feature_count" in version

    def test_list_versions_version_number_is_int(self, versioned_feature_group):
        """Test that version numbers are integers."""
        versions = versioned_feature_group.list_versions()
        for version in versions:
            assert isinstance(version["version"], int)

    def test_list_versions_ordered_descending(self, versioned_feature_group):
        """Test that versions are ordered by version number descending."""
        versions = versioned_feature_group.list_versions()
        if len(versions) > 1:
            for i in range(len(versions) - 1):
                assert versions[i]["version"] > versions[i + 1]["version"]

    def test_list_versions_unsaved_feature_group_returns_empty_list(self, spark, input_data_spark):
        """Test that list_versions returns empty list for unsaved feature group."""
        Project(name="versioning_test_project").get_or_create()

        # Create a feature group but don't save it
        unsaved_fg = FeatureGroup(
            name="unsaved_feature_group_test",
            entity=Entity(name="msisdn_versioning", join_keys=["msisdn"]).get_or_create(),
            materialization=Materialization(event_time_col="day"),
        )

        # Feature group is not saved, should return empty list
        versions = unsaved_fg.list_versions()
        assert versions == []


class TestGetVersion:
    """Tests for FeatureGroup.get_version() method."""

    def test_get_version_returns_dict(self, versioned_feature_group):
        """Test that get_version returns a dictionary for existing version."""
        version = versioned_feature_group.get_version(1)
        assert isinstance(version, dict)

    def test_get_version_contains_metadata(self, versioned_feature_group):
        """Test that get_version returns expected metadata fields."""
        version = versioned_feature_group.get_version(1)
        assert version is not None
        assert "version" in version
        assert "avro_schema" in version
        assert "created_at" in version
        assert "updated_at" in version
        assert "feature_count" in version

    def test_get_version_correct_version_number(self, versioned_feature_group):
        """Test that get_version returns correct version number."""
        version = versioned_feature_group.get_version(1)
        assert version is not None
        assert version["version"] == 1

    def test_get_version_nonexistent_returns_none(self, versioned_feature_group):
        """Test that get_version returns None for non-existent version."""
        version = versioned_feature_group.get_version(999)
        assert version is None

    def test_get_version_unsaved_feature_group_returns_none(self, spark, input_data_spark):
        """Test that get_version returns None for unsaved feature group."""
        Project(name="versioning_test_project").get_or_create()

        unsaved_fg = FeatureGroup(
            name="unsaved_fg_for_get_version",
            entity=Entity(name="msisdn_versioning", join_keys=["msisdn"]).get_or_create(),
            materialization=Materialization(event_time_col="day"),
        )

        version = unsaved_fg.get_version(1)
        assert version is None

    def test_get_version_avro_schema_is_dict(self, versioned_feature_group):
        """Test that avro_schema is parsed as a dict."""
        version = versioned_feature_group.get_version(1)
        assert version is not None
        # avro_schema should be a dict (parsed from JSON)
        if version["avro_schema"] is not None:
            assert isinstance(version["avro_schema"], dict)


class TestCompareVersions:
    """Tests for FeatureGroup.compare_versions() method."""

    def test_compare_versions_same_version_raises_error(self, versioned_feature_group):
        """Test that comparing a version with itself raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            versioned_feature_group.compare_versions(1, 1)
        assert "from_version and to_version must be different" in str(exc_info.value)

    def test_compare_versions_nonexistent_from_version_raises_error(self, versioned_feature_group):
        """Test that comparing with non-existent from_version raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            versioned_feature_group.compare_versions(999, 1)
        assert "Version 999 not found" in str(exc_info.value)

    def test_compare_versions_nonexistent_to_version_raises_error(self, versioned_feature_group):
        """Test that comparing with non-existent to_version raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            versioned_feature_group.compare_versions(1, 999)
        assert "Version 999 not found" in str(exc_info.value)

    def test_compare_versions_unsaved_feature_group_returns_none(self, spark, input_data_spark):
        """Test that compare_versions returns None for unsaved feature group."""
        Project(name="versioning_test_project").get_or_create()

        unsaved_fg = FeatureGroup(
            name="unsaved_fg_for_compare",
            entity=Entity(name="msisdn_versioning", join_keys=["msisdn"]).get_or_create(),
            materialization=Materialization(event_time_col="day"),
        )

        result = unsaved_fg.compare_versions(1, 2)
        assert result is None


class TestCompareSchemas:
    """Tests for FeatureGroupRequest.compare_schemas() static method."""

    def test_compare_schemas_identical_schemas(self):
        """Test that comparing identical schemas returns empty diff."""
        schema = json.dumps({
            "type": "record",
            "fields": [
                {"name": "field_a", "type": "int"},
                {"name": "field_b", "type": "string"},
            ]
        })

        diff = FeatureGroupRequest.compare_schemas(schema, schema)

        assert diff["added"] == []
        assert diff["removed"] == []
        assert diff["modified"] == []

    def test_compare_schemas_added_field(self):
        """Test detection of added field."""
        schema1 = json.dumps({
            "type": "record",
            "fields": [
                {"name": "field_a", "type": "int"},
            ]
        })
        schema2 = json.dumps({
            "type": "record",
            "fields": [
                {"name": "field_a", "type": "int"},
                {"name": "field_b", "type": "string"},
            ]
        })

        diff = FeatureGroupRequest.compare_schemas(schema1, schema2)

        assert "field_b" in diff["added"]
        assert diff["removed"] == []
        assert diff["modified"] == []

    def test_compare_schemas_removed_field(self):
        """Test detection of removed field."""
        schema1 = json.dumps({
            "type": "record",
            "fields": [
                {"name": "field_a", "type": "int"},
                {"name": "field_b", "type": "string"},
            ]
        })
        schema2 = json.dumps({
            "type": "record",
            "fields": [
                {"name": "field_a", "type": "int"},
            ]
        })

        diff = FeatureGroupRequest.compare_schemas(schema1, schema2)

        assert diff["added"] == []
        assert "field_b" in diff["removed"]
        assert diff["modified"] == []

    def test_compare_schemas_modified_field_type(self):
        """Test detection of modified field type."""
        schema1 = json.dumps({
            "type": "record",
            "fields": [
                {"name": "field_a", "type": "int"},
            ]
        })
        schema2 = json.dumps({
            "type": "record",
            "fields": [
                {"name": "field_a", "type": "string"},
            ]
        })

        diff = FeatureGroupRequest.compare_schemas(schema1, schema2)

        assert diff["added"] == []
        assert diff["removed"] == []
        assert len(diff["modified"]) == 1
        assert diff["modified"][0]["name"] == "field_a"
        assert diff["modified"][0]["old_type"] == "int"
        assert diff["modified"][0]["new_type"] == "string"

    def test_compare_schemas_multiple_changes(self):
        """Test detection of multiple changes (added, removed, modified)."""
        schema1 = json.dumps({
            "type": "record",
            "fields": [
                {"name": "field_a", "type": "int"},
                {"name": "field_b", "type": "string"},
                {"name": "field_c", "type": "double"},
            ]
        })
        schema2 = json.dumps({
            "type": "record",
            "fields": [
                {"name": "field_a", "type": "long"},
                {"name": "field_c", "type": "double"},
                {"name": "field_d", "type": "boolean"},
            ]
        })

        diff = FeatureGroupRequest.compare_schemas(schema1, schema2)

        assert "field_d" in diff["added"]
        assert "field_b" in diff["removed"]
        assert len(diff["modified"]) == 1
        assert diff["modified"][0]["name"] == "field_a"

    def test_compare_schemas_empty_schema(self):
        """Test comparing with empty schemas."""
        schema1 = json.dumps({"type": "record", "fields": []})
        schema2 = json.dumps({
            "type": "record",
            "fields": [{"name": "field_a", "type": "int"}]
        })

        diff = FeatureGroupRequest.compare_schemas(schema1, schema2)

        assert "field_a" in diff["added"]
        assert diff["removed"] == []
        assert diff["modified"] == []

    def test_compare_schemas_malformed_json(self):
        """Test handling of malformed JSON schemas."""
        schema1 = "not valid json"
        schema2 = json.dumps({"type": "record", "fields": []})

        diff = FeatureGroupRequest.compare_schemas(schema1, schema2)

        # Should return empty diff, not raise exception
        assert diff["added"] == []
        assert diff["removed"] == []
        assert diff["modified"] == []

    def test_compare_schemas_nullable_types(self):
        """Test handling of nullable types (union types)."""
        schema1 = json.dumps({
            "type": "record",
            "fields": [
                {"name": "field_a", "type": ["null", "int"]},
            ]
        })
        schema2 = json.dumps({
            "type": "record",
            "fields": [
                {"name": "field_a", "type": ["null", "string"]},
            ]
        })

        diff = FeatureGroupRequest.compare_schemas(schema1, schema2)

        # Types are different, should be detected as modified
        assert len(diff["modified"]) == 1
        assert diff["modified"][0]["name"] == "field_a"

    def test_compare_schemas_complex_types(self):
        """Test handling of complex types (array, map)."""
        schema1 = json.dumps({
            "type": "record",
            "fields": [
                {"name": "field_a", "type": {"type": "array", "items": "string"}},
            ]
        })
        schema2 = json.dumps({
            "type": "record",
            "fields": [
                {"name": "field_a", "type": {"type": "array", "items": "int"}},
            ]
        })

        diff = FeatureGroupRequest.compare_schemas(schema1, schema2)

        # Complex types should be compared correctly
        assert len(diff["modified"]) == 1
        assert diff["modified"][0]["name"] == "field_a"


class TestVersionSchemaParsing:
    """Tests for Avro schema parsing in version methods."""

    def test_version_schema_is_parsed(self, versioned_feature_group):
        """Test that Avro schema is correctly parsed from JSON."""
        version = versioned_feature_group.get_version(1)
        assert version is not None

        schema = version["avro_schema"]
        if schema is not None:
            assert isinstance(schema, dict)
            # Avro schemas should have a type field
            assert "type" in schema or "fields" in schema

    def test_list_versions_schemas_are_parsed(self, versioned_feature_group):
        """Test that all schemas in list_versions are parsed."""
        versions = versioned_feature_group.list_versions()

        for version in versions:
            schema = version["avro_schema"]
            if schema is not None:
                assert isinstance(schema, dict)


class TestEdgeCases:
    """Edge case tests for versioning functionality."""

    def test_feature_count_matches_schema(self, versioned_feature_group):
        """Test that feature_count matches the number of fields in schema."""
        version = versioned_feature_group.get_version(1)
        assert version is not None

        schema = version["avro_schema"]
        feature_count = version["feature_count"]

        if schema is not None and "fields" in schema:
            # Feature count should match number of fields in schema
            assert feature_count == len(schema.get("fields", []))

    def test_version_timestamps_format(self, versioned_feature_group):
        """Test that timestamps are in expected format."""
        version = versioned_feature_group.get_version(1)
        assert version is not None

        # Timestamps should be strings in YYYY-MM-DD HH:mm:ss format
        created_at = version["created_at"]
        if created_at is not None:
            assert isinstance(created_at, str)
            # Basic format validation
            assert len(created_at) >= 10  # At least date part


# Cleanup fixture
@pytest.fixture(scope="module", autouse=True)
def cleanup(request, spark, input_data_spark):
    """Cleanup test data after all tests in module complete."""
    def cleanup_fn():
        try:
            Project(name="versioning_test_project").get_or_create()
            fg = FeatureGroup(name="versioned_feature_group").get_or_create()
            fg.delete()
        except Exception:
            pass

    request.addfinalizer(cleanup_fn)
