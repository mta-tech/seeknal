"""Tests for DAG builder materializations normalization.

Tests cover:
- materializations: (plural) key parsed as list
- materialization: (singular) normalized to list with type: iceberg default
- Both keys present → validation error
- Invalid types for materializations value
- Python pipeline materialization list handling
"""

import pytest  # ty: ignore[unresolved-import]

from pathlib import Path

from seeknal.workflow.dag import DAGBuilder  # ty: ignore[unresolved-import]


# =============================================================================
# _normalize_materializations tests
# =============================================================================


class TestNormalizeMaterializations:
    """Tests for DAGBuilder._normalize_materializations()."""

    def _builder(self) -> DAGBuilder:
        """Create a DAGBuilder with a dummy path."""
        return DAGBuilder(project_path="/tmp/test")

    def test_singular_dict_normalized_to_list(self):
        builder = self._builder()
        data = {
            "materialization": {
                "enabled": True,
                "table": "atlas.ns.my_table",
            }
        }
        result = builder._normalize_materializations(data, Path("test.yml"))
        assert "materializations" in result
        assert isinstance(result["materializations"], list)
        assert len(result["materializations"]) == 1
        assert result["materializations"][0]["type"] == "iceberg"
        assert result["materializations"][0]["table"] == "atlas.ns.my_table"

    def test_singular_with_explicit_type_preserved(self):
        builder = self._builder()
        data = {
            "materialization": {
                "type": "postgresql",
                "connection": "my_pg",
                "table": "public.orders",
            }
        }
        result = builder._normalize_materializations(data, Path("test.yml"))
        assert result["materializations"][0]["type"] == "postgresql"

    def test_plural_list_kept_as_is(self):
        builder = self._builder()
        mat_list = [
            {"type": "iceberg", "table": "atlas.ns.t1"},
            {"type": "postgresql", "connection": "pg", "table": "public.t1"},
        ]
        data = {"materializations": mat_list}
        result = builder._normalize_materializations(data, Path("test.yml"))
        assert result["materializations"] == mat_list

    def test_both_keys_produces_error(self):
        builder = self._builder()
        data = {
            "materialization": {"enabled": True},
            "materializations": [{"type": "iceberg"}],
        }
        result = builder._normalize_materializations(data, Path("test.yml"))
        assert len(builder._parse_errors) == 1
        assert "Both" in builder._parse_errors[0]
        assert "materializations" in builder._parse_errors[0]

    def test_no_materialization_keys_unchanged(self):
        builder = self._builder()
        data = {"kind": "transform", "name": "foo"}
        result = builder._normalize_materializations(data, Path("test.yml"))
        assert "materializations" not in result
        assert "materialization" not in result
        assert len(builder._parse_errors) == 0

    def test_singular_none_value_ignored(self):
        builder = self._builder()
        data = {"materialization": None}
        result = builder._normalize_materializations(data, Path("test.yml"))
        assert "materializations" not in result
        assert len(builder._parse_errors) == 0

    def test_plural_not_list_produces_error(self):
        builder = self._builder()
        data = {"materializations": {"type": "iceberg"}}
        result = builder._normalize_materializations(data, Path("test.yml"))
        assert len(builder._parse_errors) == 1
        assert "must be a list" in builder._parse_errors[0]

    def test_singular_invalid_type_produces_error(self):
        builder = self._builder()
        data = {"materialization": "invalid_string"}
        result = builder._normalize_materializations(data, Path("test.yml"))
        assert len(builder._parse_errors) == 1
        assert "must be a dictionary or list" in builder._parse_errors[0]

    def test_singular_list_accepted(self):
        """Test that a list under singular key is accepted (forward compat)."""
        builder = self._builder()
        mat_list = [
            {"type": "iceberg", "table": "atlas.ns.t1"},
            {"type": "postgresql", "table": "public.t1"},
        ]
        data = {"materialization": mat_list}
        result = builder._normalize_materializations(data, Path("test.yml"))
        assert result["materializations"] == mat_list
        assert len(builder._parse_errors) == 0

    def test_backward_compat_iceberg_no_type_field(self):
        """Existing materialization: without type: gets type: iceberg."""
        builder = self._builder()
        data = {
            "materialization": {
                "enabled": True,
                "catalog": {"type": "rest", "uri": "http://lakekeeper"},
                "table": "atlas.ns.table",
                "mode": "append",
            }
        }
        result = builder._normalize_materializations(data, Path("test.yml"))
        mat = result["materializations"][0]
        assert mat["type"] == "iceberg"
        assert mat["enabled"] is True
        assert mat["catalog"]["type"] == "rest"


# =============================================================================
# YAML file integration test
# =============================================================================


class TestDAGBuilderMaterializationYAML:
    """Test materializations normalization through full YAML parsing."""

    def test_yaml_singular_materialization(self, tmp_path):
        """Test that singular materialization: in YAML is normalized."""
        seeknal_dir = tmp_path / "seeknal" / "transforms"
        seeknal_dir.mkdir(parents=True)

        yaml_content = """\
kind: transform
name: clean_data
transform: SELECT * FROM input_0
inputs:
  - ref: source.raw_data
materialization:
  enabled: true
  table: atlas.ns.clean_data
"""
        (seeknal_dir / "clean_data.yml").write_text(yaml_content)

        # Also need a source to avoid missing dependency errors
        sources_dir = tmp_path / "seeknal" / "sources"
        sources_dir.mkdir(parents=True)
        source_yaml = """\
kind: source
name: raw_data
source: csv
table: data.csv
"""
        (sources_dir / "raw_data.yml").write_text(source_yaml)

        builder = DAGBuilder(project_path=tmp_path)
        builder.build()

        node = builder.get_node("transform.clean_data")
        assert node is not None
        assert "materializations" in node.yaml_data
        mats = node.yaml_data["materializations"]
        assert isinstance(mats, list)
        assert len(mats) == 1
        assert mats[0]["type"] == "iceberg"

    def test_yaml_plural_materializations(self, tmp_path):
        """Test that plural materializations: in YAML is parsed correctly."""
        seeknal_dir = tmp_path / "seeknal" / "transforms"
        seeknal_dir.mkdir(parents=True)

        yaml_content = """\
kind: transform
name: multi_target
transform: SELECT * FROM input_0
inputs:
  - ref: source.raw_data
materializations:
  - type: iceberg
    table: atlas.ns.multi_target
  - type: postgresql
    connection: my_pg
    table: public.multi_target
    mode: full
"""
        (seeknal_dir / "multi_target.yml").write_text(yaml_content)

        sources_dir = tmp_path / "seeknal" / "sources"
        sources_dir.mkdir(parents=True)
        source_yaml = """\
kind: source
name: raw_data
source: csv
table: data.csv
"""
        (sources_dir / "raw_data.yml").write_text(source_yaml)

        builder = DAGBuilder(project_path=tmp_path)
        builder.build()

        node = builder.get_node("transform.multi_target")
        assert node is not None
        mats = node.yaml_data["materializations"]
        assert len(mats) == 2
        assert mats[0]["type"] == "iceberg"
        assert mats[1]["type"] == "postgresql"
        assert mats[1]["connection"] == "my_pg"

    def test_yaml_both_keys_error(self, tmp_path):
        """Test that both keys produce a parse error."""
        seeknal_dir = tmp_path / "seeknal" / "transforms"
        seeknal_dir.mkdir(parents=True)

        yaml_content = """\
kind: transform
name: bad_node
transform: SELECT 1
materialization:
  enabled: true
materializations:
  - type: iceberg
"""
        (seeknal_dir / "bad_node.yml").write_text(yaml_content)

        builder = DAGBuilder(project_path=tmp_path)
        builder.build()

        errors = builder.get_parse_errors()
        assert any("Both" in e for e in errors)

    def test_yaml_no_materialization(self, tmp_path):
        """Test node without any materialization config."""
        seeknal_dir = tmp_path / "seeknal" / "sources"
        seeknal_dir.mkdir(parents=True)

        yaml_content = """\
kind: source
name: simple
source: csv
table: data.csv
"""
        (seeknal_dir / "simple.yml").write_text(yaml_content)

        builder = DAGBuilder(project_path=tmp_path)
        builder.build()

        node = builder.get_node("source.simple")
        assert node is not None
        assert "materializations" not in node.yaml_data
