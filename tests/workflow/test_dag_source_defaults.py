"""Tests for DAG builder source_defaults merge logic.

Tests cover:
- YAML iceberg source gets defaults from profile
- YAML iceberg source with inline params overrides defaults
- YAML postgresql source gets connection from defaults
- Python @source iceberg gets defaults
- No source_defaults section = no-op
- Env var interpolation in source_defaults values
- postgres alias resolves to postgresql key
- params: null in YAML handled gracefully
- Non-source nodes unaffected by source_defaults
- CSV source with no defaults entry works unchanged
"""

from seeknal.workflow.dag import DAGBuilder  # ty: ignore[unresolved-import]


class TestDAGSourceDefaultsYAML:
    """Tests for source_defaults merge in YAML source parsing."""

    def _setup_project(self, tmp_path, profile_content, source_yamls):
        """Helper to create project with profiles.yml and source YAML files.

        Args:
            tmp_path: pytest tmp_path fixture
            profile_content: YAML string for profiles.yml
            source_yamls: dict of {filename: yaml_content} for seeknal/sources/

        Returns:
            DAGBuilder instance (not yet built)
        """
        # Write profiles.yml
        profile_file = tmp_path / "profiles.yml"
        profile_file.write_text(profile_content)

        # Create seeknal/sources/ directory
        sources_dir = tmp_path / "seeknal" / "sources"
        sources_dir.mkdir(parents=True)

        for filename, content in source_yamls.items():
            (sources_dir / filename).write_text(content)

        return DAGBuilder(project_path=tmp_path, profile_path=profile_file)

    def test_yaml_iceberg_source_gets_defaults(self, tmp_path):
        """Iceberg source without params gets catalog_uri + warehouse from defaults."""
        builder = self._setup_project(
            tmp_path,
            profile_content="""\
source_defaults:
  iceberg:
    catalog_uri: http://lakekeeper:8181
    warehouse: my-warehouse
""",
            source_yamls={
                "events.yml": """\
kind: source
name: events
source: iceberg
table: atlas.ns.events
"""
            },
        )
        builder.build()

        node = builder.get_node("source.events")
        assert node is not None
        params = node.yaml_data.get("params", {})
        assert params["catalog_uri"] == "http://lakekeeper:8181"
        assert params["warehouse"] == "my-warehouse"

    def test_yaml_iceberg_source_inline_overrides(self, tmp_path):
        """Inline params override source_defaults."""
        builder = self._setup_project(
            tmp_path,
            profile_content="""\
source_defaults:
  iceberg:
    catalog_uri: http://default:8181
    warehouse: default-warehouse
""",
            source_yamls={
                "events.yml": """\
kind: source
name: events
source: iceberg
table: atlas.ns.events
params:
  warehouse: custom-warehouse
"""
            },
        )
        builder.build()

        node = builder.get_node("source.events")
        assert node is not None
        params = node.yaml_data.get("params", {})
        # catalog_uri from defaults
        assert params["catalog_uri"] == "http://default:8181"
        # warehouse overridden by inline
        assert params["warehouse"] == "custom-warehouse"

    def test_yaml_postgresql_source_gets_connection(self, tmp_path):
        """PostgreSQL source gets connection from source_defaults."""
        builder = self._setup_project(
            tmp_path,
            profile_content="""\
source_defaults:
  postgresql:
    connection: local_pg
""",
            source_yamls={
                "customers.yml": """\
kind: source
name: customers
source: postgresql
table: public.customers
"""
            },
        )
        builder.build()

        node = builder.get_node("source.customers")
        assert node is not None
        params = node.yaml_data.get("params", {})
        assert params["connection"] == "local_pg"

    def test_no_source_defaults_section(self, tmp_path):
        """Profile without source_defaults section = no-op."""
        builder = self._setup_project(
            tmp_path,
            profile_content="""\
connections:
  my_pg:
    type: postgresql
""",
            source_yamls={
                "events.yml": """\
kind: source
name: events
source: csv
table: data.csv
"""
            },
        )
        builder.build()

        node = builder.get_node("source.events")
        assert node is not None
        # No params added
        assert node.yaml_data.get("params") is None or node.yaml_data.get("params") == {}

    def test_source_defaults_env_var_interpolation(self, tmp_path, monkeypatch):
        """Env vars in source_defaults are interpolated."""
        monkeypatch.setenv("TEST_CATALOG_URI", "http://prod:8181")
        builder = self._setup_project(
            tmp_path,
            profile_content="""\
source_defaults:
  iceberg:
    catalog_uri: ${TEST_CATALOG_URI}
    warehouse: ${TEST_WAREHOUSE:default-wh}
""",
            source_yamls={
                "events.yml": """\
kind: source
name: events
source: iceberg
table: atlas.ns.events
"""
            },
        )
        builder.build()

        node = builder.get_node("source.events")
        assert node is not None
        params = node.yaml_data.get("params", {})
        assert params["catalog_uri"] == "http://prod:8181"
        assert params["warehouse"] == "default-wh"

    def test_postgres_alias_resolves(self, tmp_path):
        """Source type 'postgres' resolves to 'postgresql' defaults."""
        builder = self._setup_project(
            tmp_path,
            profile_content="""\
source_defaults:
  postgresql:
    connection: local_pg
""",
            source_yamls={
                "orders.yml": """\
kind: source
name: orders
source: postgres
table: public.orders
"""
            },
        )
        builder.build()

        node = builder.get_node("source.orders")
        assert node is not None
        params = node.yaml_data.get("params", {})
        assert params["connection"] == "local_pg"

    def test_params_null_guard(self, tmp_path):
        """YAML with `params: null` (or `params:` with no value) works."""
        builder = self._setup_project(
            tmp_path,
            profile_content="""\
source_defaults:
  iceberg:
    catalog_uri: http://lakekeeper:8181
    warehouse: my-warehouse
""",
            source_yamls={
                "events.yml": """\
kind: source
name: events
source: iceberg
table: atlas.ns.events
params:
"""
            },
        )
        builder.build()

        node = builder.get_node("source.events")
        assert node is not None
        params = node.yaml_data.get("params", {})
        # Defaults still applied even though params: null
        assert params["catalog_uri"] == "http://lakekeeper:8181"
        assert params["warehouse"] == "my-warehouse"

    def test_non_source_nodes_unaffected(self, tmp_path):
        """Transform nodes are not affected by source_defaults."""
        profile_file = tmp_path / "profiles.yml"
        profile_file.write_text("""\
source_defaults:
  iceberg:
    catalog_uri: http://lakekeeper:8181
""")

        seeknal_dir = tmp_path / "seeknal"
        sources_dir = seeknal_dir / "sources"
        sources_dir.mkdir(parents=True)
        transforms_dir = seeknal_dir / "transforms"
        transforms_dir.mkdir(parents=True)

        (sources_dir / "raw.yml").write_text("""\
kind: source
name: raw
source: csv
table: data.csv
""")
        (transforms_dir / "clean.yml").write_text("""\
kind: transform
name: clean
transform: SELECT * FROM input_0
inputs:
  - ref: source.raw
""")

        builder = DAGBuilder(project_path=tmp_path, profile_path=profile_file)
        builder.build()

        transform_node = builder.get_node("transform.clean")
        assert transform_node is not None
        # Transform should NOT have any source_defaults params injected
        assert "catalog_uri" not in transform_node.yaml_data.get("params", {})

    def test_csv_source_no_defaults(self, tmp_path):
        """CSV source with no source_defaults entry works unchanged."""
        builder = self._setup_project(
            tmp_path,
            profile_content="""\
source_defaults:
  iceberg:
    catalog_uri: http://lakekeeper:8181
""",
            source_yamls={
                "events.yml": """\
kind: source
name: events
source: csv
table: data.csv
"""
            },
        )
        builder.build()

        node = builder.get_node("source.events")
        assert node is not None
        # No params added for csv (no csv entry in source_defaults)
        assert node.yaml_data.get("params") is None or node.yaml_data.get("params") == {}

    def test_no_profile_path_means_no_defaults(self, tmp_path):
        """DAGBuilder without profile_path does not inject defaults."""
        sources_dir = tmp_path / "seeknal" / "sources"
        sources_dir.mkdir(parents=True)
        (sources_dir / "events.yml").write_text("""\
kind: source
name: events
source: iceberg
table: atlas.ns.events
""")

        # No profile_path → no source defaults
        builder = DAGBuilder(project_path=tmp_path)
        builder.build()

        node = builder.get_node("source.events")
        assert node is not None
        assert node.yaml_data.get("params") is None or node.yaml_data.get("params") == {}
