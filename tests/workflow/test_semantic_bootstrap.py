"""Tests for semantic model bootstrap — data profiling and YAML generation."""

import pytest
import yaml
from pathlib import Path

from seeknal.workflow.semantic.bootstrap import (
    _classify_column,
    _build_semantic_model_dict,
    _discover_tables,
    bootstrap_semantic_models,
)
from seeknal.workflow.semantic.models import SemanticModel


# ── Column Classification Tests ─────────────────────────────────────────────


class TestClassifyColumn:
    def test_timestamp_detected_as_time_dimension(self):
        assert _classify_column("order_date", "TIMESTAMP", 100, 1000) == "time_dimension"

    def test_date_detected_as_time_dimension(self):
        assert _classify_column("created_at", "DATE", 50, 500) == "time_dimension"

    def test_timestamptz_detected_as_time_dimension(self):
        assert _classify_column("updated_at", "TIMESTAMP WITH TIME ZONE", 50, 500) == "time_dimension"

    def test_high_cardinality_id_detected_as_entity(self):
        # 800 unique out of 1000 rows = 0.8 ratio > 0.5 threshold
        assert _classify_column("customer_id", "BIGINT", 800, 1000) == "entity"

    def test_high_cardinality_key_detected_as_entity(self):
        assert _classify_column("order_key", "VARCHAR", 900, 1000) == "entity"

    def test_low_cardinality_id_detected_as_dimension(self):
        # 5 unique out of 1000 rows = 0.005 ratio < 0.5 threshold
        assert _classify_column("status_id", "INTEGER", 5, 1000) == "dimension"

    def test_numeric_detected_as_measure(self):
        assert _classify_column("amount", "DOUBLE", 500, 1000) == "measure"

    def test_integer_detected_as_measure(self):
        assert _classify_column("quantity", "INTEGER", 20, 1000) == "measure"

    def test_decimal_detected_as_measure(self):
        assert _classify_column("price", "DECIMAL(10,2)", 100, 1000) == "measure"

    def test_varchar_detected_as_dimension(self):
        assert _classify_column("region", "VARCHAR", 5, 1000) == "dimension"

    def test_text_detected_as_dimension(self):
        assert _classify_column("status", "TEXT", 3, 1000) == "dimension"

    def test_zero_rows_no_crash(self):
        # Should not divide by zero
        result = _classify_column("customer_id", "BIGINT", 0, 0)
        assert result in ("entity", "dimension")


# ── Build Semantic Model Dict Tests ─────────────────────────────────────────


class TestBuildSemanticModelDict:
    def test_basic_model_structure(self):
        columns = [
            {"name": "customer_id", "type": "BIGINT", "unique_count": 900, "classification": "entity"},
            {"name": "order_date", "type": "TIMESTAMP", "unique_count": 365, "classification": "time_dimension"},
            {"name": "region", "type": "VARCHAR", "unique_count": 5, "classification": "dimension"},
            {"name": "amount", "type": "DOUBLE", "unique_count": 500, "classification": "measure"},
        ]
        model = _build_semantic_model_dict("orders", "ref('source.orders')", columns)

        assert model["kind"] == "semantic_model"
        assert model["name"] == "orders"
        assert model["model"] == "ref('source.orders')"
        assert len(model["entities"]) == 1
        assert model["entities"][0]["name"] == "customer_id"
        assert model["entities"][0]["type"] == "primary"

    def test_time_dimension_detection(self):
        columns = [
            {"name": "order_date", "type": "TIMESTAMP", "unique_count": 365, "classification": "time_dimension"},
        ]
        model = _build_semantic_model_dict("orders", "ref('source.orders')", columns)

        dims = model["dimensions"]
        assert len(dims) == 1
        assert dims[0]["name"] == "order_date"
        assert dims[0]["type"] == "time"
        assert dims[0]["time_granularity"] == "day"
        assert model["default_time_dimension"] == "order_date"

    def test_measure_naming(self):
        columns = [
            {"name": "amount", "type": "DOUBLE", "unique_count": 500, "classification": "measure"},
        ]
        model = _build_semantic_model_dict("orders", "ref('source.orders')", columns)

        measures = model["measures"]
        assert len(measures) == 1
        assert measures[0]["name"] == "total_amount"
        assert measures[0]["expr"] == "amount"
        assert measures[0]["agg"] == "sum"

    def test_multiple_entities_primary_foreign(self):
        columns = [
            {"name": "order_id", "type": "BIGINT", "unique_count": 1000, "classification": "entity"},
            {"name": "customer_id", "type": "BIGINT", "unique_count": 800, "classification": "entity"},
        ]
        model = _build_semantic_model_dict("orders", "ref('source.orders')", columns)

        assert len(model["entities"]) == 2
        assert model["entities"][0]["type"] == "primary"
        assert model["entities"][1]["type"] == "foreign"

    def test_no_time_dimension_no_default(self):
        columns = [
            {"name": "region", "type": "VARCHAR", "unique_count": 5, "classification": "dimension"},
        ]
        model = _build_semantic_model_dict("regions", "ref('source.regions')", columns)

        assert "default_time_dimension" not in model

    def test_parseable_by_semantic_model_from_dict(self):
        """Generated dict must be parseable back through SemanticModel.from_dict()."""
        columns = [
            {"name": "customer_id", "type": "BIGINT", "unique_count": 900, "classification": "entity"},
            {"name": "order_date", "type": "TIMESTAMP", "unique_count": 365, "classification": "time_dimension"},
            {"name": "region", "type": "VARCHAR", "unique_count": 5, "classification": "dimension"},
            {"name": "amount", "type": "DOUBLE", "unique_count": 500, "classification": "measure"},
        ]
        model_dict = _build_semantic_model_dict("orders", "ref('source.orders')", columns)
        sm = SemanticModel.from_dict(model_dict)

        assert sm.name == "orders"
        assert sm.model_ref == "ref('source.orders')"
        assert len(sm.entities) == 1
        assert sm.entities[0].name == "customer_id"
        assert len(sm.dimensions) == 2  # time + categorical
        assert len(sm.measures) == 1
        assert sm.default_time_dimension == "order_date"


# ── Discover Tables Tests ────────────────────────────────────────────────────


class TestDiscoverTables:
    def test_discovers_csv_files(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "customers.csv").write_text("id,name\n1,Alice\n")
        (data_dir / "orders.csv").write_text("id,amount\n1,100\n")

        tables = _discover_tables(tmp_path)
        names = [t[0] for t in tables]
        assert "customers" in names
        assert "orders" in names

    def test_discovers_parquet_files(self, tmp_path):
        import duckdb

        intermediate_dir = tmp_path / "target" / "intermediate"
        intermediate_dir.mkdir(parents=True)

        con = duckdb.connect(":memory:")
        con.execute(
            f"COPY (SELECT 1 AS id, 'test' AS name) "
            f"TO '{intermediate_dir / 'transformed.parquet'}' (FORMAT PARQUET)"
        )
        con.close()

        tables = _discover_tables(tmp_path)
        names = [t[0] for t in tables]
        assert "transformed" in names

    def test_model_ref_for_csv(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "customers.csv").write_text("id,name\n1,Alice\n")

        tables = _discover_tables(tmp_path)
        assert tables[0][2] == "ref('source.customers')"

    def test_model_ref_for_parquet(self, tmp_path):
        import duckdb

        intermediate_dir = tmp_path / "target" / "intermediate"
        intermediate_dir.mkdir(parents=True)

        con = duckdb.connect(":memory:")
        con.execute(
            f"COPY (SELECT 1 AS id) "
            f"TO '{intermediate_dir / 'agg_sales.parquet'}' (FORMAT PARQUET)"
        )
        con.close()

        tables = _discover_tables(tmp_path)
        assert tables[0][2] == "ref('transform.agg_sales')"

    def test_empty_project(self, tmp_path):
        tables = _discover_tables(tmp_path)
        assert tables == []


# ── Full Bootstrap Integration Tests ─────────────────────────────────────────


class TestBootstrapSemanticModels:
    def test_bootstrap_from_csv_with_mixed_types(self, tmp_path):
        """Full integration: CSV with mixed types produces valid YAML."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv_content = (
            "customer_id,order_date,region,amount,quantity\n"
            "1001,2024-01-15,APAC,150.50,3\n"
            "1002,2024-01-16,EMEA,200.00,5\n"
            "1003,2024-01-17,NA,75.25,1\n"
            "1004,2024-01-18,APAC,300.00,7\n"
            "1005,2024-01-19,EMEA,50.00,2\n"
        )
        (data_dir / "orders.csv").write_text(csv_content)

        generated = bootstrap_semantic_models(tmp_path)

        assert len(generated) == 1
        model = generated[0]
        assert model["name"] == "orders"
        assert model["kind"] == "semantic_model"

        # Verify YAML file was written
        yml_path = tmp_path / "seeknal" / "semantic_models" / "orders.yml"
        assert yml_path.exists()

        # Verify auto-generated header
        content = yml_path.read_text()
        assert content.startswith("# AUTO-GENERATED by seeknal semantic bootstrap")

        # Verify YAML is parseable
        parsed = yaml.safe_load(content)
        assert parsed["name"] == "orders"

        # Verify it round-trips through SemanticModel.from_dict()
        sm = SemanticModel.from_dict(parsed)
        assert sm.name == "orders"

    def test_entity_detection_customer_id(self, tmp_path):
        """customer_id with high cardinality is detected as primary entity."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        lines = ["customer_id,name\n"]
        for i in range(100):
            lines.append(f"{i},Customer_{i}\n")
        (data_dir / "customers.csv").write_text("".join(lines))

        generated = bootstrap_semantic_models(tmp_path)
        assert len(generated) == 1
        entities = generated[0]["entities"]
        entity_names = [e["name"] for e in entities]
        assert "customer_id" in entity_names

    def test_time_dimension_detection(self, tmp_path):
        """TIMESTAMP column order_date detected as time dimension."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        # DuckDB auto-detects ISO dates as DATE/TIMESTAMP
        csv_content = (
            "id,order_date,amount\n"
            "1,2024-01-15 10:00:00,100\n"
            "2,2024-01-16 11:00:00,200\n"
        )
        (data_dir / "sales.csv").write_text(csv_content)

        generated = bootstrap_semantic_models(tmp_path)
        assert len(generated) == 1

        dims = generated[0]["dimensions"]
        time_dims = [d for d in dims if d["type"] == "time"]
        assert len(time_dims) >= 1
        time_dim_names = [d["name"] for d in time_dims]
        assert "order_date" in time_dim_names

    def test_measure_detection_amount(self, tmp_path):
        """DOUBLE column amount detected as sum measure."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv_content = (
            "id,region,amount\n"
            "1,APAC,150.50\n"
            "2,EMEA,200.00\n"
        )
        (data_dir / "sales.csv").write_text(csv_content)

        generated = bootstrap_semantic_models(tmp_path)
        assert len(generated) == 1

        measures = generated[0]["measures"]
        measure_names = [m["name"] for m in measures]
        assert "total_amount" in measure_names

        # Check aggregation is sum
        amount_measure = next(m for m in measures if m["name"] == "total_amount")
        assert amount_measure["agg"] == "sum"
        assert amount_measure["expr"] == "amount"

    def test_generated_yaml_parses_through_semantic_model(self, tmp_path):
        """End-to-end: generated YAML round-trips through SemanticModel.from_dict()."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv_content = (
            "customer_id,order_date,region,amount\n"
            "1001,2024-01-15,APAC,150.50\n"
            "1002,2024-01-16,EMEA,200.00\n"
            "1003,2024-01-17,NA,75.25\n"
        )
        (data_dir / "orders.csv").write_text(csv_content)

        generated = bootstrap_semantic_models(tmp_path)
        assert len(generated) == 1

        # Read back the written YAML file
        yml_path = tmp_path / "seeknal" / "semantic_models" / "orders.yml"
        with open(yml_path) as f:
            parsed = yaml.safe_load(f)

        sm = SemanticModel.from_dict(parsed)
        assert sm.name == "orders"
        assert sm.model_ref == "ref('source.orders')"
        assert len(sm.entities) >= 1
        assert len(sm.measures) >= 1

        # Validate: no validation errors for non-optional fields
        errors = sm.validate()
        # We expect no critical errors (entities and measures present)
        assert not any("missing" in e.lower() for e in errors)

    def test_empty_data_directory_returns_empty(self, tmp_path):
        """No data files returns empty list."""
        generated = bootstrap_semantic_models(tmp_path)
        assert generated == []

    def test_single_table_bootstrap(self, tmp_path):
        """table_name parameter filters to single table."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "customers.csv").write_text("id,name\n1,Alice\n")
        (data_dir / "orders.csv").write_text("id,amount\n1,100\n")

        generated = bootstrap_semantic_models(tmp_path, table_name="orders")
        assert len(generated) == 1
        assert generated[0]["name"] == "orders"

    def test_nonexistent_table_name_returns_empty(self, tmp_path):
        """table_name that doesn't match any file returns empty."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "orders.csv").write_text("id,amount\n1,100\n")

        generated = bootstrap_semantic_models(tmp_path, table_name="nonexistent")
        assert generated == []

    def test_multiple_tables_bootstrap(self, tmp_path):
        """Multiple CSV files each produce a model."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "customers.csv").write_text("customer_id,name\n1,Alice\n2,Bob\n")
        (data_dir / "orders.csv").write_text("order_id,amount\n1,100\n2,200\n")

        generated = bootstrap_semantic_models(tmp_path)
        assert len(generated) == 2
        names = {m["name"] for m in generated}
        assert names == {"customers", "orders"}

        # Both YAML files written
        sm_dir = tmp_path / "seeknal" / "semantic_models"
        assert (sm_dir / "customers.yml").exists()
        assert (sm_dir / "orders.yml").exists()
