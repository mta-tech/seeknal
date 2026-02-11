"""
Tests for column lineage tracking module using SQLGlot AST analysis.
"""

import pytest

from seeknal.dag.lineage import (
    LineageBuilder,
    ColumnLineage,
    ColumnDependency,
    ColumnTransformationType,
    create_lineage_builder,
    build_column_lineage,
    get_upstream_columns,
    get_downstream_impact,
    SQLGLOT_AVAILABLE,
)


@pytest.mark.skipif(not SQLGLOT_AVAILABLE, reason="sqlglot not installed")
class TestColumnDependency:
    """Tests for the ColumnDependency dataclass."""

    def test_create_dependency(self):
        """Test creating a column dependency."""
        dep = ColumnDependency(
            output_column="total",
            input_columns={"price", "quantity"},
            transformation_type=ColumnTransformationType.EXPRESSION,
            expression="price * quantity"
        )
        assert dep.output_column == "total"
        assert "price" in dep.input_columns
        assert "quantity" in dep.input_columns

    def test_direct_mapping_string(self):
        """Test string representation of direct mapping."""
        dep = ColumnDependency(
            output_column="col_a",
            input_columns={"col_a"},
            transformation_type=ColumnTransformationType.DIRECT
        )
        assert "col_a <- col_a" in str(dep)

    def test_expression_string(self):
        """Test string representation of expression."""
        dep = ColumnDependency(
            output_column="total",
            input_columns={"price", "quantity"},
            transformation_type=ColumnTransformationType.EXPRESSION
        )
        str_repr = str(dep)
        assert "total <-" in str_repr
        assert "f(" in str_repr


@pytest.mark.skipif(not SQLGLOT_AVAILABLE, reason="sqlglot not installed")
class TestColumnLineage:
    """Tests for the ColumnLineage dataclass."""

    def test_create_lineage(self):
        """Test creating column lineage."""
        lineage = ColumnLineage(
            output_columns=["a", "b"],
            input_dependencies={
                "a": {"a"},
                "b": {"b"}
            },
            dependencies=[],
            source_tables={"foo"}
        )
        assert len(lineage.output_columns) == 2
        assert len(lineage.source_tables) == 1

    def test_get_dependencies_for_column(self):
        """Test getting dependencies for a specific column."""
        lineage = ColumnLineage(
            output_columns=["total", "tax"],
            input_dependencies={
                "total": {"price", "quantity"},
                "tax": {"price"}
            },
            dependencies=[]
        )

        deps = lineage.get_dependencies_for_column("total")
        assert deps == {"price", "quantity"}

    def test_get_dependencies_for_nonexistent_column(self):
        """Test getting dependencies for a non-existent column."""
        lineage = ColumnLineage(
            output_columns=["a"],
            input_dependencies={"a": {"a"}},
            dependencies=[]
        )

        deps = lineage.get_dependencies_for_column("nonexistent")
        assert deps is None

    def test_is_direct_mapping(self):
        """Test checking if column is direct mapping."""
        lineage = ColumnLineage(
            output_columns=["a", "total"],
            input_dependencies={
                "a": {"a"},           # Direct
                "total": {"a", "b"}   # Expression
            },
            dependencies=[]
        )

        assert lineage.is_direct_mapping("a") is True
        assert lineage.is_direct_mapping("total") is False

    def test_get_lineage_graph(self):
        """Test getting lineage graph as adjacency list."""
        lineage = ColumnLineage(
            output_columns=["a", "b"],
            input_dependencies={
                "a": {"x"},
                "b": {"y", "z"}
            },
            dependencies=[]
        )

        graph = lineage.get_lineage_graph()
        assert graph["a"] == ["x"]
        assert set(graph["b"]) == {"y", "z"}


@pytest.mark.skipif(not SQLGLOT_AVAILABLE, reason="sqlglot not installed")
class TestLineageBuilder:
    """Tests for the LineageBuilder class."""

    def test_init(self):
        """Test creating a lineage builder."""
        builder = LineageBuilder()
        assert builder.parser is not None

    def test_init_with_dialect(self):
        """Test creating builder with specific dialect."""
        from seeknal.dag.sql_parser import SQLDialect
        builder = LineageBuilder(dialect=SQLDialect.DUCKDB)
        assert builder.parser is not None

    def test_build_lineage_simple_select(self):
        """Test building lineage for simple SELECT."""
        builder = LineageBuilder()
        lineage = builder.build_lineage("SELECT a, b FROM foo")

        assert lineage is not None
        assert "a" in lineage.output_columns
        assert "b" in lineage.output_columns
        assert "foo" in lineage.source_tables

    def test_build_lineage_with_alias(self):
        """Test building lineage with column aliases."""
        builder = LineageBuilder()
        lineage = builder.build_lineage("SELECT a AS x, b AS y FROM foo")

        assert lineage is not None
        assert "x" in lineage.output_columns
        assert "y" in lineage.output_columns

    def test_build_lineage_with_expression(self):
        """Test building lineage with expression."""
        builder = LineageBuilder()
        lineage = builder.build_lineage("SELECT price * quantity AS total FROM orders")

        assert lineage is not None
        assert "total" in lineage.output_columns

        # Check dependencies
        deps = lineage.get_dependencies_for_column("total")
        assert deps is not None
        # Should have price and quantity as dependencies
        assert len(deps) > 0

    def test_build_lineage_with_aggregation(self):
        """Test building lineage with aggregation."""
        builder = LineageBuilder()
        lineage = builder.build_lineage("SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM sales")

        assert lineage is not None
        assert "cnt" in lineage.output_columns
        assert "total" in lineage.output_columns

    def test_build_lineage_invalid_sql(self):
        """Test building lineage with invalid SQL."""
        builder = LineageBuilder()
        lineage = builder.build_lineage("INVALID SQL HERE")

        assert lineage is None

    def test_get_upstream_columns(self):
        """Test getting upstream columns for an output column."""
        builder = LineageBuilder()
        upstream = builder.get_upstream_columns("SELECT a, b FROM foo", "a")

        # Column 'a' should depend on 'a'
        assert "a" in upstream

    def test_get_upstream_columns_expression(self):
        """Test getting upstream columns for expression output."""
        builder = LineageBuilder()
        upstream = builder.get_upstream_columns(
            "SELECT price * quantity AS total FROM orders",
            "total"
        )

        # Should depend on price and quantity
        assert len(upstream) > 0

    def test_get_upstream_columns_nonexistent_output(self):
        """Test getting upstream for non-existent output column."""
        builder = LineageBuilder()
        upstream = builder.get_upstream_columns("SELECT a FROM foo", "nonexistent")

        assert upstream == set()

    def test_get_downstream_impact(self):
        """Test getting downstream impact of removing input column."""
        builder = LineageBuilder()

        # If we remove 'price', 'total' should be affected
        impact = builder.get_downstream_impact(
            "SELECT price * quantity AS total FROM orders",
            "price"
        )

        assert "total" in impact

    def test_get_downstream_impact_no_impact(self):
        """Test downstream impact when input is not used."""
        builder = LineageBuilder()

        impact = builder.get_downstream_impact(
            "SELECT a, b FROM foo",
            "unused_column"
        )

        assert len(impact) == 0

    def test_dependencies_transformation_types(self):
        """Test that transformation types are detected."""
        builder = LineageBuilder()
        lineage = builder.build_lineage("SELECT a, b * c AS calc FROM foo")

        assert lineage is not None

        # Check that dependencies have transformation types
        for dep in lineage.dependencies:
            assert dep.transformation_type in [
                ColumnTransformationType.DIRECT,
                ColumnTransformationType.EXPRESSION,
                ColumnTransformationType.AGGREGATION,
                ColumnTransformationType.LITERAL,
                ColumnTransformationType.UNKNOWN,
            ]


@pytest.mark.skipif(not SQLGLOT_AVAILABLE, reason="sqlglot not installed")
class TestCreateLineageBuilder:
    """Tests for the create_lineage_builder factory function."""

    def test_create_default_builder(self):
        """Test creating builder with default dialect."""
        builder = create_lineage_builder()
        assert builder.parser is not None

    def test_create_duckdb_builder(self):
        """Test creating DuckDB builder."""
        from seeknal.dag.sql_parser import SQLDialect
        builder = create_lineage_builder(SQLDialect.DUCKDB)
        assert builder.parser is not None


@pytest.mark.skipif(not SQLGLOT_AVAILABLE, reason="sqlglot not installed")
class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_build_column_lineage_function(self):
        """Test build_column_lineage convenience function."""
        lineage = build_column_lineage("SELECT a, b FROM foo")

        assert lineage is not None
        assert "a" in lineage.output_columns

    def test_get_upstream_columns_function(self):
        """Test get_upstream_columns convenience function."""
        upstream = get_upstream_columns("SELECT a, b FROM foo", "a")

        assert "a" in upstream

    def test_get_downstream_impact_function(self):
        """Test get_downstream_impact convenience function."""
        impact = get_downstream_impact(
            "SELECT a * b AS total FROM foo",
            "a"
        )

        assert "total" in impact
