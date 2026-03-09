"""Tests for seeknal.ask.report.deterministic module."""

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# parse_sections() — Schema Validation
# ---------------------------------------------------------------------------


class TestParseSections:
    """Test parse_sections() schema validation."""

    def test_valid_minimal_section(self):
        from seeknal.ask.report.deterministic import parse_sections

        sections = parse_sections([
            {
                "title": "Summary",
                "queries": [
                    {
                        "name": "kpis",
                        "sql": "SELECT 1 AS total",
                        "chart": "BigValue",
                        "value": ["total"],
                        "labels": ["Total"],
                    }
                ],
                "narrative": False,
            }
        ])
        assert len(sections) == 1
        assert sections[0].title == "Summary"
        assert len(sections[0].queries) == 1
        assert sections[0].queries[0].name == "kpis"
        assert sections[0].narrative is False

    def test_narrative_only_section(self):
        from seeknal.ask.report.deterministic import parse_sections

        sections = parse_sections([
            {"title": "Recommendations", "narrative": True}
        ])
        assert len(sections) == 1
        assert sections[0].narrative is True
        assert sections[0].queries == []

    def test_all_chart_types(self):
        from seeknal.ask.report.deterministic import ChartType, parse_sections

        raw = []
        for ct in ChartType:
            raw.append({
                "title": f"Section {ct.value}",
                "queries": [
                    {
                        "name": f"q_{ct.value.lower()}",
                        "sql": "SELECT 1 AS x",
                        "chart": ct.value,
                    }
                ],
            })
        sections = parse_sections(raw)
        assert len(sections) == len(ChartType)

    def test_empty_sections_raises(self):
        from seeknal.ask.report.deterministic import parse_sections

        with pytest.raises(ValueError, match="non-empty list"):
            parse_sections([])

    def test_non_list_raises(self):
        from seeknal.ask.report.deterministic import parse_sections

        with pytest.raises(ValueError, match="non-empty list"):
            parse_sections("not a list")

    def test_missing_title_raises(self):
        from seeknal.ask.report.deterministic import parse_sections

        with pytest.raises(ValueError, match="title"):
            parse_sections([{"queries": []}])

    def test_invalid_chart_type_raises(self):
        from seeknal.ask.report.deterministic import parse_sections

        with pytest.raises(ValueError, match="invalid chart type"):
            parse_sections([
                {
                    "title": "Bad",
                    "queries": [
                        {"name": "q1", "sql": "SELECT 1", "chart": "PieChart"}
                    ],
                }
            ])

    def test_duplicate_query_name_raises(self):
        from seeknal.ask.report.deterministic import parse_sections

        with pytest.raises(ValueError, match="Duplicate query name"):
            parse_sections([
                {
                    "title": "Section A",
                    "queries": [
                        {"name": "my_query", "sql": "SELECT 1", "chart": "DataTable"}
                    ],
                },
                {
                    "title": "Section B",
                    "queries": [
                        {"name": "my_query", "sql": "SELECT 2", "chart": "DataTable"}
                    ],
                },
            ])

    def test_missing_query_name_raises(self):
        from seeknal.ask.report.deterministic import parse_sections

        with pytest.raises(ValueError, match="name.*required"):
            parse_sections([
                {
                    "title": "Bad",
                    "queries": [{"sql": "SELECT 1", "chart": "DataTable"}],
                }
            ])

    def test_missing_chart_raises(self):
        from seeknal.ask.report.deterministic import parse_sections

        with pytest.raises(ValueError, match="chart.*required"):
            parse_sections([
                {
                    "title": "Bad",
                    "queries": [{"name": "q1", "sql": "SELECT 1"}],
                }
            ])

    def test_invalid_query_name_raises(self):
        from seeknal.ask.report.deterministic import parse_sections

        with pytest.raises(ValueError, match="alphanumeric"):
            parse_sections([
                {
                    "title": "Bad",
                    "queries": [
                        {"name": "my-query", "sql": "SELECT 1", "chart": "DataTable"}
                    ],
                }
            ])

    def test_dangerous_sql_raises(self):
        from seeknal.ask.report.deterministic import parse_sections

        with pytest.raises(ValueError):
            parse_sections([
                {
                    "title": "Bad",
                    "queries": [
                        {
                            "name": "q1",
                            "sql": "DROP TABLE users",
                            "chart": "DataTable",
                        }
                    ],
                }
            ])

    def test_passthrough_props_collected(self):
        from seeknal.ask.report.deterministic import parse_sections

        sections = parse_sections([
            {
                "title": "Chart",
                "queries": [
                    {
                        "name": "trend",
                        "sql": "SELECT 1 AS month, 100 AS revenue",
                        "chart": "LineChart",
                        "x": "month",
                        "y": "revenue",
                        "title": "Revenue Trend",
                    }
                ],
            }
        ])
        q = sections[0].queries[0]
        assert q.props["x"] == "month"
        assert q.props["y"] == "revenue"
        assert q.props["title"] == "Revenue Trend"

    def test_bigvalue_keeps_value_labels_in_props(self):
        from seeknal.ask.report.deterministic import parse_sections

        sections = parse_sections([
            {
                "title": "KPIs",
                "queries": [
                    {
                        "name": "kpis",
                        "sql": "SELECT 1 AS total, 2 AS avg_val",
                        "chart": "BigValue",
                        "value": ["total", "avg_val"],
                        "labels": ["Total", "Average"],
                    }
                ],
            }
        ])
        q = sections[0].queries[0]
        assert q.props["value"] == ["total", "avg_val"]
        assert q.props["labels"] == ["Total", "Average"]

    def test_sql_trailing_semicolon_stripped(self):
        from seeknal.ask.report.deterministic import parse_sections

        sections = parse_sections([
            {
                "title": "Test",
                "queries": [
                    {"name": "q1", "sql": "SELECT 1 AS x ;  ", "chart": "DataTable"}
                ],
            }
        ])
        assert not sections[0].queries[0].sql.endswith(";")


# ---------------------------------------------------------------------------
# generate_evidence_page() — Markdown Generation
# ---------------------------------------------------------------------------


class TestGenerateEvidencePage:
    """Test Evidence markdown generation."""

    def _make_section(self, **overrides):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
        )

        defaults = {
            "title": "Test Section",
            "queries": [
                QueryConfig(
                    name="test_query",
                    sql="SELECT 1 AS x",
                    chart=ChartType.DATA_TABLE,
                    props={},
                )
            ],
            "narrative": False,
            "description": "",
        }
        defaults.update(overrides)
        return SectionConfig(**defaults)

    def test_basic_structure(self):
        from seeknal.ask.report.deterministic import generate_evidence_page

        section = self._make_section()
        page = generate_evidence_page([section], title="My Report")

        assert "# My Report" in page
        assert "## Test Section" in page
        assert "```sql test_query" in page
        assert "SELECT 1 AS x" in page
        assert "<DataTable data={test_query} />" in page

    def test_no_title(self):
        from seeknal.ask.report.deterministic import generate_evidence_page

        page = generate_evidence_page([self._make_section()])
        # Should not have a top-level H1 header (only H2 section headers)
        lines = page.strip().splitlines()
        assert not any(line.startswith("# ") and not line.startswith("## ") for line in lines)
        assert "## Test Section" in page

    def test_section_description(self):
        from seeknal.ask.report.deterministic import generate_evidence_page

        section = self._make_section(description="Important metrics.")
        page = generate_evidence_page([section])
        assert "Important metrics." in page

    def test_narrative_slot_inserted(self):
        from seeknal.ask.report.deterministic import generate_evidence_page

        section = self._make_section(narrative=True)
        page = generate_evidence_page([section])
        assert "<!-- NARRATIVE_SLOT_0 -->" in page

    def test_no_narrative_slot_when_false(self):
        from seeknal.ask.report.deterministic import generate_evidence_page

        section = self._make_section(narrative=False)
        page = generate_evidence_page([section])
        assert "NARRATIVE_SLOT" not in page

    def test_multiple_sections_indexed(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_evidence_page,
        )

        sections = [
            SectionConfig(
                title="A",
                queries=[QueryConfig(name="q1", sql="SELECT 1", chart=ChartType.DATA_TABLE)],
                narrative=True,
            ),
            SectionConfig(title="B", narrative=False),
            SectionConfig(title="C", narrative=True),
        ]
        page = generate_evidence_page(sections)
        assert "<!-- NARRATIVE_SLOT_0 -->" in page
        assert "<!-- NARRATIVE_SLOT_1 -->" not in page
        assert "<!-- NARRATIVE_SLOT_2 -->" in page

    def test_barchart_generation(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_evidence_page,
        )

        section = SectionConfig(
            title="Revenue",
            queries=[
                QueryConfig(
                    name="rev",
                    sql="SELECT 'A' AS seg, 100 AS val",
                    chart=ChartType.BAR_CHART,
                    props={"x": "seg", "y": "val", "title": "Revenue by Segment"},
                )
            ],
        )
        page = generate_evidence_page([section])
        assert '<BarChart data={rev} x="seg" y="val" title="Revenue by Segment" />' in page

    def test_linechart_generation(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_evidence_page,
        )

        section = SectionConfig(
            title="Trend",
            queries=[
                QueryConfig(
                    name="trend",
                    sql="SELECT 1 AS month, 10 AS revenue",
                    chart=ChartType.LINE_CHART,
                    props={"x": "month", "y": "revenue"},
                )
            ],
        )
        page = generate_evidence_page([section])
        assert '<LineChart data={trend} x="month" y="revenue" />' in page

    def test_areachart_generation(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_evidence_page,
        )

        section = SectionConfig(
            title="Area",
            queries=[
                QueryConfig(
                    name="area_q",
                    sql="SELECT 1 AS t, 5 AS v",
                    chart=ChartType.AREA_CHART,
                    props={"x": "t", "y": "v"},
                )
            ],
        )
        page = generate_evidence_page([section])
        assert '<AreaChart data={area_q} x="t" y="v" />' in page

    def test_scatterplot_generation(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_evidence_page,
        )

        section = SectionConfig(
            title="Scatter",
            queries=[
                QueryConfig(
                    name="scatter_q",
                    sql="SELECT 1 AS x, 2 AS y",
                    chart=ChartType.SCATTER_PLOT,
                    props={"x": "x", "y": "y"},
                )
            ],
        )
        page = generate_evidence_page([section])
        assert '<ScatterPlot data={scatter_q} x="x" y="y" />' in page

    def test_histogram_generation(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_evidence_page,
        )

        section = SectionConfig(
            title="Hist",
            queries=[
                QueryConfig(
                    name="hist_q",
                    sql="SELECT 1 AS val",
                    chart=ChartType.HISTOGRAM,
                    props={"x": "val"},
                )
            ],
        )
        page = generate_evidence_page([section])
        assert '<Histogram data={hist_q} x="val" />' in page

    def test_funnelchart_generation(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_evidence_page,
        )

        section = SectionConfig(
            title="Funnel",
            queries=[
                QueryConfig(
                    name="funnel_q",
                    sql="SELECT 'step1' AS name, 100 AS value",
                    chart=ChartType.FUNNEL_CHART,
                    props={"name": "name", "value": "value"},
                )
            ],
        )
        page = generate_evidence_page([section])
        assert "FunnelChart" in page
        assert "data={funnel_q}" in page

    def test_bigvalue_expansion(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_evidence_page,
        )

        section = SectionConfig(
            title="KPIs",
            queries=[
                QueryConfig(
                    name="kpis",
                    sql="SELECT 100 AS total, 50.5 AS avg_val",
                    chart=ChartType.BIG_VALUE,
                    props={
                        "value": ["total", "avg_val"],
                        "labels": ["Total", "Average"],
                    },
                )
            ],
        )
        page = generate_evidence_page([section])
        assert '<BigValue data={kpis} value="total" label="Total" />' in page
        assert '<BigValue data={kpis} value="avg_val" label="Average" />' in page

    def test_bigvalue_no_values(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_evidence_page,
        )

        section = SectionConfig(
            title="KPIs",
            queries=[
                QueryConfig(
                    name="kpis",
                    sql="SELECT 1",
                    chart=ChartType.BIG_VALUE,
                    props={},
                )
            ],
        )
        page = generate_evidence_page([section])
        assert "<BigValue data={kpis} />" in page

    def test_query_without_sql_skips_block(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_evidence_page,
        )

        section = SectionConfig(
            title="Shared",
            queries=[
                QueryConfig(name="shared_q", sql="", chart=ChartType.DATA_TABLE)
            ],
        )
        page = generate_evidence_page([section])
        assert "```sql" not in page
        assert "<DataTable data={shared_q} />" in page


# ---------------------------------------------------------------------------
# format_results_as_markdown()
# ---------------------------------------------------------------------------


class TestFormatResultsAsMarkdown:
    """Test query result formatting for LLM context."""

    def test_basic_table(self):
        from seeknal.ask.report.deterministic import format_results_as_markdown

        results = {
            "q1": {
                "columns": ["name", "value"],
                "rows": [["Alice", 100], ["Bob", 200]],
                "error": None,
            }
        }
        md = format_results_as_markdown(results, ["q1"])
        assert "| name | value |" in md
        assert "| Alice | 100 |" in md
        assert "| Bob | 200 |" in md
        assert "2 rows" in md

    def test_error_result(self):
        from seeknal.ask.report.deterministic import format_results_as_markdown

        results = {
            "q1": {"columns": [], "rows": [], "error": "Table not found"}
        }
        md = format_results_as_markdown(results, ["q1"])
        assert "Error" in md
        assert "Table not found" in md

    def test_empty_result(self):
        from seeknal.ask.report.deterministic import format_results_as_markdown

        results = {
            "q1": {"columns": [], "rows": [], "error": None}
        }
        md = format_results_as_markdown(results, ["q1"])
        assert "No results" in md

    def test_null_values(self):
        from seeknal.ask.report.deterministic import format_results_as_markdown

        results = {
            "q1": {
                "columns": ["a", "b"],
                "rows": [[1, None], [None, 2]],
                "error": None,
            }
        }
        md = format_results_as_markdown(results, ["q1"])
        assert "NULL" in md

    def test_missing_query_skipped(self):
        from seeknal.ask.report.deterministic import format_results_as_markdown

        md = format_results_as_markdown({}, ["nonexistent"])
        assert md == ""

    def test_multiple_queries(self):
        from seeknal.ask.report.deterministic import format_results_as_markdown

        results = {
            "q1": {
                "columns": ["x"],
                "rows": [[1]],
                "error": None,
            },
            "q2": {
                "columns": ["y"],
                "rows": [[2]],
                "error": None,
            },
        }
        md = format_results_as_markdown(results, ["q1", "q2"])
        assert "**q1**" in md
        assert "**q2**" in md


# ---------------------------------------------------------------------------
# _sanitize_narrative()
# ---------------------------------------------------------------------------


class TestSanitizeNarrative:
    """Test LLM output sanitization."""

    def test_strips_sql_blocks(self):
        from seeknal.ask.report.deterministic import _sanitize_narrative

        text = "Insight here.\n```sql\nSELECT 1\n```\nMore text."
        result = _sanitize_narrative(text)
        assert "SELECT 1" not in result
        assert "Insight here." in result
        assert "More text." in result

    def test_strips_evidence_components(self):
        from seeknal.ask.report.deterministic import _sanitize_narrative

        text = 'Revenue grew. <BarChart data={rev} x="seg" y="val" /> End.'
        result = _sanitize_narrative(text)
        assert "BarChart" not in result
        assert "Revenue grew." in result

    def test_collapses_blank_lines(self):
        from seeknal.ask.report.deterministic import _sanitize_narrative

        text = "Line 1.\n\n\n\n\nLine 2."
        result = _sanitize_narrative(text)
        assert "\n\n\n" not in result

    def test_clean_text_unchanged(self):
        from seeknal.ask.report.deterministic import _sanitize_narrative

        text = "Revenue grew 15% YoY. **Premium** segment leads."
        result = _sanitize_narrative(text)
        assert result == text


# ---------------------------------------------------------------------------
# assemble_page()
# ---------------------------------------------------------------------------


class TestAssemblePage:
    """Test narrative slot replacement."""

    def test_replaces_placeholders(self):
        from seeknal.ask.report.deterministic import assemble_page

        skeleton = (
            "## Section A\n"
            "<!-- NARRATIVE_SLOT_0 -->\n"
            "## Section B\n"
            "<!-- NARRATIVE_SLOT_1 -->\n"
        )
        narratives = {
            0: "Revenue is strong.",
            1: "Customer growth accelerated.",
        }
        result = assemble_page(skeleton, narratives)
        assert "Revenue is strong." in result
        assert "Customer growth accelerated." in result
        assert "NARRATIVE_SLOT" not in result

    def test_unreplaced_slots_removed(self):
        from seeknal.ask.report.deterministic import assemble_page

        skeleton = "## A\n<!-- NARRATIVE_SLOT_0 -->\n## B\n<!-- NARRATIVE_SLOT_1 -->"
        # Only section 0 gets a narrative
        result = assemble_page(skeleton, {0: "Insight."})
        assert "Insight." in result
        assert "NARRATIVE_SLOT" not in result

    def test_empty_narratives(self):
        from seeknal.ask.report.deterministic import assemble_page

        skeleton = "## A\nContent here."
        result = assemble_page(skeleton, {})
        assert result == skeleton


# ---------------------------------------------------------------------------
# generate_readable_markdown() — standalone readable output
# ---------------------------------------------------------------------------


class TestGenerateReadableMarkdown:
    """Test standalone readable markdown generation."""

    def test_bigvalue_renders_as_table(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_readable_markdown,
        )

        sections = [
            SectionConfig(
                title="KPIs",
                queries=[
                    QueryConfig(
                        name="kpis",
                        sql="SELECT 1",
                        chart=ChartType.BIG_VALUE,
                        props={
                            "value": ["total", "avg"],
                            "labels": ["Total Revenue", "Average"],
                        },
                    )
                ],
            )
        ]
        results = {
            "kpis": {
                "columns": ["total", "avg"],
                "rows": [[56836.08, 447.53]],
                "error": None,
            }
        }
        md = generate_readable_markdown(sections, results, {}, "Report")
        assert "| **Total Revenue** | **56,836.08** |" in md
        assert "| **Average** | **447.53** |" in md
        assert "<BigValue" not in md

    def test_barchart_renders_as_ascii(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_readable_markdown,
        )

        sections = [
            SectionConfig(
                title="Revenue",
                queries=[
                    QueryConfig(
                        name="rev",
                        sql="SELECT 1",
                        chart=ChartType.BAR_CHART,
                        props={"x": "seg", "y": "val", "title": "By Segment"},
                    )
                ],
            )
        ]
        results = {
            "rev": {
                "columns": ["seg", "val"],
                "rows": [["Premium", 500], ["Basic", 200]],
                "error": None,
            }
        }
        md = generate_readable_markdown(sections, results, {})
        assert "**By Segment**" in md
        assert "Premium" in md
        assert "█" in md
        assert "<BarChart" not in md

    def test_linechart_renders_as_ascii(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_readable_markdown,
        )

        sections = [
            SectionConfig(
                title="Trend",
                queries=[
                    QueryConfig(
                        name="trend",
                        sql="SELECT 1",
                        chart=ChartType.LINE_CHART,
                        props={"x": "month", "y": "rev", "title": "Monthly"},
                    )
                ],
            )
        ]
        results = {
            "trend": {
                "columns": ["month", "rev"],
                "rows": [["Jan", 100], ["Feb", 200], ["Mar", 150]],
                "error": None,
            }
        }
        md = generate_readable_markdown(sections, results, {})
        assert "**Monthly**" in md
        assert "●" in md
        assert "<LineChart" not in md

    def test_datatable_renders_as_markdown_table(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_readable_markdown,
        )

        sections = [
            SectionConfig(
                title="Detail",
                queries=[
                    QueryConfig(
                        name="detail",
                        sql="SELECT 1",
                        chart=ChartType.DATA_TABLE,
                        props={},
                    )
                ],
            )
        ]
        results = {
            "detail": {
                "columns": ["name", "value"],
                "rows": [["Alice", 100], ["Bob", 200]],
                "error": None,
            }
        }
        md = generate_readable_markdown(sections, results, {})
        assert "| name | value |" in md
        assert "| Alice | 100 |" in md
        assert "<DataTable" not in md

    def test_narratives_included(self):
        from seeknal.ask.report.deterministic import (
            SectionConfig,
            generate_readable_markdown,
        )

        sections = [
            SectionConfig(title="Analysis", narrative=True),
        ]
        narratives = {0: "Revenue grew **15%** year-over-year."}
        md = generate_readable_markdown(sections, {}, narratives)
        assert "Revenue grew **15%** year-over-year." in md

    def test_error_result_shown(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_readable_markdown,
        )

        sections = [
            SectionConfig(
                title="Bad",
                queries=[
                    QueryConfig(name="bad_q", sql="SELECT 1", chart=ChartType.DATA_TABLE)
                ],
            )
        ]
        results = {
            "bad_q": {"columns": [], "rows": [], "error": "Table not found"}
        }
        md = generate_readable_markdown(sections, results, {})
        assert "Table not found" in md

    def test_no_evidence_syntax(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_readable_markdown,
        )

        sections = [
            SectionConfig(
                title="Mixed",
                queries=[
                    QueryConfig(
                        name="q1", sql="SELECT 1", chart=ChartType.BIG_VALUE,
                        props={"value": ["x"], "labels": ["X"]},
                    ),
                    QueryConfig(
                        name="q2", sql="SELECT 1", chart=ChartType.BAR_CHART,
                        props={"x": "a", "y": "b"},
                    ),
                    QueryConfig(
                        name="q3", sql="SELECT 1", chart=ChartType.DATA_TABLE,
                    ),
                ],
                narrative=True,
            )
        ]
        results = {
            "q1": {"columns": ["x"], "rows": [[42]], "error": None},
            "q2": {"columns": ["a", "b"], "rows": [["A", 10]], "error": None},
            "q3": {"columns": ["c"], "rows": [["v"]], "error": None},
        }
        md = generate_readable_markdown(sections, results, {0: "Insight."})
        # No Evidence syntax anywhere
        assert "```sql" not in md
        assert "<BigValue" not in md
        assert "<BarChart" not in md
        assert "<DataTable" not in md
        assert "<LineChart" not in md


# ---------------------------------------------------------------------------
# execute_section_queries() — with real DuckDB
# ---------------------------------------------------------------------------


class TestExecuteSectionQueries:
    """Test query execution against a temporary DuckDB."""

    def test_executes_simple_query(self, tmp_path):
        pytest.importorskip("duckdb")
        pytest.importorskip("pandas")
        import pandas as pd

        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            execute_section_queries,
        )

        # Create a parquet file so the data bridge has something to register
        intermediate = tmp_path / "target" / "intermediate"
        intermediate.mkdir(parents=True)
        df = pd.DataFrame({"customer_id": [1, 2, 3], "revenue": [100, 200, 300]})
        df.to_parquet(intermediate / "transform_orders.parquet")

        sections = [
            SectionConfig(
                title="Test",
                queries=[
                    QueryConfig(
                        name="total",
                        sql="SELECT CAST(SUM(revenue) AS BIGINT) AS total_revenue FROM transform_orders",
                        chart=ChartType.BIG_VALUE,
                    )
                ],
            )
        ]
        results = execute_section_queries(sections, tmp_path)
        assert "total" in results
        assert results["total"]["error"] is None
        assert results["total"]["columns"] == ["total_revenue"]
        assert results["total"]["rows"][0][0] == 600

    def test_invalid_sql_returns_error(self, tmp_path):
        pytest.importorskip("duckdb")
        pytest.importorskip("pandas")
        import pandas as pd

        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            execute_section_queries,
        )

        # Need at least one parquet so DuckDB file is valid
        intermediate = tmp_path / "target" / "intermediate"
        intermediate.mkdir(parents=True)
        pd.DataFrame({"x": [1]}).to_parquet(intermediate / "dummy.parquet")

        sections = [
            SectionConfig(
                title="Bad",
                queries=[
                    QueryConfig(
                        name="bad_query",
                        sql="SELECT * FROM nonexistent_table_xyz",
                        chart=ChartType.DATA_TABLE,
                    )
                ],
            )
        ]
        results = execute_section_queries(sections, tmp_path)
        assert "bad_query" in results
        assert results["bad_query"]["error"] is not None

    def test_skips_queries_without_sql(self, tmp_path):
        pytest.importorskip("duckdb")

        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            execute_section_queries,
        )

        # No parquets needed — no SQL to execute
        sections = [
            SectionConfig(
                title="Empty",
                queries=[
                    QueryConfig(name="no_sql", sql="", chart=ChartType.DATA_TABLE)
                ],
            )
        ]
        results = execute_section_queries(sections, tmp_path)
        assert "no_sql" not in results


# ---------------------------------------------------------------------------
# generate_narratives() — with mocked LLM
# ---------------------------------------------------------------------------


class TestGenerateNarratives:
    """Test LLM narrative generation with mocks."""

    def test_generates_narrative_for_true_sections(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_narratives,
        )

        sections = [
            SectionConfig(
                title="Revenue",
                queries=[
                    QueryConfig(name="rev", sql="SELECT 1", chart=ChartType.BAR_CHART)
                ],
                narrative=True,
            ),
            SectionConfig(
                title="No Narrative",
                queries=[
                    QueryConfig(name="q2", sql="SELECT 2", chart=ChartType.DATA_TABLE)
                ],
                narrative=False,
            ),
        ]
        query_results = {
            "rev": {
                "columns": ["segment", "revenue"],
                "rows": [["Premium", 5000], ["Basic", 2000]],
                "error": None,
            },
        }

        mock_response = MagicMock()
        mock_response.content = "Premium segment leads with $5,000 in revenue."

        with patch("seeknal.ask.agents.providers.get_llm") as mock_get_llm:
            mock_llm = MagicMock()
            mock_llm.invoke.return_value = mock_response
            mock_get_llm.return_value = mock_llm

            narratives = generate_narratives(sections, query_results)

        assert 0 in narratives
        assert "Premium" in narratives[0]
        assert 1 not in narratives

    def test_narrative_only_section_gets_all_prior_results(self):
        from seeknal.ask.report.deterministic import (
            ChartType,
            QueryConfig,
            SectionConfig,
            generate_narratives,
        )

        sections = [
            SectionConfig(
                title="Data",
                queries=[
                    QueryConfig(name="data_q", sql="SELECT 1", chart=ChartType.DATA_TABLE)
                ],
                narrative=False,
            ),
            SectionConfig(
                title="Recommendations",
                narrative=True,
            ),
        ]
        query_results = {
            "data_q": {
                "columns": ["val"],
                "rows": [[42]],
                "error": None,
            },
        }

        mock_response = MagicMock()
        mock_response.content = "Based on the data, we recommend action."

        with patch("seeknal.ask.agents.providers.get_llm") as mock_get_llm:
            mock_llm = MagicMock()
            mock_llm.invoke.return_value = mock_response
            mock_get_llm.return_value = mock_llm

            narratives = generate_narratives(sections, query_results)

            # The LLM should have been called with data_q results as context
            call_args = mock_llm.invoke.call_args[0][0]
            assert "data_q" in call_args or "42" in call_args

        assert 1 in narratives

    def test_llm_unavailable_returns_placeholder(self):
        from seeknal.ask.report.deterministic import SectionConfig, generate_narratives

        sections = [SectionConfig(title="Test", narrative=True)]

        with patch(
            "seeknal.ask.agents.providers.get_llm",
            side_effect=ImportError("no provider"),
        ):
            narratives = generate_narratives(sections, {})

        assert 0 in narratives
        assert "unavailable" in narratives[0].lower()

    def test_llm_error_returns_placeholder(self):
        from seeknal.ask.report.deterministic import SectionConfig, generate_narratives

        sections = [SectionConfig(title="Test", narrative=True)]

        with patch("seeknal.ask.agents.providers.get_llm") as mock_get_llm:
            mock_llm = MagicMock()
            mock_llm.invoke.side_effect = RuntimeError("API error")
            mock_get_llm.return_value = mock_llm

            narratives = generate_narratives(sections, {})

        assert 0 in narratives
        assert "unavailable" in narratives[0].lower()

    def test_no_narrative_sections_returns_empty(self):
        from seeknal.ask.report.deterministic import SectionConfig, generate_narratives

        sections = [SectionConfig(title="Test", narrative=False)]
        narratives = generate_narratives(sections, {})
        assert narratives == {}

    def test_narrative_sanitized(self):
        from seeknal.ask.report.deterministic import (
            SectionConfig,
            generate_narratives,
        )

        sections = [SectionConfig(title="Test", narrative=True)]

        mock_response = MagicMock()
        mock_response.content = (
            "Great insight.\n```sql\nSELECT 1\n```\n"
            '<BarChart data={x} />\nMore text.'
        )

        with patch("seeknal.ask.agents.providers.get_llm") as mock_get_llm:
            mock_llm = MagicMock()
            mock_llm.invoke.return_value = mock_response
            mock_get_llm.return_value = mock_llm

            narratives = generate_narratives(sections, {})

        assert "SELECT 1" not in narratives[0]
        assert "BarChart" not in narratives[0]
        assert "Great insight." in narratives[0]


# ---------------------------------------------------------------------------
# Exposure validation — sections make prompt optional
# ---------------------------------------------------------------------------


class TestExposureValidationWithSections:
    """Test that exposure validation relaxes prompt requirement for sections."""

    def _write_exposure(self, project_path, name, data):
        import yaml

        exposures_dir = project_path / "seeknal" / "exposures"
        exposures_dir.mkdir(parents=True, exist_ok=True)
        path = exposures_dir / f"{name}.yml"
        path.write_text(yaml.safe_dump(data, sort_keys=False))
        return path

    def test_sections_without_prompt_valid(self, tmp_path):
        from seeknal.ask.report.exposure import load_report_exposure

        data = {
            "kind": "exposure",
            "name": "det_report",
            "type": "report",
            "params": {"format": "markdown"},
            "sections": [
                {
                    "title": "Summary",
                    "queries": [
                        {"name": "q1", "sql": "SELECT 1", "chart": "DataTable"}
                    ],
                }
            ],
        }
        self._write_exposure(tmp_path, "det_report", data)
        result = load_report_exposure(tmp_path, "det_report")
        assert result["name"] == "det_report"
        assert "sections" in result

    def test_sections_with_prompt_valid(self, tmp_path):
        from seeknal.ask.report.exposure import load_report_exposure

        data = {
            "kind": "exposure",
            "name": "det_prompt",
            "type": "report",
            "params": {"prompt": "Analyze revenue.", "format": "markdown"},
            "sections": [
                {"title": "Summary", "narrative": True}
            ],
        }
        self._write_exposure(tmp_path, "det_prompt", data)
        result = load_report_exposure(tmp_path, "det_prompt")
        assert result["params"]["prompt"] == "Analyze revenue."

    def test_no_sections_no_prompt_raises(self, tmp_path):
        from seeknal.ask.report.exposure import load_report_exposure

        data = {
            "kind": "exposure",
            "name": "bad",
            "type": "report",
            "params": {},
        }
        self._write_exposure(tmp_path, "bad", data)
        with pytest.raises(ValueError, match="prompt"):
            load_report_exposure(tmp_path, "bad")


# ---------------------------------------------------------------------------
# render_deterministic_report() — integration (mocked build)
# ---------------------------------------------------------------------------


class TestRenderDeterministicReport:
    """Test the main entry point with mocked scaffold/build."""

    def test_full_pipeline_no_narrative(self, tmp_path):
        pytest.importorskip("duckdb")
        pytest.importorskip("pandas")
        import pandas as pd

        from seeknal.ask.report.deterministic import render_deterministic_report

        # Create parquet data
        intermediate = tmp_path / "target" / "intermediate"
        intermediate.mkdir(parents=True)
        df = pd.DataFrame({"seg": ["A", "B"], "rev": [100, 200]})
        df.to_parquet(intermediate / "transform_segments.parquet")

        exposure = {
            "name": "test_report",
            "description": "Segment Report",
            "params": {},
            "sections": [
                {
                    "title": "Segments",
                    "queries": [
                        {
                            "name": "seg_data",
                            "sql": "SELECT seg, rev FROM transform_segments",
                            "chart": "DataTable",
                        }
                    ],
                    "narrative": False,
                }
            ],
        }

        with (
            patch("seeknal.ask.report.scaffolder.scaffold_report") as mock_scaffold,
            patch("seeknal.ask.report.builder.build_report") as mock_build,
        ):
            mock_scaffold.return_value = tmp_path / "target" / "reports" / "test"
            mock_build.return_value = "/path/to/report.html"

            html_path, markdown = render_deterministic_report(exposure, tmp_path)

        assert html_path == "/path/to/report.html"
        assert "## Segments" in markdown
        # Readable markdown should contain actual data, not Evidence syntax
        assert "| seg | rev |" in markdown
        assert "| A | 100 |" in markdown
        # No Evidence syntax in saved markdown
        assert "<DataTable" not in markdown
        assert "```sql" not in markdown

        # Verify scaffold was called with the Evidence page (not readable md)
        pages_arg = mock_scaffold.call_args[1]["pages"]
        assert len(pages_arg) == 1
        assert pages_arg[0]["name"] == "index"
        # Evidence page should still have Evidence syntax
        assert "<DataTable" in pages_arg[0]["content"]

    def test_full_pipeline_with_narrative(self, tmp_path):
        pytest.importorskip("duckdb")
        pytest.importorskip("pandas")
        import pandas as pd

        from seeknal.ask.report.deterministic import render_deterministic_report

        intermediate = tmp_path / "target" / "intermediate"
        intermediate.mkdir(parents=True)
        df = pd.DataFrame({"metric": ["users"], "count": [500]})
        df.to_parquet(intermediate / "transform_metrics.parquet")

        exposure = {
            "name": "narrative_report",
            "description": "Metrics Report",
            "params": {"prompt": "Focus on growth."},
            "sections": [
                {
                    "title": "Metrics",
                    "queries": [
                        {
                            "name": "metrics_q",
                            "sql": "SELECT metric, count FROM transform_metrics",
                            "chart": "DataTable",
                        }
                    ],
                    "narrative": True,
                }
            ],
        }

        mock_response = MagicMock()
        mock_response.content = "User count stands at 500, showing strong growth."

        with (
            patch("seeknal.ask.report.scaffolder.scaffold_report") as mock_scaffold,
            patch("seeknal.ask.report.builder.build_report") as mock_build,
            patch("seeknal.ask.agents.providers.get_llm") as mock_get_llm,
        ):
            mock_scaffold.return_value = tmp_path / "target" / "reports" / "test"
            mock_build.return_value = "/path/to/report.html"
            mock_llm = MagicMock()
            mock_llm.invoke.return_value = mock_response
            mock_get_llm.return_value = mock_llm

            html_path, markdown = render_deterministic_report(exposure, tmp_path)

        assert "User count stands at 500" in markdown
        assert "NARRATIVE_SLOT" not in markdown
