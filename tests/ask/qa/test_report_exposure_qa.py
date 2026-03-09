"""QA tests for Report Exposure system — exercises Mode 1, Mode 2, and security.

Derived from qa/specs/ask-report-exposure-*.yml specs.
Uses real CSV data through a medallion pipeline to validate the full flow:
  1. Pipeline produces parquet files
  2. save_rendered_markdown writes dated markdown
  3. save_report_exposure tool generates valid YAML
  4. load_report_exposure reads back the YAML
  5. resolve_prompt resolves Jinja2 variables against real DuckDB data
"""

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
import yaml


# ---------------------------------------------------------------------------
# Fixtures: build a real seeknal project with parquet data
# ---------------------------------------------------------------------------


@pytest.fixture
def qa_project(tmp_path):
    """Create a realistic seeknal project with CSV data and parquet outputs.

    Mirrors the csv-medallion QA spec: orders + customers → clean_orders → customer_ltv.
    """
    # Seed CSV data
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    orders_df = pd.DataFrame({
        "order_id": [f"ORD{i:03d}" for i in range(1, 11)],
        "customer_id": ["C001", "C002", "C001", "C003", "C002",
                        "C001", "C004", "C003", "C001", "C005"],
        "amount": [150.0, 75.5, 200.0, 50.0, 300.0,
                   125.75, 89.99, 175.0, 60.0, 220.0],
        "order_date": pd.date_range("2026-01-15", periods=10, freq="D"),
        "status": ["completed", "completed", "pending", "completed", "cancelled",
                   "completed", "completed", "pending", "completed", "completed"],
    })

    customers_df = pd.DataFrame({
        "customer_id": ["C001", "C002", "C003", "C004", "C005"],
        "name": ["Alice Johnson", "Bob Smith", "Charlie Brown",
                 "Diana Prince", "Eve Wilson"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com",
                  "diana@example.com", "eve@example.com"],
        "signup_date": pd.to_datetime(["2025-06-01", "2025-07-15", "2025-08-20",
                                       "2025-09-10", "2025-10-05"]),
        "tier": ["gold", "silver", "bronze", "gold", "silver"],
    })

    # Write intermediate parquets (simulating seeknal run output)
    intermediate = tmp_path / "target" / "intermediate"
    intermediate.mkdir(parents=True)

    orders_df.to_parquet(intermediate / "source_raw_orders.parquet", index=False)
    customers_df.to_parquet(intermediate / "source_raw_customers.parquet", index=False)

    # Clean orders (completed only)
    clean = orders_df[orders_df["status"] == "completed"].copy()
    clean.to_parquet(intermediate / "transform_clean_orders.parquet", index=False)

    # Customer LTV (join + aggregate)
    merged = clean.merge(customers_df, on="customer_id")
    ltv = merged.groupby(["customer_id", "name", "tier"]).agg(
        total_orders=("order_id", "count"),
        lifetime_value=("amount", "sum"),
        first_order=("order_date", "min"),
        last_order=("order_date", "max"),
    ).reset_index()
    ltv.to_parquet(intermediate / "transform_customer_ltv.parquet", index=False)

    # Create seeknal directory structure
    (tmp_path / "seeknal").mkdir()

    return tmp_path


# ---------------------------------------------------------------------------
# Mode 1 Tests: Interactive → Markdown + YAML codification
# ---------------------------------------------------------------------------


class TestMode1SaveMarkdown:
    """QA spec: ask-report-exposure-mode1 / test_save_markdown"""

    def test_saves_to_correct_path(self, qa_project):
        from seeknal.ask.report.exposure import save_rendered_markdown

        content = (
            "# Regional Revenue Report\n\n"
            "## Summary\n"
            "Analysis of revenue by region for Q1 2026.\n\n"
            "| Region | Revenue |\n"
            "|--------|---------|\n"
            "| North  | $749.96 |\n"
        )

        result = save_rendered_markdown(qa_project, "Regional Revenue Analysis", content)

        # Validate: file exists at expected path
        assert result.exists()
        assert "target/reported/regional-revenue-analysis" in str(result)
        assert result.name == f"{date.today().isoformat()}.md"

        # Validate: content preserved
        saved = result.read_text()
        assert "Regional Revenue Report" in saved
        assert "$749.96" in saved

    def test_slug_generation(self, qa_project):
        from seeknal.ask.report.exposure import save_rendered_markdown

        result = save_rendered_markdown(qa_project, "My Complex Report!!!", "content")
        assert "my-complex-report" in str(result)

    def test_rejects_empty_content(self, qa_project):
        from seeknal.ask.report.exposure import save_rendered_markdown

        with pytest.raises(ValueError, match="empty"):
            save_rendered_markdown(qa_project, "empty", "")


class TestMode1SaveExposureYaml:
    """QA spec: ask-report-exposure-mode1 / test_save_exposure_yaml"""

    def test_generates_valid_yaml(self, qa_project):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        result = _do_save(
            name="regional_revenue_report",
            prompt="Analyze revenue by region, showing top performers and trends",
            inputs_json='["transform.sales_enriched", "transform.regional_revenue"]',
            output_format="markdown",
            project_path=qa_project,
        )

        # Validate: success message
        assert "saved" in result.lower()

        # Validate: YAML file exists with correct schema
        yml_path = qa_project / "seeknal" / "exposures" / "regional_revenue_report.yml"
        assert yml_path.exists()

        data = yaml.safe_load(yml_path.read_text())
        assert data["kind"] == "exposure"
        assert data["type"] == "report"
        assert data["name"] == "regional_revenue_report"
        assert "revenue by region" in data["params"]["prompt"]
        assert data["params"]["format"] == "markdown"
        assert len(data["inputs"]) == 2
        assert data["inputs"][0]["ref"] == "transform.sales_enriched"

    def test_rejects_invalid_name(self, qa_project):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        result = _do_save(
            name="Invalid-Name!",
            prompt="test",
            inputs_json="[]",
            output_format="markdown",
            project_path=qa_project,
        )
        assert "Error" in result
        assert "snake_case" in result.lower()


class TestMode1Roundtrip:
    """QA spec: ask-report-exposure-mode1 / test_load_saved_exposure"""

    def test_save_then_load(self, qa_project):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save
        from seeknal.ask.report.exposure import load_report_exposure

        # Save
        _do_save(
            name="roundtrip_test",
            prompt="Analyze customer behavior and revenue trends",
            inputs_json='["transform.customer_ltv"]',
            output_format="both",
            project_path=qa_project,
        )

        # Load
        result = load_report_exposure(qa_project, "roundtrip_test")

        assert result["kind"] == "exposure"
        assert result["type"] == "report"
        assert "customer behavior" in result["params"]["prompt"]
        assert result["params"]["format"] == "both"
        assert result["inputs"][0]["ref"] == "transform.customer_ltv"


# ---------------------------------------------------------------------------
# Mode 2 Tests: YAML spec → template resolution → execution
# ---------------------------------------------------------------------------


class TestMode2LoadExposure:
    """QA spec: ask-report-exposure-mode2 / test_load_exposure"""

    def _write_exposure(self, project_path, name, data):
        exposures_dir = project_path / "seeknal" / "exposures"
        exposures_dir.mkdir(parents=True, exist_ok=True)
        path = exposures_dir / f"{name}.yml"
        path.write_text(yaml.safe_dump(data, sort_keys=False))

    def test_loads_valid_exposure(self, qa_project):
        from seeknal.ask.report.exposure import load_report_exposure

        self._write_exposure(qa_project, "monthly_ltv_report", {
            "kind": "exposure",
            "name": "monthly_ltv_report",
            "type": "report",
            "params": {
                "prompt": "Analyze customer lifetime value for period ending {{ run_date }}.",
                "format": "markdown",
            },
            "inputs": [{"ref": "transform.customer_ltv"}],
        })

        result = load_report_exposure(qa_project, "monthly_ltv_report")
        assert result["kind"] == "exposure"
        assert result["type"] == "report"
        assert "lifetime value" in result["params"]["prompt"]

    def test_rejects_invalid_format(self, qa_project):
        from seeknal.ask.report.exposure import load_report_exposure

        self._write_exposure(qa_project, "bad_format", {
            "kind": "exposure",
            "name": "bad_format",
            "type": "report",
            "params": {"prompt": "test", "format": "pdf"},
            "inputs": [],
        })

        with pytest.raises(ValueError, match="format"):
            load_report_exposure(qa_project, "bad_format")

    def test_rejects_missing_prompt(self, qa_project):
        from seeknal.ask.report.exposure import load_report_exposure

        self._write_exposure(qa_project, "no_prompt", {
            "kind": "exposure",
            "name": "no_prompt",
            "type": "report",
            "params": {"format": "markdown"},
            "inputs": [],
        })

        with pytest.raises(ValueError, match="prompt"):
            load_report_exposure(qa_project, "no_prompt")

    def test_not_found(self, qa_project):
        from seeknal.ask.report.exposure import load_report_exposure

        with pytest.raises(FileNotFoundError, match="nonexistent"):
            load_report_exposure(qa_project, "nonexistent_report")


class TestMode2ResolvePromptWithRealData:
    """QA spec: ask-report-exposure-mode2 / test_resolve_template_with_data

    This is the key integration test: resolve Jinja2 variables against
    real parquet files produced by the pipeline.
    """

    def test_resolves_all_variables(self, qa_project):
        from seeknal.ask.report.exposure import resolve_prompt

        template = (
            "Analyze LTV data for {{ run_date }}. "
            "Columns: {{ inputs.customer_ltv.columns }}. "
            "Row count: {{ inputs.customer_ltv.row_count }}. "
            "Focus: {{ params.focus_area }}."
        )

        result = resolve_prompt(
            template,
            qa_project,
            inputs=[{"ref": "transform.customer_ltv"}],
            params={"focus_area": "gold-tier"},
        )

        # run_date resolves to today
        assert date.today().isoformat() in result

        # columns from customer_ltv parquet
        assert "customer_id" in result
        assert "lifetime_value" in result
        assert "total_orders" in result

        # row_count = 5 customers with completed orders (C001-C005 all have >=1)
        assert "5" in result

        # params resolve
        assert "gold-tier" in result

    def test_missing_table_graceful(self, qa_project):
        from seeknal.ask.report.exposure import resolve_prompt

        result = resolve_prompt(
            "Columns: {{ inputs.nonexistent.columns }}",
            qa_project,
            inputs=[{"ref": "transform.nonexistent"}],
            params={},
        )
        assert "not found" in result.lower()

    def test_strict_undefined_raises(self, qa_project):
        from seeknal.ask.report.exposure import resolve_prompt

        with pytest.raises(Exception):
            resolve_prompt(
                "{{ undefined_variable }}",
                qa_project,
                inputs=[],
                params={},
            )

    def test_run_date_resolves(self, qa_project):
        from seeknal.ask.report.exposure import resolve_prompt

        result = resolve_prompt(
            "Report for {{ run_date }}",
            qa_project,
            inputs=[],
            params={},
        )
        assert date.today().isoformat() in result

    def test_multiple_inputs(self, qa_project):
        from seeknal.ask.report.exposure import resolve_prompt

        result = resolve_prompt(
            "Orders cols: {{ inputs.clean_orders.columns }}. "
            "LTV cols: {{ inputs.customer_ltv.columns }}.",
            qa_project,
            inputs=[
                {"ref": "transform.clean_orders"},
                {"ref": "transform.customer_ltv"},
            ],
            params={},
        )
        # Both tables' columns resolved
        assert "order_id" in result  # from clean_orders
        assert "lifetime_value" in result  # from customer_ltv


# ---------------------------------------------------------------------------
# Security Tests
# ---------------------------------------------------------------------------


class TestSecurityPathTraversal:
    """QA spec: ask-report-exposure-security"""

    def test_markdown_path_stays_under_target(self, qa_project):
        from seeknal.ask.report.exposure import save_rendered_markdown

        # Slugifier converts ../../etc/passwd → "etc-passwd"
        result = save_rendered_markdown(qa_project, "../../etc/passwd", "content")
        target_dir = (qa_project / "target").resolve()
        assert str(result.resolve()).startswith(str(target_dir))

    def test_exposure_rejects_traversal_name(self, qa_project):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        result = _do_save(
            name="../../../evil",
            prompt="test",
            inputs_json="[]",
            output_format="markdown",
            project_path=qa_project,
        )
        assert "Error" in result
        # No file should exist outside project
        assert not (qa_project / ".." / ".." / ".." / "evil.yml").exists()


class TestSecurityInputValidation:
    """QA spec: ask-report-exposure-security / input validation"""

    def test_invalid_json_inputs(self, qa_project):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        result = _do_save("test", "test", "{not valid json", "markdown", qa_project)
        assert "Error" in result
        assert "JSON" in result

    def test_non_array_inputs(self, qa_project):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        result = _do_save("test", "test", '{"key": "value"}', "markdown", qa_project)
        assert "Error" in result
        assert "array" in result

    def test_invalid_format(self, qa_project):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        result = _do_save("test", "test", "[]", "pdf", qa_project)
        assert "Error" in result

    def test_empty_prompt(self, qa_project):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        result = _do_save("test", "   ", "[]", "markdown", qa_project)
        assert "Error" in result


class TestSecurityOverwrite:
    """QA spec: ask-report-exposure-security / overwrite detection"""

    def test_overwrite_warns(self, qa_project):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        _do_save("overwrite_test", "v1", "[]", "markdown", qa_project)
        result = _do_save("overwrite_test", "v2", "[]", "markdown", qa_project)
        assert "overwriting" in result.lower()

        data = yaml.safe_load(
            (qa_project / "seeknal" / "exposures" / "overwrite_test.yml").read_text()
        )
        assert data["params"]["prompt"] == "v2"

    def test_all_formats_accepted(self, qa_project):
        from seeknal.ask.agents.tools.save_report_exposure import _do_save

        for fmt in ("markdown", "html", "both"):
            result = _do_save(f"fmt_{fmt}", "test", "[]", fmt, qa_project)
            assert "saved" in result.lower()
            assert (qa_project / "seeknal" / "exposures" / f"fmt_{fmt}.yml").exists()
