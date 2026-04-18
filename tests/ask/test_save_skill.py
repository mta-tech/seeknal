"""Tests for save_ingestion_skill tool."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.save_ingestion_skill import save_ingestion_skill


class _REPLStub:
    def __init__(self) -> None:
        self.conn = duckdb.connect(":memory:")


@pytest.fixture
def ctx(tmp_path: Path) -> ToolContext:
    context = ToolContext(
        repl=_REPLStub(),
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
    )
    set_tool_context(context)
    return context


def _cols_json() -> str:
    return json.dumps(
        [
            {"name": "invoice_id", "type": "BIGINT"},
            {"name": "date", "type": "DATE"},
            {"name": "amount", "type": "BIGINT"},
        ]
    )


def test_skill_md_generation(tmp_path, ctx):
    out = save_ingestion_skill(
        skill_name="ingest-acme-sales",
        table_name="acme_sales",
        business_key="invoice_id",
        columns_json=_cols_json(),
        description="Monthly ACME sales upload",
    )
    assert "Saved ingestion skill" in out

    skill = tmp_path / "seeknal/skills/ingest-acme-sales/SKILL.md"
    assert skill.exists()
    content = skill.read_text()
    # Frontmatter
    assert content.startswith("---\n")
    assert "name: ingest-acme-sales" in content
    assert 'business_key: ["invoice_id"]' in content
    # Schema table
    assert "| `invoice_id` | BIGINT |" in content
    # Phase structure mirrors report-generation canonical
    assert "## Phase 1" in content
    assert "## Critical-analyst rules" in content


def test_skill_discovery_via_resolve(tmp_path, ctx):
    save_ingestion_skill(
        skill_name="ingest-acme-sales",
        table_name="acme_sales",
        business_key="invoice_id",
        columns_json=_cols_json(),
    )

    from seeknal.ask.agents.agent import _resolve_skill_directories

    dirs = _resolve_skill_directories(tmp_path)
    # Local skills dir must be on the search path; saved SKILL.md must be under it.
    assert any(str(tmp_path / "seeknal" / "skills") in d for d in dirs)
    assert (tmp_path / "seeknal" / "skills" / "ingest-acme-sales" / "SKILL.md").exists()


def test_invalid_skill_name_rejected(ctx):
    out = save_ingestion_skill(
        skill_name="Bad Name!",
        table_name="acme_sales",
        business_key="invoice_id",
        columns_json=_cols_json(),
    )
    assert out.startswith("Error:")
    assert "invalid skill_name" in out


def test_invalid_table_name_rejected(ctx):
    out = save_ingestion_skill(
        skill_name="ingest-acme",
        table_name="bad;name",
        business_key="invoice_id",
        columns_json=_cols_json(),
    )
    assert out.startswith("Error:")


def test_invalid_columns_json_rejected(ctx):
    out = save_ingestion_skill(
        skill_name="ingest-acme",
        table_name="acme",
        business_key="invoice_id",
        columns_json="not a json",
    )
    assert out.startswith("Error:")


def test_overwrites_existing_skill(tmp_path, ctx):
    save_ingestion_skill(
        skill_name="ingest-acme",
        table_name="acme",
        business_key="invoice_id",
        columns_json=_cols_json(),
        description="v1",
    )
    save_ingestion_skill(
        skill_name="ingest-acme",
        table_name="acme",
        business_key="invoice_id",
        columns_json=_cols_json(),
        description="v2",
    )
    content = (tmp_path / "seeknal/skills/ingest-acme/SKILL.md").read_text()
    assert "v2" in content
    assert "v1" not in content
