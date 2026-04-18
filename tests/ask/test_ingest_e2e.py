"""End-to-end ship-blocker tests for the data-ingest workflow.

These exercise the full SKILL.md phase sequence through the 4 new tools
plus the REPL auto-registration path. Exercising the live LLM agent here
would be non-deterministic and expensive; instead each test drives the
tools directly in the order the SKILL.md prose mandates, which is the
same control flow the agent follows when the skill is loaded.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.check_ingestion_drift import check_ingestion_drift
from seeknal.ask.agents.tools.read_tabular import read_tabular
from seeknal.ask.agents.tools.save_ingestion_skill import save_ingestion_skill
from seeknal.ask.agents.tools.write_ingested_table import write_ingested_table
from seeknal.cli.repl import REPL


class _REPLStub:
    """Minimal REPL-like object used by the tools during a 'session'."""

    def __init__(self) -> None:
        self.conn = duckdb.connect(":memory:")

    def execute_oneshot(self, sql: str, limit=None):
        res = self.conn.execute(sql)
        cols = [d[0] for d in res.description] if res.description else []
        return cols, res.fetchall()


def _fresh_ctx(project_path: Path) -> ToolContext:
    ctx = ToolContext(
        repl=_REPLStub(),
        artifact_discovery=MagicMock(),
        project_path=project_path,
    )
    set_tool_context(ctx)
    return ctx


@pytest.fixture
def jan_xlsx(tmp_path: Path) -> Path:
    pytest.importorskip("openpyxl")
    import pandas as pd

    path = tmp_path / "raw" / "acme_sales_jan.xlsx"
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "invoice_id": [1, 2, 3, 4, 5],
            "date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"],
            "amount": [100, 150, 200, 250, 300],
            "month": ["January"] * 5,
        }
    )
    df.to_excel(path, index=False, engine="openpyxl")
    return path


@pytest.fixture
def feb_xlsx(tmp_path: Path) -> Path:
    pytest.importorskip("openpyxl")
    import pandas as pd

    path = tmp_path / "raw" / "acme_sales_feb.xlsx"
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "invoice_id": [6, 7, 8],
            "date": ["2025-02-01", "2025-02-02", "2025-02-03"],
            "amount": [120, 180, 240],
            "month": ["February"] * 3,
        }
    )
    df.to_excel(path, index=False, engine="openpyxl")
    return path


@pytest.fixture
def feb_with_overlap_xlsx(tmp_path: Path) -> Path:
    pytest.importorskip("openpyxl")
    import pandas as pd

    path = tmp_path / "raw" / "acme_sales_feb_overlap.xlsx"
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            # invoice_id=5 duplicates the Jan run
            "invoice_id": [5, 6, 7],
            "date": ["2025-01-05", "2025-02-02", "2025-02-03"],
            "amount": [999, 180, 240],  # 999 is the tweaked duplicate value
            "month": ["February", "February", "February"],
        }
    )
    df.to_excel(path, index=False, engine="openpyxl")
    return path


def _count(parquet: Path) -> int:
    con = duckdb.connect(":memory:")
    try:
        (n,) = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet}')").fetchone()
        return int(n)
    finally:
        con.close()


def test_two_session_replay(tmp_path, jan_xlsx, feb_xlsx):
    """AC-1: Ingest Jan in Session A, save skill, ingest Feb in a fresh Session B."""
    project = tmp_path / "project"
    project.mkdir()

    # --- Session A ---
    _fresh_ctx(project)
    read_tabular(str(jan_xlsx))
    write_ingested_table(
        source_path="",  # resolves from ctx.last_read_staging_path
        table_name="acme_sales",
        business_key="invoice_id",
        mode="create",
    )
    save_ingestion_skill(
        skill_name="ingest-acme-sales",
        table_name="acme_sales",
        business_key="invoice_id",
        columns_json=json.dumps(
            [
                {"name": "invoice_id", "type": "BIGINT"},
                {"name": "date", "type": "VARCHAR"},
                {"name": "amount", "type": "BIGINT"},
                {"name": "month", "type": "VARCHAR"},
            ]
        ),
        description="Monthly ACME sales upload",
    )

    parquet = project / "target/ask_ingest/acme_sales.parquet"
    prov = project / "target/ask_ingest/acme_sales_provenance.json"
    skill = project / "seeknal/skills/ingest-acme-sales/SKILL.md"

    assert parquet.exists()
    assert prov.exists()
    assert skill.exists()

    skill_md = skill.read_text()
    assert 'business_key: ["invoice_id"]' in skill_md
    assert _count(parquet) == 5

    # --- Session B: fresh REPL should auto-register ingest_acme_sales ---
    repl = REPL(project_path=project, skip_history=True)
    view_rows = repl.conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog='memory' AND table_schema='main'"
    ).fetchall()
    assert "ingest_acme_sales" in {r[0] for r in view_rows}

    # Replace tool context with one pointing at the fresh REPL (new session).
    _fresh_ctx(project)
    # Copy the view-registered conn state by keeping the live REPL's connection
    # for downstream tool calls.
    ctx_b = ToolContext(repl=repl, artifact_discovery=MagicMock(), project_path=project)
    set_tool_context(ctx_b)

    read_tabular(str(feb_xlsx))
    confirmed = write_ingested_table(
        source_path="",
        table_name="acme_sales",
        business_key="invoice_id",
        mode="append",
        user_confirmed=True,
        dedup_strategy="skip",
    )
    assert "Appended" in confirmed

    assert _count(parquet) == 5 + 3  # no overlap

    updated_prov = json.loads(prov.read_text())
    assert updated_prov["rows_before"] == 5
    assert updated_prov["rows_after"] == 8
    assert updated_prov["rows_inserted"] == 3
    assert updated_prov["rows_deduped"] == 0


def test_single_session_analytics(tmp_path, jan_xlsx):
    """AC-2: Ingest + analytical SQL in the same session via execute_sql path."""
    project = tmp_path / "project"
    project.mkdir()
    ctx = _fresh_ctx(project)

    read_tabular(str(jan_xlsx))
    write_ingested_table(
        source_path="",
        table_name="acme_sales",
        business_key="invoice_id",
        mode="create",
    )

    # Query the registered view directly via the REPL connection — same
    # connection execute_sql would use.
    (total,) = ctx.repl.conn.execute(
        "SELECT CAST(SUM(amount) AS DOUBLE) "
        "FROM ingest_acme_sales WHERE month = 'January'"
    ).fetchone()
    assert total == 100 + 150 + 200 + 250 + 300


def test_critical_agent_dedup(tmp_path, jan_xlsx, feb_with_overlap_xlsx):
    """AC-3: Dry-run drift + dedup count surfaced before any append write."""
    project = tmp_path / "project"
    project.mkdir()

    # Session A: create
    _fresh_ctx(project)
    read_tabular(str(jan_xlsx))
    write_ingested_table(
        source_path="",
        table_name="acme_sales",
        business_key="invoice_id",
        mode="create",
    )
    parquet = project / "target/ask_ingest/acme_sales.parquet"
    rows_before = _count(parquet)

    # Session B: ingest overlap file
    _fresh_ctx(project)
    read_tabular(str(feb_with_overlap_xlsx))

    # Phase 2b: drift check (agent-visible summary)
    drift_report = check_ingestion_drift(
        source_path="",
        table_name="acme_sales",
        business_key="invoice_id",
    )
    assert "Duplicate business-key rows: **1" in drift_report
    assert "skip" in drift_report.lower() and "replace" in drift_report.lower()

    # Phase 2b: self-defending drift-mode append (user_confirmed=False) → must
    # NOT write and must surface the duplicate count.
    dry = write_ingested_table(
        source_path="",
        table_name="acme_sales",
        business_key="invoice_id",
        mode="append",
        user_confirmed=False,
    )
    assert "No data was written" in dry
    assert "Duplicate business-key rows: **1" in dry
    assert _count(parquet) == rows_before  # parquet unchanged by the dry run

    # User chose "Skip duplicates" → confirmed append with skip
    confirmed = write_ingested_table(
        source_path="",
        table_name="acme_sales",
        business_key="invoice_id",
        mode="append",
        user_confirmed=True,
        dedup_strategy="skip",
    )
    assert "Rows deduped: 1" in confirmed

    # rows_before=5, new file=3, 1 dup → final=7
    assert _count(parquet) == rows_before + 3 - 1

    # The duplicate invoice_id=5 should keep its existing amount (300), not 999.
    con = duckdb.connect(":memory:")
    try:
        row = con.execute(
            f"SELECT amount FROM read_parquet('{parquet}') WHERE invoice_id = 5"
        ).fetchone()
    finally:
        con.close()
    assert row[0] == 300
