"""Tests for propose_record_table."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.propose_record_table import propose_record_table


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


def _seed_view(ctx: ToolContext, view_name: str, schema_sql: str) -> None:
    ctx.repl.conn.execute(f"CREATE TABLE {view_name}_t AS {schema_sql}")
    ctx.repl.conn.execute(f'CREATE OR REPLACE VIEW "{view_name}" AS SELECT * FROM {view_name}_t')


def test_no_existing_tables_proposes_new_orders(ctx):
    draft = json.dumps(
        {"person": "fitra", "items": [{"quantity": 1, "name": "mie ayam"}],
         "amount": 20000, "currency": "IDR"}
    )
    out = propose_record_table(draft)
    assert "New: create `ingest_orders`" in out
    assert "entered_at,merchant" in out or "entered_at,person" in out
    # Schema table present
    assert "`amount` | BIGINT" in out


def test_no_existing_tables_proposes_new_transfers(ctx):
    draft = json.dumps(
        {"kind": "transfer", "amount": 1500000, "recipient_name": "Budi",
         "bank": "BCA", "reference_number": "TRX-42"}
    )
    out = propose_record_table(draft)
    assert "New: create `ingest_transfers`" in out
    assert "reference_number" in out


def test_existing_table_match_scores_high(ctx):
    # Seed an existing orders table whose schema covers the draft fields
    _seed_view(
        ctx,
        "ingest_orders",
        "SELECT CAST(NULL AS TIMESTAMP) AS entered_at, "
        "CAST(NULL AS VARCHAR) AS person, CAST(NULL AS BIGINT) AS amount, "
        "CAST(NULL AS VARCHAR) AS currency, CAST(NULL AS VARCHAR) AS bank, "
        "CAST(NULL AS JSON) AS items, CAST(NULL AS VARCHAR) AS note, "
        "CAST(NULL AS VARCHAR) AS raw_text WHERE false",
    )
    draft = json.dumps(
        {"person": "fitra", "items": [{"quantity": 1, "name": "mie ayam"}],
         "amount": 20000, "currency": "IDR", "bank": None}
    )
    out = propose_record_table(draft)
    assert "Match: append to `ingest_orders`" in out
    assert "mode='append'" in out
    assert "user_confirmed=False" in out


def test_hint_table_name_overrides_default(ctx):
    draft = json.dumps({"person": "fitra", "amount": 20000})
    out = propose_record_table(draft, hint_table_name="daily_expenses")
    assert "ingest_daily_expenses" in out


def test_hint_rejected_if_invalid(ctx):
    # "123-bad;name" sanitizes to "123_bad_name" which fails the
    # [a-zA-Z_][a-zA-Z0-9_]* SQL-identifier guard, so the proposer falls back
    # to a kind-derived slug. Draft has no item/transfer markers so kind="other".
    draft = json.dumps({"person": "fitra", "amount": 20000})
    out = propose_record_table(draft, hint_table_name="123-bad;name")
    assert "ingest_records" in out  # "other" kind default
    assert "123" not in out  # invalid hint must not leak into the slug


def test_invalid_draft_json_rejected(ctx):
    out = propose_record_table("not-json")
    assert out.startswith("Error:")


def test_empty_draft_rejected(ctx):
    out = propose_record_table("")
    assert out.startswith("Error:")


def test_activity_kind_template(ctx):
    draft = json.dumps({"kind": "activity", "activity": "workout",
                        "exercises": [{"name": "running", "duration_min": 30}]})
    out = propose_record_table(draft)
    assert "`ingest_records" in out or "`activity`" in out.lower()
    # Activity template uses duration_min and exercises
    assert "exercises" in out
    assert "duration_min" in out
    # Business key does not require amount
    assert "entered_at,person" in out


def test_meeting_kind_template(ctx):
    draft = json.dumps({"kind": "meeting", "attendees": ["budi"],
                        "summary": "Q2 plan"})
    out = propose_record_table(draft)
    assert "attendees" in out
    assert "action_items" in out
    assert "entered_at,title" in out


def test_health_kind_template(ctx):
    draft = json.dumps({"kind": "health",
                        "metrics": {"weight_kg": 72.5, "bp": "120/80"}})
    out = propose_record_table(draft)
    assert "`metrics` | JSON" in out


def test_columns_override_wins(ctx):
    draft = json.dumps({
        "kind": "custom",
        "_columns": [
            {"name": "sku", "type": "VARCHAR"},
            {"name": "on_hand", "type": "INTEGER"},
            {"name": "counted_by", "type": "VARCHAR"},
        ],
        "sku": "ABC",
        "on_hand": 12,
        "counted_by": "fitra",
    })
    out = propose_record_table(draft, hint_table_name="inventory_check")
    assert "`sku` | VARCHAR" in out
    assert "`on_hand` | INTEGER" in out
    assert "`counted_by` | VARCHAR" in out
    # Audit columns auto-added
    assert "`entered_at` | TIMESTAMP" in out
    assert "`raw_text` | VARCHAR" in out
    assert "ingest_inventory_check" in out


def test_kind_hint_respected_when_no_explicit_kind(ctx):
    # parse_record would set kind_hint="activity"; propose_record_table honors it
    draft = json.dumps({"kind_hint": "activity", "note": "morning run"})
    out = propose_record_table(draft)
    assert "exercises" in out


def test_collision_avoided_when_default_slug_exists(ctx):
    # Seed a totally unrelated view also named ingest_orders, so the proposer
    # wants kind-default "orders" but must dodge the collision.
    _seed_view(
        ctx,
        "ingest_orders",
        "SELECT CAST(NULL AS VARCHAR) AS unrelated WHERE false",
    )
    # Draft shaped like an order (has items) so kind="order" -> base slug "orders".
    draft = json.dumps(
        {"items": [{"quantity": 1, "name": "mie ayam"}], "amount": 20000}
    )
    out = propose_record_table(draft)
    # Either a _2 suffix OR a match decision — we only require no collision.
    assert "ingest_orders_2" in out or "Match: append to `ingest_orders`" in out
