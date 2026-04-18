"""Propose record table tool — table-aware router for record drafts.

Given a draft from ``parse_record`` or ``extract_from_image``, scan the
session's existing ``ingest_*`` views, score how well each covers the
draft's fields, and either:

  - **Match**: recommend appending to an existing table (all required
    draft fields map to existing columns).
  - **New**: propose a new table with a schema derived from the draft
    plus a sensible business key.

The tool returns a recommendation as markdown; it NEVER writes. The agent
must take the proposal into ``ask_user`` for confirmation, then call
``write_ingested_table`` with ``mode='create'`` or ``mode='append'``.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

_INGEST_VIEW_PREFIX = "ingest_"
_TABLE_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Known draft shapes. Keys are field names that parse_record /
# extract_from_image emit; values are (preferred column name, DuckDB type).
_TEXT_DRAFT_COLUMNS = [
    ("entered_at",     "TIMESTAMP"),
    ("person",         "VARCHAR"),
    ("amount",         "BIGINT"),
    ("currency",       "VARCHAR"),
    ("bank",           "VARCHAR"),
    ("items",          "JSON"),
    ("note",           "VARCHAR"),
    ("raw_text",       "VARCHAR"),
]
_IMAGE_TRANSFER_COLUMNS = [
    ("entered_at",        "TIMESTAMP"),
    ("transaction_date",  "TIMESTAMP"),
    ("amount",            "BIGINT"),
    ("currency",          "VARCHAR"),
    ("sender_name",       "VARCHAR"),
    ("sender_account",    "VARCHAR"),
    ("recipient_name",    "VARCHAR"),
    ("recipient_account", "VARCHAR"),
    ("bank",              "VARCHAR"),
    ("reference_number",  "VARCHAR"),
    ("note",              "VARCHAR"),
    ("raw_text",          "VARCHAR"),
]
_IMAGE_RECEIPT_COLUMNS = [
    ("entered_at",        "TIMESTAMP"),
    ("transaction_date",  "TIMESTAMP"),
    ("amount",            "BIGINT"),
    ("currency",          "VARCHAR"),
    ("merchant",          "VARCHAR"),
    ("items",             "JSON"),
    ("note",              "VARCHAR"),
    ("raw_text",          "VARCHAR"),
]
_ACTIVITY_COLUMNS = [
    ("entered_at",  "TIMESTAMP"),
    ("person",      "VARCHAR"),
    ("activity",    "VARCHAR"),
    ("exercises",   "JSON"),
    ("duration_min","INTEGER"),
    ("note",        "VARCHAR"),
    ("raw_text",    "VARCHAR"),
]
_MEETING_COLUMNS = [
    ("entered_at",  "TIMESTAMP"),
    ("title",       "VARCHAR"),
    ("attendees",   "JSON"),
    ("summary",     "VARCHAR"),
    ("action_items","JSON"),
    ("note",        "VARCHAR"),
    ("raw_text",    "VARCHAR"),
]
_HEALTH_COLUMNS = [
    ("entered_at", "TIMESTAMP"),
    ("person",     "VARCHAR"),
    ("metrics",    "JSON"),
    ("note",       "VARCHAR"),
    ("raw_text",   "VARCHAR"),
]
_NOTE_COLUMNS = [
    ("entered_at", "TIMESTAMP"),
    ("person",     "VARCHAR"),
    ("kind",       "VARCHAR"),
    ("note",       "VARCHAR"),
    ("raw_text",   "VARCHAR"),
]
_EXPENSE_COLUMNS = [
    ("entered_at",  "TIMESTAMP"),
    ("person",      "VARCHAR"),
    ("amount",      "BIGINT"),
    ("currency",    "VARCHAR"),
    ("category",    "VARCHAR"),
    ("merchant",    "VARCHAR"),
    ("note",        "VARCHAR"),
    ("raw_text",    "VARCHAR"),
]


@dataclass
class _TableScore:
    table_name: str
    matched: list[str]
    missing: list[str]
    extra_required: list[str]
    score: float


def propose_record_table(
    draft_json: str,
    hint_table_name: str = "",
) -> str:
    """Recommend a target table for a record draft.

    Scans existing ``ingest_*`` views, ranks them by field overlap with
    the draft, and returns either a MATCH (append existing) or NEW
    (create table) proposal.

    Args:
        draft_json: JSON string returned by ``parse_record`` or
            ``extract_from_image`` — just the inner JSON, not the markdown
            wrapper. Pass ``json.dumps(...)`` of the draft dict.
        hint_table_name: Optional name the user already mentioned. The
            proposer will pick this over its own heuristic if it's valid
            and doesn't collide.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    draft = _parse_draft(draft_json)
    if isinstance(draft, str):
        return draft  # error

    kind = _infer_kind(draft)
    draft_keys = {k for k, v in draft.items() if v is not None}

    existing = _load_existing_tables(ctx)

    scores = [_score_table(name, cols, draft_keys) for name, cols in existing.items()]
    scores.sort(key=lambda s: s.score, reverse=True)
    best = scores[0] if scores else None

    # High-score matches pick up the existing table; ``extra_required`` is
    # informational (null-tolerable columns) and does not disqualify a match.
    if best and best.score >= 0.8:
        return _format_match(best, draft, kind)

    return _format_new(draft, kind, hint_table_name, best, existing)


# ---------------------------------------------------------------------------
# Draft parsing
# ---------------------------------------------------------------------------


def _parse_draft(raw: str) -> dict | str:
    if not raw or not raw.strip():
        return "Error: draft_json is empty."
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        return f"Error: draft_json is not valid JSON: {exc}"
    if not isinstance(payload, dict):
        return "Error: draft_json must be a JSON object."
    return payload


_KNOWN_KINDS = {
    "transfer", "receipt", "order", "activity", "meeting", "health",
    "note", "expense", "other",
}


def _infer_kind(draft: dict) -> str:
    """Classify the draft so we can pick the right schema template.

    Accepts ``kind`` (authoritative), ``kind_hint`` (from parse_record),
    or falls back to field-presence heuristics.
    """
    for key in ("kind", "kind_hint"):
        declared = (draft.get(key) or "").lower()
        if declared in _KNOWN_KINDS:
            return declared

    # Heuristics based on present fields
    has_transfer_fields = any(
        draft.get(k) for k in ("sender_name", "recipient_name", "reference_number")
    )
    if has_transfer_fields:
        return "transfer"
    if draft.get("merchant"):
        return "receipt"
    if draft.get("metrics"):
        return "health"
    if draft.get("attendees") or draft.get("action_items"):
        return "meeting"
    if draft.get("exercises") or draft.get("activity"):
        return "activity"
    if draft.get("items"):
        return "order"
    return "other"


# ---------------------------------------------------------------------------
# Existing-table introspection
# ---------------------------------------------------------------------------


def _load_existing_tables(ctx) -> dict[str, list[tuple[str, str]]]:
    """Return {view_name: [(column_name, column_type), ...]} for ingest_* views."""
    try:
        with ctx.db_lock:
            rows = ctx.repl.conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_catalog='memory' AND table_schema='main' "
                "AND table_type='VIEW' AND table_name LIKE 'ingest_%' "
                "ORDER BY table_name"
            ).fetchall()
    except Exception:
        return {}

    out: dict[str, list[tuple[str, str]]] = {}
    for (view_name,) in rows:
        try:
            with ctx.db_lock:
                cols = ctx.repl.conn.execute(f'DESCRIBE "{view_name}"').fetchall()
            out[view_name] = [(c[0], c[1]) for c in cols]
        except Exception:
            continue
    return out


def _score_table(
    view_name: str,
    columns: list[tuple[str, str]],
    draft_keys: set[str],
) -> _TableScore:
    """Score overlap between draft fields and existing columns."""
    col_names = {c for c, _ in columns}
    # Strip ingest_ prefix from view name for display purposes
    display_name = view_name[len(_INGEST_VIEW_PREFIX):] if view_name.startswith(_INGEST_VIEW_PREFIX) else view_name

    matched = sorted(k for k in draft_keys if k in col_names)
    missing = sorted(k for k in draft_keys if k not in col_names)
    # Required columns in the existing table that the draft can't populate
    required_cols = {c for c, _ in columns}
    extra_required = sorted(c for c in required_cols if c not in draft_keys and c != "entered_at")

    denom = max(len(draft_keys), 1)
    score = len(matched) / denom

    return _TableScore(
        table_name=display_name,
        matched=matched,
        missing=missing,
        extra_required=extra_required,
        score=score,
    )


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


def _format_match(best: _TableScore, draft: dict, kind: str) -> str:
    lines = [f"## Match: append to `ingest_{best.table_name}`"]
    lines.append("")
    lines.append(
        f"Existing table `ingest_{best.table_name}` covers all draft fields "
        f"(score {best.score:.0%}). Suggested call:"
    )
    lines.append("")
    bk = _suggest_business_key(best.table_name, kind)
    lines.append("```")
    lines.append(
        f"write_ingested_table(\n"
        f"    source_path=<staged parquet from read_tabular/extract>,\n"
        f"    table_name='{best.table_name}',\n"
        f"    business_key='{bk}',\n"
        f"    mode='append', user_confirmed=False,\n"
        f")"
    )
    lines.append("```")
    lines.append("")
    lines.append(
        f"Matched columns: {', '.join(f'`{c}`' for c in best.matched) or '(none)'}"
    )
    if best.missing:
        lines.append(
            f"Draft fields NOT in the existing schema (will be dropped or "
            f"need a migration): {', '.join(f'`{c}`' for c in best.missing)}. "
            f"Surface this to the user via `ask_user` before appending."
        )
    lines.append("")
    lines.append(
        "Next: first convert the draft to a single-row parquet via "
        "`read_tabular` (write it to a temp CSV then parse) OR pass it to a "
        "helper that stages the row. Then call the append with "
        "`user_confirmed=False` first to get the drift report."
    )
    return "\n".join(lines)


def _format_new(
    draft: dict,
    kind: str,
    hint_table_name: str,
    best: _TableScore | None,
    existing: dict,
) -> str:
    slug = _slug_from_hint_or_kind(hint_table_name, kind, existing)
    # Agent-provided column override wins over the kind template.
    override = draft.get("_columns")
    columns = _normalise_column_override(override) if override else _columns_for_kind(kind)
    business_key = _suggest_business_key(slug, kind)

    lines = [f"## New: create `ingest_{slug}`"]
    lines.append("")
    if best:
        lines.append(
            f"Closest existing table is `ingest_{best.table_name}` "
            f"(score {best.score:.0%}), but that isn't a strong enough fit "
            f"for this draft's shape. Proposing a new table instead."
        )
    else:
        lines.append("No existing `ingest_*` tables. Proposing a fresh schema.")
    lines.append("")
    lines.append(f"**Kind:** `{kind}`")
    lines.append("")
    lines.append("**Proposed schema**")
    lines.append("| Column | Type |")
    lines.append("| --- | --- |")
    for name, ctype in columns:
        lines.append(f"| `{name}` | {ctype} |")
    lines.append("")
    lines.append(f"**Business key suggestion:** `{business_key}`")
    lines.append("")
    lines.append("**Suggested call**")
    lines.append("```")
    lines.append(
        f"write_ingested_table(\n"
        f"    source_path=<staged parquet with the confirmed draft row>,\n"
        f"    table_name='{slug}',\n"
        f"    business_key='{business_key}',\n"
        f"    mode='create',\n"
        f")"
    )
    lines.append("```")
    lines.append("")
    lines.append(
        "Before calling, walk the user through each field in `ask_user` "
        "(required fields only — anything still null in the draft must be "
        "resolved first). After the write, call `save_ingestion_skill` so "
        "the next identical record auto-routes to this table."
    )
    return "\n".join(lines)


def _slug_from_hint_or_kind(
    hint: str, kind: str, existing: dict
) -> str:
    """Pick a table name, preferring a valid user hint."""
    if hint:
        clean = re.sub(r"[^a-zA-Z0-9_]+", "_", hint.strip()).strip("_").lower()
        if clean and _TABLE_NAME_RE.match(clean):
            return clean
    base = {
        "transfer": "transfers",
        "receipt": "receipts",
        "order": "orders",
        "other": "records",
    }.get(kind, "records")
    # Avoid colliding with an existing view
    candidate = base
    idx = 2
    taken = {n[len(_INGEST_VIEW_PREFIX):] for n in existing if n.startswith(_INGEST_VIEW_PREFIX)}
    while candidate in taken:
        candidate = f"{base}_{idx}"
        idx += 1
    return candidate


def _columns_for_kind(kind: str) -> list[tuple[str, str]]:
    mapping = {
        "transfer": _IMAGE_TRANSFER_COLUMNS,
        "receipt":  _IMAGE_RECEIPT_COLUMNS,
        "order":    _IMAGE_RECEIPT_COLUMNS,
        "activity": _ACTIVITY_COLUMNS,
        "meeting":  _MEETING_COLUMNS,
        "health":   _HEALTH_COLUMNS,
        "expense":  _EXPENSE_COLUMNS,
        "note":     _NOTE_COLUMNS,
    }
    return mapping.get(kind, _TEXT_DRAFT_COLUMNS)


def _normalise_column_override(raw) -> list[tuple[str, str]]:
    """Convert an agent-supplied _columns list into [(name, type), ...].

    Accepts either [{"name": "...", "type": "..."}] or [["name", "type"], ...]
    so the agent can pass the shape most natural to it.
    """
    out: list[tuple[str, str]] = []
    if not isinstance(raw, list):
        return out
    for entry in raw:
        if isinstance(entry, dict):
            name = str(entry.get("name", "")).strip()
            ctype = str(entry.get("type", "VARCHAR")).strip() or "VARCHAR"
        elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
            name = str(entry[0]).strip()
            ctype = str(entry[1]).strip() or "VARCHAR"
        else:
            continue
        if name:
            out.append((name, ctype))
    # Ensure entered_at and raw_text are always present for audit
    names = {n for n, _ in out}
    if "entered_at" not in names:
        out.insert(0, ("entered_at", "TIMESTAMP"))
    if "raw_text" not in names:
        out.append(("raw_text", "VARCHAR"))
    return out


def _suggest_business_key(table_name: str, kind: str) -> str:
    mapping = {
        "transfer": "reference_number",
        "receipt":  "entered_at,merchant",
        "order":    "entered_at,merchant",
        "activity": "entered_at,person",
        "meeting":  "entered_at,title",
        "health":   "entered_at,person",
        "expense":  "entered_at,person,amount",
        "note":     "entered_at,person",
    }
    return mapping.get(kind, "entered_at,person")
