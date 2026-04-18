"""Save ingestion skill tool — generate a reusable SKILL.md for an ingestion.

Writes {project}/seeknal/skills/{skill_name}/SKILL.md that pydantic-deep's
SkillsToolset will auto-discover on the next session, letting the agent
re-run same-shape ingestions via ``load_skill(skill_name)``.
"""

from __future__ import annotations

import json
import re

_SKILL_NAME_RE = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$")
_COLUMN_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def save_ingestion_skill(
    skill_name: str,
    table_name: str,
    business_key: str,
    columns_json: str,
    description: str = "",
) -> str:
    """Save a reusable ingestion skill as a SKILL.md file.

    Creates {project}/seeknal/skills/{skill_name}/SKILL.md with frontmatter
    and step-by-step prose for re-ingesting same-shape data in future sessions.

    Args:
        skill_name: Slug for the skill directory (e.g., 'ingest-acme-sales').
        table_name: Target DuckDB table name.
        business_key: Comma-separated business key columns.
        columns_json: JSON array of {"name": ..., "type": ...} objects describing the expected schema.
        description: Human-readable description of what this data represents.
    """
    from seeknal.ask.agents.tools._context import get_tool_context
    from seeknal.ask.agents.tools._write_security import validate_write_path

    ctx = get_tool_context()

    if not _SKILL_NAME_RE.match(skill_name):
        return (
            f"Error: invalid skill_name '{skill_name}'. "
            "Must be a lowercase slug (letters, digits, hyphens), e.g. 'ingest-acme-sales'."
        )
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name):
        return f"Error: invalid table_name '{table_name}'."

    business_key_cols = [c.strip() for c in business_key.split(",") if c.strip()]
    if not business_key_cols:
        return "Error: business_key must name at least one column."
    for col in business_key_cols:
        if not _COLUMN_NAME_RE.match(col):
            return f"Error: invalid business key column '{col}'."

    try:
        columns = json.loads(columns_json)
        if not isinstance(columns, list):
            raise ValueError("columns_json must be a JSON array")
        parsed_columns: list[tuple[str, str]] = []
        for entry in columns:
            if not isinstance(entry, dict):
                raise ValueError("each column entry must be an object with name+type")
            name = str(entry.get("name", "")).strip()
            ctype = str(entry.get("type", "")).strip()
            if not name or not ctype:
                raise ValueError("column entries need non-empty name and type")
            parsed_columns.append((name, ctype))
    except Exception as exc:
        return f"Error: columns_json invalid: {exc}"

    rel_path = f"seeknal/skills/{skill_name}/SKILL.md"
    try:
        skill_path = validate_write_path(rel_path, ctx.project_path)
    except ValueError as exc:
        return f"Error: {exc}"

    skill_path.parent.mkdir(parents=True, exist_ok=True)

    content = _render_skill_md(
        skill_name=skill_name,
        table_name=table_name,
        business_key_cols=business_key_cols,
        columns=parsed_columns,
        description=description.strip(),
    )

    with ctx.fs_lock:
        skill_path.write_text(content, encoding="utf-8")

    return (
        f"Saved ingestion skill at `{skill_path}`.\n"
        f"- Skill name: `{skill_name}`\n"
        f"- Table: `ingest_{table_name}`\n"
        f"- Business key: {', '.join(business_key_cols)}\n"
        f"- Columns: {len(parsed_columns)}\n\n"
        "Next session: the agent can `load_skill('"
        f"{skill_name}')` to re-run this ingestion."
    )


def _render_skill_md(
    skill_name: str,
    table_name: str,
    business_key_cols: list[str],
    columns: list[tuple[str, str]],
    description: str,
) -> str:
    desc = description or (
        f"Re-ingest same-shape data into the `ingest_{table_name}` table "
        f"with business key {', '.join(business_key_cols)}."
    )
    bk_csv = ",".join(business_key_cols)
    bk_list_yaml = "[" + ", ".join(f'"{c}"' for c in business_key_cols) + "]"
    schema_table_lines = ["| Column | Type |", "| --- | --- |"]
    for name, ctype in columns:
        schema_table_lines.append(f"| `{name}` | {ctype} |")
    schema_table = "\n".join(schema_table_lines)

    return f"""---
name: {skill_name}
description: "{desc}"
tags: [data-ingest, {skill_name}]
version: "1.0.0"
table_name: {table_name}
business_key: {bk_list_yaml}
---

# {skill_name}

Use this skill when the user provides another file with the same shape as
`ingest_{table_name}`. The goal is to append-and-dedup new rows while
surfacing any schema drift or duplicate business keys to the user.

## Expected schema

{schema_table}

Business key: **{', '.join(business_key_cols)}**

## Phase 1 — Parse & inspect

1. Call `read_tabular(path_or_url=<user file>)` and confirm the columns
   match the schema above.
2. If the schema differs, call `check_ingestion_drift(source_path=...,
   table_name='{table_name}', business_key='{bk_csv}')` and present the
   drift report to the user before proceeding.

## Phase 2 — Self-defending drift check

1. Call `write_ingested_table(source_path=..., table_name='{table_name}',
   business_key='{bk_csv}', mode='append', user_confirmed=False)`.
2. The tool returns a drift + duplicate report **without writing**.
3. Present the report via `ask_user` with options:
   - `Skip duplicates (keep existing)`
   - `Replace duplicates (keep new)`
   - `Skip this file`
   - `Type your own`

## Phase 3 — Write

After the user confirms, call:

```
write_ingested_table(
    source_path=...,
    table_name='{table_name}',
    business_key='{bk_csv}',
    mode='append',
    user_confirmed=True,
    dedup_strategy='skip',   # or 'replace' per user choice
)
```

Then verify:

```
execute_sql("SELECT COUNT(*) FROM ingest_{table_name}")
```

## Phase 4 — Analytics

Offer to answer an analytical question about the ingested data using
`execute_sql`.

## Critical-analyst rules

- NEVER call `write_ingested_table` with `user_confirmed=True` before
  showing the user the drift + duplicate report.
- NEVER silently drop rows — always report the dedup count.
- If the user chose `Skip this file`, do not call the write tool; report
  that nothing was ingested.
"""
