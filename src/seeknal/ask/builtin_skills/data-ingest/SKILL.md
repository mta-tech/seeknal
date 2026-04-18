---
name: data-ingest
description: "Conversational data ingestion — parse files, brainstorm schema, create queryable tables, and save reusable skills"
tags: [data-ingest, import, upload]
version: "1.0.0"
---

# Data Ingestion

Use this workflow when the user provides a file (xlsx, csv, tsv, json)
or a direct-download URL and wants to ingest it into a queryable table.

## Tools you will call

- `read_tabular` — parse and preview the file
- `describe_table` / `list_tables` — check for existing tables
- `ask_user` — confirm schema, business key, table name
- `write_ingested_table` — persist data to Parquet + register view
- `save_ingestion_skill` — create reusable SKILL.md
- `execute_sql` — answer follow-up analytics questions
- `check_ingestion_drift` — compare schemas on re-runs

## Phase 1 — Parse & Inspect

1. Call `read_tabular(path_or_url=<user file or URL>)`.
2. Present the inferred schema to the user.
3. Use `ask_user` to confirm / adjust:
   - Table name suggestion (derived from filename)
   - Business key suggestion (inferred from unique columns)
   - Any column renames or type overrides

## Phase 2 — Check for Existing Table

1. Call `list_tables` to see whether `ingest_{table_name}` already exists.
2. If it exists: load the matching skill (if any) and switch to re-run mode
   (Phase 2b).
3. If not: proceed to Phase 3 (First Ingestion).

### Phase 2b — Re-run Mode (existing table found)

1. Call `check_ingestion_drift(source_path=..., table_name=...,
   business_key=...)` to compare schemas and get a human-readable report.
2. Call `write_ingested_table(source_path=..., table_name=...,
   business_key=..., mode='append', user_confirmed=False)` to get the
   self-defending drift report (schema diff + dedup count).
3. Present both reports via `ask_user` with options:
   - `Skip duplicates (keep existing)`
   - `Replace duplicates (keep new)`
   - `Skip this file`
   - `Type your own`
4. Based on the user's answer, call
   `write_ingested_table(mode='append', user_confirmed=True,
   dedup_strategy='skip'|'replace')`.
5. Skip to Phase 5.

## Phase 3 — First Ingestion

1. Call `write_ingested_table(source_path=..., table_name=...,
   business_key=..., mode='create')`.
2. Verify: `execute_sql("SELECT COUNT(*) FROM ingest_{table_name}")`.

## Phase 4 — Save Skill

1. Call `save_ingestion_skill(skill_name=..., table_name=...,
   business_key=..., columns_json=..., description=...)`.
2. Confirm: "Skill saved at {path}. Next time you provide similar data,
   I'll auto-detect and use this skill."

## Phase 5 — Analytics

After ingestion, ask the user if they have a question about the data.
Use `execute_sql` to answer it (standard analytics path).

## Critical-Analyst Rules

- NEVER insert data without showing the user a schema preview first.
- NEVER silently skip or drop columns on schema drift.
- NEVER silently dedup rows — always report the count and ask.
- If schema drift is detected, present EXACTLY what changed and offer
  options via `ask_user`.
- If duplicates are detected, present the count AND a recommendation
  (e.g., "I suggest skipping duplicates") and offer skip/replace
  options via `ask_user`.
- ALWAYS call `write_ingested_table` with `user_confirmed=False` first in
  append mode to get the drift report. Only call with `user_confirmed=True`
  after the user explicitly confirms via `ask_user`.

## Housekeeping

Staging files in `target/ask_ingest/_staging/` are temporary downloads and
uploads. They can be safely deleted after ingestion completes. Cleanup is
manual in v1; automated TTL-based cleanup is planned for v2.
