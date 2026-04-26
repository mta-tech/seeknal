---
name: database-analyst
description: "Answer business questions from read-only connected databases using deterministic schema discovery and SQL evidence"
tags: [database, connected-source, read-only, business-analysis]
version: "1.0.0"
---

# Database Analyst

Use this workflow when the user asks a business or analytical question and the
project has a connected/read-only database source such as PostgreSQL, MySQL,
DuckDB, or a warehouse catalog. This is the default path for "tap into my
existing database" users.

## Principle

Tools stay thin. The skill owns the workflow:

- `list_source_context` / `read_source_context` load generated source docs,
  column profiles, and relationship hints.
- `list_sql_pairs` / `execute_sql_pair` / `read_sql_pair` load and run
  project-owned prompt→SQL examples.
- `list_ask_tests` / `read_ask_test` / `run_ask_test` inspect and run
  project-owned QA oracles when the user asks about test coverage or failures.
- `list_tables` discovers queryable relations.
- `describe_table` inspects exact columns/types.
- `execute_sql` runs one read-only query at a time.
- `execute_python` is only for statistics/ML/visualization after SQL scopes the
  dataset; do not use it for ordinary trend tables that SQL already answered.
  A request for a "trend" means table/text analysis unless the user explicitly
  asks for a chart, plot, dashboard, report, or visual.

Do not invent schema. Do not suggest building pipelines unless the user asks for
pipeline creation or durable transformation work.

## Workflow

1. **Discover**
   - For connected-source business questions, start with generated context
     when it exists: call `list_source_context` with a query derived from the
     user's business terms and read the relevant `SOURCE.md`,
     `relationships.md`, `columns.md`, or `profiling.md`.
   - Also call `list_sql_pairs` with the same business terms; project-owned
     SQL pairs are reusable examples, not harness hardcoding.
   - When a listed SQL pair directly matches the user's question, prefer
     `execute_sql_pair` so the pair's SQL runs as-is. Use `read_sql_pair` first
     only when you need to inspect the pair notes.
   - For QA/failure investigation, call `list_ask_tests` or
     `read_ask_test_result` before proposing fixes.
   - Call `list_tables` when generated context is absent, too broad, or needs
     verification, unless the user already supplied a fully-qualified table
     and columns.
   - Prefer fully-qualified connected-source names such as `warehouse.analytics.orders` when available.

2. **Inspect**
   - Call `describe_table` for the most likely fact/view tables before writing joins or aggregations.
   - If the user asks for a metric, identify the measure column, grain, and dimensions from the schema.
   - Do not query columns from a candidate table until context or
     `describe_table` confirms the column exists.

3. **Query**
   - Call `execute_sql_pair` for a directly matching SQL pair; otherwise call
     `execute_sql` with the canonical argument `sql`.
   - Correct form: `execute_sql(sql="SELECT ...")`.
   - `query` may be accepted as an alias, but prefer `sql` so tool calls are portable across providers.
   - DuckDB case-insensitive match syntax is `column ILIKE '%term%'`, not
     function-style `ILIKE(column, '%term%')`.
   - Use explicit casts for numeric aggregates when helpful: `CAST(SUM(revenue) AS DOUBLE) AS revenue`.
   - For nullable/blank text dimensions, normalize labels with
     `COALESCE(NULLIF(TRIM(CAST(column AS VARCHAR)), ''), 'Unknown')`.
   - If a dimension contains opaque numeric/string codes, keep them as codes
     unless a dictionary/context/query result provides the label mapping. Do
     not infer labels from general domain knowledge, and do not reuse labels
     from another dictionary category that happens to share the same code.
   - Do not include trailing semicolons.
   - Do not rewrite a matching SQL pair after it succeeds unless the user asks
     for a different filter/grain or the tool result proves the pair is wrong.
     Reuse already successful SQL results instead of re-running the same query.

4. **Recover**
   - If a table is missing, call `list_tables` and retry with the discovered qualified name.
   - If a column is missing, call `describe_table` and retry with the correct column.
   - Retry at least once before asking the user for schema details.
   - If an optional Python/visualization dependency is unavailable, do not
     retry the same charting/import path. Fall back to a text/table answer or
     non-visual SQL/Python statistics.

5. **Answer**
   - Lead with the direct answer and concrete numbers.
   - State which source/table was used.
   - Explain caveats such as grain, date range, filters, and missing data.
   - Suggest 1–3 next actions or follow-up analyses.
   - Stop after roughly 12–20 useful tool calls. If the answer is directionally
     clear, answer with caveats instead of continuing to hunt for a perfect
     query, chart, or optional Python output.

## Multi-turn behavior

Carry forward the discovered source/table names in the conversation. When the
user asks "now by region" or "compare with segment", reuse the prior table and
only run the additional SQL needed. Do not re-ask what can be inferred from the
previous turn.
