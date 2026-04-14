---
name: profile-data
description: "Profile CSV files in data/ for schema, quality, null counts, unique counts, sample values, and join-key candidates"
tags: [data-quality, profiling, exploration]
version: "1.0.0"
---

# Profile Data

Use this workflow to understand the schema and quality of data files BEFORE
building transforms against them. It's the first step when onboarding a new
project or when the user drops a CSV into `data/` and wants to know what's
inside.

## Tool you will call

- `profile_data` — profiles CSV files in `data/`

## When to use

Trigger when the user says things like:

- "what's in data/"
- "profile the customers file"
- "show me the schema"
- "what's the join key"
- "how many nulls in X"

Also use PROACTIVELY at the start of a session when:

1. The user is new to the project and asks an analytical question
2. The user mentions a table you haven't seen yet
3. You're about to build a new transform and need to verify column types

## Phase 1 — Decide scope

The tool has two modes:

### Overview mode (`file_path=""`)

Lists ALL CSVs in `data/` with:
- Row count per file
- Column name + type summary per file
- A "potential join keys" section showing columns that appear in 2+ files,
  with type mismatch detection flagged with `TYPE MISMATCH`.

Use this when you don't know what files exist or when the user asks an
open-ended "what's available" question.

### Detail mode (`file_path="data/<name>.csv"`)

Detailed profile of ONE CSV:
- Row count
- Per-column: type, null count / total, unique count, up to 5 sample values

Use this when the user mentioned a specific file OR when you need to verify
the shape of a table before building a transform against it.

## Phase 2 — Call the tool

Args:

- `file_path` (optional): path to a specific CSV (e.g. `"data/customers.csv"`).
  Leave empty for overview mode.

## Phase 3 — Interpret the results

### Join keys with type mismatches

When the overview output flags `TYPE MISMATCH`, DO NOT silently ignore it.
This is the most common cause of broken JOINs: one file has `customer_id`
as `VARCHAR`, another as `BIGINT`, and the JOIN silently returns zero rows.

Tell the user about the mismatch and suggest either:
1. Adding a `CAST` in the transform's SQL, OR
2. Normalizing the CSV upstream so both files agree on the type.

### Null counts

If a column has high null density, flag it before the user builds a
transform that assumes it's populated.

### Sample values

Use the sample values to verify your mental model of the column (e.g.,
`order_status` has values `Pending`, `Shipped`, `Cancelled` — not a boolean).

## Phase 4 — Next steps

After profiling, suggest next actions based on what you found:

- Clean schema → use `bootstrap-semantic-model` skill to auto-generate a
  semantic layer
- Complex shape / many files → use `search_project_files` to find existing
  transforms that already work with this data
- Obvious join keys → use `build-pipeline-node` skill to draft a new
  transform that JOINs them

## Output location

`profile_data` is read-only — it writes nothing to disk. All results are
returned as the tool response.
