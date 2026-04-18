---
name: build-pipeline-node
description: "End-to-end workflow for adding a new pipeline node to a seeknal project — scaffold, validate, apply, and (optionally) run via the 5 thin pipeline-build tools"
tags: [pipeline, scaffolding, dry-run, apply, run]
version: "1.0.0"
---

# Build Pipeline Node

Use this workflow when the user wants to add or modify a pipeline node in a
seeknal project — a new source, transform, feature_group, model, semantic_model,
metric, exposure, rule, profile, or aggregation. The workflow chains 5 thin
tools to take the user from idea → working node, with explicit validation
and confirmation gates between steps.

## Tools you will call

- `draft_node` — generate a template draft under `.seeknal/drafts/`
- `dry_run_draft` — validate the draft (YAML syntax, schema, SQL ref consistency)
- `apply_draft` — move the validated draft into `seeknal/<subdir>/` (gated by `confirmed=True`)
- `edit_node` — modify an already-applied node (gated by `confirmed=True`)
- `run_pipeline` — execute the pipeline against the new/modified node (gated by `confirmed=True`)

You will also use the THIN primitives `read_pipeline`, `search_pipelines`,
`search_project_files`, `read_project_file`, `inspect_output`, and `plan_pipeline`
to discover existing structure and verify outputs.

For Python-heavy ingestion/transformation work, you may first prototype code
with `execute_uv_script(code=..., deps=[...])` before committing it to a
drafted node.

## Supported node types

`source`, `transform`, `feature_group` (alias `feature-group`), `model`,
`aggregation`, `rule`, `profile`, `exposure`, `semantic_model`
(alias `semantic-model`), `metric`.

Each gets a different YAML or Python template + lands in a different subdirectory:
- `source` → `seeknal/sources/<name>.yml`
- `transform` → `seeknal/transforms/<name>.yml` (or `.py`)
- `feature_group` → `seeknal/feature_groups/<name>.yml`
- `model` → `seeknal/models/<name>.yml` (or `.py`)
- `second_order_aggregation` → `seeknal/second_order_aggregations/<name>.yml`
- (Other kinds follow the same `<kind>s/` pattern.)

## When to use Python vs YAML transforms

The same `draft_node` tool handles both formats — pass `python=True` for a
Python file (`.py`) or `python=False` (default) for YAML.

**Use YAML transforms when:**

- The transform is a single SQL aggregation, JOIN, window function, or
  filtered query
- The output schema can be inferred from the SQL
- The transform doesn't need `pandas`, `numpy`, `scipy`, or `sklearn`
- You want the transform to be readable by non-Python users
- Performance matters (DuckDB SQL is consistently faster than Python+pandas
  for the same operation on the same data)

**Use Python transforms when:**

- You need vectorized math: normalization, z-scores, RFM scoring,
  rolling-window analytics, statistical aggregates DuckDB doesn't expose
- The transform calls `scikit-learn`, `scipy`, or other ML/stats libraries
- The logic is too branch-heavy to express cleanly in SQL `CASE` statements
- You need to call an external library (custom geocoder, ML model, API client)

**Common pattern: YAML feeder + Python scorer.** For complex analyses,
draft a YAML transform that produces the input metrics (e.g.
`rfm_metrics.yml` that aggregates customers + orders), then draft a Python
transform that reads the YAML transform's output and computes the derived
score (e.g. `churn_risk_analysis.py` that normalizes and scores). Apply both.
This keeps the heavy SQL aggregation in DuckDB and the per-row math in
pandas where it belongs.

## Phase 1 — Discover existing structure

BEFORE drafting a new node, you MUST understand what already exists:

1. Use `search_pipelines` (or `search_project_files`) to find similar nodes
   the user might want to copy from.
2. Use `read_pipeline` on at least one existing node of the same type to
   match conventions (column names, SQL style, ref patterns).
3. Use `list_tables` / `describe_table` if the node will reference real
   data sources to confirm the schema.

Skipping discovery leads to drafts that don't match project conventions
and fail dry-run for trivial reasons.

## Phase 2 — Scaffold the draft

Call `draft_node(node_type=..., name=..., python=False)`:
- `node_type`: one of the supported types above (snake_case or hyphen)
- `name`: alphanumeric + underscores only, ≤128 chars
- `python`: `True` for a Python file (transforms / models only), `False` for YAML
- `deps`: optional Python dependency list for PEP 723 metadata when `python=True`

The draft is written to `.seeknal/drafts/draft_<type>_<name>.{yml|py}`. The
tool returns the rendered template content for you to inspect.

If a draft with the same filename already exists, the tool refuses and
suggests editing or removing it first.

## Phase 3 — Edit the draft inline (optional)

If you need to customize the rendered template before validation:
- Read the draft via `read_project_file('.seeknal/drafts/draft_<type>_<name>.yml')`
- Generate the modified content
- Apply via `edit_node(node_path='.seeknal/drafts/draft_<type>_<name>.yml',
  new_content=<full new content>, confirmed=True)`

Note that `edit_node` works on ANY YAML/Python file under writable directories
— including drafts AND already-applied nodes.

## Phase 4 — Validate (MANDATORY before apply)

ALWAYS run `dry_run_draft(file_path=<draft filename>)` before `apply_draft`.
The dry-run:
- Parses the YAML/Python and checks schema validity via `seeknal dry-run`
- Cross-checks SQL `ref('source.X')` calls against declared `inputs:` (catches
  the most common runtime failure: a JOIN that references an undeclared input)
- For Python drafts, checks PEP 723 dependency declarations resolve via uv
- Returns "Validation PASSED" or "Validation FAILED" with the exact errors

If validation FAILS:
- Read the error output carefully
- Use `edit_node` (or re-draft) to fix the issues
- Re-run `dry_run_draft` until it passes

DO NOT call `apply_draft` on a draft that hasn't been validated.

## Phase 5 — Apply (with confirmation)

Call `apply_draft(file_path=..., confirmed=False)` first to preview what
will happen. The tool returns the file content + a description of where
it will be moved. SHOW THIS TO THE USER (don't paraphrase) so they can
verify before committing.

When the user is satisfied, call `apply_draft(file_path=..., confirmed=True)`.
The tool:
- Resolves the target path from the file's `kind` + `name` (YAML) or
  `@source` / `@transform` decorator (Python)
- Moves the draft from `.seeknal/drafts/` to `seeknal/<subdir>/<name>.{yml|py}`
- Refreshes the artifact discovery cache so the new node is immediately visible
- Writes a `seeknal_state` snapshot for diff tracking

## Phase 6 — Run (optional, with confirmation)

If the user wants to actually execute the pipeline:

1. Call `run_pipeline(nodes='', full=False, confirmed=False)` to preview
   the command that will run.
2. Show the preview to the user.
3. If they approve, call `run_pipeline(nodes='<comma-sep node IDs>',
   confirmed=True)` to run only the new node, OR `run_pipeline(confirmed=True)`
   to run everything.

Pass `nodes='transform.<name>'` (with the `kind.` prefix) to run JUST the new
node and its dependencies. Use `full=True` only when the user explicitly
asks for a cache-invalidating re-run.

The default timeout is 300 seconds (override via `SEEKNAL_RUN_TIMEOUT`).

After a successful run, the tool refreshes artifact discovery and lists the
parquet outputs you can inspect via `inspect_output('<kind>.<name>')`.

## Phase 7 — Verify the output

After a successful run, call `inspect_output('<kind>.<name>')` to confirm:
- The expected schema appeared
- Row count is non-zero (or matches expectations)
- Sample rows look sensible

If the output is missing or wrong, use `edit_node` on the applied file to
fix the definition, then re-run.

## Phase 8 — Final answer

Your final answer to the user MUST include:

1. The full path to the applied node (`seeknal/<subdir>/<name>.<ext>`)
2. The dry-run result (`PASSED` is implied if you got this far)
3. If you ran the pipeline: the row count + sample output from `inspect_output`
4. The next-step command (`seeknal run`, `seeknal plan`, etc.) the user
   can use to integrate this into their dev loop

## Common mistakes to avoid

- **Skipping Phase 1** — drafts created without reading existing conventions
  almost always fail dry-run on the first attempt.
- **Skipping `dry_run_draft`** — applying an unvalidated draft creates broken
  state that's painful to roll back.
- **Forgetting `confirmed=True`** on `apply_draft` / `edit_node` / `run_pipeline`
  — these tools intentionally refuse to do destructive work without the flag.
- **Running `run_pipeline` without `nodes=`** — running the entire pipeline
  for a single new node wastes time and may obscure the failure if the new
  node breaks something.
- **Editing the draft path manually after `apply_draft`** — once applied, the
  file lives at `seeknal/<subdir>/<name>.<ext>`, NOT at the original draft
  path. Subsequent edits use `edit_node(node_path='seeknal/<subdir>/<name>.<ext>')`.

## Where things live (cheat sheet)

- Drafts (pre-apply): `.seeknal/drafts/draft_<kind>_<name>.{yml|py}`
- Applied YAML nodes: `seeknal/<kind>s/<name>.yml`
- Applied Python nodes: `seeknal/<kind>s/<name>.py`
- Pipeline outputs: `target/intermediate/<kind>.<name>.parquet`
- Run state: `target/<env>/run_state.json`
