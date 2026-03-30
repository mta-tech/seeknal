# Ask Agent Pipeline Builder — Post-Mortem & Implementation Notes

**Date:** 2026-03-17
**Status:** in-progress
**Author:** Principal Engineer Review

## QA Test Results

### Test: Star Schema E2E with ML + Feature Store

**Dataset:** 4 dimensions + 2 fact tables (7.7K rows), star schema
**Pipeline produced:** 19 parquet outputs across all medallion layers

**Nodes built:**
- Bronze (7 sources): dim_customers, dim_products, dim_stores, dim_date, fact_orders, fact_events, customers_source
- Silver (2 transforms): enriched_orders (5-way JOIN), customer_360 (aggregated per customer)
- Gold (4 transforms): revenue_by_segment, product_performance, channel_attribution, monthly_trends
- Feature Engineering (2): customer_features (feature_group), customer_features_transform
- ML Model (1): customer_segmentation_model (KMeans clustering in Python)

**What worked:**
- `profile_data()` correctly identified all 6 CSVs, schemas, and join keys (customer_id, product_id)
- Agent designed DAG first (18 nodes, 17 edges) before building
- Agent self-debugged 3 pipeline failures and fixed them
- Feature store pattern used (feature_group → transform → Python model)
- All 19 parquet outputs produced

**What failed / needs improvement:**

## Critical Issues Found

### Issue 1: Agent skips design phase — jumps straight to building (P0)

**Observed:** In a second test (`ask-agent-test-1`), the agent called `profile_data()` then immediately started creating `draft_node` calls without showing the DAG design to the user first.

**Expected:** Agent should ALWAYS show a DAG design diagram and get user confirmation before creating ANY nodes.

**Root cause:** System prompt says "plan the DAG BEFORE creating any nodes" but doesn't enforce it as a hard gate. The agent treats it as optional.

**Fix needed:** The workflow should be:
1. `profile_data()` → discover data
2. Present DAG design as text/diagram to user
3. WAIT for user confirmation before proceeding
4. Only then start creating nodes

This requires a system prompt change to make the design review a mandatory step, not a suggestion.

### Issue 2: Agent doesn't distinguish analytics vs pipeline-building requests (P0)

**Observed:** User asked "analyze customer segments" and the agent started creating pipeline nodes instead of just querying existing data.

**Expected behavior by intent:**
- **Analytics questions** ("what are the top products?", "show revenue trends") → READ-ONLY. Query with `execute_sql`, no drafts, no edits.
- **Pipeline building** ("build a pipeline", "create transforms", "add a model") → Design → Build workflow.
- **Exploration** ("what data do we have?") → `profile_data()` + `list_tables()` only.

**Fix needed:** System prompt should have explicit intent detection:
```
INTENT DETECTION — determine the user's intent BEFORE acting:
- "analyze", "show", "query", "what is", "how many" → ANALYSIS MODE (read-only, no pipeline changes)
- "build", "create", "add", "design pipeline" → BUILD MODE (design first, then build)
- "explore", "what data", "list" → DISCOVERY MODE (profile_data + list_tables only)
```

### Issue 3: `apply_draft` timeout (60s) too short for large projects (P1)

**Observed:** `apply_draft` timed out after 60 seconds in `ask-agent-test-1`.

**Root cause:** The `seeknal apply` subprocess includes YAML validation + file move + manifest regeneration. For projects with many nodes, manifest regeneration can be slow.

**Fix:** Increase `apply_draft` timeout to 120s, or make it configurable.

### Issue 4: Raw data should be transformed INTO star schema, not loaded as-is (P1)

**Observed:** User provided files named `dim_customers.csv`, `fact_orders.csv` but in real data engineering, raw data is NOT pre-structured. The pipeline should transform raw transactional data INTO a star schema.

**Best practice:**
- Bronze: Load raw data as-is (raw_transactions, raw_users, raw_products)
- Silver: Clean, type-cast, deduplicate, normalize
- Gold: Build star schema (dim tables + fact tables) from silver layer
- ML: Feature engineering from gold layer

**Fix needed:** System prompt should teach: "In a real pipeline, bronze is raw ingestion. Star schema dimensions and facts are OUTPUTS of the silver/gold layer, not inputs."

### Issue 5: Feature store pattern not properly integrated (P1)

**Observed:** Agent created a `feature_group.customer_features` YAML but hit platform errors. Fell back to a `transform.customer_features_transform` SQL node instead.

**Root cause:** The feature_group node type in seeknal has specific requirements (entity, materialization, features list) that differ from transforms. The agent didn't understand these constraints.

**Fix needed:**
1. System prompt should include feature_group YAML pattern
2. Feature store workflow: feature_group → entity → materialization → ML model reads from feature store

### Issue 6: Duplicate/stale nodes from previous runs (P2)

**Observed:** `customers_source.yml` from a previous test leaked into the new run. Agent spent tool calls debugging stale files.

**Fix needed:** `seeknal ask` should detect and warn about stale nodes that reference non-existent data files.

### Issue 7: inspect_output not called in final answer (P2)

**Observed:** Agent said "Now, let's inspect the outputs" but didn't actually call `inspect_output`. Answer described pipeline conceptually instead of showing real data.

**Fix needed:** System prompt CRITICAL RULES should be even more explicit: "You MUST call `inspect_output()` at least 3 times after run_pipeline to show real query results."

## Recommended Improvements (Priority Order)

### P0 — Must Fix

1. **Intent detection in system prompt** — analytics vs build vs explore modes
2. **Mandatory design review** — agent must present DAG and wait for confirmation
3. **Star schema guidance** — raw data → bronze → silver → gold (star schema is an OUTPUT)

### P1 — Should Fix

4. **Feature store pattern** — proper feature_group YAML example in system prompt
5. **apply_draft timeout** — increase to 120s
6. **inspect_output enforcement** — must show real data after every run

### P2 — Nice to Have

7. **Stale node detection** — warn about nodes referencing missing data files
8. **DAG visualization** — ASCII art or mermaid diagram of the pipeline
9. **Pipeline summary tool** — single call that shows: node count, edge count, last run status, output row counts

## Key Learnings

1. **System prompt instructions ≠ behavior** — saying "design first" doesn't mean the agent does it. Need hard gates (tool-level enforcement or explicit step-by-step workflow).
2. **Token efficiency matters** — `profile_data()` saved 6+ tool calls. `inspect_output()` should save similar. Every wasted call is 10-30s of latency.
3. **Self-debugging is powerful but expensive** — the agent spent ~50% of its recursion budget fixing failures. Better templates and validation upfront would reduce this.
4. **Feature store integration needs work** — the concept is right (feature_group → ML model) but the implementation path has friction.
5. **Analytics vs Build intent** — most critical UX issue. Users asking "show me revenue" should NOT trigger pipeline creation.
