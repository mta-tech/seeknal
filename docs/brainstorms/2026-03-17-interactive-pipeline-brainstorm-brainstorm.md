# Interactive Pipeline Brainstorm for Seeknal Ask

**Date:** 2026-03-17
**Status:** brainstorm
**Author:** Principal Engineer

## What We're Building

An interactive brainstorm phase for `seeknal ask chat` that guides users through structured design questions before building a data pipeline. Inspired by the ce:brainstorm skill pattern — the agent asks one question at a time with multiple-choice options, user picks or types free text, until the design is confirmed.

**Trigger:** Chat mode only (`seeknal ask chat`). One-shot mode auto-proceeds as today.

## Why This Approach

**Problem observed:** The agent currently either:
1. Jumps straight to building without asking what the user wants (wastes recursion budget building wrong things)
2. Shows a DAG design but doesn't wait for input (user can't shape the pipeline)

**Solution:** A seeknal skill file (`.md`) loaded when BUILD mode is detected. The skill defines 4 structured phases of questions, each with multiple-choice options. The agent asks one question per turn, collects answers, then presents the final DAG for confirmation before building.

**Why skill file over system prompt:**
- Keeps the main system prompt lean (reduces Gemini empty-response rate)
- Skill content is injected only when needed (BUILD mode)
- Can be iterated independently without touching core agent code
- Follows the established ce:brainstorm pattern the user referenced

## Key Decisions

1. **Chat mode only** — one-shot mode auto-proceeds. Interactive brainstorm requires multi-turn conversation.
2. **Structured phases** — 4 phases: Data Scope → Pipeline Layers → ML/Features → Confirm DAG. Each has 1-2 questions with options.
3. **Show DAG and wait for 'go'** — after brainstorm, present final DAG and wait for explicit user confirmation before building.
4. **Skill file implementation** — create `src/seeknal/workflow/templates/skill_pipeline_design.md` as a seeknal skill that the agent loads during BUILD mode.

## Design: Brainstorm Phases

### Phase 1: Data Scope
After `profile_data()` returns, ask:

**Q1:** "I found N data files. Which ones should be included in this pipeline?"
- Options: list each CSV with row count + columns. Include "All of them" as first option.

**Q2:** "What are the key entities in this data?"
- Options: auto-detected from column names (e.g., "customer (customer_id)", "product (product_id)", "order (order_id)")
- User can type custom entities

### Phase 2: Pipeline Architecture
**Q3:** "What kind of pipeline do you want to build?"
- Analytics pipeline (sources → transforms → business metrics)
- ML pipeline (sources → features → model training)
- Full stack (analytics + ML + reporting)
- Custom (describe what you need)

**Q4:** "What silver-layer transforms should we build?"
- Options: auto-suggested based on join keys found in Phase 1
  - e.g., "enriched_orders (orders JOIN customers JOIN products)"
  - "customer_360 (per-customer aggregations)"
- User can add/remove/modify

### Phase 3: ML & Features (if ML selected in Phase 2)
**Q5:** "What ML approach do you want?"
- Clustering (KMeans — segment customers/products)
- Classification (predict churn, conversion, etc.)
- Regression (predict revenue, lifetime value)
- Skip ML for now

**Q6:** "What features should we engineer?"
- Options: auto-suggested RFM features based on data
  - Recency (days since last order)
  - Frequency (total orders)
  - Monetary (total spend)
  - Engagement (event counts, session duration)
- User can add custom features

### Phase 4: Confirm & Build
Present the complete DAG design:
```
Pipeline Design:
  Bronze: source.customers (200 rows), source.orders (2K rows), ...
  Silver: transform.enriched_orders, transform.customer_360
  Gold:   transform.revenue_by_segment, transform.product_performance
  ML:     feature_group.customer_features → model.customer_segments (KMeans)
  Total:  12 nodes, 11 edges

Ready to build? (yes / modify / cancel)
```

## Additional Decision: Environment Staging

**Pipeline should deploy to dev first, then promote to prod on user confirmation.**

### Flow:
1. Agent builds pipeline nodes → applied to `seeknal/` directory
2. `run_pipeline` executes in **dev environment** by default
3. Agent shows results from dev run (via `inspect_output`)
4. Agent asks: "Pipeline ran successfully in dev. Promote to production?"
   - **Yes** → agent runs `seeknal env apply --env prod` (or similar)
   - **No, modify** → user gives feedback, agent iterates in dev
   - **Cancel** → pipeline stays in dev only

### Integration with seeknal env system:
- Seeknal already has `seeknal env plan --env dev` and `seeknal env apply --env prod`
- Dev run uses namespace prefixing (e.g., `dev_schema.table` for PostgreSQL)
- Promotion = run with `--env prod` which writes to production targets
- In chat mode: agent always starts with dev, asks before promoting
- In one-shot mode: runs in dev only, user must manually promote

### Key rule for system prompt:
```
When building pipelines:
- ALWAYS run in dev environment first: run_pipeline(confirmed=True, full=True)
- Show results from dev run
- Ask user: "Pipeline succeeded in dev. Promote to production?"
- Only promote when user explicitly confirms
```

## Open Questions

None — all resolved through dialogue above.

## Next Steps

1. Create the skill file at `src/seeknal/workflow/templates/skill_pipeline_design.md`
2. Update the system prompt to load the skill when BUILD mode is detected in chat
3. Test with the e-commerce dataset in chat mode
