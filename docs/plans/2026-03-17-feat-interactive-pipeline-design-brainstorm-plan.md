---
title: "feat: Interactive Pipeline Design Brainstorm for Ask Agent"
type: feat
status: completed
date: 2026-03-17
origin: docs/brainstorms/2026-03-17-interactive-pipeline-brainstorm-brainstorm.md
---

# Interactive Pipeline Design Brainstorm for Ask Agent

## Overview

Add an interactive brainstorm phase to `seeknal ask chat` that guides users through structured design questions before building a data pipeline. The agent asks one question at a time with multiple-choice options across 4 phases (Data Scope → Pipeline Architecture → ML/Features → Confirm DAG), then builds only after user confirms the design.

Implemented as a seeknal skill file (`.md.j2`) loaded when BUILD mode is detected in chat. One-shot mode auto-proceeds as today.

(see brainstorm: `docs/brainstorms/2026-03-17-interactive-pipeline-brainstorm-brainstorm.md`)

## Problem Statement

**Observed in QA testing:**
1. Agent jumps straight to building without asking what the user wants — wastes 50%+ recursion budget on wrong transforms
2. Agent uses 1 of 3 CSVs, creates placeholder transforms, builds incomplete pipelines
3. No way for user to shape the pipeline design before code is generated
4. Agent builds in "production" directly — no dev staging

## Proposed Solution

### Skill File: `skill_pipeline_design.md.j2`

A new skill template that defines the complete interactive brainstorm flow. The agent loads this skill when it detects BUILD mode in chat and follows it step-by-step.

### System Prompt: BUILD Mode Update

Update the BUILD mode workflow in `SYSTEM_PROMPT` to reference the skill and distinguish chat vs one-shot behavior.

### Environment Staging

After build, pipeline runs in dev first. User must confirm before promotion to production.

## Acceptance Criteria

- [ ] New skill file `src/seeknal/workflow/templates/skill_pipeline_design.md.j2` with 4 brainstorm phases
- [ ] System prompt updated: chat BUILD mode loads skill, one-shot auto-proceeds
- [ ] Phase 1 (Data Scope): agent shows profile_data output, asks which files + entities
- [ ] Phase 2 (Architecture): agent asks pipeline type (analytics/ML/full), suggests silver transforms
- [ ] Phase 3 (ML): conditional — only if ML selected. Asks approach + features
- [ ] Phase 4 (Confirm): shows complete DAG, waits for "go" / "modify" / "cancel"
- [ ] After "go": agent builds all nodes, runs in dev, shows results via inspect_output
- [ ] After dev success: asks "Promote to production?" — only promotes on explicit confirmation
- [ ] One-shot mode: auto-proceeds with design shown for transparency (no interactive questions)
- [ ] Skill file is a Jinja2 template that can reference `{{ project_name }}` context

## Technical Approach

### Phase 1: Create Skill File

**File:** `src/seeknal/workflow/templates/skill_pipeline_design.md.j2`

```markdown
# Design Data Pipeline (Interactive)

Guide the user through designing a data pipeline before building it.

## Phase 1: Data Scope

After calling `profile_data()`, present the results and ask:

**Question 1 — Data Sources:**
"I found these data files. Which should be included?"
- All of them (recommended)
- [list each file with row count]
- Let me specify...

**Question 2 — Key Entities:**
"Based on shared columns, I detected these entities:"
- [auto-list from profile_data join keys]
- "Type your own entities..."

Wait for user response before proceeding.

## Phase 2: Pipeline Architecture

**Question 3 — Pipeline Type:**
"What kind of pipeline do you want?"
1. Analytics (sources → transforms → business metrics/dashboards)
2. ML Pipeline (sources → feature engineering → model training)
3. Full Stack (analytics + ML + reporting)
4. Custom — describe what you need

**Question 4 — Silver Transforms:**
Based on join keys from Phase 1, suggest:
"I recommend these silver-layer transforms:"
- enriched_orders = orders JOIN customers JOIN products (via customer_id, product_id)
- customer_360 = per-customer aggregations
- [other suggestions based on data]
"Add, remove, or modify?"

## Phase 3: ML & Features (if ML selected)

**Question 5 — ML Approach:**
1. Clustering (KMeans — segment customers/products)
2. Classification (predict churn, conversion)
3. Regression (predict revenue, LTV)
4. Skip ML

**Question 6 — Feature Engineering:**
"Based on your data, I suggest these features:"
- Recency (days since last activity)
- Frequency (total transactions)
- Monetary (total spend)
- Engagement (event counts, session duration)
"Add or remove features?"

## Phase 4: Confirm & Build

Present complete DAG:
```
Pipeline Design:
  Bronze: source.X, source.Y, ...
  Silver: transform.A, transform.B
  Gold:   transform.C, transform.D
  ML:     feature_group.E → model.F
  Total:  N nodes, M edges
```

"Ready to build? (yes / modify / cancel)"

- **yes**: proceed to build all nodes
- **modify**: ask what to change, loop back to relevant phase
- **cancel**: abort

## Phase 5: Build & Deploy

1. Build all nodes in topological order (draft → edit → dry-run → apply)
2. Run `plan_pipeline()` to verify DAG
3. Run `run_pipeline(confirmed=True, full=True)` — dev environment
4. Call `inspect_output()` on 3+ key nodes
5. Ask: "Pipeline succeeded in dev. Promote to production?"
```

### Phase 2: Update System Prompt

**File:** `src/seeknal/ask/agents/agent.py`

Update the BUILD mode section to detect chat vs one-shot:

```
### 3. Building Pipelines (BUILD mode only)

**In chat mode:**
Load the pipeline design skill and follow its interactive phases:
1. Profile data → ask data scope questions → ask architecture questions
2. If ML requested → ask ML/feature questions
3. Show DAG design → wait for confirmation
4. Build → run in dev → inspect results → ask to promote

**In one-shot mode:**
Show DAG design for transparency, then auto-proceed:
Profile → Design → Build → Run → Inspect
```

The key change: the system prompt tells the agent to ask questions in chat but auto-proceed in one-shot. The skill file provides the structured questions.

### Phase 3: Environment Staging Integration

**No new tools needed** — use existing `run_pipeline` tool. Add system prompt guidance:

```
After building:
- Run pipeline (dev by default): run_pipeline(confirmed=True, full=True)
- Show results: inspect_output on 3+ key nodes
- In chat: ask "Promote to production?" before running with --env prod
- In one-shot: run dev only, user promotes manually
```

For production promotion, the agent can call:
```python
run_pipeline(nodes="", full=True, confirmed=True)  # with env context
```

Or instruct the user to run `seeknal env apply --env prod` manually.

## Implementation Phases

### Phase 1: Skill File (Core)
- [x] Create `src/seeknal/workflow/templates/skill_pipeline_design.md.j2`
- [x] Phases 1-5 with question templates and auto-suggestion logic
- [x] Template variables: `{{ project_name }}`, `{{ connections }}`
- [x] Register in `seeknal init` generation (like other skill files)
- **Verify:** skill file renders correctly with Jinja2

### Phase 2: System Prompt Integration
- [x] Update BUILD mode in `SYSTEM_PROMPT` to reference skill
- [x] Add chat vs one-shot detection logic
- [x] Add environment staging rules (dev first, promote on confirmation)
- [x] Update CRITICAL RULES section
- **Verify:** one-shot still auto-proceeds, chat asks questions

### Phase 3: Testing
- [ ] Test in `seeknal ask chat` with e-commerce dataset
- [ ] Verify 4 phases execute in order
- [ ] Verify user can modify design at Phase 4
- [ ] Verify pipeline builds and runs after confirmation
- [ ] Verify one-shot mode is unaffected
- [ ] Update QA spec if applicable

## Dependencies & Risks

- **Risk:** Gemini may not follow the multi-turn question flow reliably
  - **Mitigation:** Skill file uses explicit "Wait for user response" instructions; sync fallback handles empty responses
- **Risk:** Chat session doesn't have state between turns (each turn is independent)
  - **Mitigation:** LangGraph MemorySaver checkpointer maintains conversation history; agent can reference previous answers
- **Dependency:** `profile_data()` tool must work correctly (already tested)
- **Dependency:** `inspect_output()` tool must work after run (already tested)

## Sources & References

### Origin
- **Brainstorm document:** [docs/brainstorms/2026-03-17-interactive-pipeline-brainstorm-brainstorm.md](docs/brainstorms/2026-03-17-interactive-pipeline-brainstorm-brainstorm.md)
  - Key decisions: chat mode only, structured phases, skill file implementation, dev-first staging

### Internal References
- Existing skill templates: `src/seeknal/workflow/templates/skill_*.md.j2`
- System prompt: `src/seeknal/ask/agents/agent.py:54-210`
- Chat session: `src/seeknal/ask/streaming.py:361-396`
- Env system: `src/seeknal/cli/main.py` — `env_plan`, `env_apply`, `env_promote`
- Profile tool: `src/seeknal/ask/agents/tools/profile_data.py`

### External References
- Inspiration: [ce:brainstorm skill pattern](https://raw.githubusercontent.com/EveryInc/compound-engineering-plugin/refs/heads/main/plugins/compound-engineering/skills/ce-brainstorm/SKILL.md)
