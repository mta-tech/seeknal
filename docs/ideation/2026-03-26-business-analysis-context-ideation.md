---
date: 2026-03-26
topic: business-analysis-context
focus: Business analysis context for seeknal ask — glossary, semantic layer integration, concurrent reads, tiered experience by company size
---

# Ideation: Business Analysis Context for Seeknal Ask

## Codebase Context

**Current state:**
- seeknal has a FULL semantic layer (`workflow/semantic/` — SemanticModel, Metric, MetricCompiler with auto-join via EntityGraph, MetricRegistry, MetricDeployer) — 1200+ lines, fully tested. But the AI agent DOESN'T USE IT.
- NO business glossary exists. Agent has zero understanding of business terms.
- Agent context injection is purely technical metadata (tables, columns, pipelines via ArtifactDiscovery).
- DuckDB is in-process with threading locks. No client-server. No FlightSQL.
- No persistent domain knowledge across sessions.

**Reference projects:**
- **nao** (getnao.io): File-based context engineering — `nao sync` introspects warehouses to markdown, `RULES.md` for business rules, Skills as markdown. No semantic layer.
- **KAI**: Three context pillars — Instructions (condition+rules+embedding search), Business Glossary (metric+aliases+canonical SQL), Skills (markdown). Tool-based context loading.

**Key insight:** seeknal uniquely has both production (pipelines, feature stores) AND consumption (AI agent) in one platform. The semantic layer is the bridge — it encodes business meaning in computable form. Connecting it to the agent is the highest-leverage single change.

## Ranked Ideas

### 1. Wire Semantic Layer to Agent (`query_metric` tool)
**Description:** Add a `query_metric` agent tool that compiles MetricQuery objects via the existing MetricCompiler. Inject semantic model metadata into the agent's system prompt via ArtifactDiscovery. The agent uses metric/dimension language instead of raw SQL.
**Rationale:** Everything exists — MetricCompiler, EntityGraph auto-joins, SemanticModelExecutor, `seeknal query` CLI. But the agent is completely unaware. This turns a generic SQL chatbot into a governed metrics interface.
**Downsides:** Requires semantic models to be defined first.
**Confidence:** 90%
**Complexity:** Medium
**Status:** Unexplored

### 2. Unified Glossary via Semantic Layer (aliases + descriptions)
**Description:** Add `aliases: list[str]` to Measure, Dimension, Metric dataclasses. The semantic layer IS the glossary — "MRR" maps to `monthly_recurring_revenue` measure. No separate system.
**Rationale:** One source of truth. KAI's separate glossary drifts from models. Seeknal avoids this by design.
**Downsides:** Less formal than enterprise governance. Sufficient for small-mid companies.
**Confidence:** 85%
**Complexity:** Low
**Status:** Unexplored

### 3. Auto-Bootstrap Semantic Models from Data Profiling
**Description:** Agent uses `profile_data` output to draft semantic model YAML: entity keys from cardinality, dimensions from categoricals, measures from numerics, time dimensions from datetimes. User reviews and approves.
**Rationale:** Cold-start problem killer. Solo founder with CSVs never writes YAML manually. First session bootstraps, subsequent sessions compile.
**Downsides:** Heuristic-based, needs user review.
**Confidence:** 75%
**Complexity:** Medium
**Status:** Unexplored

### 4. Pipeline Run Metadata as Agent Context
**Description:** Read `target/run_state.json` at agent startup. Inject run metadata into prompt: last run, row counts, duration, success/failure.
**Rationale:** ~20 lines. Near-zero effort, high context value.
**Downsides:** Only useful after at least one `seeknal run`.
**Confidence:** 95%
**Complexity:** Low
**Status:** Unexplored

### 5. Conversational Drill-Down (query stack)
**Description:** Maintain query_stack in ToolContext. Follow-ups resolve "that" to previous query. "Break down by region" modifies the last MetricQuery's dimensions.
**Rationale:** How analysts actually work. Existing chat loop supports multi-turn. Missing piece: context carry-over.
**Downsides:** Only works in chat mode.
**Confidence:** 80%
**Complexity:** Low-Medium
**Status:** Unexplored

### 6. Pipeline Impact Analysis Before Apply
**Description:** Before apply_draft, traverse DAG manifest to show downstream blast radius. Affected nodes, row counts, semantic model dependencies.
**Rationale:** Manifest has all edges. `find_downstream_nodes()` exists. Prevents cascade breakage.
**Downsides:** Only valuable for projects with 5+ nodes.
**Confidence:** 85%
**Complexity:** Low
**Status:** Unexplored

### 7. Natural Language Metric Definition ("save as metric")
**Description:** After agent answers via query_metric, offer to save the MetricQuery as a permanent metric YAML. Future questions resolve through it.
**Rationale:** Closes the exploration-to-governance loop.
**Downsides:** Depends on #1.
**Confidence:** 80%
**Complexity:** Low
**Status:** Unexplored

## Rejection Summary

| # | Idea | Reason Rejected |
|---|------|-----------------|
| 1 | Separate Business Glossary (file-based) | Duplicates unified approach — would drift from semantic models |
| 2 | Auto-Harvested Glossary from Conversations | Unreliable LLM extraction; trust problem |
| 3 | Cross-Session Agent Memory | High value but depends on #1-3 being stable first |
| 4 | FlightSQL Serve Mode | Current REPL is in-memory/ephemeral. Future direction, not next step. |
| 5 | Bifurcate Read/Write Agents | Profiles already handle this. Splitting doubles maintenance. |
| 6 | Progressive Disclosure Agent | Absorbed into #1 as fast-path optimization |
| 7 | Metric Disambiguation Tool | Absorbed into #2 (aliases on measures) |
| 8 | Confidence/Provenance Scores | "Confidence" on LLM SQL is theater |
| 9 | Metric Anomaly Detection | No scheduler, alerting, daemon — different product |
| 10 | Headless BI API | Same infra gap as FlightSQL. Premature. |
| 11 | Schema-First Build | Adds friction; current flow works better |
| 12 | Skill Crystallization | Undefined extraction; research project |
| 13 | Portable Project Bundle | git clone + templates serves the need |
| 14 | Producer/Consumer Roles | No auth layer — unenforceable |
| 15 | Live Data Source Connectors | Breadth expansion; focus on depth first |
| 16 | Query Validation vs Semantic Layer | Absorbed into #1 |

## Future Direction: Client-Server / FlightSQL

Not rejected, but sequenced later:
1. Wire semantic layer to agent (local, single-user) — **now**
2. `seeknal serve` with MetricCompiler over HTTP — **when team access needed**
3. FlightSQL for BI tool connectivity — **when enterprise features needed**

The semantic layer work is a prerequisite for any serving mode.

## Session Log
- 2026-03-26: Initial ideation — 48 generated across 6 frames, deduped to 25, 7 survived. Referenced nao (getnao.io) and KAI projects for context patterns. User selected brainstorm #1-#7 full stack.
