---
date: 2026-03-25
topic: seeknal-ask-subagents
focus: implement subagents in seeknal ask using deepagents framework
---

# Ideation: Seeknal Ask Subagent Architecture

## Codebase Context

**Current Architecture:**
- Single LLM agent (Gemini 2.5 Flash) with 10-21 tools across 3 profiles (analysis/build/full)
- Built on LangGraph with deepagents backend (SubAgentMiddleware available but dormant)
- Ralph Loop: 3-retry nudge for empty responses
- Modular Jinja2 prompts (5-12KB depending on profile)
- Single DuckDB REPL connection with threading locks
- QA score: 52/53 (98%), 0 nudges after Round 5

**Deepagents Framework (already integrated):**
- SubAgentMiddleware: adds `task()` tool for synchronous delegation
- TypedDict subagent specs: name, description, system_prompt, tools, model override
- State isolation: subagent gets clean message history, filtered state
- AGENTS.md config: YAML frontmatter + markdown body
- Parallel invocation: multiple `task()` calls in one message

**Key Constraints (from institutional learnings):**
- NO keyword-based intent detection (let LLM decide autonomously)
- System prompt instructions != behavior (need structural enforcement)
- Token efficiency paramount (prompt went from 11.7KB to 5KB)
- Sub-agents deferred 3 times as "future work" (Mar 2026)
- Bridge pattern needed for state transfer between isolated contexts

## Ranked Ideas

### 1. Activate the Existing Subagent ("Intern Pattern")
**Description:** deepagents already creates a general-purpose subagent and SubAgentMiddleware provides the `task()` tool. Activation = adding delegation guidance to `core.j2` with 3-4 task templates. No new code, no new subagent specs.
**Rationale:** Lowest-risk entry point. Tests whether the LLM can effectively self-delegate before investing in specialization. Zero new infrastructure.
**Downsides:** General-purpose subagent has ALL parent tools (no structural enforcement). May not produce meaningfully better results than parent doing the work.
**Confidence:** 90%
**Complexity:** Low
**Status:** Explored (brainstormed as Phase 1)

### 2. Build Delegation Subagent with Self-Correction
**Description:** Parent designs the DAG, then delegates the entire build phase to a specialized subagent with only write-path tools (draft_node, edit_file, dry_run_draft, apply_draft). The builder handles the draft-validate-fix loop internally. Returns structured summary.
**Rationale:** Addresses #1 bottleneck: 20+ sequential tool calls for a 5-node pipeline. Error recovery in isolated context. Reduces parent calls from ~20 to ~5.
**Downsides:** Parent must produce sufficiently detailed plan. Adds LLM init latency per delegation.
**Confidence:** 85%
**Complexity:** Medium
**Status:** Explored (brainstormed as Phase 2)

### 3. Context Firewall: Subagents as Context Management
**Description:** Primary value of subagents is context window management, not specialization. Route all heavy tool execution through subagents that consume 20KB internally but return 200-byte summaries. Parent context stays under 10KB regardless of complexity.
**Rationale:** Reframes the justification from "smarter routing" (rejected 3 times) to "context management" (never addressed). LLM quality degrades with context length.
**Downsides:** Adds latency for simple tasks. Parent loses visibility into intermediate results.
**Confidence:** 80%
**Complexity:** Medium
**Status:** Explored (brainstormed as Phase 3)

### 4. Post-Run Verification Subagent
**Description:** After run_pipeline succeeds, automatically spawn read-only subagent with execute_sql, inspect_output, execute_python. Checks NULL rates, row counts, join fanout, value ranges. Replaces Ralph Loop inspect nudge with structural enforcement.
**Rationale:** Structural enforcement > prompt enforcement. The inspect nudge exists because the agent skips inspection. A verification subagent guarantees it. Fresh context eliminates confirmation bias.
**Downsides:** Adds 5-15s latency after every pipeline run. May produce false positives for legitimate patterns.
**Confidence:** 80%
**Complexity:** Low-Medium
**Status:** Explored (brainstormed as Phase 4)

### 5. Speculative Data Discovery Subagent
**Description:** At session start, profile ALL data sources in parallel: column types, null rates, cardinality, join key candidates. Produce structured "data map" injected before parent's first tool call. Parent never calls profile_data.
**Rationale:** Discovery burns 2-5 tool calls before agent can plan. Post-profile stall detection exists because agent profiles but doesn't act. Pre-computed discovery eliminates this.
**Downsides:** Cold-start latency for large datasets. DuckDB single-connection constraint limits true parallelism. Wasted work for simple questions.
**Confidence:** 75%
**Complexity:** Medium
**Status:** Explored (brainstormed as Phase 5)

### 6. Profiles-as-Subagents (Replace Static Profile System)
**Description:** Replace analysis/build/full profiles with subagent delegation. Parent becomes thin router with only task() tool. Profile auto-detection (select_profile with keyword matching) deleted. LLM decides which subagent via tool calls.
**Rationale:** Resolves tension between "no keyword detection" (autonomy feedback) and "structural enforcement" (post-mortem). Profiles-as-subagents provide both.
**Downsides:** Larger refactor. Every interaction requires subagent call. Regression risk to QA baseline.
**Confidence:** 70%
**Complexity:** Medium-High
**Status:** Explored (brainstormed as Phase 6)

## Rejection Summary

| # | Idea | Reason Rejected |
|---|------|-----------------|
| 1 | SQL Artisan Subagent | SQL generation interleaved with YAML; separation adds coordination overhead |
| 2 | Async Pipeline Execution | AsyncSubAgentMiddleware requires remote LangGraph server; CLI users wait anyway |
| 3 | Pre-Build Design Critic | Adds latency for validation dry_run mostly covers; better as build subagent internal |
| 4 | Adversarial Pipeline Stress Tester | Too complex for v1; post-run verification covers 80% of value |
| 5 | Multi-Model Delegation | Feature of subagent specs, not standalone idea |
| 6 | Executor-Mirror Subagents | Over-engineering; one build subagent handles all node types |
| 7 | Auto-Source Generation | Good tool idea but not a subagent pattern |
| 8 | Session Continuity | Too complex; better addressed by deepagents memory middleware |
| 9 | Intent Decomposer | Build subagent receiving structured plan already implies decomposition |
| 10 | SQL Dialect Translation | Strategic gap but too expensive; Spark barely used in ask |
| 11 | Peer Agents (No Orchestrator) | Too radical for current pain points |
| 12 | Dynamic Subagent Spec Generation | Interesting metaprogramming but premature |
| 13 | Analyst Ensemble | DuckDB single-connection constraint kills parallelism |
| 14 | Tournament Drafting | 3x cost for uncertain improvement |
| 15 | Pipeline Modification Surgeon | Great future idea; too complex for initial implementation |
| 16 | Federated DuckDB Pool | Infrastructure enabler, not standalone value |
| 17 | Hot-Path / Cold-Path Split | Overlaps with profiles-as-subagents but less elegant |
| 18 | Context-Compressing Relay Chain | Vision compelling but massive implementation; context firewall achieves 80% |
| 19 | Schema Evolution Negotiator | Niche; better as build subagent knowledge |
| 20 | Skill-as-Subagent | Converges with profiles-as-subagents; merged |
| 21 | Conversation Handoff | Better solved by project config and artifact discovery |

## Session Log
- 2026-03-25: Initial ideation — 48 raw ideas generated across 6 frames, ~27 unique after dedup, 6 survived adversarial filtering. All 6 survivors explored via brainstorm (requirements doc: `docs/brainstorms/2026-03-25-seeknal-ask-subagents-requirements.md`)
