---
date: 2026-03-31
topic: seeknal-ask-claude-code-inspired-improvements
focus: "System prompt, agent harness, skills, progressive disclosure, subagents, compaction — inspired by Claude Code architecture"
---

# Ideation: Seeknal Ask Agent Improvements (Claude Code-Inspired)

## Codebase Context

**Seeknal Ask** is a data analysis agent with 12 tools (SQL, Python, file search, reports) that just migrated from LangChain deepagents to pydantic-deepagents. Current architecture:
- Monolithic 152-line system prompt with `{context}` placeholder
- Flat FunctionToolset with all 12 tools always loaded
- PRE_TOOL_USE hook for SQL security validation
- Rich streaming UI with Ralph Loop retry
- No skills system, no subagents, no memory, no custom compaction
- `context_manager=True` for auto-summarization (framework default)

**Claude Code patterns studied:**
- Modular system prompt with static/dynamic cache boundary
- Progressive skill disclosure (frontmatter-only index, full content on demand)
- Tool deference (ToolSearch for lazy schema loading)
- Fork/named subagents with context isolation
- Multi-strategy compaction (snip + summarize + post-restore)
- Dynamic tool descriptions, concurrency safety flags

**Institutional learnings:** "Agent harness must NOT use keyword matching for intent; let the AI agent decide autonomously"

## Ranked Ideas

### 1. Report Skills System
**Description:** Extract ~80 lines of Evidence.dev report instructions from the monolithic system prompt into domain-specific report skills (`.md` files with frontmatter + templates). Only skill name/description (~50 tokens) appears in base prompt. Full content loads on demand via `load_skill`. Leverages pydantic-deep's existing SkillsToolset.
**Rationale:** Most questions are simple data queries paying ~1,500 tokens of Evidence.dev overhead. Skills save those tokens AND enable richer template-based report generation when needed.
**Downsides:** Report template maintenance as separate artifacts. One extra round-trip for skill loading.
**Confidence:** 85%
**Complexity:** Low
**Status:** Explored

### 2. Modular System Prompt with Conditional Sections
**Description:** Split the 152-line monolithic SYSTEM_PROMPT into composable sections: core analyst identity (~20 lines), DuckDB rules (~15 lines), data context (dynamic via `get_instructions()`), report instructions (via skill), security rules (~5 lines). Only relevant sections appear per-turn.
**Rationale:** Mirrors Claude Code's architecture. Reduces base prompt from ~2,000 to ~500 tokens for simple questions. Enables prompt caching in multi-turn chat.
**Downsides:** More moving parts in prompt assembly. Risk of missing an edge-case section.
**Confidence:** 80%
**Complexity:** Medium
**Status:** Explored

### 3. Snip-Based SQL Result Compaction
**Description:** Custom `HistoryProcessor` that replaces old SQL result markdown tables with compact digests: `{query_sql, row_count, columns, key_stats}`. Preserves SQL text and agent's interpretation, discards raw rows.
**Rationale:** SQL results dominate context growth (2-5K tokens each). Snipping to ~200 tokens reclaims 90% without losing analytical continuity. pydantic-deep already accepts `history_processors`.
**Downsides:** Agent can't re-read old results (must re-query). Digest format needs careful design.
**Confidence:** 85%
**Complexity:** Low
**Status:** Explored

### 4. Project Memory with Analysis Fingerprints
**Description:** Enable pydantic-deep's memory with `.seeknal/ask_memory.md`. Persist schema quirks, business definitions, column corrections, and "analysis fingerprints" (SQL patterns that worked, useful table/column combos). Loaded into system prompt at session start.
**Rationale:** Every session starts cold. Memory compounds expertise — the 10th session should be dramatically better than the 1st. Infrastructure (`include_memory=True`) already exists.
**Downsides:** Memory can become stale. Needs invalidation mechanism. Adds ~200-500 tokens per session.
**Confidence:** 75%
**Complexity:** Low
**Status:** Explored

### 5. POST_TOOL_USE Hooks for Self-Correction
**Description:** Hooks that intercept `execute_sql` errors, parse DuckDB error type, inject structured correction hints. "Column not found" -> auto-DESCRIBE + inject schema. DuckDB dialect error -> inject correct syntax. Persistent patterns written to memory.
**Rationale:** Cuts retry loops from 3 to 1 for common SQL mistakes. Combined with memory, same mistake never happens twice across sessions.
**Downsides:** Must parse DuckDB error messages reliably. Risk of over-helping.
**Confidence:** 70%
**Complexity:** Medium
**Status:** Explored

### 6. Lineage Investigator Subagent
**Description:** Named subagent with only pipeline/file tools. Delegates lineage questions ("how is this table calculated?") to specialist that returns a clean summary instead of polluting main agent's SQL-focused context.
**Rationale:** Pipeline files generate 5K+ tokens of YAML. Mid-chat lineage questions evict recent SQL results. Subagent isolation preserves analytical state.
**Downsides:** Subagent orchestration complexity. Agent must decide when to delegate.
**Confidence:** 65%
**Complexity:** Medium
**Status:** Explored

### 7. Structured Error Taxonomy for Tool Failures
**Description:** Replace generic `"SQL execution error: {e}"` with typed returns: `{retryable, category, hint}`. Distinguish retryable (syntax, missing table) from terminal (security, crash).
**Rationale:** Agent retries identically regardless of error type. Structured errors enable smarter recovery — corrected SQL for "missing column", give up for "security blocked".
**Downsides:** Requires parsing DuckDB error messages. Taxonomy may not cover all cases.
**Confidence:** 65%
**Complexity:** Medium
**Status:** Explored

## Rejection Summary

| # | Idea | Reason Rejected |
|---|------|-----------------|
| 1 | Tool Concurrency Classification | pydantic-ai lacks parallel tool dispatch |
| 2 | Intent-Classified Skill Prefetch | Violates "let AI decide autonomously" constraint |
| 3 | Self-Assembling Agent by Complexity | Same — keyword scoring IS intent routing |
| 4 | Query Result Caching | DuckDB sub-100ms; caching adds complexity for negligible gain |
| 5 | Streaming as Async Generator | No consumer exists — YAGNI |
| 6 | SQL Dry-Run via EXPLAIN | EXPLAIN cost ≈ execution cost in DuckDB |
| 7 | Artifact-Aware Context Refresh | Rare edge case; over-engineering |
| 8 | Pipeline Suggestion Tool | Scope creep — violates "ask is read-only" boundary |
| 9 | Speculative Query Prefetch | Speculative LLM calls mostly miss; too expensive |
| 10 | Checkpoint Branching | Better to fix Evidence syntax fixer |
| 11 | Schema Narrowing via Skill Resources | Over-engineered — describe_table already does this |
| 12 | Dynamic get_instructions() | Merged into Modular System Prompt |
| 13 | Parallel Hypothesis Forks | Too ambitious for current maturity |
| 14 | Report Scout Subagent | Subsumed by Report Skills (templates skip exploration) |
| 15 | Multi-Entity Report Subagents | Too ambitious for current maturity |
| 16 | Post-Compaction Restoration | Merged into Snip-Based Compaction |
| 17 | Analysis Playbooks | Duplicates Report Skills with different framing |
| 18 | Tiered Toolset with Deferred Tools | pydantic-ai doesn't support tool deference natively |

## Session Log
- 2026-03-31: Initial ideation — 32 candidates from 4 parallel agents, 7 survived adversarial filtering. All 7 marked Explored for brainstorming.
