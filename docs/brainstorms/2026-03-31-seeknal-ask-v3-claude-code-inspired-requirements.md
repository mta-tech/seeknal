---
date: 2026-03-31
topic: seeknal-ask-v3-claude-code-inspired
---

# Seeknal Ask v3: Claude Code-Inspired Agent Improvements

## Problem Frame

Seeknal Ask just migrated to pydantic-deepagents (v3 harness), but the agent architecture is still naive: a monolithic 152-line system prompt with all instructions upfront, a flat 12-tool toolset with no progressive disclosure, no memory across sessions, no domain-aware compaction, and generic string-based error handling. These limitations cause:

- **Token waste**: Simple "how many customers?" questions pay ~1,500 tokens of Evidence.dev report instructions they'll never use
- **Context overflow in long sessions**: SQL result tables accumulate unbounded (2-5K tokens each), degrading analysis quality after 5-10 turns
- **Cold start on every session**: The agent re-discovers schema quirks, business definitions, and working patterns from scratch
- **Slow error recovery**: Generic error strings cause 2-3 retry loops for common DuckDB mistakes
- **Context pollution**: Lineage questions dump 5K+ tokens of YAML into the analytical context

Seven targeted improvements — inspired by Claude Code's architecture and grounded in pydantic-deep's existing capabilities — address these issues as a cohesive enhancement package.

## Requirements

### R1. Report Skills System
- R1.1. Extract Evidence.dev report instructions (~80 lines) from the monolithic system prompt into one or more skill `.md` files
- R1.2. Register skills via pydantic-deep's `SkillsToolset` with `include_skills=True`
- R1.3. Base system prompt includes only skill name + one-line description (~50 tokens)
- R1.4. Full skill content loads on demand when agent calls `load_skill`
- R1.5. At minimum, create a "report-generation" skill containing the Evidence.dev syntax rules, chart component reference, report quality bar, and section pattern

### R2. Modular System Prompt
- R2.1. Split the monolithic SYSTEM_PROMPT into composable sections: core analyst identity, DuckDB rules, security rules, data context
- R2.2. Report instructions become a skill (R1), not a prompt section
- R2.3. Data context section uses pydantic-deep's `get_instructions()` for dynamic per-turn injection rather than static `{context}` replacement
- R2.4. Base prompt (without data context or skills) should be under 500 tokens
- R2.5. Each section is independently testable (can assert on section content without loading the full prompt)

### R3. Snip-Based SQL Result Compaction
- R3.1. Implement a custom `HistoryProcessor` that scans message history for SQL result markdown tables
- R3.2. Replace old result tables (from turns older than the last 2) with a compact digest: SQL query text, row count, column names, key aggregate stats
- R3.3. Preserve the agent's interpretation text (the narrative around the results) unchanged
- R3.4. Register the processor via `create_deep_agent(history_processors=[...])`
- R3.5. Digest format should be human-readable so the agent can reference prior findings by name

### R4. Project Memory with Analysis Fingerprints
- R4.1. Enable `include_memory=True` with memory stored at `.seeknal/ask_memory.md`
- R4.2. Agent can write to memory via the built-in memory tools (read_memory, write_memory, update_memory)
- R4.3. Memory is loaded into the system prompt at session start
- R4.4. Encourage the agent (via system prompt instruction) to persist: schema quirks, business definitions, column corrections, and successful query patterns
- R4.5. Memory file is project-scoped (per seeknal project directory)

### R5. POST_TOOL_USE Self-Correction Hooks
- R5.1. Add a POST_TOOL_USE hook matching `execute_sql` that fires when the tool returns an error
- R5.2. Parse DuckDB error messages into categories: missing_table, missing_column, syntax_error, type_error, other
- R5.3. For missing_table/missing_column errors: auto-run DESCRIBE on the referenced table and inject the actual schema as a correction hint via `HookResult.modified_result`
- R5.4. For DuckDB dialect errors (e.g., wrong timestamp syntax): inject the correct DuckDB syntax pattern
- R5.5. Log persistent error patterns (3+ occurrences) to project memory (R4)

### R6. Lineage Investigator Subagent
- R6.1. Define a named subagent "lineage_investigator" with access to only: search_pipelines, read_pipeline, search_project_files, read_project_file
- R6.2. The main agent can delegate lineage questions via pydantic-deep's SubAgentToolset
- R6.3. The subagent returns a concise summary (under 500 tokens) instead of raw YAML content
- R6.4. The main agent's context is not polluted with pipeline file contents

### R7. Structured Error Taxonomy
- R7.1. Define error categories: `retryable_syntax`, `retryable_missing_ref`, `retryable_type`, `terminal_security`, `terminal_crash`, `terminal_timeout`
- R7.2. Tool error returns include: `{category, retryable, message, hint}`
- R7.3. Apply to execute_sql and execute_python tools at minimum
- R7.4. The agent's system prompt includes guidance on how to handle each error category

## Success Criteria

- Simple data queries ("how many customers?") use <500 tokens of system prompt (excluding data context)
- A 10-turn chat session with 3 SQL queries per turn stays under 50K tokens of message history (via snip compaction)
- Second session on the same project starts with memory of prior schema discoveries
- Common SQL errors (missing column, wrong timestamp syntax) are auto-corrected in 1 retry instead of 3
- Lineage questions don't evict recent SQL results from context
- All 12 existing tools continue working identically
- Existing QA tests pass without modification

## Scope Boundaries

- **NOT changing**: Tool function bodies (execute_sql, execute_python, etc. stay identical)
- **NOT changing**: CLI command surface (seeknal ask, seeknal ask chat, seeknal ask report)
- **NOT changing**: Security model (SQL validation, filesystem protection, sandbox isolation)
- **NOT adding**: Web UI, API endpoints, Jupyter widgets
- **NOT adding**: Pipeline modification/generation capabilities
- **NOT doing**: Intent classification or keyword-based routing (per memory constraint)

## Key Decisions

- **Skills over prompt sections for report instructions**: Skills are the right abstraction because report instructions are needed only ~20% of the time but cost ~1,500 tokens. The agent autonomously decides when to load the skill — no keyword matching.
- **Snip over summarize for SQL results**: LLM summarization is expensive and lossy. Snipping (mechanical replacement of markdown tables with structured digests) is deterministic, cheap, and preserves the exact information the agent needs.
- **Memory at project level, not global**: Different seeknal projects have different schemas, business definitions, and query patterns. Memory is scoped to the project directory.
- **POST_TOOL_USE for self-correction, not PRE_TOOL_USE**: Correction hints come AFTER seeing the error, not before. This is reactive enrichment, not gatekeeping.
- **Named subagent for lineage, not a general delegation pattern**: Only lineage questions benefit from isolation. SQL and Python analysis should stay in the main agent's context.

## Dependencies / Assumptions

- pydantic-deep's SkillsToolset, memory, and SubAgentToolset work as documented
- pydantic-deep's `history_processors` parameter accepts custom HistoryProcessor implementations
- POST_TOOL_USE hooks can modify the tool result via `HookResult.modified_result`
- DuckDB error messages are parseable enough for basic categorization

## Outstanding Questions

### Deferred to Planning
- [Affects R1][Needs research] How to structure skill `.md` files — single comprehensive skill vs multiple domain-specific templates?
- [Affects R3][Technical] Exact HistoryProcessor interface in pydantic-deep — what message types are available for scanning?
- [Affects R4][Technical] How does pydantic-deep's memory system handle the memory_dir path relative to the agent's backend?
- [Affects R5][Technical] Can POST_TOOL_USE hooks distinguish between error returns and successful returns? (The hook fires on both)
- [Affects R6][Technical] How to configure SubAgentConfig with a restricted tool set from the existing FunctionToolset?
- [Affects R7][Technical] Whether to return structured JSON in error strings (agent parses) or use a separate error metadata channel

## Next Steps

-> `/ce:plan` for structured implementation planning (all 7 requirements as a cohesive v3 enhancement)
