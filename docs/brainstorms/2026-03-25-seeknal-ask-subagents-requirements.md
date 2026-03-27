---
date: 2026-03-25
topic: seeknal-ask-subagents
---

# Seeknal Ask: Subagent Architecture

## Problem Frame

The seeknal ask agent is a single-agent system that handles everything — data discovery, pipeline design, node creation, validation, execution, and inspection — in one context window. This creates three compounding problems:

1. **Context window pollution**: A 5-node pipeline build generates 20-40KB of tool output (schema dumps, YAML drafts, dry-run results, pipeline logs). By node 4-5, the agent "forgets" earlier decisions, producing inconsistent refs and broken joins. Complex pipelines (10+ nodes) reliably degrade.

2. **Sequential bottleneck**: Each node requires 3-6 tool calls (draft, edit, dry_run, apply). A 5-node pipeline = 15-30 tool calls at 10-30s each = 5-15 minutes of serial execution. There is no mechanism to batch or parallelize.

3. **Structural enforcement gap**: Tool profiles provide structural enforcement (analysis mode literally cannot call write tools), but profile selection uses keyword matching (`_BUILD_KEYWORDS`, `_REPORT_KEYWORDS`) — the exact anti-pattern flagged in the autonomy feedback. The harness decides intent instead of letting the LLM decide.

The deepagents framework (already integrated as a dependency, `SubAgentMiddleware` available but dormant) provides the mechanism to solve all three: state-isolated subagent delegation via a `task()` tool that the LLM invokes autonomously.

## Requirements

### Phase 1: Activate Existing Subagent ("Intern Pattern")

- R1. The agent's system prompt teaches when and how to use the `task()` tool for delegation, with 3-4 task templates (data profiling, node building, pipeline inspection)
- R2. The existing general-purpose subagent (auto-created by `create_deep_agent()`) is available for delegation with all parent tools
- R3. The agent decides autonomously when to delegate vs. handle directly — no keyword-based routing in the harness
- R4. Subagent results appear as tool results in the parent's conversation, following standard tool result rendering

### Phase 2: Build Delegation Subagent

- R5. A specialized "builder" subagent exists with only write-path tools: `draft_node`, `edit_file`, `dry_run_draft`, `apply_draft`
- R6. The parent agent designs the pipeline (decides node count, types, refs, SQL approach), then delegates the entire build phase to the builder subagent via a single `task()` call with a structured plan
- R7. The builder subagent handles the draft-validate-fix loop internally, iterating up to N times on dry_run failures without returning to the parent
- R8. The builder subagent returns a structured summary: nodes created, validation status, any unfixable errors
- R9. The parent's context window never contains intermediate YAML drafts, dry_run output, or edit_file results from the build phase — only the summary

### Phase 3: Context Firewall

- R10. All tool-heavy operations (profiling, building, execution, inspection) can be routed through subagents that consume full tool output internally but return compact summaries to the parent
- R11. The parent agent's context stays under a reasonable bound (target: <15KB of tool output) regardless of pipeline complexity
- R12. Summary quality is domain-aware: a SQL result summary preserves NULL rates, value ranges, and statistical anomalies — not just truncated rows

### Phase 4: Post-Run Verification Subagent

- R13. After `run_pipeline` succeeds, a read-only verification subagent automatically runs with only `execute_sql`, `inspect_output`, and `execute_python` tools
- R14. The verification subagent checks: NULL rates in output columns, row count sanity (vs. source counts), join fanout detection, type consistency, value range reasonableness
- R15. The verification subagent returns a structured quality report that the parent surfaces to the user
- R16. This replaces the current inspect_output nudge in the Ralph Loop with structural enforcement — the verification always runs, the agent cannot skip it

### Phase 5: Speculative Data Discovery Subagent

- R17. At session start or when a question arrives, a discovery subagent profiles all data sources: column types, null rates, cardinality, sample values, join key candidates
- R18. The discovery subagent produces a structured "data map" that is injected into the parent's context before the first tool call
- R19. The parent agent does not call `profile_data` or `describe_table` for initial discovery — it starts with pre-computed data awareness
- R20. For large datasets (1M+ rows), the discovery subagent uses sampling strategies (first/last/random 10K rows) to stay fast

### Phase 6: Profiles-as-Subagents

- R21. The static profile system (analysis/build/full with keyword-based `select_profile()`) is replaced by subagent delegation
- R22. The parent agent becomes a thin router with the `task()` tool plus minimal direct tools (e.g., `list_tables` for quick lookups)
- R23. An "analyst" subagent has read-only tools; a "builder" subagent has write tools; a "reporter" subagent has report tools
- R24. The LLM decides which subagent to invoke based on the user's question — no keyword matching, no static profile selection
- R25. Each subagent has its own focused system prompt (analyst gets analysis rules, builder gets build workflow + DuckDB rules, reporter gets Evidence.dev syntax). The parent's prompt shrinks to routing guidance only.

### Cross-Cutting: UX & Transparency

- R26. Subagent execution is transparently displayed in the terminal using a Claude Code-style collapsible tree view:
  - Show running subagents with names and descriptions
  - Show per-subagent tool use count and token usage
  - Show current activity per subagent (e.g., "Validating 3 nodes...")
  - Collapsible/expandable via keyboard shortcut
- R27. Subagent errors are surfaced clearly to the user, not silently swallowed. If a subagent fails after N retries, the parent reports the failure with context.
- R28. Each phase preserves or improves the existing QA baseline (currently 52/53, 98%)

### Cross-Cutting: Configuration

- R29. Subagents are defined declaratively (name, description, system_prompt, tools, optional model override) — not hardcoded in Python
- R30. Per-subagent model override is supported: mechanical tasks (validation, source creation) can use cheaper/faster models; complex tasks (SQL design, debugging) use the primary model
- R31. Users can disable specific subagents via `seeknal_agent.yml` project config (e.g., `disabled_subagents: [verification]` for projects that don't need post-run checks)

## Success Criteria

- **Phase 1**: Agent successfully delegates at least one task per session when appropriate. No regression in QA baseline. Delegation is autonomous (no keyword detection added).
- **Phase 2**: A 5-node pipeline build uses <= 8 parent tool calls (down from 20+). Build errors are resolved inside the subagent. Parent context does not contain YAML draft content.
- **Phase 3**: Parent context stays under 15KB of tool output for a 10-node pipeline build.
- **Phase 4**: Every successful pipeline run is followed by automated quality checks. Zero inspect_output nudges needed in the Ralph Loop.
- **Phase 5**: Initial data discovery completes before the parent's first design decision. No `profile_data` calls from the parent agent.
- **Phase 6**: `select_profile()` and `_BUILD_KEYWORDS` / `_REPORT_KEYWORDS` are deleted. Profile selection is purely LLM-driven via subagent choice.
- **All phases**: QA score >= 52/53 (98%). No new keyword-based intent detection.

## Scope Boundaries

- **In scope**: Synchronous subagents via deepagents SubAgentMiddleware. All 6 phases as an incremental roadmap.
- **Not in scope**: Async subagents (AsyncSubAgentMiddleware requires remote LangGraph server infrastructure). Peer-agent architectures (no orchestrator). Dynamic subagent spec generation at runtime. SQL dialect translation (DuckDB-only for now). Pipeline modification/surgery (edit existing pipelines via dependency-aware subagent). Federated DuckDB connection pooling.
- **Explicitly deferred**: Cross-session memory for subagents. Parallel subagent invocation (depends on DuckDB connection concurrency). User-defined custom subagents via AGENTS.md files in the project directory.

## Key Decisions

- **Phased adoption over big-bang**: Each phase is independently shippable and testable. Phase N+1 builds on Phase N. Rollback = revert one phase.
- **Transparent delegation UX**: Claude Code-style collapsible tree view showing subagent activity, tool counts, and token usage. Users see what's happening, not a black box.
- **LLM-driven delegation**: The parent agent decides when to delegate via the `task()` tool. The harness never forces delegation based on keywords or heuristics.
- **Structural enforcement via tool restriction**: Each subagent gets only the tools it needs. The builder cannot query data; the analyst cannot write files. This replaces prompt-based enforcement.
- **Context isolation as primary value**: The main justification for subagents is not smarter routing — it's keeping the parent's context window clean. Every subagent call is a context firewall.
- **Cheap models for mechanical tasks**: Validation, source creation, and inspection subagents can run on cheaper/faster models. Design and debugging stay on the primary model.

## Dependencies / Assumptions

- deepagents `SubAgentMiddleware` works correctly with the current `create_deep_agent()` setup (already integrated, needs activation)
- The global `ToolContext` (shared DuckDB connection with `db_lock`) is accessible from subagent tool calls without modification
- Gemini 2.5 Flash (or the configured model) can effectively use the `task()` tool with structured task descriptions
- The streaming pipeline (`streaming.py`) can be extended to render subagent events in a tree view

## Outstanding Questions

### Deferred to Planning

- [Affects R5, R23][Technical] How should subagent specs be stored? Options: Python dicts in a registry module, AGENTS.md files in the project, or entries in `seeknal_agent.yml`. Need to evaluate deepagents' native loading patterns.
- [Affects R7][Technical] What should the max retry count be for the builder subagent's internal dry_run loop? The parent's Ralph Loop uses 3. The builder may need more (5-7) since each retry is cheaper (isolated context).
- [Affects R9, R12][Needs research] What is the optimal summary format for subagent results? Structured JSON that the parent parses, or natural language that the parent reads? deepagents returns `ToolMessage` with text content — need to verify if structured formats work.
- [Affects R17][Technical] Should the discovery subagent run synchronously (blocking until done) or be fire-and-forget with the parent proceeding and receiving results via state update? Sync is simpler but adds cold-start latency.
- [Affects R26][Needs research] How does `streaming.py` need to change to render subagent events as a tree view? deepagents may emit different event types than the current `on_tool_start`/`on_tool_end` handlers expect.
- [Affects R28][Technical] How to run the QA suite against each phase incrementally? The QA tests use subprocess `seeknal ask` — need to verify subagent behavior is exercised end-to-end.
- [Affects R30][Needs research] Which models work well as cheap subagent models? Gemini 2.0 Flash Lite for validation? Need to test tool-calling capability of cheaper models with seeknal's tool schemas.

## Next Steps

→ `/ce:plan` for structured implementation planning
