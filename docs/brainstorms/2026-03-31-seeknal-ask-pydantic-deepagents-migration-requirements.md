---
date: 2026-03-31
topic: seeknal-ask-pydantic-deepagents-migration
---

# Seeknal Ask: Migrate from LangChain deepagents to Pydantic deepagents

## Problem Frame

Seeknal Ask currently uses LangChain deepagents (`deepagents>=0.4.0`) built on LangGraph. This pulls in a heavy dependency tree (langchain-core, langchain, langgraph, langsmith) that is orthogonal to seeknal's Pydantic/SQLModel/Typer stack. The agent harness lacks lifecycle hooks for cross-cutting security, has rigid middleware ordering, and uses string-based event streaming that requires provider-specific workarounds (Gemini cumulative token dedup).

Pydantic deepagents, built on pydantic-ai, offers a lighter, more composable architecture with Claude Code-style hooks, typed streaming, dependency injection, and YAML declarative specs — all on the same Pydantic foundation seeknal already uses.

## Requirements

- R1. Replace `deepagents.create_deep_agent()` (LangChain) with `pydantic_deep.create_deep_agent()` in the agent factory (`agent.py`)
- R2. Rewrite streaming layer (`streaming.py`) to use `agent.iter()` typed node pattern instead of LangGraph `astream_events()`
- R3. Convert all 12 tools from LangChain `@tool` decorator to pydantic-ai `@toolset.tool` pattern, preserving identical function bodies
- R4. Replace LLM provider setup (`providers.py`) with pydantic-ai model strings (`"google-gla:gemini-2.5-flash"`, `"ollama:llama3.1"`)
- R5. Implement `PRE_TOOL_USE` hook to extract SQL validation from `execute_sql` tool into a cross-cutting security hook
- R6. Replace `MemorySaver` checkpointer with pydantic-ai native message history for multi-turn chat continuity
- R7. Replace `[ask]` optional dependencies: drop `deepagents`, `langchain`, `langgraph`, `langchain-google-genai`, `langchain-ollama`; add `pydantic-deep` (with appropriate extras)
- R8. Preserve the TUI using Typer + Rich (seeknal's existing CLI framework), adopting pydantic-deepagents' TUI patterns where they improve UX
- R9. All existing security boundaries remain intact: SQL validation, filesystem path traversal protection, Python sandbox isolation, credential redaction

## Success Criteria

- All existing `seeknal ask` QA tests pass with the new agent backend
- One-shot mode (`seeknal ask "question"`) works identically
- Interactive chat mode (`seeknal ask chat`) maintains multi-turn conversation
- Hooks system blocks dangerous SQL (same behavior as current in-tool validation, but via hook)
- `langchain`, `langgraph`, `langsmith` no longer appear in `[ask]` dependencies
- Streaming output renders progressively with tool visibility (same UX quality as current Rich panels)

## Scope Boundaries

- **NOT migrating**: security.py, sandbox.py, artifact_discovery, report builder, deterministic reports — these are framework-agnostic and stay as-is
- **NOT adding**: new agent features (teams, skills, plan mode) — migration only, feature parity
- **NOT changing**: CLI command surface (`seeknal ask`, `seeknal ask chat`, `seeknal ask report`)
- **NOT adopting**: pydantic-deepagents' built-in filesystem tools (agent should NOT have general filesystem access, same as current decision)
- **Deferred**: YAML declarative agent spec, persistent memory, checkpoint save/rewind, cost tracking — these are future enhancements enabled by the new foundation

## Key Decisions

- **Full replacement, not coexistence**: Drop LangChain stack entirely. No fallback to `create_react_agent`. The ReAct fallback path was a transitional safety net that's no longer needed.
- **Hooks for security**: SQL validation moves from inside `execute_sql` to a `PRE_TOOL_USE` hook. This is cleaner (cross-cutting concern) and enables future hooks (audit logging, cost gates) without touching tool code.
- **Keep Typer + Rich TUI**: Seeknal's CLI is already Typer-based. Adopt pydantic-deepagents' TUI patterns (lighter than LangChain deepagents' Textual-based TUI) but keep seeknal's existing Rich panel rendering style.
- **Checkpoint is a non-issue**: Current `MemorySaver` is in-memory, session-scoped, lost on exit. Pydantic-ai handles multi-turn via message list accumulation — simpler and equivalent. Optional `FileCheckpointStore` available for future persistence.

## Dependencies / Assumptions

- `pydantic-deep` package is installable via pip and supports Google Gemini and Ollama providers
- pydantic-ai model string format supports seeknal's current providers (`google-gla:*`, `ollama:*`)
- Tool function bodies (SQL execution, Python sandbox, filesystem search) are framework-agnostic and transfer without logic changes

## Outstanding Questions

### Deferred to Planning

- [Affects R4][Needs research] Exact pydantic-ai model string format for Google Gemini and Ollama — verify provider prefix syntax
- [Affects R2][Technical] How to replicate the "Ralph Loop" retry pattern (nudge agent if it ends on tool calls without text) in pydantic-ai's execution model
- [Affects R3][Technical] Whether `ToolContext` singleton pattern maps cleanly to `DeepAgentDeps` or needs restructuring
- [Affects R5][Technical] Whether the `PRE_TOOL_USE` hook can access the full tool input dict to validate SQL strings

## Next Steps

→ `/ce:plan` for structured implementation planning
