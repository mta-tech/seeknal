---
date: 2026-03-26
topic: interactive-ask-user
---

# Interactive Agent Questions for Seeknal Ask

## Problem Frame

When users type ambiguous requests in `seeknal ask chat` (e.g., "build me a pipeline"), the agent guesses at pipeline shape, entity keys, time columns, and output targets. Wrong guesses waste 2-5 minutes of build time and often require the user to start over. The agent's system prompt says "ask design questions ONE at a time" but there is no mechanism for the agent to pause execution, present options, and collect user input mid-run.

Users need: the agent asks targeted questions with selectable options when genuinely uncertain, collects answers, and proceeds with confidence.

## Requirements

### Core Primitive

- R1. A new `ask_user` LangChain tool that the agent calls mid-execution to ask a single question with 2-4 selectable options plus a "Type your own" free-text fallback.
- R2. Options render as an arrow-key navigable menu in the terminal using `simple-term-menu`. User navigates with arrow keys and presses Enter to select. Selecting "Type your own" drops to an inline `input()` prompt.
- R3. The tool blocks execution until the user responds, then returns the selected option text (or free-text input) as the tool result. The agent continues with the answer.
- R4. In one-shot mode (`seeknal ask "question"`), `ask_user` returns a skip sentinel (`"(skipped — no interactive input available)"`) so the agent proceeds with its best guess. One-shot mode must never block on stdin.
- R5. `ask_user` is registered in all tool profiles (analysis, build, full). The agent decides autonomously when to call it — no keyword-based gating in the harness.

### Schema-Seeded Options

- R6. The `ask_user` tool accepts an `options: list[str]` parameter. The agent populates options from live schema data (column names from `describe_table`, join key candidates from `profile_data`) so users pick from real data, not from memory.
- R7. When proposing a default, the agent places its recommended option first and marks it (e.g., "(recommended)"). The option-first pattern: "I'll use `customer_id` as entity key. (1) Yes, use it (2) `user_id` (3) Type your own".

### Question Budget

- R8. A `max_questions` counter on `ToolContext` (default: 5, configurable via `seeknal_agent.yml` key `max_questions`). Each `ask_user` call decrements the counter. When exhausted, `ask_user` returns `"(question budget exceeded — proceeding with best-effort assumptions)"` and the agent continues autonomously.
- R9. The budget resets per invocation of `stream_ask` (per turn in chat mode).

### Opinionated Defaults

- R10. A `defaults:` section in `seeknal_agent.yml` with free-form key-value pairs. Any key-value pair is injected into the system prompt as context. The agent interprets them and uses them to pre-answer questions it would otherwise ask.
- R11. When a default exists for a question the agent would ask, the agent skips the question and uses the default. No ask_user call is made for pre-answered questions.

### Application-Level Guidance (Prompt-Driven)

- R12. The `build.j2` prompt template includes guidance for when to call `ask_user`:
  - After `dry_run_draft` fails: ask with fix options (retry automatically / show YAML / skip node)
  - Before `apply_draft` in chat mode: ask to confirm the draft content
  - After `inspect_output`: ask "Does this look right?" with options (yes / describe issue)
- R13. These are prompt suggestions, not hardcoded tool behavior. The agent decides autonomously whether to follow them based on context.

### Streaming Integration

- R14. The `streaming.py` event loop recognizes `ask_user` as a tool call and renders the question via `_show_tool_start`. The actual menu interaction happens inside the tool (via `ToolContext.console`), not in the streaming layer.
- R15. While `ask_user` is blocking on user input, the streaming event loop is paused (tool hasn't returned yet). This is the natural behavior of LangGraph tool execution.

## Success Criteria

- Agent can ask clarifying questions in chat mode and proceed with answers
- One-shot mode is unaffected (no blocking, no regression on 98% QA baseline)
- Arrow-key menu renders cleanly in standard terminals (macOS Terminal, iTerm2, VS Code terminal)
- Question budget prevents infinite question loops
- Defaults in seeknal_agent.yml suppress known-answer questions
- The agent uses ask_user for schema-related decisions (entity key, time column) where multiple plausible options exist

## Scope Boundaries

- No keyword-based intent detection in the harness — agent calls ask_user autonomously
- No persistent preference learning (v2) — defaults are static in seeknal_agent.yml
- No checkpoint resumption or session persistence — out of scope
- No $EDITOR integration — out of scope
- No multi-select questions in v1 — single-select only
- Application-level question patterns (dry-run recovery, pre-apply confirm, post-inspect feedback) are prompt guidance only, not hardcoded in tool code
- The March 17 brainstorm's "4 rigid phases" approach is superseded by this general-purpose tool approach

## Key Decisions

- **General tool > rigid phases**: `ask_user` as a general tool the agent calls autonomously replaces the March 17 brainstorm's hardcoded 4-phase interview. Respects the autonomy constraint.
- **simple-term-menu for arrow selection**: Lightweight dependency, renders arrow-key navigable menus. Selected over questionary (heavier) and custom Rich (more code).
- **Skip sentinel in one-shot**: Returns descriptive skip message, not an error. Agent proceeds with best guess. One-shot happy path is fully preserved.
- **Prompt guidance over hardcoded hooks**: Application-level patterns (dry-run recovery, etc.) live in `build.j2` as agent guidance, not as code in tools. The agent decides when they apply.
- **Free-form defaults**: `seeknal_agent.yml` defaults section accepts any key-value pairs, not a predefined schema. More flexible, simpler to implement.
- **Budget of 5**: Balanced default — enough for a scoping interview, not an interrogation. Configurable.

## Dependencies / Assumptions

- `simple-term-menu` package must be added to pyproject.toml dependencies
- `ToolContext` already carries `console` — ask_user will use it for rendering
- LangGraph tool execution is synchronous within a thread — blocking on input is safe
- `asyncio.to_thread` is already used for `input()` in `chat_session` — same pattern applies

## Outstanding Questions

### Deferred to Planning
- [Affects R2][Technical] How to handle terminal environments that don't support arrow keys (piped input, CI, Jupyter). Graceful fallback to numbered input()?
- [Affects R14][Technical] Exact integration point: does ask_user render via `_show_tool_start` in streaming.py, or does it handle its own rendering entirely within the tool?
- [Affects R1][Needs research] Does simple-term-menu work within an asyncio.to_thread context, or does it need special handling for the event loop?

## Next Steps

-> /ce:plan for structured implementation planning
