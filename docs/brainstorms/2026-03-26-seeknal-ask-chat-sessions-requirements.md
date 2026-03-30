---
date: 2026-03-26
topic: seeknal-ask-chat-sessions
---

# Seeknal Ask Chat Sessions

## Problem Frame

When a user exits `seeknal ask chat` and returns later, all context is lost:
- **Conversation context**: The agent forgets what was discussed — the user must re-explain data, goals, and prior analysis.
- **Build progress**: Long pipeline builds interrupted by timeout, crash, or Ctrl-C cannot be resumed.
- **Iterative workflow**: Users want to work in multiple short sessions (analyze -> leave -> come back -> build report) rather than one long session.

Currently, `MemorySaver()` (in-memory) is used for LangGraph checkpointing. The thread ID is hardcoded to `"default"`. Nothing survives process exit.

## Requirements

- R1. **Named sessions**: Each chat session gets a unique, human-readable name (auto-generated `adjective-noun-number` pattern like `calm-river-315`). Users can optionally provide a custom name via `--name`.
- R2. **Session resume**: `seeknal ask chat --session <name>` resumes a previous session with full conversation context and agent state restored.
- R3. **Default is fresh start**: Plain `seeknal ask chat` always starts a new session. No auto-resume. Explicit `--session` required to continue.
- R4. **Full LangGraph checkpoint persistence**: Sessions persist the complete LangGraph state via `SqliteSaver`, not just message history. This enables exact state recovery including internal agent memory and tool call history.
- R5. **Per-project SQLite storage**: Session data stored in `.seeknal/sessions.db` within the project directory. Each project has its own sessions.
- R6. **Session management CLI**: Full management commands:
  - `seeknal session list` — list all sessions for the current project (name, status, created, last updated, message count)
  - `seeknal session show <name>` — display session details and conversation summary
  - `seeknal session delete <name>` — delete a session (with confirmation)
- R7. **Session metadata**: Each session stores: name, status (active/closed), created timestamp, updated timestamp, message count, last question summary.
- R8. **Session name display**: When entering a chat session (new or resumed), display the session name so the user knows how to resume later (e.g., `Session: calm-river-315 (use --session calm-river-315 to resume)`).

## Success Criteria

- A user can exit `seeknal ask chat`, come back hours/days later, and resume with `--session <name>` — the agent remembers the full conversation and can continue from where it left off.
- A pipeline build interrupted by Ctrl-C can be resumed — the agent sees what was already built and continues.
- `seeknal session list` shows all sessions with enough info to pick the right one.

## Scope Boundaries

- No server/API — this is CLI-only, local SQLite storage
- No cross-project sessions — sessions are scoped to the project directory
- No session sharing or export (can be added later)
- No automatic session cleanup/expiry (manual delete only, can add TTL later)
- No cross-session memory (KAI's "shared memories" tier) — each session is isolated
- One-shot mode (`seeknal ask "question"`) does NOT create sessions — sessions are chat-only

## Key Decisions

- **SQLite per-project over global**: Sessions stored in `.seeknal/sessions.db` in the project root. Keeps sessions co-located with the project. No need for a global registry.
- **SqliteSaver over custom serialization**: LangGraph's `SqliteSaver` handles checkpoint serialization natively. No need to build custom message serialization.
- **Auto-name + optional override**: Auto-generated names remove friction. Optional `--name` allows intentional naming for important sessions.
- **Always-new default**: Avoids accidental context carry-over. Users must explicitly opt into resuming. This is safer for data analysis where stale context could mislead.
- **KAI-inspired naming**: Adjective-noun-number pattern (`calm-river-315`) provides ~4M unique combinations. Memorable and easy to type.

## Dependencies / Assumptions

- `langgraph-checkpoint-sqlite` package provides `SqliteSaver` — needs to be added as a dependency
- `SqliteSaver` supports async operations needed by `stream_ask` in `streaming.py`
- The `.seeknal/` directory may need to be created per-project (check if it already exists)

## Outstanding Questions

### Deferred to Planning

- [Affects R4][Needs research] Does `SqliteSaver` from `langgraph-checkpoint-sqlite` support async? If not, can we use `AsyncSqliteSaver` or need a sync wrapper?
- [Affects R2][Technical] When resuming, should the agent get a system message summarizing what happened in the session so far, or does the full checkpoint restore handle this automatically?
- [Affects R6][Technical] How to display conversation history in `seeknal session show` — should it show raw messages or a summarized view?
- [Affects R5][Technical] Should `.seeknal/sessions.db` be added to `.gitignore` automatically?

## Next Steps

-> `/ce:plan` for structured implementation planning
