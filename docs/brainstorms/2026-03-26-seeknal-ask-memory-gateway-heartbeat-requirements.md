---
date: 2026-03-26
topic: seeknal-ask-memory-gateway-heartbeat
---

# Seeknal Ask: Memory, Gateway, Temporal & Heartbeat

Reference implementation: [openclaw](~/project/self/bmad-new/openclaw)

## Problem Frame

Seeknal ask is currently a CLI-only agent that loses all learned context between sessions. It can only be reached from the terminal, cannot be integrated into team chat workflows, and has no ability to proactively monitor data pipelines. This limits adoption for teams who want conversational data engineering assistance accessible from mobile/chat, with persistent project knowledge and autonomous monitoring.

This work transforms seeknal ask from a stateless CLI tool into an always-on, multi-channel data engineering copilot with persistent memory and proactive capabilities.

## Requirements

### Phase 1: Memory

- R1. **Memory storage as Markdown files** — Agent memories stored as Markdown files in the project's `.seeknal/memory/` directory. `MEMORY.md` as curated index, `memory/YYYY-MM-DD.md` as daily logs. Files are the source of truth (human-readable, inspectable, git-friendly). Modeled after openclaw's `MemoryIndexManager` file-based approach.

- R2. **Three memory categories** — Agent stores memories in three categories:
  - **Project knowledge**: data patterns, schema conventions, pipeline decisions, data quality findings, business rules
  - **User preferences**: SQL style, naming conventions, default entities/time columns, report preferences
  - **Operational history**: past pipeline runs, error patterns, what queries/reports were generated

- R3. **Simple file-based retrieval** — Memory search starts as simple file reading: load `MEMORY.md` + recent daily logs into agent context at session start. No embedding index or vector search in v1. Add BM25/semantic search later when memory volume warrants it.

- R4. **Memory tools for the agent** — Two new agent tools:
  - `memory_write`: Save a memory entry (auto-categorized, appended to daily log, optionally promoted to MEMORY.md)
  - `memory_search`: Read memories relevant to current context (file grep in v1)

- R5. **Session-memory integration** — Memory is loaded at session start and available across all sessions for the same project. Sessions see the same memory store. Agent is prompted to write durable memories before session ends or context compaction.

- R6. **Auto-flush before compaction** — Like openclaw's pre-compaction reminder: when context window is getting full, prompt the agent to write important learnings to memory before they're lost.

### Phase 2: Gateway + Telegram

- R7. **WebSocket gateway server** — A persistent gateway process with WebSocket multiplexing on a single port. Serves as control plane for routing messages between channels and the seeknal ask agent. Modeled after openclaw's gateway architecture (persistent connections, event streaming).

- R8. **In-process agent execution** — Gateway imports and calls `create_agent()` directly (same Python process). No subprocess spawning. Shares memory, faster startup, enables streaming events back over WebSocket.

- R9. **Per-user sessions** — Each Telegram user gets one dedicated seeknal session regardless of which chat they message from. Session key format: `telegram:<user_id>`. Simpler than openclaw's per-chat model but sufficient for v1.

- R10. **Telegram bot integration** — Telegram bot (via python-telegram-bot or grammY-equivalent) receives messages, routes to gateway, gateway invokes agent in the user's session, streams response back to Telegram. Supports:
  - Text messages (natural language questions)
  - Streaming progress indicators (typing action while agent works)
  - Multi-message responses for long outputs

- R11. **Gateway CLI command** — `seeknal gateway start` launches the gateway server. Configuration via `seeknal_agent.yml`:
  ```yaml
  gateway:
    host: 127.0.0.1
    port: 18789
    telegram:
      token: ${TELEGRAM_BOT_TOKEN}
      project_path: /path/to/seeknal/project
  ```

- R12. **Channel abstraction** — Gateway uses a channel plugin interface so Telegram is the first implementation but Slack/Discord/etc. can follow the same pattern. Each channel handles inbound message parsing + outbound delivery formatting.

### Phase 3: Temporal Worker

- R13. **Seeknal ask as Temporal activity** — The seeknal ask agent runs as a Temporal activity worker, listening to a configured task queue. External systems (CI/CD, schedulers, other services) submit workflows directly to Temporal. This is an independent entry point to the agent, not routed through the gateway.

- R14. **Per-session SSE endpoint** — Each session has a persistent SSE endpoint (`/sessions/{id}/events`). When the Temporal worker processes an agent turn, it publishes events (tool calls, streaming tokens, final response) to the session's SSE stream. Clients subscribe to their session's stream for real-time updates.

- R15. **Temporal workflow definition** — A `SeekналAskWorkflow` that:
  - Accepts: question, project_path, session_name, optional config overrides
  - Creates/resumes session
  - Runs agent turn as a Temporal activity
  - Publishes streamed events to SSE endpoint
  - Returns final response

- R16. **Temporal worker CLI** — `seeknal gateway temporal-worker` starts the Temporal activity worker. Configuration:
  ```yaml
  gateway:
    temporal:
      server: localhost:7233
      namespace: seeknal
      task_queue: seeknal-ask
      sse_endpoint: http://localhost:18789
  ```

### Phase 4: Heartbeat

- R17. **HEARTBEAT.md driven** — Pure openclaw style: agent reads `HEARTBEAT.md` from the project's `.seeknal/` directory for instructions on what to check. Fully user-configurable, no hardcoded monitoring logic. Agent uses its existing tools (execute_sql, inspect_output, etc.) to perform checks.

- R18. **Configured alert channel** — Heartbeat alerts delivered to an explicitly configured target channel per project (e.g., a specific Telegram group). Not "last active" — predictable for team setups. Configuration:
  ```yaml
  heartbeat:
    every: 30m
    target:
      channel: telegram
      chat_id: -1001234567890
    active_hours:
      start: "08:00"
      end: "22:00"
      timezone: Asia/Jakarta
  ```

- R19. **HEARTBEAT_OK protocol** — Like openclaw: if the agent determines nothing needs attention, it replies with `HEARTBEAT_OK` (message is suppressed/not delivered). Only actual findings are sent to the configured channel.

- R20. **Heartbeat runner** — Built into the gateway process. Periodic scheduler that triggers heartbeat agent turns at the configured interval during active hours. Each heartbeat run gets an isolated session (no history carryover) to keep token costs low.

- R21. **Heartbeat CLI** — `seeknal heartbeat run` for manual one-off heartbeat execution (useful for testing). `seeknal heartbeat status` shows last run time and result.

## Success Criteria

- Agent can recall project-specific knowledge (schema patterns, past decisions) across sessions without being re-told
- A Telegram bot can receive natural language questions and return seeknal ask responses with tool execution results
- External systems can submit questions to seeknal ask via Temporal workflow and receive streamed responses via SSE
- Heartbeat proactively surfaces pipeline issues to a configured Telegram group without user prompting

## Scope Boundaries

- **In scope**: Memory, WebSocket gateway, Telegram channel, Temporal worker, SSE streaming, Heartbeat
- **Out of scope for v1**: Slack/Discord channels (channel abstraction enables later), multi-tenant auth (single project per gateway instance for now), embedding-based memory search, gateway UI/dashboard, end-to-end encryption
- **Out of scope permanently**: openclaw's device identity/signature auth (not needed for single-project deployment), protocol version negotiation (start with v1, evolve as needed)

## Key Decisions

- **Memory as files**: Markdown files are source of truth, not a database. Human-readable, git-trackable, inspectable. Mirrors openclaw's philosophy.
- **Simple grep for memory search**: No embeddings or vector DB in v1. Load MEMORY.md + recent daily logs into context. Add BM25/semantic search when memory volume warrants it.
- **WebSocket gateway**: Full WebSocket multiplexing from day one (not minimal HTTP). Enables real-time streaming, multiple simultaneous clients, and future channel plugins.
- **In-process agent**: Gateway calls `create_agent()` directly. No subprocess overhead, enables streaming events natively.
- **Per-user sessions (Telegram)**: Simpler than per-chat. One session per Telegram user, shared across DMs and groups.
- **Temporal as independent entry point**: Not routed through gateway. External systems submit directly to Temporal. Gateway and Temporal are parallel paths to the same agent.
- **Per-session SSE**: Persistent SSE endpoint per session for Temporal response delivery. Clients subscribe to their session's stream.
- **HEARTBEAT.md driven**: No hardcoded monitoring. Agent reads user-defined instructions and uses its tools to check. Pure openclaw pattern.
- **Configured alert target**: Heartbeat alerts go to an explicitly configured channel, not "last active." Predictable for teams.

## Dependencies / Assumptions

- Telegram Bot API token available (user creates bot via @BotFather)
- Temporal server available for Phase 3 (self-hosted or Temporal Cloud)
- Current LangGraph + SqliteSaver session infrastructure (from chat-sessions requirements) is implemented before this work begins
- `seeknal_agent.yml` config system supports the new gateway/heartbeat sections

## Outstanding Questions

### Resolve Before Planning

(None — all product decisions resolved)

### Deferred to Planning

- [Affects R7][Needs research] Which Python WebSocket server library? (websockets, Starlette, FastAPI WebSocket, or something else)
- [Affects R8][Technical] How to handle concurrent agent invocations in a single process? (asyncio task per request vs. thread pool)
- [Affects R10][Needs research] python-telegram-bot vs. aiogram vs. telethon for Telegram integration
- [Affects R13][Technical] Temporal Python SDK activity/workflow patterns for long-running LLM agent turns
- [Affects R14][Technical] SSE server implementation — part of the gateway process or separate service?
- [Affects R5, R9][Technical] Memory file locking when multiple sessions (gateway + CLI) access the same project's memory concurrently
- [Affects R6][Technical] How to detect context window pressure in LangGraph to trigger auto-flush

## Next Steps

→ `/ce:plan` for structured implementation planning
