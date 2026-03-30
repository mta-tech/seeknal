---
summary: Manage chat sessions for seeknal ask agent
read_when: You want to list, inspect, resume, or delete agent chat sessions
related:
  - ask
  - gateway
---

# seeknal session

Manage persistent chat sessions for the seeknal ask agent.

## Synopsis

```bash
seeknal session list [--project PATH]
seeknal session show NAME [--project PATH]
seeknal session delete NAME [--project PATH] [--force]
```

## Description

Sessions track multi-turn conversations with the ask agent. Each session stores conversation history and LangGraph checkpoints in `.seeknal/sessions.db` (SQLite with WAL mode).

Sessions are created automatically when using `seeknal ask chat` or the Telegram gateway. Each Telegram user gets a session named `telegram:{user_id}`.

## Commands

### list

List all sessions with their status, message count, and last question.

```bash
seeknal session list
```

Output shows a table:

```
Name             Status   Messages   Updated              Last Question
calm-river-315   active   12         2026-03-28 14:30:00   How many customers?
telegram:42      active   5          2026-03-28 15:00:00   Revenue by month
```

### show

Show details for a specific session.

```bash
seeknal session show calm-river-315
```

Displays session metadata and the command to resume:

```
Session: calm-river-315
Status: active
Messages: 12
Last question: How many customers?
Resume with: seeknal ask chat --session calm-river-315
```

### delete

Delete a session and its LangGraph checkpoints.

```bash
seeknal session delete calm-river-315
seeknal session delete calm-river-315 --force  # skip confirmation
```

## Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--project` | PATH | Auto-detected | Project path |
| `--force` | FLAG | False | Skip confirmation on delete |

## Session Names

Sessions are auto-named with a human-readable format: `{adjective}-{noun}-{3digits}` (e.g., `calm-river-315`, `swift-cloud-042`). Telegram sessions use `telegram:{user_id}`.

## Storage

Sessions are stored in `.seeknal/sessions.db` with the following schema:

| Field | Description |
|-------|-------------|
| `name` | Primary key (auto-generated or custom) |
| `status` | Session status (active, completed) |
| `created_at` | Creation timestamp |
| `updated_at` | Last activity timestamp |
| `message_count` | Number of messages in the session |
| `last_question` | Last question asked (truncated to 500 chars) |

## See Also

- [seeknal ask](ask.md) — `seeknal ask chat` creates sessions
- [seeknal gateway](gateway.md) — Telegram gateway creates `telegram:*` sessions
