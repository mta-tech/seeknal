---
summary: HTTP gateway server for seeknal ask — WebSocket, SSE, REST, Telegram, and Temporal
read_when: You want to expose seeknal ask as an API or connect a web/mobile client
related:
  - ask
  - report-server
---

# seeknal gateway

HTTP gateway server that exposes seeknal ask as an API with WebSocket, SSE, REST, and optional Telegram bot and Temporal durable execution.

## Synopsis

```bash
seeknal gateway start [OPTIONS]
seeknal gateway backend [OPTIONS]
seeknal gateway worker [OPTIONS]
```

## Description

The gateway server wraps the seeknal ask agent behind HTTP endpoints so web clients, mobile apps, and bots can interact with your data. It supports multiple transport protocols:

- **WebSocket** — Streaming, bidirectional chat
- **SSE (Server-Sent Events)** — One-way streaming for web UIs
- **REST** — Standard request/response for one-shot questions
- **Telegram** — Optional bot integration via `--telegram`
- **Temporal** — Optional durable execution for long-running agent tasks via `--temporal`

## Prerequisites

The Ask gateway and Telegram channel are included in the default seeknal installation.

For Temporal support:

```bash
pip install seeknal[temporal]
```


## Commands

### `seeknal gateway start`

Start the full gateway server (API + optional worker).

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--project` | PATH | Auto-detected | Project path |
| `--port` | INT | `8000` | Port to listen on |
| `--host` | TEXT | `0.0.0.0` | Host to bind to |
| `--telegram` | FLAG | False | Enable Telegram bot channel |
| `--temporal` | FLAG | False | Enable Temporal durable execution |
| `--no-worker` | FLAG | False | Gateway-only mode (no local Temporal worker) |
| `--max-activities` | INT | `15` | Max concurrent Temporal activities per worker |
| `--redis` | TEXT | None | Redis URL for multi-replica mode |
| `--callback-url` | TEXT | None | Base URL for worker event callbacks |
| `--callback-auth-token` | TEXT | None | Shared secret for callback POST auth |
| `--worker-project-path` | TEXT | None | Project path on the remote worker (split topology) |

### `seeknal gateway backend`

Start a cloud-only gateway (no local project or worker). Useful when the gateway runs on a separate machine from the data.

### `seeknal gateway worker`

Start a standalone Temporal worker that connects to an existing gateway. Use in split topologies where the gateway and worker run on different machines.

## Examples

```bash
# Basic local gateway
seeknal gateway start --project ./my-project

# Gateway with Telegram bot
seeknal gateway start --telegram

# Cloud mode: gateway + remote worker
seeknal gateway backend --port 8000
seeknal gateway worker --project ./my-project --callback-url http://gateway:8000

# Multi-replica with Redis
seeknal gateway start --redis redis://localhost:6379

# Temporal durable execution
seeknal gateway start --temporal --max-activities 20
```

## Architecture

```
Client (browser/app/bot)
    │
    ▼
┌──────────────┐
│   Gateway    │  ← seeknal gateway start
│  (Starlette) │
├──────────────┤
│ WebSocket    │  /ws/{session_id}
│ SSE          │  /events/{session_id}
│ REST         │  /ask
│ Cancel       │  /sessions/{session_id}/cancel
│ Telegram     │  (webhook)
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  Ask Agent   │  Same agent as `seeknal ask chat`
│  (per-session)│
└──────────────┘
```

## Runtime behavior

- Runs for the same tenant/session are serialized to protect conversation
  history and DuckDB state.
- `POST /sessions/{session_id}/cancel` requests cancellation of the active
  run. WebSocket clients can also send `{"type":"cancel"}` while a run is
  streaming.
- Tool result events include `elapsed_ms` when timing is available; `done`
  events include total turn `elapsed_ms`.
- When `--redis` is configured, SSE fan-out and session locks use Redis so
  multi-replica gateway deployments preserve per-session serialization.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GOOGLE_API_KEY` | Google Gemini API key |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token (with `--telegram`) |
| `TEMPORAL_ADDRESS` | Temporal server address (with `--temporal`) |
| `TEMPORAL_NAMESPACE` | Temporal namespace |
| `TEMPORAL_MAX_CONCURRENT_ACTIVITIES` | Max concurrent activities |
| `CALLBACK_AUTH_TOKEN` | Worker callback auth secret |
| `WORKER_PROJECT_PATH` | Remote worker project path |

## See Also

- [seeknal ask](ask.md) — Interactive CLI agent
- [seeknal report-server](report-server.md) — Host published reports
