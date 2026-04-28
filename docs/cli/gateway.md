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
| `--token-config` | PATH | `SEEKNAL_TOKEN_CONFIG` | JSON/YAML API token registry for tenant-scoped worker routing |

### `seeknal gateway backend`

Start a cloud-only gateway (no local project or worker). Useful when the gateway runs on a separate machine from the data.

Supports `--token-config` so `/temporal/start`, `/events/{session_id}`, `/sessions`, and worker callbacks derive tenant identity from bearer tokens instead of caller-provided tenant headers.

### `seeknal gateway worker`

Start a standalone Temporal worker that connects to an existing gateway. Use in split topologies where the gateway and worker run on different machines.

For secure multi-tenant deployments, start the worker with `--gateway-url` and `--api-token`. The worker fetches `/internal/worker/config`, then uses the token-derived tenant queue, callback bearer token, and optional Temporal address/namespace. Existing local workers can still use `--tenant` or `TEMPORAL_TASK_QUEUE` when token mode is not configured.

## Examples

```bash
# Basic local gateway
seeknal gateway start --project ./my-project

# Gateway with Telegram bot
seeknal gateway start --telegram

# Cloud mode: gateway + remote worker
seeknal gateway backend --port 8000
seeknal gateway worker --project ./my-project --callback-url http://gateway:8000

# Multi-tenant token mode: queue/callback config is derived from the token
seeknal gateway backend --token-config ./gateway-tokens.yml --port 8000
seeknal gateway worker --project ./my-project --gateway-url http://gateway:8000 --api-token "$SEEKNAL_API_TOKEN"

# Docker worker image
docker build -f docker/Dockerfile.worker -t seeknal-worker:local .
docker run --rm \
  -v "$PWD/my-project:/app/project" \
  --env-file "$PWD/my-project/.env" \
  -e TEMPORAL_ADDRESS=host.docker.internal:7233 \
  seeknal-worker:local --project /app/project

# Docker gateway image
docker build -f docker/Dockerfile.gateway -t seeknal-gateway:local .
docker run --rm \
  -p 8000:8000 \
  -v "$PWD/my-project:/app/project" \
  --env-file "$PWD/my-project/.env" \
  -e TEMPORAL_ADDRESS=host.docker.internal:7233 \
  seeknal-gateway:local

# Docker worker with token-derived tenant routing
docker run --rm \
  -v "$PWD/my-project:/app/project" \
  --env-file "$PWD/my-project/.env" \
  -e SEEKNAL_GATEWAY_URL=http://host.docker.internal:8000 \
  -e SEEKNAL_API_TOKEN="$SEEKNAL_API_TOKEN" \
  seeknal-worker:local --project /app/project

# Multi-replica with Redis
seeknal gateway start --redis redis://localhost:6379

# Temporal durable execution
seeknal gateway start --temporal --max-activities 20
```

## Multi-tenant token registry

When a token registry is configured, tenant identity is authenticated instead of trusted from `X-Tenant-ID` or `?tenant=`. The gateway derives the Temporal task queue, callback token, and tenant-scoped session/event access from the bearer token. Request body overrides such as `task_queue`, `project_path`, `push_url`, and `api_key` are rejected in token mode.

Example `gateway-tokens.yml`:

```yaml
tokens:
  - token: <random-worker-or-client-token>
    tenant_id: acme
    task_queue: seeknal-ask-acme
    callback_token: <random-callback-token>
    callback_url: https://gateway.example.com
    temporal_address: temporal.example.com:7233
    temporal_namespace: default
    project_path: /srv/seeknal/acme
```

Do not commit token files with real secrets; provision them from your secret manager or deployment environment. Use `Authorization: Bearer <token>` for HTTP clients. Browser SSE/WebSocket clients that cannot set headers may pass a short-lived `api_token` or `access_token` query parameter. If no token registry is configured, Seeknal keeps the previous single-tenant/local behavior.

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
| `CALLBACK_AUTH_TOKEN` | Worker callback auth secret (legacy compatibility mode) |
| `WORKER_PROJECT_PATH` | Remote worker project path |
| `SEEKNAL_TOKEN_CONFIG` | JSON/YAML API token registry path |
| `SEEKNAL_API_TOKENS` | Inline JSON token registry for tests/small deployments |
| `SEEKNAL_GATEWAY_URL` | Gateway URL used by `seeknal gateway worker` bootstrap |
| `SEEKNAL_API_TOKEN` | Worker/client API token used for token-derived routing |

## Docker images

`docker/Dockerfile.gateway` builds a container that runs `seeknal gateway
start`, and `docker/Dockerfile.worker` builds a container that runs `seeknal
gateway worker`.
Mount a Seeknal project at `/app/project`, pass project secrets through an
environment file or secret manager, and configure Temporal either directly with
`TEMPORAL_ADDRESS`/`TEMPORAL_TASK_QUEUE` or indirectly with
`SEEKNAL_GATEWAY_URL`/`SEEKNAL_API_TOKEN` in token-routed deployments.

```bash
docker build -f docker/Dockerfile.gateway -t seeknal-gateway:local .
docker build -f docker/Dockerfile.worker -t seeknal-worker:local .
docker compose -f deploy/docker-compose.worker.yml up --build
```

The compose file is intended for local or on-prem workers running near the data.
Scale it with `docker compose -f deploy/docker-compose.worker.yml up --scale
seeknal-worker=3` when the project database and DuckDB state are safe for that
deployment pattern.

## See Also

- [seeknal ask](ask.md) — Interactive CLI agent
- [seeknal report-server](report-server.md) — Host published reports
