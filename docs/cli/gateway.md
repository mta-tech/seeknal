---
summary: Multi-channel gateway server for seeknal ask agent (Telegram, WebSocket, SSE)
read_when: You want to serve the ask agent over Telegram, WebSocket, or SSE
related:
  - ask
  - session
  - heartbeat
---

# seeknal gateway

Multi-channel gateway server that exposes the seeknal ask agent over Telegram, WebSocket, and SSE. Includes an interactive setup wizard for configuration.

## Synopsis

```bash
seeknal gateway setup [OPTIONS]
seeknal gateway start [OPTIONS]
seeknal gateway temporal-worker [OPTIONS]
```

## Description

The `gateway` command group manages the HTTP gateway server that bridges external messaging channels to the seeknal ask agent. It supports:

1. **Telegram** — Bot integration with real-time tool progress streaming
2. **WebSocket** — Direct browser/client connections at `/ws/{session_id}`
3. **SSE** — Server-Sent Events at `/events/{session_id}` for Temporal worker output
4. **Health check** — `GET /health` endpoint

## Prerequisites

Install the gateway dependencies:

```bash
pip install seeknal[gateway]
```

For Telegram, create a bot via [@BotFather](https://t.me/BotFather) and get a token.

## Commands

### setup

Interactive wizard to generate `seeknal_agent.yml`.

```bash
seeknal gateway setup [--project PATH]
```

Walks through 4 optional sections:

1. **Agent model** — LLM provider, model name, temperature, default profile
2. **Telegram channel** — Bot token (validated live via Bot API), pairing mode to auto-capture chat_id, polling/webhook mode
3. **Temporal worker** — Server address, namespace, task queue
4. **Heartbeat monitoring** — Check interval, delivery channel, target chat_id, active hours

The token is stored as `${TELEGRAM_BOT_TOKEN}` env var reference (git-safe). During setup, the wizard validates the token by calling Telegram's `getMe` API and shows the bot username.

**Pairing mode**: After token validation, the wizard generates a code (e.g., `SEEKNAL-7294`). Send this code to your bot on Telegram — the wizard auto-captures your `chat_id` for heartbeat delivery.

If `seeknal_agent.yml` already exists, the wizard asks whether to merge (update specific sections) or overwrite.

### start

Start the gateway HTTP server.

```bash
seeknal gateway start [--host HOST] [--port PORT] [--channel NAME] [--project PATH]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--host` | TEXT | `127.0.0.1` | Bind address |
| `--port` | INT | `18789` | Listen port |
| `--channel` | TEXT | All | Enable specific channel(s) only (e.g., `telegram`) |
| `--project` | PATH | CWD | Project path |

CLI flags override values from `seeknal_agent.yml`.

### temporal-worker

Start a Temporal activity worker for distributed agent execution.

```bash
seeknal gateway temporal-worker [--server ADDR] [--namespace NS] [--task-queue QUEUE] [--project PATH]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--server` | TEXT | `localhost:7233` | Temporal server address |
| `--namespace` | TEXT | `seeknal` | Temporal namespace |
| `--task-queue` | TEXT | `seeknal-ask` | Task queue name |
| `--project` | PATH | CWD | Project path |

## Configuration

All gateway settings live in `seeknal_agent.yml` at the project root:

```yaml
model: gemini-2.5-flash
temperature: 0.0
default_profile: analysis

compaction:
  trigger_tokens: 50000    # Auto-compact at this token count
  keep_messages: 10         # Keep last N messages after compaction

gateway:
  host: 127.0.0.1
  port: 18789
  telegram:
    token: ${TELEGRAM_BOT_TOKEN}
    polling: true           # true for local dev, false for production webhooks

  temporal:
    server: localhost:7233
    namespace: seeknal
    task_queue: seeknal-ask

heartbeat:
  every: 1h
  target:
    channel: telegram
    chat_id: -100123456
  active_hours:
    start: "08:00"
    end: "22:00"
    timezone: Asia/Jakarta
```

## Examples

### Quick start with Telegram

```bash
# 1. Run the setup wizard
seeknal gateway setup --project .

# 2. Set the bot token
export TELEGRAM_BOT_TOKEN="your-token-from-botfather"

# 3. Start the gateway
seeknal gateway start --channel telegram
```

### Start on a custom port

```bash
seeknal gateway start --port 8080 --channel telegram
```

### Production deployment with webhook

```bash
# seeknal_agent.yml: polling: false
seeknal gateway start --host 0.0.0.0 --port 443
# Then set webhook URL via Telegram API:
# https://api.telegram.org/bot{token}/setWebhook?url=https://your-domain/telegram/webhook
```

## Architecture

```
User (Telegram/Browser)
        |
   Gateway Server (Starlette + Uvicorn)
        |
   +---------+---------+--------+
   |         |         |        |
Telegram  WebSocket   SSE    Health
Channel   Endpoint   Stream  Check
   |         |         |
   +----+----+---------+
        |
   Agent (LangGraph)
        |
   +----+----+
   |         |
 Tools    Memory
 (21+)   (.seeknal/)
```

## Telegram Features

- **Real-time streaming** — Tool usage messages sent as the agent works (e.g., "Querying your data...", "Examining table structure...")
- **Session persistence** — Each Telegram user gets a persistent session (`telegram:{user_id}`) with conversation history
- **Memory** — Agent reads/writes `.seeknal/MEMORY.md` for cross-session context
- **Plain text output** — Markdown is stripped for clean Telegram rendering
- **Context compaction** — Auto-compresses long conversations to stay within model limits
- **Message splitting** — Long responses split at 4096-char Telegram limit (newline-aware)

## Security

- Session IDs validated via regex (alphanumeric + hyphens/underscores, max 128 chars)
- Project paths validated for security (rejects `/tmp`, `/var/tmp`, world-writable dirs)
- WebSocket messages limited to 64KB
- Agent errors sanitized — internal details not leaked to clients
- Telegram token stored as env var reference, never in YAML

## See Also

- [seeknal ask](ask.md) — CLI agent interface
- [seeknal session](session.md) — Session management
- [seeknal heartbeat](heartbeat.md) — Automated monitoring
