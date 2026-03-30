---
summary: HEARTBEAT.md-driven automated project monitoring
read_when: You want to set up periodic health checks via the ask agent
related:
  - gateway
  - ask
---

# seeknal heartbeat

Automated project monitoring driven by `.seeknal/HEARTBEAT.md` instructions.

## Synopsis

```bash
seeknal heartbeat run [--project PATH]
seeknal heartbeat status [--project PATH]
```

## Description

The heartbeat system runs the ask agent periodically to check your project's health. You write monitoring instructions in `.seeknal/HEARTBEAT.md`, and the agent executes them on a schedule.

When run via the gateway (`seeknal gateway start`), heartbeats are automatic. The CLI commands provide manual execution and status inspection.

## Commands

### run

Execute a one-off heartbeat check.

```bash
seeknal heartbeat run --project /path/to/project
```

The agent reads `.seeknal/HEARTBEAT.md`, runs the analysis, and prints the result. If the response contains `HEARTBEAT_OK`, the check is considered healthy and no alert is sent.

### status

Show the last heartbeat result and next scheduled run.

```bash
seeknal heartbeat status --project /path/to/project
```

Output:

```
Last run:    2026-03-28T14:30:00
Result:      ok
Message:     HEARTBEAT_OK — all pipelines healthy, no stale data
```

## HEARTBEAT.md

Create `.seeknal/HEARTBEAT.md` in your project with monitoring instructions:

```markdown
Check the following:

1. Are all pipeline nodes producing data? Look for empty parquet files.
2. Is the latest data fresh (within the last 24 hours)?
3. Are there any NULL values in critical columns (customer_id, order_id)?
4. Does revenue_by_segment have reasonable values (no negatives)?

If everything looks good, respond with HEARTBEAT_OK.
If you find issues, describe them clearly.
```

## HEARTBEAT_OK Protocol

The agent's response is checked for `HEARTBEAT_OK`:

- If the response **starts or ends** with `HEARTBEAT_OK`, the check is marked as "ok" and delivery is suppressed (no alert sent)
- If the response does **not** contain `HEARTBEAT_OK`, it's treated as findings and delivered to the configured channel

## Configuration

Heartbeat scheduling is configured in `seeknal_agent.yml`:

```yaml
heartbeat:
  every: 1h                          # Check interval (30m, 1h, 2h, 90s)
  target:
    channel: telegram                 # Delivery channel for findings
    chat_id: -100123456               # Telegram group/user chat ID
  active_hours:                       # Optional — only run during these hours
    start: "08:00"
    end: "22:00"
    timezone: Asia/Jakarta            # Timezone for active hours
```

### Active Hours

Heartbeats skip outside the configured active hours window. Supports overnight windows (e.g., `start: "22:00"`, `end: "06:00"`).

### Intervals

| Format | Duration |
|--------|----------|
| `30m` | 30 minutes |
| `1h` | 1 hour |
| `2h` | 2 hours |
| `90s` | 90 seconds |

## State

Heartbeat state is persisted in `.seeknal/heartbeat_state.json`:

```json
{
  "last_run": "2026-03-28T14:30:00",
  "last_result": "ok",
  "last_message": "HEARTBEAT_OK — all pipelines healthy",
  "next_due": "2026-03-28T15:30:00"
}
```

State writes are atomic (temp file + rename) to prevent corruption.

## Examples

### Set up heartbeat monitoring

```bash
# 1. Create HEARTBEAT.md
cat > .seeknal/HEARTBEAT.md << 'EOF'
Check all pipeline outputs for freshness and data quality.
If everything is healthy, respond with HEARTBEAT_OK.
EOF

# 2. Configure in seeknal_agent.yml (or use seeknal gateway setup)
# heartbeat:
#   every: 1h
#   target:
#     channel: telegram
#     chat_id: YOUR_CHAT_ID

# 3. Run manually to test
seeknal heartbeat run

# 4. Start gateway for automatic scheduling
seeknal gateway start --channel telegram
```

### Check status

```bash
seeknal heartbeat status
```

## See Also

- [seeknal gateway](gateway.md) — Runs heartbeat automatically as a background task
- [seeknal ask](ask.md) — The agent that executes heartbeat checks
