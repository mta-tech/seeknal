---
date: 2026-03-28
topic: gateway-onboarding-setup
---

# Gateway Onboarding Setup Wizard

## Problem Frame

The gateway requires a manually-created `seeknal_agent.yml` with multiple config sections (agent model, Telegram token, Temporal connection, heartbeat schedule). Users must know the exact YAML structure, field names, and valid values. There's no guided setup — users either copy an example or guess. This creates friction for first-time gateway deployment.

The `seeknal iceberg setup` command already proves interactive config wizards work in seeknal. The gateway needs the same treatment.

## Requirements

- R1. New `seeknal gateway setup` CLI command that interactively generates `seeknal_agent.yml` in the project root
- R2. Wizard walks through 4 sections in order: Agent model, Telegram channel, Temporal worker, Heartbeat monitoring
- R3. Each section is optional — user can skip sections they don't need (e.g., skip Temporal if not using it)
- R4. Telegram token stored as `${TELEGRAM_BOT_TOKEN}` env var reference in YAML (git-safe). Wizard prompts user to export the env var.
- R5. Telegram token validated live during setup by calling the Bot API `getMe` endpoint. Show bot username on success, fail with clear error on bad token.
- R6. If `seeknal_agent.yml` already exists, wizard asks whether to overwrite or merge (update specific sections while preserving others)
- R7. After successful setup, print a summary of what was configured and the command to start the gateway (`seeknal gateway start`)

## Success Criteria

- A new user can go from zero to a working Telegram gateway in under 2 minutes using only `seeknal gateway setup` + `seeknal gateway start`
- The generated `seeknal_agent.yml` is valid and complete — no manual editing needed
- Bad Telegram tokens are caught during setup, not at gateway startup

## Scope Boundaries

- Not adding a web UI for config — CLI wizard only
- Not handling SSL/webhook URL setup for production Telegram deployments (polling mode is the default for local dev)
- Not auto-creating the Telegram bot via BotFather API (user creates bot manually, brings token)
- Not adding config hot-reload — changes require gateway restart

## Key Decisions

- **Command**: `seeknal gateway setup` under existing gateway Typer group
- **Token storage**: Env var reference `${TELEGRAM_BOT_TOKEN}` in YAML, not raw token
- **Validation**: Live API call to Telegram `getMe` during setup
- **Existing config**: Merge by default when `seeknal_agent.yml` exists

## Dependencies / Assumptions

- `python-telegram-bot` or `httpx`/`requests` available for the getMe validation call (can use stdlib `urllib` to avoid new deps)
- User has already created a Telegram bot via @BotFather

## Outstanding Questions

### Deferred to Planning
- [Affects R5][Technical] Should validation use `python-telegram-bot` SDK or raw HTTP to call getMe? Raw HTTP avoids importing the full SDK during setup.
- [Affects R2][Technical] What model choices to present? Should it list available Gemini/Ollama models or accept freeform input?

## Next Steps

`/ce:plan` for structured implementation planning
