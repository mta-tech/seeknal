---
summary: Initialize a new Seeknal project
read_when: You are starting a new project with Seeknal
related:
  - run
  - source
  - ask
  - list
---

# seeknal init

Initialize a new Seeknal project in the current directory or at a target path.

## Synopsis

```bash
seeknal init [OPTIONS]
```

## Description

Creates the initial project structure including:

- `seeknal_project.yml` - Project configuration
- `profiles.yml` - Local credentials and engine configuration (gitignored)
- `seeknal_agent.yml` - Ask mode/source registry scaffold
- `SEEKNAL_ASK.md` - Durable project instructions loaded by Ask
- `.env.example` - Safe environment-variable placeholders
- `.gitignore` - Generated ignores for profiles, `.env`, and build outputs
- `AGENTS.md` - Project-local guidance for coding/agent assistants
- `CLAUDE.md` - Claude Code compatibility guide that points to `AGENTS.md`
- `context/` - User-taught durable Ask memory and SQL pairs
- `seeknal/` - Durable project definitions, Ask skills, SQL pairs, and tests
- `target/` - Generated pipeline outputs

Project layout:

```text
my-project/
├── seeknal_project.yml
├── profiles.yml
├── seeknal_agent.yml
├── SEEKNAL_ASK.md
├── .env.example
├── AGENTS.md
├── CLAUDE.md
├── .gitignore
├── context/
│   ├── sql_pairs/       # Agent/user-taught reusable SQL examples
│   └── tests/           # Optional context-local Ask tests
├── seeknal/
│   ├── sources/
│   ├── transforms/
│   ├── feature_groups/
│   ├── models/
│   ├── pipelines/
│   ├── templates/
│   ├── common/
│   ├── sql_pairs/       # Ask context examples: prompt -> SQL
│   ├── tests/           # Ask SQL QA tests
│   └── skills/          # Project-local Ask skills copied from built-ins
└── target/
    └── intermediate/
```

`seeknal init` does not configure external data credentials. Add pipeline
sources with `seeknal draft source`, or connect an existing analytical database
for read-only Ask with `seeknal source connect`.

## Options

| Option | Description |
|--------|-------------|
| `--name`, `-n` | Project name (defaults to directory name) |
| `--description`, `-d` | Project description |
| `--path`, `-p` | Project path (default `.`) |
| `--force`, `-f` | Reinitialize existing project configuration |

## Examples

### Basic initialization

```bash
seeknal init
```

### With project name

```bash
seeknal init --name my_project --description "My ML feature store"
```

### Initialize at a specific path

```bash
seeknal init --name analytics --path ./analytics-project
```

### Add a read-only Ask source after init

```bash
export WAREHOUSE_URL="postgresql://user:pass@host/db?sslmode=require"

seeknal source connect warehouse \
  --connector postgresql \
  --namespace warehouse \
  --dsn-env WAREHOUSE_URL

seeknal source sync warehouse --project .
seeknal source test warehouse --project .
seeknal ask chat --project .
```

## Generated agent guidance

`seeknal init` creates both runtime Ask context files and coding-agent guidance.
They are intentionally generic, so a project can be set up like a BPOM RPO
read-only database project without hardcoding that domain into Seeknal.

Runtime Ask context:

- `seeknal_agent.yml` steers connected-source mode.
- `SEEKNAL_ASK.md` is global Ask prompt context for vocabulary, metric
  definitions, SQL-pair conventions, source-context workflow, and QA workflow.
- `seeknal/sql_pairs/` stores examples the agent can read as context.
- `seeknal/tests/` stores executable Ask SQL tests.
- Generated source context under `.seeknal/context/sources/` is derived state.

Coding-agent guidance:

- `AGENTS.md` and `CLAUDE.md` tell coding agents to use `seeknal docs --list`,
  `seeknal docs <topic>`, and `seeknal docs --json <topic>` before guessing CLI
  syntax or generated file conventions.
- `AGENTS.md` tells coding agents how to set up read-only connected-source
  projects: source registry, `.env`, `SEEKNAL_ASK.md`, `context/*.md`,
  SQL pairs, Ask tests, source sync, and TUI smoke tests.
- It also distinguishes the main setup modes:
  - **Tap-in / read-only connected-source analyst**: ask questions against an
    existing database, using source context, SQL pairs, and Ask tests.
  - **Data pipeline builder**: create managed Seeknal source/transform/
    feature-group/model definitions, then plan/run the pipeline.
  - **Hybrid**: keep `mode.default: auto`, register connected sources, and keep
    managed pipeline definitions under `seeknal/`.
- `CLAUDE.md` mirrors the essentials for Claude Code and points to `AGENTS.md`
  as canonical.

The intended workflow for a connected-source project is:

```bash
cp .env.example .env
# edit .env with the real DSN
seeknal source connect warehouse --connector postgresql --dsn-env WAREHOUSE_URL --project .
seeknal source sync warehouse --project .
# add reusable SME query notes to seeknal/sql_pairs/*.yml
# add regression cases to seeknal/tests/*.yml
seeknal ask test --project . --sql-only
seeknal ask chat --project .
```

## See Also

- [seeknal source](source.md) - Configure connected read-only sources
- [seeknal ask](ask.md) - Ask chat, reports, SQL pairs, and Ask tests
- [seeknal run](run.md) - Execute pipelines
- [seeknal list](list.md) - List project resources
