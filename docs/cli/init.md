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
- `.gitignore` - Generated ignores for profiles and build outputs
- `AGENTS.md` - Project-local guidance for coding/agent assistants
- `CLAUDE.md` - Claude Code compatibility guide that points to `AGENTS.md`
- `seeknal/` - Durable project definitions, Ask skills, SQL pairs, and tests
- `target/` - Generated pipeline outputs

Project layout:

```text
my-project/
├── seeknal_project.yml
├── profiles.yml
├── AGENTS.md
├── CLAUDE.md
├── .gitignore
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

`AGENTS.md` and `CLAUDE.md` document the project-local Ask conventions:

- `seeknal_agent.yml` steers connected-source mode.
- `SEEKNAL_ASK.md` is optional global Ask prompt context.
- `seeknal/sql_pairs/` stores examples the agent can read as context.
- `seeknal/tests/` stores executable Ask SQL tests.
- Generated source context under `.seeknal/context/sources/` is derived state.

## See Also

- [seeknal source](source.md) - Configure connected read-only sources
- [seeknal ask](ask.md) - Ask chat, reports, SQL pairs, and Ask tests
- [seeknal run](run.md) - Execute pipelines
- [seeknal list](list.md) - List project resources
