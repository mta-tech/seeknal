---
summary: Manage read-only data sources for REPL and Seeknal Ask
read_when: You want Ask to answer questions against an existing database without building a pipeline
related:
  - ask
  - repl
  - init
---

# seeknal source

Manage data sources for the SQL REPL and the Seeknal Ask agent.

## Synopsis

```bash
seeknal source list [OPTIONS]
seeknal source connect NAME [OPTIONS]
seeknal source status [OPTIONS]
seeknal source inspect NAME [OPTIONS]
seeknal source sync [NAME...] [OPTIONS]
seeknal source test NAME [OPTIONS]
seeknal source add NAME --url URL
seeknal source remove NAME
```

## Description

`seeknal source` has two related roles:

1. **Ask source registry** — project-local read-only sources declared in
   `seeknal_agent.yml`. This is the recommended path for existing analytical
   databases where no pipeline is needed.
2. **Legacy REPL source store** — encrypted saved connection URLs managed by
   `source add/list/remove` when no project source registry is configured.

For connected-source Ask projects, keep the database external and let the Ask
agent attach to it read-only. Use generated source context, SQL pairs, and Ask
SQL tests to steer and validate the agent without hardcoding domain logic.

## Ask source workflow

```bash
# Put the secret in .env or your shell, not in seeknal_agent.yml
export WAREHOUSE_URL="postgresql://user:pass@host/db?sslmode=require"

# Create/update the project source registry
seeknal source connect warehouse \
  --connector postgresql \
  --namespace warehouse \
  --dsn-env WAREHOUSE_URL \
  --description "Production analytics warehouse"

# Inspect registry and generated context status
seeknal source status --project .
seeknal source inspect warehouse --project .

# Generate metadata context under .seeknal/context/sources/
seeknal source sync warehouse --project .

# Verify the source attaches read-only and tables are visible
seeknal source test warehouse --project .

# Start TUI/chat
seeknal ask chat --project .
```

## Commands

### `seeknal source connect`

Create or update a read-only Ask source registry entry in `seeknal_agent.yml`.
The command stores only metadata and the environment variable name for the DSN.
It never stores the secret DSN value.

```bash
seeknal source connect warehouse \
  --connector postgresql \
  --namespace warehouse \
  --dsn-env WAREHOUSE_URL \
  --role business_source_of_truth \
  --mode auto
```

| Option | Description |
|--------|-------------|
| `NAME` | Source registry name |
| `--connector` | Connector type, for example `postgresql` |
| `--namespace` | Queryable namespace/catalog exposed to Ask |
| `--dsn-env` | Environment variable containing the connection DSN |
| `--access` | Access policy, default `read_only` |
| `--role` | Behavioral role for agent steering |
| `--mode` | Default Ask mode, usually `auto` |
| `--description` | Human-readable source description |
| `--project` | Project path |
| `--force` | Replace an existing source entry |

### `seeknal source status`

Show all configured Ask sources and their context-sync state.

```bash
seeknal source status --project .
```

### `seeknal source inspect`

Show one source declaration, including role, access policy, context templates,
and sync status.

```bash
seeknal source inspect warehouse --project .
```

### `seeknal source sync`

Write metadata-only source context for Ask steering. Generated files are placed
under `.seeknal/context/sources/` and sync state under `.seeknal/catalog/`.

```bash
seeknal source sync --project .              # all sources
seeknal source sync warehouse --project .    # one source
```

Source context can include overview, columns, profiling, preview, and
relationship hints depending on the configured templates and connector support.
The Ask agent can read this context with `list_source_context` and
`read_source_context` during chat.

### `seeknal source test`

Run read-only discovery against a configured source to verify that the REPL/Ask
attachment works and that visible tables can be counted.

```bash
seeknal source test warehouse --project .
```

### `seeknal source list`

List configured Ask sources. If the project has no `seeknal_agent.yml` source
registry, falls back to the legacy saved REPL source store.

```bash
seeknal source list --project .
```

### `seeknal source add` / `seeknal source remove`

Manage legacy encrypted REPL sources. Prefer `source connect` for new Ask
connected-source projects.

```bash
export SEEKNAL_ENCRYPT_KEY="..."
seeknal source add mydb --url postgres://user:pass@localhost/mydb
seeknal source remove mydb
```


## Teaching Ask reusable context

Connected-source mode is read-only toward the database, but the chat agent can
remember user-taught project knowledge when the user explicitly asks it to save
or remember something. Short rules go to `preferences.yml`; longer notes and
join patterns go under `context/`; reusable SQL examples go under
`context/sql_pairs/`.

Examples users can type in `seeknal ask chat`:

```text
Remember: use net_sales for revenue questions.
Write this down: product.company_id joins to company.id.
Save this query as a SQL pair for AMDK trend by industry scale: ...
```

This keeps the connected database safe while allowing the project to accumulate
validated business definitions and SQL patterns. Do not save secrets, passwords,
DSNs, API keys, or temporary one-off filters as memory.

## `seeknal_agent.yml` example

```yaml
mode:
  default: auto
sources:
  warehouse:
    source_kind: connected
    source_type: database
    connector: postgresql
    namespace: warehouse
    dsn_env: WAREHOUSE_URL
    access: read_only
    role: business_source_of_truth
    priority: 100
    description: Production analytics warehouse
    context_sync:
      enabled: true
      refresh_policy: manual
      stale_after_hours: 24
      templates:
        - overview
        - columns
        - relationships
        - profiling
```

## See Also

- [seeknal ask](ask.md) - Ask chat, reports, SQL pairs, and Ask tests
- [seeknal repl](repl.md) - Interactive SQL REPL
- [seeknal init](init.md) - Initialize a project
