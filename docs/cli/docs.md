---
summary: Search CLI documentation from the terminal
read_when: You want to find docs on a specific command or topic
related:
  - info
---

# seeknal docs

Search and browse Seeknal CLI documentation from the terminal.

## Synopsis

```bash
seeknal docs [QUERY] [OPTIONS]
```

## Description

The `docs` command provides searchable access to CLI documentation directly from your terminal. It searches all doc topics in `docs/cli/` using full-text search powered by ripgrep.

When given an exact topic name (matching a filename stem like `repl`, `env`, `run`), it displays that topic directly. Otherwise, it performs a full-text search across all documentation files.

## Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `QUERY` | TEXT | None | Search query or exact topic name |
| `--list`, `-l` | FLAG | False | List all available documentation topics |
| `--json` | FLAG | False | Output results as JSON (useful for tooling) |
| `--max-results`, `-n` | INTEGER | 10 | Maximum number of search results |

## Examples

### List all topics

```bash
seeknal docs --list
```

### Search for a topic

```bash
seeknal docs repl
```

### Full-text search

```bash
seeknal docs "materialization"
```

### JSON output

```bash
seeknal docs --json "environment"
```

### Limit results

```bash
seeknal docs "transform" --max-results 5
```

## Prerequisites

Full-text search requires [ripgrep](https://github.com/BurntSushi/ripgrep) (`rg`). If not installed, the command will show platform-specific installation instructions:

- **macOS**: `brew install ripgrep`
- **Linux**: `apt install ripgrep` or `snap install ripgrep`
- **Windows**: `choco install ripgrep` or `scoop install ripgrep`

Exact topic matches work without ripgrep.

## See Also

- [seeknal info](info.md) - Show version information
- [CLI Reference](../reference/cli.md) - Complete CLI command reference
