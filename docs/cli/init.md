---
summary: Initialize a new Seeknal project
read_when: You are starting a new project with Seeknal
related:
  - run
  - list
---

# seeknal init

Initialize a new Seeknal project in the current directory.

## Synopsis

```bash
seeknal init [OPTIONS]
```

## Description

Creates the initial project structure including:
- `seeknal.yaml` - Project configuration file
- `features/` - Directory for feature definitions
- `sources/` - Directory for source configurations

## Options

| Option | Description |
|--------|-------------|
| `--name` | Project name (defaults to directory name) |
| `--description` | Project description |

## Examples

### Basic initialization

```bash
seeknal init
```

### With project name

```bash
seeknal init --name my_project --description "My ML feature store"
```

## See Also

- [seeknal run](run.md) - Execute pipelines
- [seeknal list](list.md) - List project resources
