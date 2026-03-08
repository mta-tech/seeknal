---
title: "Seeknal Ask Enhanced Tooling: Filesystem Search, Python Execution, Auto-Documentation"
date: 2026-03-08
status: complete
participants: [user, claude]
---

# Seeknal Ask Enhanced Tooling

## What We're Building

Three new capability layers for `seeknal ask` that transform it from a SQL-only analyst into a full-stack data investigation platform:

1. **Filesystem search tools** (`grep_project`, `read_file`) — Let the agent search and read any file in the project to understand data lineage, find table definitions, and trace data pipelines
2. **Python code execution** (`execute_python`) — Let the agent write and run Python code for advanced analysis (pandas, matplotlib, scipy, statistical modeling) beyond what SQL can do
3. **Auto-documentation CLI** (`seeknal document`) — LLM-powered documentation generation that enriches pipeline YAML files with descriptions and column-level docs

The seeknal project itself becomes the source of truth. The agent can navigate it like a human developer would — reading code, searching for patterns, querying data, and running analysis code.

**Inspiration:** nao (external product) — where the project is the knowledge base and the LLM navigates it. Also KAI for Python execution and auto-documentation patterns.

## Why This Approach

### Current Limitations

The agent currently has 7 tools, but they're narrow:
- `search_pipelines` — substring match on pipeline YAML only, max 20 results
- `read_pipeline` — reads pipeline files only (not arbitrary project files)
- `execute_sql` — DuckDB SQL only, no Python analytics
- No documentation generation capability at all

### What Changes

| Capability | Before | After |
|-----------|--------|-------|
| File search | Substring on pipeline YAML only | Regex grep across entire project |
| File reading | Pipeline YAML/Python only | Any file in project (with security) |
| Data analysis | SQL only | SQL + full Python (pandas, matplotlib, scipy) |
| Documentation | Manual YAML `description` fields | LLM-generated, enriched in-place |

## Key Decisions

1. **Filesystem search scope: Full project directory** — Not limited to seeknal artifacts. The agent can search README files, notebooks, scripts, and custom code. Exclusions: `.git/`, `target/`, `__pycache__/`, `node_modules/`, `*.pyc`.

2. **Python execution: Full access (Jupyter-like)** — No sandboxing. The agent can use any installed library, access the DuckDB connection, generate plots. User trusts the agent like they'd trust a Jupyter notebook. Pre-load the DuckDB connection as `conn` so `conn.sql("SELECT ...")` works immediately.

3. **Auto-documentation: On-demand CLI command** — Not embedded in the agent conversation. User runs `seeknal document` to generate docs for the entire project. This keeps the agent focused on answering questions, not writing docs during Q&A.

4. **Documentation storage: Enrich pipeline YAML in-place** — Add `description` and `column_descriptions` fields directly in the pipeline YAML files. Keeps docs co-located with code, version-controlled, and visible to the agent via existing `read_pipeline` tool.

5. **Tool convention: Follow existing patterns** — Each tool is a `@tool`-decorated function in its own file under `src/seeknal/ask/agents/tools/`, returns strings, uses `get_tool_context()`, and handles errors as return strings.

## Capability Details

### 1. Filesystem Search Tools

#### `grep_project` tool

- **Purpose:** Regex search across all files in the project directory
- **Parameters:** `pattern` (regex string), optional `file_pattern` (glob like `*.yml`)
- **Returns:** Matching lines with file path, line number, and content (markdown formatted)
- **Limits:** Max 50 results to avoid overwhelming the LLM context
- **Exclusions:** `.git/`, `target/`, `__pycache__/`, `node_modules/`, `.env`, `profiles.yml`, binary files
- **Security:** No path traversal outside project root, blocked sensitive files same as existing tools

#### `read_file` tool

- **Purpose:** Read any file in the project by relative path
- **Parameters:** `path` (relative to project root), optional `max_lines` (default 200)
- **Returns:** File content with line numbers (markdown code fence)
- **Security:** Path traversal protection (must resolve within project root), blocked sensitive files (`.env`, `profiles.yml`, `profiles.yaml`)
- **Replaces:** Could subsume `read_pipeline` but we keep both for backward compatibility and clearer LLM tool selection

### 2. Python Code Execution

#### `execute_python` tool

- **Purpose:** Execute Python code for data analysis
- **Parameters:** `code` (Python code string)
- **Returns:** stdout output + last expression value (like Jupyter cell output)
- **Pre-loaded namespace:**
  - `conn` — DuckDB connection with all project tables registered
  - `pd` — pandas
  - `np` — numpy
  - `plt` — matplotlib.pyplot
  - Standard library modules
- **Plot handling:** If matplotlib figures are created, save to temp PNG and return the file path
- **Error handling:** Return traceback as string (don't raise)
- **No sandboxing:** Full Python execution, user trusts the agent

### 3. Auto-Documentation CLI

#### `seeknal document` command

- **Purpose:** Generate LLM-powered documentation for all pipeline artifacts
- **Workflow:**
  1. Scan all pipeline YAML/Python files in the project
  2. For each pipeline, gather context: schema info, sample data (via DuckDB), upstream/downstream lineage
  3. Send to LLM with prompt: "Generate a description and column descriptions for this pipeline"
  4. Write `description` and `column_descriptions` back into the YAML file
  5. Only fill in missing/empty fields (preserve existing manual docs)
- **Options:**
  - `--provider` / `--model` — Same LLM provider options as `seeknal ask`
  - `--dry-run` — Show what would be generated without writing
  - `--force` — Overwrite existing descriptions
  - `--pipeline <name>` — Document a specific pipeline only
- **Output format in YAML:**
  ```yaml
  kind: transform
  name: orders_cleaned
  description: "Filters cancelled orders and calculates revenue by multiplying amount by quantity"
  column_descriptions:
    order_id: "Unique identifier for each order"
    revenue: "Calculated as amount * quantity for completed orders"
  inputs:
    - ref: source.raw_orders
  sql: |
    SELECT *, amount * quantity AS revenue
    FROM raw_orders
    WHERE status = 'completed'
  ```

## Open Questions

*None — all questions resolved during brainstorming.*

## Scope Boundaries

### In Scope
- Three new agent tools: `grep_project`, `read_file`, `execute_python`
- One new CLI command: `seeknal document`
- System prompt updates to teach the agent about new tools
- Unit tests for all new tools
- Updated artifact discovery to surface generated docs

### Out of Scope
- Web UI for documentation browsing
- Automated doc generation on `seeknal run` (future consideration)
- Data lineage visualization (separate feature, see existing brainstorm)
- Python execution sandboxing/containerization
- Agent-initiated documentation during conversations (only CLI for now)

## Success Criteria

1. Agent can answer "where is column X defined?" by grepping project files
2. Agent can read any project file to understand custom Python transforms
3. Agent can run pandas/scipy code for statistical analysis and return results
4. `seeknal document` generates useful descriptions for all pipelines in a project
5. Generated docs are available to the agent via existing `read_pipeline` tool
