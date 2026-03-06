---
title: "Seeknal Ask v2: Deep Agent + Source-Level Context"
date: 2026-03-06
status: active
topic: seeknal-ask-v2-deep-agent
---

# Seeknal Ask v2: Deep Agent + Source-Level Context

## What We're Building

Enhance Seeknal Ask in two ways:

1. **Source-level artifact context** — Expand ArtifactDiscovery to also scan the seeknal project's source folder, reading YAML pipeline definitions, Python transforms, flow configs, and entity definitions. Currently only `target/` outputs are used. Inspired by nao's pattern of making the full project folder explorable by the agent.

2. **Deep Agent harness** — Replace the current LangGraph `create_react_agent` with `deepagents.create_deep_agent()` from `langchain-ai/deepagents` (v0.4.5). This adds built-in planning (todo tool), sub-agent spawning for parallel analysis, filesystem middleware for scratch space, and auto-summarization to prevent context overflow.

---

## Why This Approach

### Problem with current architecture

The current Seeknal Ask agent (v1 MVP) has two limitations:

**Shallow context**: Only `target/` artifacts are loaded into the system prompt — entity catalogs, DAG manifests, intermediate parquet file names. The agent has no visibility into *how* the data was produced: the SQL transforms, source definitions, feature group configs, aggregation logic. When a user asks "why is revenue calculated this way?", the agent can only look at the output data, not the pipeline definition that computed it.

**Shallow agent loop**: The ReAct pattern (LLM -> tool -> LLM -> tool -> END) works for simple queries but struggles with complex multi-step analysis. There's no planning capability, no ability to decompose a research question into sub-tasks, and no context management for long sessions.

### Inspiration: How nao does it

Nao treats the entire project folder as the agent's workspace. Key patterns:

- **Dynamic discovery via tools**: The agent has `read`, `grep`, `list`, `search` tools to explore project files on demand — it doesn't dump everything into the system prompt
- **Database schemas as markdown**: Synced to a hierarchical folder structure (`databases/type=postgres/database=prod/schema=public/table=users/columns.md`)
- **Virtual path system**: Agent uses `/` = project root, converted to real paths with path traversal prevention
- **`.naoignore`**: Glob patterns to hide sensitive files from agent exploration
- **Memory system**: Extracts persistent facts and rules from conversations for cross-session context

**Key insight**: nao doesn't dump all context into the system prompt. The agent *discovers* context through tool use, loading only what's relevant to the current question. This scales to large projects.

### Why deepagents fits

`deepagents` (from `langchain-ai/deepagents`, v0.4.5, MIT license) solves the "shallow agent" problem by adding four capabilities on top of ReAct:

| Capability | What it does | Why we need it |
|-----------|-------------|----------------|
| **Planning tool** (`write_todos`) | Agent decomposes questions into sub-tasks | Complex analyses like "compare cohort retention across segments" need multiple SQL queries with intermediate reasoning |
| **Auto-summarization** | Prevents context window overflow | Multi-turn sessions with 10+ exchanges currently risk losing early context |
| **Sub-agent spawning** (`task` tool) | Delegate sub-analyses with isolated context | Future: parallel exploration of different data dimensions |
| **Filesystem middleware** | Read/write scratch files | Agent can save intermediate findings, build up reports |

It's still a LangGraph compiled graph — works with existing checkpointers, streaming, and LangGraph Studio. Migration from `create_react_agent` to `create_deep_agent` is straightforward since both return the same graph type.

---

## Key Decisions

### 1. Source artifact discovery: Hybrid static + dynamic

**Decision**: Expand the system prompt with a *summary* of source artifacts, plus add tools for on-demand exploration.

**Static context** (in system prompt):
- List of all pipeline YAML files with their `kind` and `name`
- Project metadata from `seeknal_project.yml`
- Connection names from `profiles.yml` (redacted — no credentials)

**Dynamic tools** (agent calls on demand):
- `read_pipeline(name)` — Read a specific YAML/Python pipeline definition
- `search_pipelines(query)` — Search across pipeline definitions by content

This mirrors nao's pattern: overview in the prompt, details via tools.

### 2. What source artifacts to expose

Seeknal projects have this source structure:

```
my_project/
├── seeknal/                    # Pipeline definitions (YAML + Python)
│   ├── sources/               # kind: source
│   ├── transforms/            # kind: transform
│   ├── feature_groups/        # kind: feature_group
│   ├── aggregations/          # kind: aggregation
│   └── models/                # kind: model
├── pipelines/                 # Python pipeline files
├── profiles.yml               # Connection configs
├── seeknal_project.yml        # Project metadata
└── target/                    # Output artifacts (already scanned)
```

**Expose**: All YAML pipeline definitions, Python transforms, project metadata, connection names.
**Redact**: Credentials from profiles.yml, `.env` files, any secrets.
**Skip**: `target/` (already handled), `__pycache__`, `.git`, virtual envs.

### 3. Agent harness: create_react_agent -> create_deep_agent

**Decision**: Use `create_deep_agent()` with custom tools, disable filesystem middleware.

```python
from deepagents import create_deep_agent

agent = create_deep_agent(
    model=llm,
    tools=[execute_sql, list_tables, describe_table, get_entities,
           get_entity_schema, read_pipeline, search_pipelines],
    system_prompt=SYSTEM_PROMPT.format(context=context),
)
```

**What we keep from v1:**
- All existing tools (execute_sql, list_tables, describe_table, get_entities, get_entity_schema)
- Security layer (validate_sql_for_agent, configure_safe_connection)
- Provider abstraction (Google, Ollama)
- ArtifactDiscovery (extended with source scanning)

**What deepagents adds:**
- `write_todos` tool — agent can plan multi-step analyses
- Auto-summarization — handles long multi-turn sessions gracefully

**What we DON'T use from deepagents (yet):**
- Filesystem middleware (read_file, write_file, etc.) — agent should NOT have general filesystem access
- Sub-agent spawning (`task` tool) — adds complexity and cost; enable later if needed

### 4. Security boundaries for source file access

**Decision**: Read-only, scoped to project, filtered.

- Agent can only read files within the seeknal project root
- Credentials in `profiles.yml` are redacted (show connection names + types only)
- `.env` files are never exposed
- Python files are read-only (no execution of arbitrary Python)
- File paths validated against path traversal (reuse `path_security.py`)
- Add `.seeknalignore` support (like nao's `.naoignore`) for user-controlled filtering

### 5. Dependency management

**Decision**: Add `deepagents` to `[ask]` optional dependencies.

```toml
[project.optional-dependencies]
ask = [
    "deepagents>=0.4.0",
    "langchain-google-genai>=2.1.0",
    # langgraph and langchain-core come as deepagents transitive deps
]
```

---

## Resolved Questions

1. **Should the agent modify pipeline YAML?** — No. Read-only for v2. A future "Seeknal Copilot" mode could allow writing pipelines.

2. **How to handle large projects (100+ YAML files)?** — The system prompt includes only a summary (kind, name per file). Actual content loaded on-demand via tools. This scales fine.

3. **Memory persistence across sessions?** — Not in v2 MVP. deepagents has MemoryMiddleware we can enable later. Per-session memory via LangGraph MemorySaver is sufficient for now.

---

## Open Questions

1. **deepagents API stability**: v0.4.5 is pre-1.0. How stable is `create_deep_agent()`'s API? Should we pin to exact version or allow minor upgrades?

2. **Auto-summarization behavior**: How does deepagents' auto-summarization interact with our custom tools? Does it preserve tool call results in the summary, or could it lose important SQL output?

3. **Planning overhead for simple queries**: The planning tool adds overhead for simple "how many customers?" questions. Should we detect query complexity and route simple questions through a lightweight path (ReAct) and complex ones through the deep agent?

---

## Success Criteria

1. Agent can answer "how is X calculated?" by reading the pipeline YAML that produces X
2. Agent uses planning for complex multi-step analyses (visible todo decomposition)
3. Long multi-turn sessions (10+ turns) don't degrade due to context overflow
4. All existing QA tests continue passing
5. No security regressions (source file access is read-only, scoped, filtered)
6. `deepagents` dependency integrates cleanly with existing provider abstraction

---

## Scope

### In scope (v2)
- [ ] Expand ArtifactDiscovery to scan source-level artifacts (YAML summary)
- [ ] Add `read_pipeline` tool (read specific YAML/Python pipeline)
- [ ] Add `search_pipelines` tool (search across pipeline definitions)
- [ ] Replace `create_react_agent` with `create_deep_agent`
- [ ] Credential redaction for profiles.yml
- [ ] Update system prompt with source artifact context
- [ ] Update and expand tests

### Out of scope (future)
- Pipeline modification/generation (Copilot mode)
- Memory persistence across sessions
- Sub-agent spawning
- Filesystem middleware (general file access)
- Web UI / chat interface
- MCP server integration
- `.seeknalignore` support
