# Seeknal Ask: NL-to-Analysis Agent

**Date**: 2026-03-06
**Status**: Draft
**Author**: AI-assisted brainstorm

---

## What We're Building

**Seeknal Ask** is a natural-language-to-analysis agent that lets users query seeknal project artifacts conversationally. Instead of writing SQL or Python, users type questions like "What's the average order value by region last month?" and get answers with data, charts, and explanations.

### Core Concept

Seeknal already produces rich data artifacts (consolidated entity parquets, feature groups, intermediate outputs, DAG manifests, entity catalogs). Seeknal Ask adds an intelligent query layer on top: an LLM agent that understands the project's data model and translates natural language into DuckDB SQL, executes it, and presents results.

### Target Users

1. **Phase 1 (CLI)**: Data engineers and scientists who work with seeknal projects daily. A smarter REPL that understands context.
2. **Phase 2 (Web UI)**: Business analysts and stakeholders who need self-service access to seeknal-managed data without SQL knowledge.

---

## Why This Approach

### Architecture: Full KAI Fork + nao Frontend

We're combining the best of two open-source data agent projects:

- **Backend**: Fork of [KAI](https://github.com/...) (Python/FastAPI) — provides a mature, modular agent framework with LangChain, LangGraph, and DeepAgents integration. 26 feature modules covering SQL generation, sessions, analytics, dashboards, skills, memory, and more.
- **Frontend**: Adapted from [nao](https://getnao.io) (React/TanStack Router + Shadcn) — provides a clean chat interface with streaming, tool output rendering, stories/artifacts, and a lightweight UX.

### Why not build from scratch?

KAI provides ~172 Python files of battle-tested agent infrastructure (tool specs, LLM adapters, session graphs, analytics pipelines). Rewriting this would take months. By forking, we get:
- Multi-step SQL generation with self-correction (DeepAgents subagents)
- LangGraph session management with state persistence
- Streaming output with thinking/suggestions
- Analytics (forecasting, anomaly detection, correlation)
- Dashboard generation
- Skill library (reusable analysis patterns)
- Memory system for learned preferences

### Why nao's frontend over KAI's?

- nao's frontend is simpler and more focused on the chat experience
- TanStack Router + Shadcn is lighter than KAI's Next.js 14 setup
- nao has better streaming UX with tool output rendering and "stories" (interactive artifacts)
- Easier to adapt since nao's frontend already talks to a separate backend process

---

## Key Decisions

### 1. Location: Inside seeknal repo

**Path**: `src/seeknal/ask/` (Python backend) + `ui/` (React frontend)

**Rationale**: Tight integration with seeknal's existing code. The agent imports seeknal's REPL, EntityCatalog, PipelineContext, and Manifest directly. Single repo, single deployment.

### 2. Data Context: Seeknal-only

The agent exclusively queries seeknal project artifacts:
- **Consolidated entities**: `target/feature_store/{entity}/features.parquet` (via DuckDB REPL views)
- **Intermediate outputs**: `target/intermediate/{kind}_{name}.parquet`
- **Entity catalogs**: `_entity_catalog.json` (schema discovery, FG lineage, join keys)
- **DAG manifests**: Node types, configs, dependencies
- **Run state**: Execution history, node status, timing, row counts
- **Materialized tables**: PostgreSQL and Iceberg (via DuckDB ATTACH/Iceberg extension)

No direct external database connections. This keeps the agent focused and leverages seeknal's existing security model (read-only REPL, validated paths).

### 3. Storage: SQLite + DuckDB (replace Typesense)

Replace KAI's Typesense dependency with:
- **SQLite**: Sessions, chat history, instructions, skills, memory, business glossary (structured data)
- **DuckDB vss extension**: Vector search for few-shot example retrieval, semantic table search, RAG

**Rationale**: Fewer dependencies. Seeknal already uses SQLite. DuckDB's vss extension provides HNSW-based vector search sufficient for project-scale data. No external service to run.

**Trade-off**: Typesense is more mature for vector search at scale. For single-project use, DuckDB vss is adequate. Can revisit if performance becomes an issue.

### 4. LLM Providers: All KAI providers (Google Gemini priority)

Keep KAI's full LLM adapter supporting:
- Google Gemini (primary)
- OpenAI (GPT-4.1/GPT-5)
- Anthropic (Claude)
- Ollama (local models)
- OpenRouter (multi-model gateway)

### 5. CLI: `seeknal ask` subcommand

```bash
# Interactive chat
seeknal ask chat

# One-shot query
seeknal ask "how many customers placed orders last week?"

# Start web UI server
seeknal ask serve

# Manage context
seeknal ask knowledge glossary add "MRR" --definition "Monthly Recurring Revenue = SUM(subscription_amount)"
seeknal ask knowledge instruction add "Always filter by is_active = true for customer queries"
seeknal ask knowledge skill discover
```

### 6. Frontend: nao's React stack (adapted)

- TanStack Router (file-based routing)
- TanStack Query (server state)
- Zustand (client state)
- Shadcn/Radix UI (components)
- Tailwind CSS (styling)
- Streaming via fetch + Server-Sent Events

Connects to seeknal's FastAPI backend instead of nao's Fastify server.

### 7. Agent Architecture

```
User Question
    |
    v
[Session Graph - LangGraph]
    |-- route_query() --> classify intent
    |       |
    |       v
    |-- process_database_query()
    |       |-- Build context from EntityCatalog + Manifest
    |       |-- Invoke SQL generation (DeepAgents)
    |       |       |-- sql_drafter subagent
    |       |       |-- sql_validator subagent
    |       |-- Execute SQL via seeknal REPL (read-only)
    |       |-- Summarize results
    |       v
    |-- process_reasoning_only() (follow-ups)
    |       v
    +-- generate_response() --> stream to user
```

### 8. Tool Set (seeknal-specific)

Core tools the agent can call:

| Tool | Description |
|------|-------------|
| `execute_sql` | Run read-only SQL via seeknal REPL |
| `get_entities` | List all consolidated entities |
| `get_entity_schema` | Get EntityCatalog for an entity (FGs, features, types) |
| `get_features` | Retrieve specific features cross-FG (`ctx.features()`) |
| `get_dag_info` | Get DAG manifest (nodes, dependencies, lineage) |
| `get_run_state` | Get execution history (status, timing, row counts) |
| `search_context` | Semantic search over project metadata |
| `display_chart` | Generate chart visualization spec |
| `suggest_follow_ups` | Generate next-step questions |

---

## Integration Points with Existing Seeknal

### REPL Integration
- Agent uses `REPL.execute_oneshot(sql, limit)` for query execution
- Inherits REPL's three-phase auto-registration (parquets, PostgreSQL, Iceberg)
- Read-only enforcement (SELECT/DESCRIBE/SHOW/PRAGMA only)
- Environment-aware: respects `--env-name` for non-production queries

### Context Discovery
- Auto-scans `target/feature_store/` for consolidated entities
- Reads `_entity_catalog.json` for schema metadata
- Loads `manifest.json` for DAG structure
- Reads `target/run_state.json` for execution history
- Loads `profiles.yml` for source connection info

### PipelineContext Integration
- `ctx.features(entity, feature_list)` for cross-FG feature selection
- `ctx.entity(entity)` for full struct column access
- Point-in-time join support for temporal queries

---

## Module Mapping (KAI -> Seeknal Ask)

| KAI Module | Seeknal Ask Equivalent | Adaptation |
|-----------|----------------------|------------|
| `sql_generation` | `sql_generation` | Replace DB scanning with EntityCatalog context |
| `session` | `session` | Replace Typesense checkpointer with SQLite |
| `table_description` | `artifact_discovery` | Auto-scan seeknal target/ directory |
| `instruction` | `instruction` | Keep as-is (SQLite storage) |
| `business_glossary` | `glossary` | Keep as-is (SQLite storage) |
| `skill` | `skill` | Keep as-is (markdown-based) |
| `memory` | `memory` | Replace Typesense with SQLite |
| `analytics` | `analytics` | Keep (forecasting, anomaly, correlation) |
| `dashboard` | `dashboard` | Keep (NL-to-dashboard) |
| `chartviz` | `chartviz` | Keep (chart spec generation) |
| `rag` | `rag` | Replace Typesense with DuckDB vss |
| `database_connection` | Remove | Not needed (seeknal-only) |
| `context_store` | `context_store` | Few-shot examples via DuckDB vss |
| `autonomous_agent` | `cli` | Adapt to `seeknal ask` subcommands |
| `mdl` | Remove | Seeknal has its own semantic layer (entities) |

---

## Resolved Questions

1. **Project auto-detection**: Scan current directory for seeknal project markers (seeknal.yml, target/ dir), same as `seeknal run`. Add `--project` flag as override.

2. **Multi-project support**: One project per session. Switch projects by starting a new session. Simpler context management, no ambiguity.

3. **Deployment model**: `seeknal ask serve` starts FastAPI backend + serves bundled React static assets. Single command, single port. Like nao's `nao chat` pattern.

## Open Questions

1. **DuckDB vss maturity**: Is DuckDB's vector search extension mature enough for production use? Need to evaluate HNSW index performance for few-shot retrieval at project scale (~100-1000 examples).

2. **nao frontend ↔ FastAPI backend**: nao's frontend expects a Fastify/Node backend with specific API contracts (tRPC, streaming JSON). How much adaptation is needed to make it talk to a FastAPI backend? Key areas: SSE streaming format, UIMessage schema, tool result rendering.

---

## Success Criteria

1. User can run `seeknal ask "question"` in a seeknal project directory and get a data-backed answer
2. Agent correctly discovers all entities, feature groups, and schemas from seeknal artifacts
3. Generated SQL executes successfully via the REPL (read-only)
4. Streaming chat works in both CLI and web UI
5. Session history persists across conversations
6. Analytics features (forecasting, anomaly detection) work on seeknal data
