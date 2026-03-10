---
title: "feat: Seeknal Ask — NL-to-Analysis Agent"
type: feat
status: active
date: 2026-03-06
origin: docs/brainstorms/2026-03-06-seeknal-ask-nl-to-analysis-brainstorm.md
---

# feat: Seeknal Ask — NL-to-Analysis Agent

## Enhancement Summary

**Deepened on:** 2026-03-06
**Sections enhanced:** All 5 phases + security + performance + architecture
**Research agents used:** 13 (agent-native, python-reviewer, typescript-reviewer, performance-oracle, security-sentinel, architecture-strategist, code-simplicity, pattern-recognition, data-integrity, agent-native-architecture-skill, frontend-design-skill, best-practices-researcher, learnings-researcher)

### Key Improvements

1. **MVP Scope Reduction**: Cut from 17 KAI modules to 3 core modules (artifact_discovery, sql_generation, session). Drop DeepAgents dependency — use single LangGraph ReAct agent with built-in self-correction. Start CLI-only, defer React frontend to Phase 2.
2. **Critical Security Fix**: DuckDB `read_text()`/`read_csv_auto()` functions bypass REPL allowlist. Must add `SET enable_external_access = false` and block `INSTALL`/`LOAD`/`ATTACH`/`SET` keywords for agent connections.
3. **Performance**: REPL must be singleton (not per-request). DuckDB single-writer requires connection pooling or serialized access under FastAPI async.
4. **Missing Agent Tools**: `list_tables` and `describe_table` are critical gaps — agent cannot discover queryable tables without them.
5. **Storage Simplification**: Use single DuckDB for both relational and vector storage (drop SQLite + DuckDB dual-database). Use LangGraph's built-in `SqliteSaver` for checkpointing.
6. **Institutional Learnings**: 9 documented solutions apply — DuckDB CAST for timestamps, HUGEINT→BIGINT for aggregations, GROUP BY completeness, path traversal defense-in-depth.

### New Considerations Discovered

- **Prompt injection defense-in-depth**: User messages go directly to LLM — need input sanitization layer and restrictive `validate_sql_for_agent()` function
- **DuckDB function blocklist**: `read_text`, `read_blob`, `read_csv_auto`, `read_json`, `read_parquet` (arbitrary paths), `http_get`, `http_post` must be blocked for agent-generated SQL
- **VectorStore abstraction**: Define `VectorStore` protocol/ABC for DuckDB vss with fallback to FTS5-only — prevents painful rewrite if vss is too immature
- **Dependency boundary**: `seeknal.ask` may import from `seeknal.cli.repl`, `seeknal.workflow.consolidation.catalog`, `seeknal.pipeline.context`, `seeknal.dag.manifest` — nothing else. No reverse imports.

---

## Overview

Seeknal Ask is a natural-language-to-analysis agent that lets users query seeknal project artifacts conversationally. It combines a **full fork of KAI** (Python/FastAPI backend with LangChain, LangGraph, and DeepAgents) with **nao's React frontend** (TanStack Router + Shadcn chat UI), adapted to exclusively query seeknal data artifacts.

Users type questions like "What's the average order value by region?" and the agent discovers seeknal entities, generates DuckDB SQL, executes it via the read-only REPL, and streams results with charts and follow-up suggestions.

(see brainstorm: `docs/brainstorms/2026-03-06-seeknal-ask-nl-to-analysis-brainstorm.md`)

## Problem Statement

Seeknal produces rich data artifacts (consolidated entity parquets, feature groups, intermediate outputs, DAG manifests, entity catalogs) but accessing them requires SQL knowledge and manual REPL usage. There is no conversational interface, no semantic search over metadata, and no way for non-technical users to explore seeknal-managed data.

KAI provides a battle-tested NL-to-SQL agent framework (172 Python files, 26 modules) but targets generic database connections. nao provides an excellent streaming chat UI but uses a Node.js backend. Neither integrates with seeknal's artifact system.

## Proposed Solution

Fork KAI's Python backend into `src/seeknal/ask/`, replace Typesense storage with SQLite + DuckDB vss, replace database scanning with seeknal artifact discovery, and adapt nao's React frontend to communicate with a FastAPI backend. The result is a self-contained agent that understands seeknal's data model natively.

## Technical Approach

### Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    React Frontend (nao fork)              │
│  TanStack Router + Query + Shadcn + Recharts             │
│  Streaming via fetch (NDJSON) + REST API                 │
├──────────────────────────────────────────────────────────┤
│  FastAPI Backend (KAI fork)                              │
│  ┌────────────┐ ┌────────────┐ ┌─────────────────────┐  │
│  │ Agent      │ │ Session    │ │ Artifact Discovery  │  │
│  │ (LangGraph │ │ (SQLite    │ │ (EntityCatalog +    │  │
│  │ +DeepAgents│ │ checkpoint)│ │  Manifest + REPL)   │  │
│  └─────┬──────┘ └────────────┘ └─────────────────────┘  │
│        │                                                  │
│  ┌─────┴──────┐ ┌────────────┐ ┌─────────────────────┐  │
│  │ SQL Gen    │ │ Analytics  │ │ Knowledge           │  │
│  │ (DeepAgent │ │ (Forecast, │ │ (Glossary, Skills,  │  │
│  │ subagents) │ │ Anomaly)   │ │  Instructions)      │  │
│  └─────┬──────┘ └────────────┘ └─────────────────────┘  │
│        │                                                  │
│  ┌─────┴──────────────────────────────────────────────┐  │
│  │ Storage Layer (SQLite + DuckDB vss)                │  │
│  │ Replaces KAI's Typesense                           │  │
│  └────────────────────────────────────────────────────┘  │
├──────────────────────────────────────────────────────────┤
│  Seeknal Core (existing)                                 │
│  REPL.execute_oneshot() │ EntityCatalog │ PipelineContext │
│  Manifest │ RunState │ profiles.yml                      │
└──────────────────────────────────────────────────────────┘
```

### Implementation Phases

---

#### Phase 1: Foundation — Module Scaffolding & Storage Layer

**Goal**: Establish the `src/seeknal/ask/` module structure and build the SQLite + DuckDB vss storage layer that replaces KAI's Typesense.

**Tasks:**

1.1. **Create module structure**
   - `src/seeknal/ask/__init__.py` — public API exports
   - `src/seeknal/ask/server/` — FastAPI app, config, middleware
   - `src/seeknal/ask/modules/` — KAI module ports (17 modules, see mapping below)
   - `src/seeknal/ask/storage/` — Storage layer (SQLite + DuckDB vss)
   - `src/seeknal/ask/agents/` — LLM providers, tool definitions, DeepAgents factory
   - `src/seeknal/ask/utils/` — Shared utilities (model adapter, SQL tools, embeddings)
   - `tests/ask/` — Test directory
   - Verify: `import seeknal.ask` succeeds

1.2. **Add optional dependency group in `pyproject.toml`**
   ```toml
   [project.optional-dependencies]
   ask = [
     "fastapi>=0.112.0",
     "uvicorn>=0.30.0",
     "langchain>=0.3.0",
     "langgraph>=0.4.0",
     "deepagents>=0.2.8",
     "langchain-google-genai>=2.0.0",
     "langchain-openai>=0.3.0",
     "langchain-ollama>=0.3.0",
     "httpx>=0.28.0",
     "scipy>=1.11.0",
     "plotly>=5.0.0",
   ]
   ```
   - Use atlas-pattern dynamic loading in CLI (`try/except ImportError`)
   - Verify: `pip install -e ".[ask]"` installs all deps

1.3. **Build SQLite storage layer** (`src/seeknal/ask/storage/`)
   - `src/seeknal/ask/storage/sqlite_storage.py` — Core Storage class replacing KAI's `TypeSenseDB`
   - Database file: `~/.seeknal/ask.db` (or project-local `.seeknal/ask.db`)
   - Implement KAI's Storage interface:
     - `insert_one(collection, doc) -> str` (UUID generation)
     - `find_one(collection, filter) -> dict | None`
     - `find_by_id(collection, id) -> dict | None`
     - `find(collection, filter, sort, page, limit) -> list[dict]`
     - `find_all(collection, page, limit) -> list[dict]`
     - `update_or_create(collection, filter, doc) -> str`
     - `delete_by_id(collection, id) -> bool`
     - `full_text_search(collection, query, columns) -> list[dict]`
   - Schema management: Auto-create tables from KAI's 19 collection schemas
   - JSON columns for nested objects/arrays (SQLite JSON1 extension)
   - FTS5 virtual tables for full-text search
   - Verify: All CRUD operations pass with unit tests

1.4. **Build DuckDB vss vector search** (`src/seeknal/ask/storage/vector_search.py`)
   - DuckDB vss extension for HNSW-based vector similarity search
   - `hybrid_search(collection, query, query_by, vector_query, filter_by, limit) -> list[dict]`
   - Embedding storage: `float[]` columns in DuckDB
   - Support 5 embedding-enabled collections: instructions, table_descriptions, skills, memories, context_stores
   - Configurable embedding dimensions (768/1536)
   - Verify: Vector similarity search returns ranked results

1.5. **Port KAI's embedding model adapter** (`src/seeknal/ask/utils/embedding_model.py`)
   - Support: Google GenAI (primary), OpenAI, Ollama
   - Unified interface: `get_embedding(text) -> list[float]`
   - Configurable via environment variables or config
   - Verify: Embeddings generated for test strings

1.6. **Build LangGraph SQLite checkpointer** (`src/seeknal/ask/storage/checkpointer.py`)
   - Replace KAI's `TypesenseCheckpointer` with SQLite-backed implementation
   - Implement `BaseCheckpointSaver` interface: `aget`, `aget_tuple`, `aput`, `alist`
   - Store checkpoints as JSON blobs in `session_checkpoints` table
   - Verify: LangGraph session state persists across restarts

**Success criteria**: Storage layer passes all CRUD + search tests. Module imports work. Optional deps install cleanly.

### Research Insights (Phase 1)

**Simplification (from code-simplicity review):**
- Drop DuckDB vss for MVP. Use SQLite FTS5 for text search. Add vector search only when FTS5 proves insufficient (~100-1000 documents scale).
- Consider using **single DuckDB database** for both relational CRUD and vector search instead of SQLite + DuckDB dual-database. DuckDB supports JSON columns + FTS extension + vss extension in one process.
- Drop the embedding model adapter for MVP. No embeddings needed if using FTS5 only.

**LangGraph checkpointer (from framework docs):**
- LangGraph provides **built-in `SqliteSaver`**: `from langgraph.checkpoint.sqlite import SqliteSaver`. No custom checkpointer needed!
- Supports encryption via `EncryptedSerializer.from_pycryptodome_aes()` if needed.
- For MVP CLI, use `MemorySaver` (in-memory, no persistence). Add `SqliteSaver` for session persistence later.

**Data integrity (from data-integrity review):**
- SQLite concurrent access from FastAPI requires **WAL mode**: `PRAGMA journal_mode=WAL`
- Use `aiosqlite` for async access or serialize writes via a connection pool
- JSON serialization: use `json.dumps()` for nested objects. Test round-trip fidelity with complex types.

**Performance (from performance-oracle):**
- REPL must be a **singleton** — created once at server/CLI startup, reused across requests. REPL construction takes 1-3s (extension loading, parquet scanning, PostgreSQL attach).
- Add a lightweight `repl.refresh()` method for when project artifacts change (new `seeknal run`).
- DuckDB in-memory connections are **not thread-safe** for concurrent writes. Under FastAPI async, serialize DuckDB access via `asyncio.Lock` or use a single-threaded executor.

```python
# Singleton REPL pattern
_repl: Optional[REPL] = None

def get_repl(project_path: Path) -> REPL:
    global _repl
    if _repl is None:
        _repl = REPL(project_path=project_path, skip_history=True)
    return _repl
```

**Files created:**
- `src/seeknal/ask/__init__.py`
- `src/seeknal/ask/storage/__init__.py`
- `src/seeknal/ask/storage/sqlite_storage.py`
- `src/seeknal/ask/storage/vector_search.py` (deferred — FTS5 only for MVP)
- `src/seeknal/ask/utils/__init__.py`
- `tests/ask/test_sqlite_storage.py`

---

#### Phase 2: Agent Core — LLM Adapter, Tools & SQL Generation

**Goal**: Port KAI's agent orchestration layer, adapted for seeknal artifacts.

**Tasks:**

2.1. **Port LLM adapter factory** (`src/seeknal/ask/agents/providers.py`)
   - Fork KAI's `ChatModel` with support for:
     - Google Gemini (primary): `langchain-google-genai`
     - OpenAI: `langchain-openai`
     - Anthropic: `langchain-anthropic`
     - Ollama: `langchain-ollama`
     - OpenRouter: via `langchain-openai` with custom base URL
   - Configuration via `~/.seeknal/ask_config.yaml` or env vars:
     ```yaml
     llm:
       provider: google
       model: gemini-2.5-pro
       api_key: ${GOOGLE_API_KEY}
     embedding:
       provider: google
       model: text-embedding-004
     ```
   - Verify: Each provider generates text and embeddings

2.2. **Build seeknal artifact discovery** (`src/seeknal/ask/modules/artifact_discovery/`)
   - Replaces KAI's `table_description` module
   - `ArtifactDiscoveryService`:
     - Scan `target/feature_store/` for consolidated entities
     - Load `_entity_catalog.json` for each entity (schema, FGs, features, types)
     - Load `manifest.json` for DAG structure (nodes, edges, types)
     - Load `target/run_state.json` for execution history
     - Scan `target/intermediate/` for node outputs
     - Load `profiles.yml` for materialized source info
   - Generate markdown context docs per entity (like nao's `columns.md`/`preview.md`):
     ```
     .seeknal/ask/context/
     ├── entity_customer.md        # Schema, FGs, sample data
     ├── entity_product.md
     ├── transform_clean_orders.md  # Intermediate node info
     └── dag_overview.md           # DAG structure summary
     ```
   - `get_context_for_prompt() -> str` — builds LLM-ready context string
   - Verify: Context generated for a sample seeknal project with entities

2.3. **Build seeknal-specific tool set** (`src/seeknal/ask/agents/tools/`)
   - Fork KAI's tool spec pattern, adapted for seeknal:

   | Tool | File | Description |
   |------|------|-------------|
   | `execute_sql` | `execute_sql.py` | `REPL.execute_oneshot(sql, limit)` wrapper. Read-only. Returns columns + rows. |
   | `get_entities` | `get_entities.py` | List consolidated entities from `target/feature_store/` |
   | `get_entity_schema` | `get_entity_schema.py` | Load `EntityCatalog` — FGs, features, types, join keys |
   | `get_features` | `get_features.py` | `ctx.features(entity, feature_list)` — cross-FG retrieval |
   | `get_dag_info` | `get_dag_info.py` | Manifest nodes, edges, lineage |
   | `get_run_state` | `get_run_state.py` | Execution history, timing, row counts |
   | `search_context` | `search_context.py` | Semantic search over entity/node metadata (DuckDB vss) |
   | `display_chart` | `display_chart.py` | Generate Recharts-compatible chart spec from query results |
   | `story` | `story.py` | Create/update narrative markdown report with embedded charts |
   | `suggest_follow_ups` | `suggest_follow_ups.py` | Generate 3 follow-up questions |

   - Each tool follows KAI's `ToolSpec` pattern:
     ```python
     @dataclass
     class ToolSpec:
         name: str
         description: str
         input_schema: type[BaseModel]
         output_schema: type[BaseModel]
         execute: Callable
         to_model_output: Callable  # Format result for LLM
     ```
   - `ToolContext` carries: `project_path`, `repl`, `pipeline_ctx`, `session_id`
   - Verify: Each tool executes against test data and returns valid output

2.4. **Port DeepAgents SQL generation** (`src/seeknal/ask/agents/deep_agent_factory.py`)
   - Fork KAI's `create_kai_sql_agent()` → `create_seeknal_sql_agent()`
   - Subagents:
     - `sql_drafter` — Generate DuckDB SQL from natural language + seeknal context
     - `sql_validator` — Validate SQL syntax, check read-only, verify table references
   - System prompt includes:
     - Seeknal entity schemas (from artifact discovery)
     - DuckDB SQL dialect rules (struct access with `.`, `CAST` for HUGEINT, etc.)
     - User instructions and business glossary
     - Few-shot examples from context store
   - `TagStreamParser` for streaming `<thinking>`, `<answer>`, `<suggestions>` tags
   - Verify: Agent generates valid DuckDB SQL for sample questions against test entities

2.5. **Port session graph** (`src/seeknal/ask/modules/session/graph/`)
   - Fork KAI's LangGraph `SessionState` and graph:
     - `route_query()` — classify: database_query vs reasoning_only
     - `process_database_query()` — build context → SQL generation → execute → summarize
     - `process_reasoning_only()` — follow-up reasoning over previous results
     - `generate_response()` — format for streaming
   - Use SQLite checkpointer from Phase 1
   - Message compaction for long conversations (summarize old turns)
   - Verify: Multi-turn conversation maintains state across turns

**Success criteria**: Agent answers questions about a seeknal project, generating and executing correct DuckDB SQL. Multi-turn conversations work.

### Research Insights (Phase 2)

**CRITICAL — Security (from security-sentinel):**

The REPL's read-only allowlist (SELECT/WITH/DESCRIBE/SHOW/PRAGMA) is **insufficient** for agent-generated SQL. DuckDB has built-in functions that can read arbitrary files:

```sql
-- These pass the current allowlist (all start with SELECT):
SELECT * FROM read_text('/etc/passwd')
SELECT * FROM read_csv_auto('/etc/shadow')
SELECT * FROM read_parquet('/sensitive/data.parquet')
```

**Mandatory mitigations:**
1. **Set `enable_external_access = false`** on the agent's DuckDB connection — prevents file system and network access beyond registered views
2. **Add to `DANGEROUS_SQL_KEYWORDS`**: `INSTALL`, `LOAD`, `ATTACH`, `DETACH`, `SET`, `PRAGMA` (for agent connections)
3. **Create `validate_sql_for_agent()`** — more restrictive than REPL's `validate_sql()`. Only allow queries referencing registered views/tables.
4. **DuckDB function blocklist**: Block `read_text`, `read_blob`, `read_csv_auto`, `read_json`, `read_parquet`, `http_get`, `http_post`, `read_json_auto` in agent-generated SQL

```python
AGENT_BLOCKED_FUNCTIONS = {
    'read_text', 'read_blob', 'read_csv_auto', 'read_json',
    'read_json_auto', 'read_parquet', 'http_get', 'http_post',
    'read_csv', 'glob', 'read_json_objects',
}

def validate_sql_for_agent(sql: str) -> None:
    """More restrictive validation for LLM-generated SQL."""
    validate_sql(sql)  # Existing REPL validation
    sql_lower = sql.lower()
    for func in AGENT_BLOCKED_FUNCTIONS:
        if func in sql_lower:
            raise ValueError(f"Function '{func}' not allowed in agent queries")
```

**Prompt injection defense:**
- Wrap user messages in clear delimiters: `<user_question>...</user_question>`
- Never include system instructions after user content
- Post-validate all generated SQL before execution (defense-in-depth)

**Simplification — Drop DeepAgents (from code-simplicity review):**

A single LangGraph ReAct agent with tool-calling provides the same self-correction:
1. LLM generates SQL via `execute_sql` tool call
2. If REPL returns error, LLM sees the error and retries (built-in ReAct loop)
3. No need for separate `sql_drafter` + `sql_validator` subagents

This eliminates `deepagents>=0.2.8` dependency and `deep_agent_factory.py`.

**Missing Tools (from agent-native review):**

Two critical tools are missing — without them, the agent cannot discover what's queryable:

| Tool | Why Critical |
|------|-------------|
| `list_tables` | Maps to REPL's `.tables` — shows all registered views (intermediates, entities, attached DBs). Without this, agent guesses table names. |
| `describe_table` | Maps to REPL's `.schema <table>` — shows columns and types for any table. `get_entity_schema` only covers consolidated entities, not intermediates or attached sources. |

**Revised MVP tool set (5 tools):**
1. `execute_sql` — Run read-only SQL via REPL
2. `list_tables` — List all registered views/tables
3. `describe_table` — Get column names and types for a table
4. `get_entities` — List consolidated entities with metadata
5. `get_entity_schema` — EntityCatalog detail (FGs, features, join keys)

Defer: `get_features`, `get_dag_info`, `get_run_state`, `search_context`, `display_chart`, `story`, `suggest_follow_ups`

**Institutional Learnings (from learnings-researcher):**

Include these in the SQL generation system prompt / validation:

1. **DuckDB CAST for timestamps**: When generating time-range queries, use `CAST('{value}' AS TIMESTAMP)` before INTERVAL arithmetic. DuckDB doesn't auto-cast string literals.
2. **HUGEINT→BIGINT**: Always use `CAST(COUNT(*) AS BIGINT)` and `CAST(SUM(...) AS DOUBLE)` in aggregations. DuckDB returns HUGEINT by default.
3. **GROUP BY completeness**: DuckDB enforces strict SQL — all non-aggregate columns in SELECT must be in GROUP BY. Add validation to detect incomplete GROUP BY.
4. **Path traversal**: Sanitize any user-provided identifiers used in file paths with `_sanitize_*()` methods. Validate base paths with `is_insecure_path()`.

**LLM Providers (from simplicity review):**

MVP: Support 2 providers, not 5:
- **Google Gemini** (primary cloud) — user preference
- **Ollama** (local models) — for offline/privacy-sensitive use

Add OpenAI, Anthropic, OpenRouter later as users request.

**Files created:**
- `src/seeknal/ask/agents/__init__.py`
- `src/seeknal/ask/agents/providers.py`
- `src/seeknal/ask/agents/tools/execute_sql.py`
- `src/seeknal/ask/agents/tools/list_tables.py`
- `src/seeknal/ask/agents/tools/describe_table.py`
- `src/seeknal/ask/agents/tools/get_entities.py`
- `src/seeknal/ask/agents/tools/get_entity_schema.py`
- `src/seeknal/ask/modules/artifact_discovery/service.py`
- `src/seeknal/ask/modules/session/graph.py`
- `tests/ask/test_tools.py`
- `tests/ask/test_session_graph.py`

---

#### Phase 3: Knowledge, Memory & CLI

**Goal**: Port KAI's knowledge management modules and build the `seeknal ask` CLI.

**Tasks:**

3.1. **Port repository layer** (`src/seeknal/ask/modules/*/repositories/`)
   - Adapt all 17 KAI repositories to use SQLite storage instead of Typesense
   - Priority repositories:
     - `SessionRepository` — chat history persistence
     - `InstructionRepository` — custom SQL generation rules (+ vector search)
     - `SkillRepository` — reusable analysis patterns (+ vector search)
     - `MemoryRepository` — long-term learned preferences (+ vector search)
     - `ContextStoreRepository` — few-shot prompt-to-SQL cache (+ vector search)
     - `BusinessGlossaryRepository` — metric definitions
   - Lower priority (port but can be stubbed initially):
     - `AnalysisResultRepository`, `DashboardRepository`, `NotebookRepository`
   - Remove: `DatabaseConnectionRepository` (not needed — seeknal-only)
   - Verify: Each repository CRUD works with SQLite backend

3.2. **Port service layer** (`src/seeknal/ask/modules/*/services/`)
   - Port KAI's service classes, wiring repositories to business logic:
     - `SessionService` — create/list/delete sessions, stream queries
     - `InstructionService` — CRUD for SQL instructions
     - `SkillService` — discover/manage analysis skills
     - `MemoryService` — extract/store/recall learned preferences
     - `GlossaryService` — manage business metric definitions
     - `ContextStoreService` — manage few-shot examples
     - `SQLGenerationService` — orchestrate SQL generation via DeepAgents adapter
   - Verify: Services integrate correctly with repositories and agent

3.3. **Build CLI** (`src/seeknal/cli/ask.py`)
   - Create `ask_app = typer.Typer(name="ask", help="AI-powered data analysis")`
   - Register in `main.py` using atlas-pattern dynamic loading:
     ```python
     def _register_ask_commands():
         try:
             from seeknal.cli.ask import ask_app
             app.add_typer(ask_app, name="ask")
         except ImportError:
             @app.command("ask", hidden=True)
             def ask_not_installed():
                 _echo_error("Seeknal Ask requires: pip install seeknal[ask]")
                 raise typer.Exit(1)
     ```
   - Commands:
     ```
     seeknal ask                    # Default: interactive chat (alias for `seeknal ask chat`)
     seeknal ask chat               # Interactive multi-turn chat in terminal
     seeknal ask "question"         # One-shot query (detect quoted string as question)
     seeknal ask serve              # Start FastAPI + bundled frontend
     seeknal ask serve --port 8015  # Custom port
     seeknal ask knowledge glossary add/list/remove
     seeknal ask knowledge instruction add/list/remove
     seeknal ask knowledge skill discover/list/remove
     seeknal ask session list/show/delete
     seeknal ask config             # Show current LLM/embedding config
     ```
   - Interactive chat uses Rich for terminal rendering:
     - Streaming text output
     - Tool call indicators (spinner + result summary)
     - SQL syntax highlighting
     - Table rendering for query results (tabulate)
   - Project discovery: scan current directory for `seeknal.yml` or `target/` dir
   - Verify: `seeknal ask "how many entities do I have?"` returns correct answer

3.4. **Port analytics module** (`src/seeknal/ask/modules/analytics/`)
   - Fork KAI's analytics service:
     - Time series forecasting (Prophet/statsmodels)
     - Anomaly detection (IQR, Z-score, isolation forest)
     - Correlation analysis (Pearson, Spearman)
   - Adapts to work with DuckDB query results (pandas DataFrames)
   - Verify: Forecast generates predictions from seeknal time-series data

**Success criteria**: CLI works end-to-end. Interactive chat streams responses.

### Research Insights (Phase 3)

**Simplification (from code-simplicity + architecture reviews):**

- **Drop service-repository-model pattern** for MVP. KAI uses three-layer architecture for a multi-tenant SaaS. Seeknal Ask is a single-user local tool. Use plain functions or a single thin class per concern.
- **Cut analytics module from Phase 1** — forecasting, anomaly detection, correlation add 4,117 LOC and heavy dependencies (`scipy`, `plotly`, `prophet`). Users just want to query data.
- **Cut memory, skill, glossary, context_store, instruction modules** from Phase 1. These are enhancements for when the core agent loop is proven.
- **MVP CLI**: Just `seeknal ask "question"` (one-shot) and `seeknal ask chat` (interactive). Defer `knowledge`, `session`, `config` subcommands.

**Architecture boundary (from architecture-strategist):**

`seeknal.ask` may import from:
- `seeknal.cli.repl` (REPL, execute_oneshot)
- `seeknal.workflow.consolidation.catalog` (EntityCatalog)
- `seeknal.pipeline.context` (PipelineContext)
- `seeknal.dag.manifest` (Manifest)

No code in `seeknal.*` (outside `ask`) should ever import from `seeknal.ask`. Document this as a hard rule.

**Pattern consistency (from pattern-recognition):**

- Use `ask_config.yaml` (not `config.toml`) to avoid confusion with seeknal's existing `config.toml`
- Follow atlas-pattern dynamic loading for CLI registration
- Match existing `_echo_success/error/warning/info` CLI helpers
- Use `@handle_cli_error` decorator for error handling

**Files created:**
- `src/seeknal/cli/ask.py`
- `tests/ask/test_cli.py`

---

#### Phase 4: Frontend & API Layer

**Goal**: Adapt nao's React frontend and build the FastAPI API that bridges frontend to agent.

**Tasks:**

4.1. **Build FastAPI application** (`src/seeknal/ask/server/`)
   - `src/seeknal/ask/server/app.py` — FastAPI app factory
   - `src/seeknal/ask/server/config.py` — Settings (pydantic-settings)
   - `src/seeknal/ask/server/middleware.py` — CORS, auth (simple token or session-based)
   - Routes:
     ```
     POST /api/agent              # Streaming agent endpoint (NDJSON)
     GET  /api/chats              # List chats
     GET  /api/chats/{id}         # Get chat with messages
     DELETE /api/chats/{id}       # Delete chat
     POST /api/chats/{id}/rename  # Rename chat
     POST /api/chats/{id}/stop    # Stop active agent
     GET  /api/entities           # List seeknal entities
     GET  /api/entities/{name}    # Entity schema detail
     GET  /api/dag                # DAG manifest
     POST /api/knowledge/glossary # CRUD for glossary
     POST /api/knowledge/instructions # CRUD for instructions
     POST /api/knowledge/skills   # CRUD for skills
     GET  /api/config             # Current LLM/project config
     ```
   - Verify: All endpoints return correct responses with test data

4.2. **Implement NDJSON streaming** (`src/seeknal/ask/server/routes/agent.py`)
   - Must match AI SDK UIMessageStream format exactly:
     ```python
     async def stream_agent(request: AgentRequest) -> StreamingResponse:
         async def event_generator():
             # Custom events
             yield ndjson({"type": "data-newChat", "data": {...}})
             yield ndjson({"type": "data-newUserMessage", "data": {...}})

             # Agent stream
             async for chunk in agent.stream(messages):
                 if chunk.type == "text":
                     yield ndjson({"type": "text", "textDelta": chunk.delta})
                 elif chunk.type == "tool_call":
                     yield ndjson({
                         "type": f"tool-{chunk.tool_name}",
                         "toolCallId": chunk.id,
                         "state": chunk.state,
                         "input": chunk.input,
                         "output": chunk.output,
                     })

         return StreamingResponse(
             event_generator(),
             media_type="application/x-ndjson",
             headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
         )
     ```
   - Verify: Frontend receives and renders streamed chunks correctly

4.3. **Fork and adapt nao's React frontend** (`ui/`)
   - Copy nao's `apps/frontend/` to `ui/`
   - **Replace tRPC client with REST client**:
     - Create `ui/src/lib/api-client.ts` — fetch-based client matching REST endpoints
     - Update all `trpc.*` calls to use the new client
     - Keep TanStack Query for caching/invalidation
   - **Update streaming hook** (`ui/src/hooks/use-agent.ts`):
     - Change `/api/agent` URL if needed
     - Remove nao-specific custom data parts, add seeknal-specific ones
   - **Rebrand**:
     - Title: "Seeknal Ask"
     - Remove nao references (logo, footer, about)
     - Keep Shadcn theme, adjust primary color to match seeknal brand
   - **Add seeknal-specific UI**:
     - Entity browser panel (list entities, show schemas)
     - DAG visualization (optional, can use existing lineage ASCII)
     - Project info header (project name, last run, entity count)
   - **Remove nao-specific features**:
     - Slack integration
     - Google OAuth (simplify to token auth or no auth for local)
     - MCP server management UI
   - **Build configuration** (`ui/vite.config.ts`):
     - Dev proxy: `/api` → `http://localhost:8015`
     - Build output: `ui/dist/` (static assets)
   - Verify: Chat UI streams responses, tool outputs render, entities browsable

4.4. **Static asset serving from FastAPI**
   - `seeknal ask serve` bundles everything:
     1. Build frontend if `ui/dist/` doesn't exist (or `--build` flag)
     2. Start uvicorn with FastAPI app
     3. Mount `ui/dist/` as static files at `/`
     4. API routes at `/api/*`
     5. Open browser automatically
   - For development: `seeknal ask serve --dev` starts both uvicorn + vite dev server
   - Verify: `seeknal ask serve` opens working chat UI in browser

**Success criteria**: Full web UI works — user types question, sees streaming response with SQL, tables, and charts.

### Research Insights (Phase 4)

**MVP Recommendation (from code-simplicity review):**

The React frontend is **deferred to Phase 2**. Seeknal is a CLI-first data engineering tool — its users live in terminals. The CLI (`seeknal ask chat` with Rich rendering) covers the primary user. Adding a React app, Node.js build dependency, FastAPI server, NDJSON streaming, and static asset bundling for a business-user persona that may not exist is premature.

**However**, design the agent's output format to be frontend-ready from day one. Use structured tool results (JSON with columns/rows/chart specs) so the React frontend can render them without agent changes later.

**When building the frontend (Phase 2):**

- **tRPC → REST migration risk** (from TypeScript reviewer): tRPC provides end-to-end type safety. Replacing it with REST loses this. Mitigate by generating TypeScript types from FastAPI's OpenAPI spec (`openapi-typescript-codegen`).
- **NDJSON streaming format** (from agent-native review): The AI SDK UIMessageStream format uses specific chunk types (`text`, `tool-{name}`, `data-*`). Must match exactly or the frontend's `useChat` hook won't parse responses. Document the precise wire format.
- **FastAPI streaming** (from framework docs):
  ```python
  return StreamingResponse(
      event_generator(),
      media_type="application/x-ndjson",
      headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache, no-transform"},
  )
  ```
- **Frontend design** (from frontend-design skill): Avoid generic AI chat aesthetics. Add seeknal-specific elements: entity browser sidebar, SQL syntax highlighting, data table with sorting, chart type selector. Make it feel like a data tool, not a chatbot.

**Files created (Phase 2 — deferred):**
- `src/seeknal/ask/server/__init__.py`
- `src/seeknal/ask/server/app.py`
- `src/seeknal/ask/server/config.py`
- `src/seeknal/ask/server/routes/agent.py`
- `ui/` (forked from nao)
- `tests/ask/test_api.py`

---

#### Phase 5: Polish, Testing & Documentation

**Goal**: End-to-end testing, documentation, and production hardening.

**Tasks:**

5.1. **Integration tests** (`tests/ask/e2e/`)
   - E2E test with real seeknal project (jaffle-shop example or similar):
     1. Run `seeknal run` to produce artifacts
     2. Run `seeknal ask "how many customers?"` → verify correct count
     3. Run `seeknal ask chat` → multi-turn conversation → verify context maintained
     4. Run `seeknal ask serve` → test API endpoints with httpx
   - Mock LLM calls for CI (use recorded responses)
   - Verify: All E2E tests pass

5.2. **Port KAI's remaining modules** (lower priority)
   - `dashboard/` — NL-to-dashboard generation
   - `chartviz/` — Advanced chart configuration
   - `rag/` — Document search (for user-provided docs in project)
   - `notebook/` — Analysis notebooks
   - These can be stubbed initially and filled in incrementally

5.3. **Documentation**
   - `docs/ask/` directory:
     - `getting-started.md` — Install, configure LLM, first query
     - `configuration.md` — LLM providers, embedding models, config file
     - `knowledge-management.md` — Glossary, instructions, skills
     - `api-reference.md` — REST API endpoints
   - Update `README.md` with Seeknal Ask section
   - Update `CLAUDE.md` with new module patterns

5.4. **Security hardening** (CRITICAL — must be Phase 1, not Phase 5)
   - **DuckDB external access**: `conn.execute("SET enable_external_access = false")` on agent connections — prevents `read_text()`, `read_csv_auto()`, `http_get()` bypass of read-only allowlist
   - **Agent SQL validation**: Create `validate_sql_for_agent()` with function blocklist (see Phase 2 Research Insights)
   - **Keyword blocklist expansion**: Add `INSTALL`, `LOAD`, `ATTACH`, `DETACH`, `SET` to dangerous keywords for agent connections
   - **Prompt injection defense**: Wrap user messages in `<user_question>` delimiters; never include system instructions after user content; post-validate all generated SQL
   - **Path validation**: Agent tools use `path_security.is_insecure_path()` — sanitize all identifiers used in file paths
   - **API auth** (Phase 2): Simple bearer token for `seeknal ask serve`
   - Verify: Test with payloads like `SELECT * FROM read_text('/etc/passwd')`, `../../../etc/shadow`, prompt injection attempts

5.5. **Configuration & environment**
   - Config file: `~/.seeknal/ask_config.yaml` (global) or `.seeknal/ask_config.yaml` (project)
   - Environment variables: `SEEKNAL_ASK_LLM_PROVIDER`, `SEEKNAL_ASK_API_KEY`, etc.
   - Environment-aware: `seeknal ask --env-name staging "question"` queries staging artifacts
   - Verify: Config loads correctly, env vars override file config

---

## Alternative Approaches Considered

### 1. Build from scratch (rejected)
KAI provides ~172 Python files of agent infrastructure. Rewriting would take months and miss KAI's self-correcting SQL generation, session management, analytics, and memory system.

### 2. KAI + KAI's Next.js frontend (rejected)
KAI's Next.js frontend is heavier and less focused on the chat experience. nao's frontend is simpler, has better streaming UX, and tool output rendering with "stories". (see brainstorm)

### 3. Pure Python / no frontend (rejected)
A CLI-only agent misses the business user audience. The web UI is essential for Phase 2 adoption. Starting with both ensures the API contract is designed correctly from day one. (see brainstorm)

### 4. Keep Typesense (rejected)
Adding a Typesense dependency for a data engineering tool is heavy. SQLite + DuckDB vss provide sufficient capability for single-project scale without running an external service. (see brainstorm)

---

## System-Wide Impact

### Interaction Graph

`seeknal ask "question"` → CLI parses args → creates `REPL` instance (triggers three-phase auto-registration: parquets → PostgreSQL → Iceberg) → creates `AgentService` → LangGraph session graph routes query → DeepAgents SQL generation → `REPL.execute_oneshot(sql)` → results returned → LLM summarizes → output streamed to CLI/UI.

`seeknal ask serve` → FastAPI startup → mounts static frontend → `/api/agent` POST → same agent flow above but streaming via NDJSON → React frontend renders incrementally.

### Error & Failure Propagation

- LLM API errors → caught in agent service → streamed as error message to user
- SQL execution errors → REPL returns error → agent sees error → self-corrects (up to 3 retries via DeepAgents validator)
- Storage errors (SQLite) → logged + error returned to user
- Frontend streaming errors → fetch catches → displayed as error banner
- REPL auto-registration failures → best-effort (warnings, not errors — existing pattern)

### State Lifecycle Risks

- **Session checkpoints**: SQLite transactions ensure atomic writes. Partial failure leaves last-known-good state.
- **Chat messages**: Saved after agent completes each turn. Interruption loses only the current response.
- **Knowledge items**: Standard SQLite CRUD — no orphan risk.
- **Vector embeddings**: Computed on insert. If embedding fails, document saved without embedding (text search still works).

### API Surface Parity

New interfaces introduced:
- `seeknal ask` CLI commands (new)
- FastAPI REST endpoints at `/api/*` (new)
- `src/seeknal/ask/` Python module (new)

Existing interfaces unchanged — no modifications to seeknal's core CLI, REPL, PipelineContext, or EntityCatalog.

### Integration Test Scenarios

1. **Full pipeline → ask**: `seeknal run` → `seeknal ask "count entities"` → correct count from consolidated parquets
2. **Multi-turn with context**: Ask about customers → follow up "break down by region" → agent uses previous SQL as context
3. **Self-correcting SQL**: Ask question that generates invalid SQL → agent detects error → retries with corrected SQL
4. **Knowledge injection**: Add instruction "always filter active=true" → subsequent queries include the filter
5. **Web UI streaming**: Open browser → type question → see streaming text + tool calls + chart render

---

## Acceptance Criteria

### Functional Requirements

- [x] `seeknal ask "question"` returns data-backed answer in terminal
- [x] `seeknal ask chat` supports multi-turn conversation with session persistence
- [ ] `seeknal ask serve` starts web UI accessible at `http://localhost:8015`
- [x] Agent discovers all entities, feature groups, and schemas from seeknal artifacts
- [x] Generated SQL executes via REPL (read-only) and returns correct results
- [ ] Streaming works in both CLI and web UI
- [ ] Tool outputs render correctly: SQL results as tables, charts as Recharts visualizations
- [ ] Knowledge management: glossary, instructions, skills persist and influence generation
- [ ] Session history persists across CLI invocations and web UI refreshes
- [ ] Multiple LLM providers work (Google Gemini, OpenAI, Anthropic, Ollama)
- [ ] Environment-aware: `--env-name` queries correct environment's artifacts

### Non-Functional Requirements

- [ ] First response token streams within 2s of query submission
- [ ] SQL generation completes within 10s for typical queries
- [ ] Storage operations < 50ms (SQLite)
- [ ] Vector search < 200ms for collections up to 1000 documents
- [ ] Frontend bundle < 2MB gzipped
- [x] No external services required beyond LLM API (no Typesense, no Redis)

### Quality Gates

- [ ] Unit tests for storage layer (SQLite CRUD, vector search, checkpointer)
- [ ] Unit tests for each tool (execute_sql, get_entities, etc.)
- [ ] Integration test for CLI one-shot and chat modes
- [ ] API endpoint tests with mock agent
- [ ] E2E test with real seeknal project + mocked LLM

---

## Success Metrics

1. **Correctness**: Agent generates valid SQL for 90%+ of questions about seeknal entities
2. **Adoption**: Data team uses `seeknal ask` daily instead of raw REPL for exploratory queries
3. **Self-service**: Business users can get answers via web UI without SQL knowledge
4. **Retention**: Session persistence enables multi-day analysis workflows

---

## Dependencies & Prerequisites

- **Seeknal project with artifacts**: Requires `seeknal run` to have been executed (entities, intermediates, manifests exist)
- **LLM API key**: At least one provider configured (Google Gemini recommended)
- **Python 3.11+**: Required by seeknal
- **Node.js 18+**: Required for frontend build
- **Optional**: PostgreSQL/Iceberg connections in `profiles.yml` for materialized data access

---

## Risk Analysis & Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| **DuckDB file-read bypass** | High | Critical | `SET enable_external_access = false` + function blocklist + `validate_sql_for_agent()`. **NEW — discovered by security review.** |
| **Prompt injection** | High | High | Input delimiters, post-validation of generated SQL, defense-in-depth. **NEW.** |
| DuckDB vss too immature | Medium | High | Start with FTS5 only. Define `VectorStore` protocol/ABC for future swap. |
| REPL per-request initialization | High | High | Singleton REPL pattern. Add `repl.refresh()` for artifact changes. **NEW — discovered by performance review.** |
| DuckDB concurrency under async | Medium | High | `asyncio.Lock` or single-threaded executor for DuckDB access. **NEW.** |
| LLM generates invalid DuckDB SQL | Medium | Medium | LangGraph ReAct self-correction (REPL error → retry) + DuckDB dialect rules + CAST/GROUP BY hints in prompt. |
| nao frontend adaptation harder than expected | Medium | Medium | Deferred to Phase 2. Start CLI-only. |
| KAI fork diverges from upstream | High | Low | Accept divergence. Minimize ported code (3 modules not 17). |
| Optional dependency bloat | Low | Medium | Strict `[ask]` optional group. Core seeknal remains lightweight. |

---

## Resource Requirements

- **Backend (Python)**: ~40-50 files ported/adapted from KAI
- **Frontend (React)**: ~30-40 files adapted from nao
- **Tests**: ~15-20 test files
- **Documentation**: ~5 doc files

---

## Future Considerations

- **Multi-project**: Agent serving multiple seeknal projects (currently one per session)
- **Slack/Teams integration**: Forward questions from chat platforms
- **Scheduled reports**: Cron-based analysis runs with email delivery
- **Custom tools**: Plugin system for user-defined agent tools
- **Fine-tuned models**: Seeknal-specific SQL generation model
- **Collaborative sessions**: Multiple users in one chat session

---

## Documentation Plan

- `docs/ask/getting-started.md` — Quick start guide
- `docs/ask/configuration.md` — LLM, embedding, and project config
- `docs/ask/knowledge-management.md` — Glossary, instructions, skills
- `docs/ask/api-reference.md` — REST API documentation
- Update `docs/getting-started-comprehensive.md` with Seeknal Ask section
- Update `README.md` with feature mention

---

## Sources & References

### Origin

- **Brainstorm document:** [docs/brainstorms/2026-03-06-seeknal-ask-nl-to-analysis-brainstorm.md](docs/brainstorms/2026-03-06-seeknal-ask-nl-to-analysis-brainstorm.md) — Key decisions carried forward: full KAI fork + nao frontend, seeknal-only context, SQLite+DuckDB storage, `seeknal ask` CLI

### Internal References

- CLI patterns: `src/seeknal/cli/main.py` (Typer subcommands, error handling, output helpers)
- REPL: `src/seeknal/cli/repl.py:execute_oneshot()` (SQL execution, read-only enforcement)
- Atlas pattern: `src/seeknal/cli/atlas.py` (optional dependency loading, FastAPI/uvicorn)
- PipelineContext: `src/seeknal/pipeline/context.py:features()`, `entity()`
- EntityCatalog: `src/seeknal/workflow/consolidation/catalog.py`
- Manifest: `src/seeknal/dag/manifest.py`
- pyproject.toml: packaging, optional deps, entry points

### External References

- KAI codebase: `/Volumes/Hyperdisk/project/self/KAI` (Python agent framework — 172 files, 26 modules)
- nao codebase: `/Volumes/Hyperdisk/project/self/nao` (React frontend — TanStack Router + Shadcn)
- KAI storage layer: `KAI/app/data/db/storage.py` (19 Typesense collections, 17 repositories)
- nao API contract: `nao/apps/frontend/src/hooks/use-agent.ts` (NDJSON streaming, UIMessage schema)
- DuckDB vss extension: https://duckdb.org/docs/extensions/vss
- LangGraph checkpointing: https://langchain-ai.github.io/langgraph/concepts/persistence/
- AI SDK UIMessageStream: https://sdk.vercel.ai/docs/ai-sdk-ui/stream-protocol
