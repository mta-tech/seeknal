---
date: 2026-03-25
topic: kai-integration-analysis
focus: Evaluate whether to integrate KAI capabilities into Seeknal or keep projects separate
---

# Ideation: KAI Integration Analysis

## Codebase Context

### KAI (~/project/mta/KAI)
AI-powered data agent for natural language database querying:
- **Stack:** FastAPI + LangGraph + LangChain + Typesense
- **Capabilities:** NL-to-SQL for external DBs (PostgreSQL, MySQL, SQLite), multi-LLM support (OpenAI, Google, Ollama, OpenRouter), long-term memory, business glossary, skills, analytics (forecasting/anomaly), dashboards
- **Interfaces:** 40+ REST endpoints, CLI (8 command groups), Next.js Web UI
- **Architecture:** Service-Repository-Model pattern per module

### Seeknal (current project)
Data engineering platform for pipelines and feature store:
- **Stack:** Typer CLI + DuckDB + Spark + SQLite/Turso
- **Capabilities:** Multi-engine data processing, feature store, Flow orchestration, entity consolidation, materialization (PostgreSQL/Iceberg)
- **Existing Agent:** Seeknal Ask module with LangGraph agent
  - Tools: execute_sql, execute_python, generate_report, list_tables, describe_table, get_entities, read_pipeline
  - LLM providers: Google Gemini, Ollama (fewer than KAI)
  - Only queries DuckDB views (no external DB support)

### Key Observation
**Both systems already use LangGraph + LangChain** - architectural alignment exists. Seeknal Ask already has core agent tools. The question is whether to merge or selectively adopt.

## Ranked Ideas

### 1. DAGRunner Hooks for Agent-Aware Pipelines
**Description:** Add lifecycle hooks (pre_node, post_node, on_error) to DAGRunner that can invoke agent callbacks, enabling Seeknal Ask to provide real-time analysis during pipeline execution rather than only post-run.

**Rationale:** DAGRunner is the core orchestrator (~600 lines) with no callback mechanism. Hooks would enable live monitoring, AI-assisted debugging when data quality issues occur mid-pipeline, and alerting. Low implementation burden: add `List[Callable]` hooks at `__init__`, invoke at key points.

**Downsides:** Requires thread safety; hooks could slow execution if misused.

**Confidence:** 92%
**Complexity:** Medium
**Status:** Explored (2026-03-25 brainstorm)

---

### 2. REST API Exposure for Integrations
**Description:** Expose REPL and Ask agent capabilities via FastAPI endpoints, enabling web UIs, Slack bots, external orchestrators (Prefect, Airflow), and BI platforms to consume Seeknal programmatically.

**Rationale:** Currently CLI-only. REPL and Ask agent are already encapsulated classes with clean interfaces. API exposure unlocks headless operation, scheduled runs, and integration with existing toolchains. CLI already exposes all functionality - just needs HTTP wrapper.

**Downsides:** Adds security surface; requires authentication/authorization design.

**Confidence:** 87%
**Complexity:** Medium
**Status:** Unexplored

---

### 3. Terminal Visualization Fallback
**Description:** Add terminal-native charts (ASCII/Unicode via Rich/Textual) as fallback when Node.js unavailable, instead of requiring Evidence.dev (Node.js 18+ and npm ~500MB).

**Rationale:** Data engineers often work in terminals. `report/builder.py` requires Node.js and errors if missing. Rich/Textual libraries provide terminal charts and tables. Reduces onboarding friction for users without Node.js environment.

**Downsides:** Terminal charts are simpler than Evidence HTML; this is a complement, not replacement. Best as fallback mode when Node unavailable.

**Confidence:** 75%
**Complexity:** Medium
**Status:** Unexplored

---

### 4. Analytics Tools Extension
**Description:** Extend execute_python sandbox with scipy, statsmodels, scikit-learn for built-in forecasting, anomaly detection, and clustering capabilities.

**Rationale:** Sandbox already has pandas, numpy, matplotlib. Adding stats libraries enables "forecast next quarter revenue" or "detect anomalies in this time series" as one-shot NL queries without custom Python code each time.

**Downsides:** Increases dependency footprint; some models require significant memory for large datasets.

**Confidence:** 71%
**Complexity:** Low
**Status:** Unexplored

---

### 5. Manifest Diff as Change Intelligence API
**Description:** Expose ManifestDiff.get_nodes_to_rebuild() as public API that agents can query to understand "what changed since last run" for change intelligence, drift detection, and impact analysis.

**Rationale:** Change detection logic already exists in `dag/diff.py` with `ManifestDiff` class. Exposing it enables agents to answer "why is this metric stale?" by checking snapshot IDs and fingerprints. Enables CI/CD integration and drift detection.

**Downsides:** API design needed; currently tightly coupled to runner internals.

**Confidence:** 68%
**Complexity:** Low-Medium
**Status:** Unexplored

---

### 6. Semantic Layer Extraction
**Description:** Extract MetricRegistry, EntityGraph, and semantic models into a shared library that any agent (Seeknal Ask, KAI, future tools) can import for consistent metric definitions.

**Rationale:** Both systems need to understand business metrics. Manifest has metric nodes, column descriptions, feature definitions. Sharing prevents duplicate modeling and ensures consistency across tools.

**Downsides:** May be overengineering without specific external consumer identified; adds indirection layer.

**Confidence:** 62%
**Complexity:** Medium-High
**Status:** Unexplored

## Rejection Summary

| # | Idea | Reason Rejected |
|---|------|-----------------|
| 1 | Unified LLM Providers | Already solved - providers.py has clean factory pattern for Google + Ollama |
| 2 | External DB NL-to-SQL | REPL already supports `.connect postgres://` and DuckDB ATTACH |
| 3 | Long-Term Memory | Too vague, high implementation burden, unclear value vs complexity |
| 4 | Business Glossary | Thin wrapper on existing column_descriptions in YAML |
| 5 | Skills System | Not grounded in codebase; duplicates Evidence report templates |
| 6 | Interactive Dashboards | Evidence.dev already chosen; double-down on one visualization stack |
| 7 | Tool Protocol Extraction | LangGraph tool interface is standard; no need to reinvent |
| 8 | Evidence Report as Service | Duplicates builder.py; no clear new consumer |
| 9 | DuckDB REPL as Service | REPL is already encapsulated; covered by REST API exposure |
| 10 | Artifact Discovery Graph | ArtifactDiscovery already returns structured context; overengineering |
| 11 | Implicit Project Discovery | Decorators are intentional design; implicit behavior causes confusion |
| 12 | NL Pipeline Definition | Too risky; YAML is precise, NL is ambiguous; no grounding |
| 13 | Merge REPL + Ask | Different UX modes (SQL power users vs NL casual users) |
| 14 | Pull Materialization | Push model is core to feature store architecture |
| 15 | Feature Store Simplification | Parquet + views is already DuckDB implementation; misunderstood |
| 16 | Schema-First Generation | Not grounded in codebase; speculative feature |
| 17 | Stateless Pipelines | Caching is intentional; run_state.json is feature, not bug |
| 18 | Unified Connection Registry | ProfileLoader + connections/ already do this |

## Strategic Recommendation

**Separation over Integration:** The ideation reveals that full KAI integration is NOT the right path.

**Why separation:**
1. **Architectural alignment already exists** - Both use LangGraph + LangChain
2. **Seeknal Ask already has core tools** - execute_sql, execute_python, generate_report
3. **KAI's unique capabilities add complexity** - Typesense memory, multi-DB NL-to-SQL, dashboards don't clearly benefit Seeknal's pipeline-focused use case
4. **Different primary users** - KAI for analysts exploring data, Seeknal for engineers building pipelines

**Selective adoption path:** Keep projects separate, but implement the top-ranked ideas (hooks, API, analytics) to strengthen Seeknal's agent capabilities independently.

**If KAI capabilities are truly needed:** Consider API-based integration where Seeknal calls KAI as a service for specific tasks (external DB querying, dashboard creation) rather than merging codebases.

## Session Log
- 2026-03-25: Initial ideation — 24 candidates generated, 6 survived critique
- 2026-03-25: Selected DAGRunner Hooks for brainstorming
