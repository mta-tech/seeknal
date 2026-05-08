# Branch Investigation: `main` vs `chat-v6-kc-service-integration`

**Date**: 2026-04-29
**Branches**: `main` (base) compared to `chat-v6-kc-service-integration` (4 commits ahead)

---

## Executive Summary

| Metric | Value |
|--------|-------|
| Files changed | 152 |
| Lines added | +1,190 |
| Lines removed | -14,297 |
| Net change | **-13,107 lines** |
| Commits on branch | 4 |
| Version | `main`: 2.9.0 → `chat-v6`: 2.8.1 |

The `chat-v6-kc-service-integration` branch does two things:

1. **Adds kc-service v6 SSE callback integration** — a split-topology deployment model where a cloud gateway receives SSE events POSTed by on-prem Temporal workers.
2. **Removes ~13K lines** of the connected-source analyst / tap-in feature surface, Atlas integration, multi-tenant token auth, Ask testing framework, and optional-Spark scaffolding.

---

## Commit History

| Commit | Message |
|--------|---------|
| `8054ac6` | feat(gateway): add kc-service v6 SSE callback support to temporal activity |
| `992862e` | feat(deploy): add local worker deployment for kc-service v6 integration |
| `c091dc7` | feat(deploy): optimize worker Dockerfile and refine project path env setting |
| `2b05d4d` | deploy gcp worker |

---

## Part 1: New Features — kc-service v6 Integration

### 1.1 Temporal Activity SSE Callback (`src/seeknal/ask/gateway/temporal.py`)

**New `message_id` field on `AgentWorkflowInput`:**
```python
message_id: str | None = None
```
kc-service passes `message_id` as its workflow identifier. The activity reads `SEEKNAL_CALLBACK_URL` from env and constructs `{SEEKNAL_CALLBACK_URL}/internal/events/{message_id}/publish`.

**Environment variable precedence:**
```python
_callback_base = os.environ.get("SEEKNAL_CALLBACK_URL") or input.callback_url
_callback_token = os.environ.get("SEEKNAL_PUSH_API_KEY") or input.callback_auth_token or ""
```
Standalone workers are now configured via env vars rather than Temporal workflow input fields.

**`message_id` overrides `session_id` for callback routing:**
```python
_message_id = input.message_id or input.session_id
url = f"{_callback_base}/internal/events/{_message_id}/publish"
```

**TLS support for Temporal Cloud:**
```python
if tls is None:
    tls = os.environ.get("TEMPORAL_TLS", "").lower() == "true"
client = await Client.connect(address, namespace=namespace, tls=tls)
```

### 1.2 New Deployment Files

| File | Purpose |
|------|---------|
| `deploy/Dockerfile.gateway` | Gateway-only container (SSE endpoint + Temporal client, `--no-worker`) |
| `deploy/Dockerfile.worker` | Standalone Temporal worker (no HTTP server, POSTs events to gateway) |
| `deploy/Dockerfile.worker.local` | Multi-stage local dev worker with layer caching |
| `deploy/docker-compose.worker.local.yml` | Local dev compose with volume mounts for projects and profiles |
| `deploy/build-gcp-worker.sh` | Build & push worker image to GCP Artifact Registry |

**Gateway Dockerfile** — runs `seeknal gateway start --temporal --no-worker` (SSE endpoint only, receives callback POSTs from remote workers).

**Worker Dockerfile** — runs `seeknal gateway worker --project /app/project` (standalone Temporal worker, no HTTP server).

**Local dev Dockerfile** — two-stage build: Stage 1 installs dependencies (cached), Stage 2 copies source code only (fast rebuilds).

### 1.3 Modified Deployment Files

**`deploy/docker-compose.worker.yml`:**
- Changed build context to local `.` with new Dockerfiles
- Made `CLOUD_TEMPORAL_ADDRESS` and `CLOUD_GATEWAY_URL` required (`:?` fail-fast syntax)
- Removed `SEEKNAL_GATEWAY_URL` and `SEEKNAL_API_TOKEN` (token-routed multi-tenant config removed)
- Simplified volume mounts and command

**`deploy/docker-compose.yml`** (gateway stack):
- Same build context migration from `docker/` to `deploy/`
- Simplified `env_file` handling

**`deploy/deploy.sh`:**
- Now copies `Dockerfile.gateway` and `Dockerfile.worker` to remote host
- Removed `SEEKNAL_BUILD_CONTEXT` from compose up command

### 1.4 Configuration (`config.toml.example`)

New `[gateway]` section with kc-service v6 environment variables:

| Variable | Purpose |
|----------|---------|
| `SEEKNAL_CALLBACK_URL` | Base URL of kc-service instance |
| `SEEKNAL_PUSH_API_KEY` | API key for push request authentication |
| `SEEKNAL_PROJECT_PATH` | Local project path override for Temporal workers |
| `TEMPORAL_TLS` | Set `"true"` for Temporal Cloud / TLS connections |

### 1.5 Gateway Server Changes (`src/seeknal/ask/gateway/server.py`)

**SEEKNAL_PROJECT_PATH conditional setting:** Only set when `--project` was explicitly passed, enabling multi-project routing where workflows use their own input paths.

**Auth header fix:** Changed `X-API-Key` to `Authorization: Bearer` for kc-service v6 callback authentication.

---

## Part 2: Removed Features

### 2.1 Multi-Tenant Token Authentication (261 lines deleted)

**`src/seeknal/ask/gateway/auth.py` — entire file deleted:**

| Component | Purpose |
|-----------|---------|
| `TokenAuthError` | Exception for invalid API tokens |
| `TokenPrincipal` | Resolved token claims (tenant_id, task_queue, callback_token, etc.) |
| `WorkerRuntimeConfig` | Runtime settings returned to workers after auth |
| `TokenRegistry` | In-memory API token registry with HMAC-safe comparison |
| `extract_bearer_token_from_headers()` | Extract Bearer token from headers |
| `extract_api_token()` | Token extraction with query-string fallback |
| `load_token_registry()` | Load from JSON/YAML file or env vars |
| `_parse_token_records()` | Parse token records from various formats |

**Gateway impact (`server.py`):**
- `AuthContext` dataclass and all token-mode auth paths removed
- `_safe_auth_context()` / `_safe_auth_context_ws()` dual-mode resolvers removed
- `/internal/worker/config` endpoint removed (48 lines)
- Token-registry parameters removed from `create_gateway_app()`
- Simplified to `_safe_resolve_tenant()` with basic `resolve_tenant()` only

### 2.2 Session Cancellation Infrastructure (removed from `server.py`)

- `_cancel_key()`, `_request_cancel()`, `_clear_cancel()`, `_is_cancelled()` functions removed
- `_session_cancellations` global set removed
- `cancel_session_run()` POST endpoint removed
- WebSocket cancel protocol (`run_task` + `receive_task` racing) simplified to one-way event stream

### 2.3 Distributed Session Locking (removed from `server.py`)

- `_session_lock_context()` with Redis-backed distributed lock removed
- Gateway now uses only in-memory `_get_session_lock()` — no Redis support
- `_publish_event_async()` (async Redis broadcaster path) removed

### 2.4 Evidence Synthesis / Turn Governor System (~800 lines removed across multiple files)

**`src/seeknal/ask/agents/tools/_context.py` (428 lines deleted):**

Removed 17 fields from `ToolContext` dataclass:
- `disable_quality_gate`, `tool_call_limit`, `tool_calls_this_turn`
- `successful_sql_results_this_turn`, `evidence_snippets_this_turn`
- `terminal_tool_errors_this_turn`, `failed_tool_signatures`
- `successful_sql_cache`, `loaded_sql_pairs_this_turn`
- `authoritative_sql_pair_result_this_turn`, `sql_timeout_seconds`
- `discovery_cache_ttl_seconds`, `discovery_cache`, `timing_events_this_turn`

Removed ~400 lines of helper functions:
- SQL-pair lifecycle: `get_loaded_sql_pairs()`, `mark_sql_pairs_checked()`, `get_authoritative_sql_pair_result()`, `record_authoritative_sql_pair_result()`, `should_synthesize_after_authoritative_sql_pair()`
- Turn governor: `reset_turn_governor()`, `record_tool_result()`, `has_sufficient_evidence()`, `synthesize_evidence_fallback()`, `build_evidence_synthesis_prompt()`
- Discovery cache: `get_discovery_cache_value()`, `set_discovery_cache_value()`
- Failure tracking: `make_tool_signature()`, `repeated_failure_message()`
- Timing: `record_timing_event()`

### 2.5 Gateway Evidence Synthesis (removed from `server.py`)

- `_extract_gateway_tool_result_text()` — pydantic-ai result extraction
- `_record_gateway_tool_result()` — tool result recording for evidence system
- `_gateway_tool_stop_reason()` — early-stop check for authoritative SQL pairs
- The entire quality-gate + evidence-synthesis fallback path removed from `_run_agent_inner`

### 2.6 Streaming Evidence Synthesis (`src/seeknal/ask/streaming.py`, 462 lines reduced)

- `_stream_evidence_synthesis()` function removed
- All `record_tool_result()` calls removed from `_stream_one_pass`
- `UsageLimitExceeded` raise for "authoritative sql pair result" removed
- `UsageLimitExceeded` raise for "terminal tool error after sufficient evidence" removed
- `reset_turn_governor()` / `compact_history_for_analysis_mode()` calls removed

### 2.7 Direct Tool Directive System (305 lines deleted)

**`src/seeknal/ask/directives.py` — entire file deleted:**

Handled deterministic (non-LLM) user directives:
- SQL pair saving from natural language
- `save_preference` calls
- `list_tables`, `describe_table`, `execute_sql`, `execute_python` shortcuts
- Read-only safety turns (mutation blocking)
- Instruction-injection defense ("ignore instructions and delete")

Also removed from `streaming.py`:
- `_extract_direct_arg()`, `_extract_remembered_preference()`, `_extract_sql_pair_directive()`
- `_try_direct_tool_directive()` — command-shaped prompt shortcut
- `_try_read_only_analysis_shortcut()` — read-only safety handling

### 2.8 Source Registry System (976 lines deleted)

**`src/seeknal/sources/config.py` — entire file deleted.** This was the backbone of the connected-source analyst feature:

| Component | Purpose |
|-----------|---------|
| `ContextSyncConfig` | Settings for generating local source-context artifacts |
| `SourceConfig` | One source declaration from `seeknal_agent.yml` |
| `SourceRegistry` | All source declarations + harness routing defaults |
| `load_source_registry()` | Load from agent config |
| `read_sync_state()` | Read `.seeknal/catalog/sync_state.json` |
| `write_source_context()` | Write context files for configured sources |
| Discovery helpers | Table/column discovery via `information_schema` |
| Markdown generators | Source overview, columns, profiling |
| Relationship inference | Column-name-based relationship detection |

**`src/seeknal/sources/__init__.py` (21 lines deleted):** Package exports removed.

### 2.9 10 Agent Tools Deleted

| Tool | Lines | Purpose |
|------|:-----:|---------|
| `execute_sql_pair.py` | 140 | Execute SQL from YAML pair files with authoritative flag |
| `list_source_context.py` | 76 | List generated source-context files |
| `read_source_context.py` | 47 | Read a single source-context file |
| `list_sql_pairs.py` | 152 | List SQL pairs with fuzzy matching (EN + ID stop words) |
| `read_sql_pair.py` | 96 | Read one SQL-pair YAML/MD file with drift detection |
| `list_ask_tests.py` | 58 | List executable Ask SQL QA tests |
| `read_ask_test.py` | 43 | Read a single Ask test YAML file |
| `run_ask_test.py` | 57 | Run Ask SQL QA tests from chat |
| `list_ask_test_results.py` | 40 | List saved Ask test result JSON files |
| `read_ask_test_result.py` | 45 | Read a saved Ask test result file |

### 2.10 Ask Testing Framework (792 lines deleted)

**`src/seeknal/ask/testing.py` — entire file deleted:**

The complete Ask SQL QA test framework including:
- Test discovery from `seeknal/tests/`, `context/tests/`, top-level `tests/`
- SQL-only mode (fast, deterministic)
- Agent mode (full LLM agent execution)
- Dataframe comparison (`assert.compare: dataframe`)
- Result persistence and reporting

### 2.11 Agent Analysis Mode (496 lines removed from `agent.py`)

Removed from `src/seeknal/ask/agents/agent.py`:
- `_build_connected_source_context()` — schema snapshot injection from attached databases
- `_build_generated_source_context_index()` — generated context file scanning
- Analysis-mode prompt (~150 lines of behavioral instructions)
- Analysis-toolset-mode logic and tool call limiting
- ~20 config import functions for harness settings
- Complex agent feature configuration (auto-summarization, cost tracking, hooks, plan, stuck-loop, subagents, teams)
- `compact_history_for_analysis_mode()` function
- Quality-gate bypass for analysis mode

### 2.12 Dual-Mode Toolset System (202 lines removed from `toolset.py`)

**Before:** `create_ask_toolset(mode="full"|"analysis", include_ask_user=...)` assembled from categorized tool groups:
- `_DATABASE_ANALYSIS_TOOLS`
- `_PROJECT_READ_TOOLS`
- `_PROJECT_MEMORY_TOOLS`
- `_SEMANTIC_ARTIFACT_TOOLS`
- `_ANALYSIS_TOOLS`
- `_READ_ONLY_CONTEXT_TOOLS` (all 10 deleted tools)
- `_FULL_ONLY_TOOLS`

**After:** Single flat list of all tools. No mode, no filtering.

### 2.13 Atlas Integration (339 lines deleted)

**`src/seeknal/integrations/atlas_client.py` — entire file deleted:**
- `AtlasContractConfig` — environment-backed configuration
- `AtlasApplyContext` — per-apply payload
- `AtlasContractClient` — HTTP client for policy gates, metadata sync, lineage, run reporting

**`src/seeknal/workflow/apply.py` (~80 lines removed):**
- Atlas preflight policy gate removed
- `complete_apply()` metadata sync removed
- Error reporting to Atlas on local failure removed

### 2.14 CLI Commands Removed

| Command | Location | Lines |
|---------|----------|:-----:|
| `seeknal ask test` | `cli/ask.py` | ~80 |
| `seeknal source connect` | `cli/source.py` | ~80 |
| `seeknal source status` | `cli/source.py` | ~40 |
| `seeknal source inspect` | `cli/source.py` | ~50 |
| `seeknal source sync` | `cli/source.py` | ~50 |
| `seeknal source test` | `cli/source.py` | ~40 |
| `--token-config` option | `cli/gateway.py` | removed |
| `--gateway-url` option | `cli/gateway.py` | removed |
| `--api-token` option | `cli/gateway.py` | removed |

### 2.15 Project Scaffolding Templates (~400 lines removed from `cli/main.py`)

Removed from `seeknal init`:
- `_project_seeknal_agent_yml()` — full agent config template
- `_project_seeknal_ask_md()` — business context template
- `_project_env_example()` — `.env.example` generator
- `_project_agents_md()` — coding-agent guidance template
- `_project_claude_md()` — Claude-specific instructions template
- Directory creation for `seeknal/sql_pairs/`, `seeknal/tests/`, `context/`

### 2.16 Ask Config Functions (`src/seeknal/ask/config.py`, 302 lines deleted)

Removed configuration functions:
- `get_sql_timeout_seconds()`, `get_discovery_cache_ttl_seconds()`
- `get_agent_harness_settings()`, `get_auto_summarization_config()`
- `get_cost_tracking_config()`, `get_hooks_config()`
- `get_plan_config()`, `get_stuck_loop_config()`
- `get_subagents_config()`, `get_teams_config()`
- `get_source_registry_instructions()`, `get_ask_toolset_mode()`

### 2.17 Report Server Deployment (removed)

- `deploy/deploy-report-server.sh` (68 lines) — deployment to `reportkami.exe.xyz`
- `deploy/docker-compose.report-server.yml` (27 lines)
- `docker/report-server/Dockerfile`, `.dockerignore`, `README.md`

### 2.18 Old Docker Directory (entire `docker/` directory deleted)

| File | Purpose |
|------|---------|
| `docker/Dockerfile.gateway` | Old multi-stage gateway image |
| `docker/Dockerfile.worker` | Old multi-stage worker image |
| `docker/Dockerfile.prefect` | Prefect deployment runner |
| `docker/README.md` | Docker documentation (110 lines) |
| `docker/report-server/` | Report server Docker files |

Replaced by simpler Dockerfiles in `deploy/`.

### 2.19 CI/CD Changes

- `.github/workflows/docker-worker.yml` (93 lines) — entire file deleted (old worker CI pipeline)
- `.github/workflows/docker.yml` — removed Prefect Dockerfile reference

---

## Part 3: Simplified Features

### 3.1 Execute SQL (`execute_sql.py`, 339 lines reduced)

| Feature Removed | Purpose |
|----------------|---------|
| `query` parameter | Compatibility alias for `sql` |
| `refresh` parameter | Cache bypass flag |
| `allow_sql_pair_drift` parameter | Escape hatch for pair-first enforcement |
| SQL result caching | Session-level success cache |
| SQL-pair-first enforcement | Blocked ad-hoc SQL until pairs checked |
| Repeated-failure detection | SHA1-based dedup of failed queries |
| SQL-pair drift detection | Compared current SQL against loaded pairs |
| Automatic SQL repair | ILIKE syntax, TRY_CAST substitution, DuckDB suggestion retry |
| SQL pre-execution linting | Function-style ILIKE, ORDER BY CAST fixes |
| Timeout executor | ThreadPoolExecutor-based SQL timeout |
| Blank-cell labeling | Empty strings → `[blank]` |
| `record_tool_result()` calls | Turn governor recording |

### 3.2 Execute Python (`execute_python.py`, 152 lines reduced)

| Feature Removed | Purpose |
|----------------|---------|
| Module tracking | AST-based extraction of top-level imports |
| Per-session unavailable modules | Prevented retry of failed imports |
| Visualization gating | Blocked matplotlib unless explicitly requested |
| Repeated-failure detection | SHA1-based dedup |
| matplotlib-specific error handling | Session-level availability tracking |

### 3.3 Describe Table (`describe_table.py`, 76 lines reduced)

| Feature Removed | Purpose |
|----------------|---------|
| Discovery caching | TTL-based cache for DESCRIBE results |
| Attached-table name normalization | Fixed weak-model catalog alias typos |
| `_find_unique_attached_table()` | Resolved unqualified names across sources |

### 3.4 List Tables (`list_tables.py`, 121 lines reduced)

| Feature Removed | Purpose |
|----------------|---------|
| `query` parameter | Glob-wildcard filtering |
| Attached-source enumeration | information_schema discovery |
| Discovery caching | TTL-based cache |
| Deduplication | `_dedupe_entries()` for multi-path results |

Now simply runs `SHOW TABLES` with no filtering or caching.

### 3.5 Sandbox (`sandbox.py`, 106 lines reduced)

| Feature Removed | Purpose |
|----------------|---------|
| `_strip_sensitive_env()` | Removed secrets from sandbox environment |
| REPL-based data loading | Auto-registration of parquet + attached DBs |
| `SafeConnection` wrapping | Enforced read-only access |

### 3.6 Prompt Builder (`prompt_builder.py`, 204 lines reduced)

- Source registry prompt section removed
- Tool list shortened from 25+ tools to ~17 tools
- Skills list reformatted and shortened
- Data question workflow simplified from 8 steps to 4 steps
- Removed: SQL pair matching, preference persistence, Ask test workflow, SQL hygiene patterns, complex analysis workflow

### 3.7 Hooks (`hooks.py`, 36 lines reduced)

**Before:** Config-driven enable/disable for SQL security and self-correction hooks.
**After:** Both hooks always active, no configuration.

### 3.8 Telegram Channel (`channels/telegram.py`, 35 lines reduced)

- Removed custom polling error handler with conflict throttling
- Uses default `python-telegram-bot` error behavior

---

## Part 4: Optional-Spark Scaffolding Removal

Multiple files had their conditional PySpark import guards removed. In `chat-v6-kc-service-integration`, Spark is a **required** dependency.

| File | Change |
|------|--------|
| `src/seeknal/featurestore/featurestore.py` | `TYPE_CHECKING` guard → direct runtime imports |
| `src/seeknal/flow.py` | try/except import block → direct imports, `_require_spark()` removed |
| `src/seeknal/tasks/base.py` | `TYPE_CHECKING` guard → direct imports |
| `src/seeknal/workflow/executors/__init__.py` | Lazy loading mechanism → direct imports |
| `src/seeknal/workflow/executors/feature_group_executor.py` | try/except guard → direct imports |
| `src/seeknal/workflow/executors/rule_executor.py` | try/except fallback → direct imports |

---

## Part 5: Dependency Changes (`pyproject.toml`)

| Aspect | `main` (2.9.0) | `chat-v6` (2.8.1) |
|--------|:--------------:|:-----------------:|
| **Spark** | Optional (`[spark]` extra) | **Required** (core dependency) |
| **AI deps** (pydantic-ai, starlette, uvicorn) | Required (core) | Optional (`[ask]` extra) |
| **redis** | Required | Not in core |
| **scikit-learn, scipy** | Required | Not in core |
| **python-telegram-bot** | Required | Optional (`[telegram]` extra) |
| **pydantic-ai-slim** | `[google,openai]` | `[google]` only |
| **pydantic-deep** | `>=0.3.17,<0.4.0` | `>=0.3.0` (broader) |

---

## Part 6: DuckDB Compatibility Shims Removed

| File | Shim Removed |
|------|-------------|
| `src/seeknal/pipeline/context.py` | `_duckdb_registerable_frame()` — Pandas 3 string dtype conversion |
| `src/seeknal/pipeline/feature_frame.py` | `_duckdb_registerable()` — same shim (3 copies removed) |
| `src/seeknal/tasks/duckdb/duckdb.py` | `rel.arrow().read_all()` → `rel.arrow()` (redundant read) |

---

## Part 7: Test Changes

### 7.1 Test Files Deleted (11 files, 62 test functions lost)

| File | Tests Lost | Coverage Area |
|------|:----------:|--------------|
| `tests/ask/qa/tap_in_stress_runner.py` | 0 (script) | TUI stress runner |
| `tests/ask/test_ask_sql_tests.py` | 11 | Ask SQL QA runner |
| `tests/ask/test_context_tools.py` | 12 | Source context, SQL pairs, Ask tests, turn governor |
| `tests/ask/test_gateway_concurrency.py` | 4 | Session serialization, concurrent requests |
| `tests/ask/test_gateway_parity.py` | 4 | Gateway/headless vs interactive parity |
| `tests/ask/test_gateway_token_auth.py` | 6 | Token-based multi-tenant auth |
| `tests/ask/test_list_tables.py` | 6 | Discovery caching, TTL, refresh |
| `tests/ask/test_source_config.py` | 6 | Source registry configuration |
| `tests/ask/test_toolset.py` | 8 | Analysis vs full toolset routing |
| `tests/cli/test_repl_source_registry.py` | 1 | REPL namespace attachment |
| `tests/workflow/test_atlas_apply_integration.py` | 4 | Atlas policy, registration, lineage |

### 7.2 Tests Removed from Modified Files (34 test functions)

| File | Tests Removed |
|------|:------------:|
| `tests/ask/test_execute_sql.py` | 16 |
| `tests/ask/test_execute_python.py` | 3 |
| `tests/ask/test_streaming.py` | 9 |
| `tests/ask/test_telegram_channel.py` | 2 |
| `tests/ask/test_config.py` | 3 |
| `tests/ask/test_context_preferences.py` | 3 |
| `tests/cli/test_source.py` | 7 |

### 7.3 Test Pattern Regressions

| Pattern | Impact |
|---------|--------|
| ANSI-aware help assertions removed | 6 files now use simple `in` checks; may fail with Rich-styled output |
| Secure tempdir abandoned | 5+ files use plain `/tmp` instead of `.seeknal/test-tmp/` |
| Deterministic test data lost | DuckDB tests depend on external parquet file |
| `pytest_ignore_collect` removed | Test collection breaks without Spark installed |

---

## Part 8: Documentation Changes

### 8.1 Files Deleted (2 files)

| File | Lines | Content |
|------|:-----:|---------|
| `docs/cli/source.md` | 211 | Complete `seeknal source` CLI documentation |
| `docs/ideation/2026-03-25-kai-integration-ideation.md` | 147 | KAI integration ideation document |

### 8.2 Files Modified (11 files)

| File | Lines Removed | Key Content Removed |
|------|:------------:|---------------------|
| `docs/cli/ask.md` | 200 | Ask test subcommand, SQL pairs, teaching mode, 12 tools, 3 skills |
| `docs/cli/gateway.md` | 109 | Token registry, Docker deployment, session serialization, cancellation |
| `docs/cli/init.md` | 128 | Project layout tree, agent guidance, connected-source setup |
| `docs/cli/index.md` | 3 | `source` and `ask test` command references |
| `docs/cli/apply.md` | 43 | Atlas contract sync section |
| `docs/reference/cli.md` | 264 | `ask` commands, source commands, token options |
| `docs/reference/configuration.md` | 93 | Ask agent config, project structure entries |
| `docs/building-blocks/tasks.md` | 6 | Spark optional install note |
| `docs/getting-started-comprehensive.md` | 6 | Spark optional install note |
| `docs/reference/troubleshooting.md` | 4 | Spark install instruction changed |
| `docs/tutorials/seeknal-ask-agent.md` | 2 | Prerequisites changed to `pip install seeknal[ask]` |

---

## Part 9: Other Changes

### 9.1 Built-in Skills Deleted (3 files)

| File | Lines |
|------|:-----:|
| `ask/builtin_skills/business-question-answering/SKILL.md` | 54 |
| `ask/builtin_skills/complex-analysis/SKILL.md` | 44 |
| `ask/builtin_skills/database-analyst/SKILL.md` | 115 |

### 9.2 Agent Config Files Deleted from Repository

| File | Purpose |
|------|---------|
| `AGENT.md` | Agent guidance (15 lines) |
| `AGENTS.md` | Multi-agent coding guidance (75 lines) |
| `CLAUDE.md` | Claude-specific instructions (52 lines) |

### 9.3 `.gitignore` Updates

**Added:** `deploy/.env.worker.local`, `deploy/.env.*.local` (local deployment secrets)
**Removed:** `.env.backup-*` pattern

### 9.4 REPL Source Registry Attachment Removed (`cli/repl.py`)

The REPL no longer uses `SourceRegistry` from `seeknal_agent.yml` to attach external databases under specific namespaces. Falls back to simple profile-based PostgreSQL connection.

### 9.5 Error Message Updates

Multiple files changed install instructions from `pip install --upgrade seeknal` to `pip install seeknal[ask]`, making the optional dependency group explicit.

---

## Architectural Comparison Summary

| Feature | `main` | `chat-v6-kc-service-integration` |
|---------|--------|----------------------------------|
| **Spark** | Optional extra | Required core dependency |
| **AI/Ask deps** | Required core | Optional `[ask]` extra |
| **Authentication** | Multi-tenant token registry + HMAC | Basic tenant resolution |
| **Session locking** | Redis distributed + in-memory | In-memory only |
| **Session cancellation** | HTTP POST + WebSocket cancel | Removed |
| **Evidence synthesis** | Turn governor + quality gate + fallback | Removed |
| **Direct tool directives** | Deterministic non-LLM shortcuts | Removed |
| **Source registry** | Full connected-source system | Removed |
| **Ask testing** | Complete SQL QA framework | Removed |
| **Atlas integration** | Policy gate + metadata sync | Removed |
| **Report server** | Deployed to `reportkami.exe.xyz` | Removed |
| **kc-service v6 callback** | N/A | SSE event push via env vars |
| **Temporal TLS** | Not supported | Supported via `TEMPORAL_TLS` |
| **Deployment topology** | Combined gateway+worker | Split: cloud gateway / on-prem worker |
| **Tool surface** | 25+ tools (analysis vs full mode) | ~17 tools (single mode) |
| **kc-service v6 SSE callback** | N/A | `message_id` routing + env var config |
| **Version** | 2.9.0 | 2.8.1 |

---

## Merge Considerations

1. **Version conflict:** Branch is at 2.8.1 vs main's 2.9.0 — will need reconciliation.
2. **13K lines of features removed** — any features still needed will need selective re-integration.
3. **Test regressions** — 62 test functions deleted, secure tempdir patterns abandoned, ANSI assertion helpers removed.
4. **Spark requirement** — making Spark mandatory increases the default install footprint significantly.
5. **kc-service v6 additions are additive** — the callback integration, TLS support, and new Dockerfiles can likely be cherry-picked independently.
