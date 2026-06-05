# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.10.0] - 2026-06-05

Atlas data-access governance — first-class, config-activated access enforcement for
`seeknal run` and the Ask SQL path, plus the requester/owner access-request CLI loop.
Previously available only on the `feat/atlas-access-request-flow` branch; now mainline.

### Added

- **Config-activated runtime governance gate** — when `ATLAS_API_URL` is set, a pre-run
  access check enforces `can_select` (via the Atlas backend / OpenFGA) for every source
  node a run reads. Denial aborts the run with exit code 1 and a `seeknal gov
  request-access` hint, **before any data is read**. With `ATLAS_API_URL` unset the gate
  is a no-op — fully backward compatible.
- **`seeknal gov request-access <table>`** — request access to a governed dataset through
  the Atlas governance API (`--reason`, `--access read|write`, `--duration`).
- **`seeknal auth login` / `status` / `logout`** — authenticate against the Atlas Keycloak
  realm via PKCE loopback; credentials are cached at `~/.config/seeknal/credentials.json`
  and supply the per-user identity (token + `sub`) used for access checks.
- **Governed Ask SQL execution** — `execute_sql` / REPL reads run through the same
  access-check and apply column masking when the backend reports masked columns.

### Notes

- Activation is opt-in via `ATLAS_API_URL` (+ `KEYCLOAK_ISSUER` for login). The gate fails
  **closed** on denial; set `ATLAS_FAIL_OPEN=true` to allow on transport error.

## [2.9.7] - 2026-06-01

Ask agent harness robustness + table-name discoverability — fixes that make
`seeknal ask chat` reliable for multi-turn consultant use and let the agent query
the friendly node names it is shown.

### Fixed

- **Ask chat crash on context compaction (F1)** — the `auto_summarization` history
  processors could leave the message history ending in a `ModelResponse`, tripping
  pydantic-ai's "Processed history must end with a `ModelRequest`" invariant and
  crashing ~27% of chat / ask-test turns. A final `ensure_trailing_model_request`
  history processor now guarantees the invariant; `stream_ask` and the gateway
  `_generate` degrade gracefully on `UserError` / `UnexpectedModelBehavior` instead
  of crashing; the Ask `FunctionToolset` now allows `max_retries=3` so one SQL
  self-correction retry no longer kills the turn. `auto_summarization` now defaults
  OFF in `seeknal init` and the config default, and is crash-safe to re-enable.
- **Fabricated answers in `ask test` mode (F3)** — `testing.run_agent_answer` did not
  seed the per-turn governor, so `current_question` stayed `None` and the grounding /
  anti-fabrication guards silently disabled. It now calls `reset_report_approval` +
  `reset_turn_governor`, mirroring the live chat/gateway paths.
- **Stale `__version__`** — `seeknal.__version__` was 2.9.5 while `pyproject.toml`
  was 2.9.6; both now track the release version.

### Added

- **Clean table-name aliases (F2)** — nodes registered as `transform_X` / `source_X` /
  `model_X` are now also queryable under their bare node name (`X`) via a final
  catalog-aware registration pass in the REPL, so the name the Ask agent is shown in
  the manifest / `list_tables` is the name it can query. Aliases never shadow a real
  relation (legacy cache view, consolidated `entity_`, `ingest_`, or attached table);
  base-name collisions resolve deterministically (`source_` before `transform_`).

## [2.9.6] - 2026-05-24

Heartbeat smart-inbox loop — three seeknal-core bugs that blocked
`seeknal heartbeat tick` from completing the full
Scan → Ingest → DAG → Exposure loop for any project with transforms,
plus a UX cleanup to the verbatim re-ask gate.

### Fixed

- **`Manifest` adjacency cache invalidation** (`seeknal.dag.manifest`) —
  partial per-node `pop` left stale entries for transitively-affected
  nodes, but the lazy rebuild in `get_upstream_nodes` /
  `get_downstream_nodes` only fires when the cache dict is *entirely*
  empty. Popped keys then fell through to `.get(node_id, set())` and
  silently reported zero neighbors, so Kahn's algorithm in
  `DAGRunner._get_topological_order` misreported acyclic DAGs as cyclic
  (`"DAG contains cycles"`). Now clears both caches in one shot on every
  mutation. Regression test in `tests/dag/test_manifest.py`.
- **`heartbeat/runner.py::_run_dag` AttributeError** — referenced
  `summary.fingerprints`, which is not on `ExecutionSummary`. Now reads
  from `runner._current_fingerprints` (where `DAGRunner` actually stores
  the per-node fingerprints it computed this run) and maps each to its
  `.combined` hash, so `DagResult.fingerprints: dict[str, str]` populates
  correctly.
- **Cached transform-input view registration**
  (`workflow/executors/transform_executor.py`) — `_load_views_from_inputs`
  was creating views as quoted-literal `"source.x"`, while the existence
  check in the same method and every transform SQL reference
  `source.x` schema-qualified. DuckDB parses unquoted `source.x` as
  `schema.table`, so incremental DAG re-runs hit
  `Catalog Error: Table with name x does not exist`. Now emits
  `CREATE SCHEMA IF NOT EXISTS "kind"` + `CREATE OR REPLACE VIEW
  "kind"."name"`, matching the rest of the executor.

### Changed

- **Verbatim re-ask gate is a silent passthrough**
  (`ask/agents/tools/_context.py`) —
  `build_verbatim_restate_response(prior_answer)` now returns
  `prior_answer` unchanged. The old bilingual banner ("Pertanyaan ini
  sama dengan giliran sebelumnya / This question is the same as the
  prior turn") is gone. `seed_prior_turn_from_history` is triple-ask
  safe by construction — a restate is byte-identical to the canonical
  answer, so re-seeding can never compound. Gate tests
  (`tests/ask/test_verbatim_restate_gate.py`) updated for the no-banner
  behaviour.
- **Heartbeat success-notification log**
  (`heartbeat/tick_notifier.py`) — emits
  `[heartbeat] tick notification delivered to chat <id>` when the
  Telegram POST returns `ok=true`. Used as an assertion target by the
  heartbeat QA scenarios.

### Docs

- `docs/cli/heartbeat.md` is now part of the published CLI reference,
  listed in `docs/cli/index.md` under a new **Smart Inbox / Daemon**
  category alongside the existing Servers & APIs section.

## [2.9.5] - 2026-05-21

Ask agent reliability — PostgreSQL EXTRACT pushdown, psycopg2 oracle path,
verbatim-restate gate, and connection / assertion hardening.

### Added

- **`_rewrite_for_pg_pushdown` in `execute_sql.py`** (closes #64) — transforms
  `EXTRACT(YEAR/MONTH+YEAR/QUARTER+YEAR/DAY+MONTH+YEAR FROM col) ∈ {=, !=, <>,
  IN, BETWEEN}` to half-open date-range form so DuckDB's `postgres_scanner`
  can push the filter to PostgreSQL instead of full-table-`COPY`-ing then
  filtering locally. Step-0 masking guards string-literal / `--` / `/* */`
  false positives; boundary years (1..9999), invalid date triples (Feb 29
  non-leap), inverted `BETWEEN`, and dedup `IN (2023, 2023)` are all handled.
  Wired into `_repair_common_sql_before_execution` so `execute_sql_pair` (via
  delegation) and ad-hoc agent SQL both benefit; `_sql_pair_drift_notice`
  normalizes both sides through the same rewrite to prevent spurious drift
  warnings on `EXTRACT`-based pairs.
- **`seeknal.ask._pg_oracle` module** (closes #64) — `detect_pg_only_namespace`,
  `resolve_pg_dsn`, `strip_namespace`, `execute_via_psycopg2`. `execute_expected_sql`
  in `testing.py` routes PG-only oracle SQL through `psycopg2` directly for
  server-side ground truth, with deterministic namespace-scan routing
  (multi-source SQL falls back to DuckDB with the rewrite applied; psycopg2
  execution errors surface as `SqlOracleResult.error`, never silent fallback).
  Uses `contextlib.closing(...)` + `autocommit=True` + `set_session(readonly=True)`
  set before any cursor; distinct timing label `execute_sql_pg_direct`.
- **TCP keepalives on `PostgreSQLConfig`** (closes #67) — four new fields
  (`keepalives=1`, `keepalives_idle=30`, `keepalives_interval=10`,
  `keepalives_count=3`) emitted by `to_libpq_string()`. Keeps connections
  alive over SSH tunnels and VPNs with idle-timeout policies during
  long-running queries. Tunable via DSN query params (`?keepalives_idle=60`)
  or `profiles.yml`; set `keepalives=0` to disable.
- **Verbatim re-ask short-circuit gate** — when Turn N's normalized question
  equals Turn N-1's, bypass LLM/tool dispatch and return the prior answer
  with a bilingual (Indonesian / English) restate prefix. Lives in
  seeknal-core; benefits every Ask project (BPOM, KAFI, future tap-in /
  managed) without per-project config. Deterministic (zero LLM call on
  verbatim turns), safe (empty answers and tool-error JSON never become a
  "prior answer"), session-coherent (gateway persists via `store.save_messages`;
  streaming appends to in-place `message_history`), and no wrapper compounding
  on triple-asks. Helpers in `agents/tools/_context.py`
  (`_normalize_question`, `record_prior_turn_answer`, `lookup_prior_turn_answer`,
  `build_verbatim_restate_response`); gate wired on both
  `streaming.stream_ask` and `gateway.server._run_agent_inner`.

### Fixed

- **`execute_expected_sql` REPL leak** (closes #66) — DuckDB REPL block now
  wrapped in `try/finally` with explicit `repl.conn.close()` so libpq
  connections release deterministically. Pre-fix, suites of 20+ oracle tests
  could exhaust PG `max_connections` mid-run with
  `IO Error: server closed the connection unexpectedly`.
- **Zero-assertion silent pass** (closes #68) — `check_answer` now emits
  `warnings.warn` when a test has no `answer_contains`,
  `answer_not_contains`, or `expected_values: true` active, so misconfigured
  YAML surfaces in CI logs instead of always passing.
- **`_value_variants` locale-unaware** (closes #68) — threads an optional
  `locale` parameter through `run_ask_sql_tests → _run_one_case →
  check_answer → _check_answer_dataframe → _value_variants`. Reads the
  project's `locale.number_format` (fallback `locale.language`) once via
  the new `_read_project_number_locale` helper. Dot-as-thousands locales
  (`id`, `de`, `nl`, `pt`, `it`, `es`, `fr`, `tr`, `el`, `ro`, `pl`, `cs`)
  get the extra `30.276` form for ints and `1.234,56` form for floats so
  `expected_values: true` stops always-failing on Indonesian/German/etc.
  projects.
- **`check_answer` docstring** (closes #68) — clarifies that `expected_values`
  is mutually exclusive with `answer_contains` and is treated as `false`
  when `answer_contains` is present. No behavior change.

### Notes

- 88 new ACs total across `test_pg_pushdown_rewrite.py` (65),
  `test_oracle_psycopg2.py` (23), and `test_verbatim_restate_gate.py` (18),
  plus B-tier integration in `test_execute_sql.py`.
- Engine divergence between DuckDB and PostgreSQL semantics (integer
  division, string coercion, type rules) is real and explicitly accepted
  for the psycopg2 oracle path — deterministic namespace-scan routing
  keeps blast radius small.
- CTE-aliased `EXTRACT` (`WITH x AS (SELECT EXTRACT(...) AS y) WHERE y = N`)
  is out of scope for the regex-based pass; queued for a future AST-based
  upgrade.
- Lint notice for the EXTRACT rewrite is visible only on success branches,
  matching existing `TRY_CAST` / `ILIKE` repair UX.
- `psycopg2-binary>=2.9.0` was already a top-level dependency; no install
  change.
- `ProfileLoader._load_profile_data()` is private API; AC-D6 regression
  test guards against silent breakage if it's ever renamed (the public
  promotion is a follow-up).

## [2.9.4] - 2026-05-21

HTTP-only Ask worker — in-process concurrency.

### Added

- **`--max-concurrency` on `seeknal gateway worker`** (closes #63) — HTTP-only worker mode now supports fanning out N work items per process via `asyncio.Semaphore` + `asyncio.create_task`. Default `1` preserves the historical sequential behavior; bump via the flag or `SEEKNAL_WORKER_CONCURRENCY` env. Reaches parity with the Temporal worker's `--max-activities` for deployments that cannot run Temporal. Backpressure: the semaphore is acquired before polling the gateway so the worker does not claim items it cannot start — preserves broker fairness across horizontally scaled workers.
- **`--shutdown-timeout` on `seeknal gateway worker`** — bounded graceful shutdown for HTTP-only workers. On `SIGINT`, in-flight tasks drain up to `SEEKNAL_WORKER_SHUTDOWN_TIMEOUT` (default `60`s) and the broker receives `complete` POSTs for every claimed item (no leaks on rolling deploys). Stragglers are cancelled cleanly after the timeout.
- **Per-task lifecycle logging** — every concurrent task tags its log lines with `work_id` + `session_id` so concurrent execution stays debuggable.
- **Operator sizing guidance** — `deploy/docker-compose.worker.yml` documents the memory / LLM rate-limit / DB-connection math for sizing `SEEKNAL_WORKER_CONCURRENCY` per deployment tier.
- **Multi-worker demo harness** (`tools/`) — `demo_gateway.py`, `demo_worker.py`, `demo_dispatcher.py`, `run_multi_worker_demo.sh`. Spawns a tmux session with 1 gateway + 3 workers + 1 dispatcher to validate horizontal scaling end-to-end without an LLM key.

### Fixed

- HTTP worker no longer blocks the entire container on a single slow agent run (30-90s LLM latency). Pre-fix, one in-flight item blocked all subsequent items per container; horizontal scaling was the only mitigation.
- HTTP worker now installs explicit `SIGINT` / `SIGTERM` handlers via `loop.add_signal_handler` so K8s / systemd / `docker stop` trigger the bounded drain reliably. The default `asyncio.run` SIGINT handling can fail to cancel the main task when child tasks are in flight, leaving the worker polling after a shutdown signal — verified against the live multi-worker harness.

### Notes

- Backward-compatible by construction: with `--max-concurrency 1` (default) the worker is observationally identical to the pre-#63 sequential loop. Existing regression test for the retry path continues to pass unchanged.
- No changes to `HttpWorkerBroker` (already concurrency-safe via `asyncio.Condition`) or to `_run_agent_streaming` (already concurrency-safe via per-task `ContextVar` isolation — proven in production by the Temporal worker at `max_activities=15`).

## [2.9.3] - 2026-05-21

Ask agent reliability — SQL-pair guard fixes + multi-turn hygiene.

### Fixed

- **SQL-pair lock-in (Fix A)** — Guard 3 (`execute_sql` after authoritative pair) now demotes the first un-overridden drift attempt to RETRYABLE and only escalates to TERMINAL on a second attempt without `allow_sql_pair_drift=True`. The override flag is now a meaningful escape hatch instead of being silently ignored.
- **Zero-row authoritative rejection (Fix B)** — `execute_sql_pair(authoritative=True)` no longer locks the turn to a result that is empty or all-NULL/zero aggregates. A SQL pair with a wrongly quoted qualified table name (silent UNION-ALL failure shape) now returns a non-authoritative `SQL_PAIR_RESULT` with explicit "the pair's SQL likely has a bug" guidance, freeing the agent to query directly.
- **Read multi-doc YAML pairs without arming Guard 1 forever** — `read_sql_pair` now calls `mark_sql_pairs_checked` unconditionally on a successful read. Previously, list-format pair files (top-level YAML list of pairs) failed the dict-only check inside `_record_loaded_sql_pair` and skipped marking, so Guard 1 re-fired turn after turn.
- **Session-scoped `sql_pairs_checked`** — `reset_turn_governor` no longer clears the checked flag between turns. Once the agent has consulted project pairs in a session, Guard 1 stays dormant for the rest of the session. Stops the per-turn "list pairs again" tax on simple follow-up questions.

### Added

- **`sql_pairs.mode: advisory` project config (Fix D)** — opt-in setting in `seeknal_agent.yml` that treats curated pairs as cheatsheets only: Guard 2 lets drifting SQL through without `allow_sql_pair_drift=True`, and Guard 3 never escalates to TERMINAL. Default remains `authoritative` (legacy behavior).

### Changed

- **Filter-tweak triggers in `_question_requests_post_sql_analysis` (Fix C)** — added Indonesian/English keywords (`exclud`, `tanpa`, `kecuali`, `selain`, `filter`, `breakdown`, `saja`, `khusus`) so follow-ups that narrow scope release Guard 3 instead of being treated as ordinary business questions.
- **Drop `context/sql_pairs/` from Guard 1 roots (Fix E)** — only `seeknal/sql_pairs/` now triggers the mandatory-lookup nudge. `context/sql_pairs/` is reserved for generated source context per CLAUDE.md and was conflating two storage layers.
- **Temporal-scope hygiene prompt rule** — workflow section teaches the agent to issue a fresh `execute_sql` with an open-ended filter or `MAX(<col>)` probe when follow-ups expand the time window (`dan seterusnya`, `sampai sekarang`, `terkini`, `onwards`, `since`, `to date`, `latest`, or years beyond the prior SQL's date bound). Fixes the "Q3 returned 50 instead of 94" extrapolation bug.
- **Multi-turn hygiene prompt rule** — workflow section teaches the agent to reuse the SAME date column, filter values, and predicate set from the prior turn — change ONLY the dimension the user varied. For verbatim re-asks, restate the prior answer instead of re-querying. Eliminates filter drift across multi-turn conversations.

### Tests

- 84 ask-agent tests pass, including new regression tests covering: Guard 3 first-hit RETRYABLE, second-hit TERMINAL escalation; zero-row + null-only authoritative rejection; advisory-mode dormant guards; `context/sql_pairs/` not triggering Guard 1; filter-tweak Guard 3 release; multi-turn hygiene rule presence; hardcode-guard against domain-specific tokens leaking into the generic rule.
- Base prompt still under the 160-line leanness budget.

## [2.8.0] - 2026-04-18

Ask agent improvements batch — addresses open issues #23, #24, #25, #26, #27.

### Added

- **OpenAI + Anthropic provider support** (#23) — `get_model_string()` now resolves `openai` and `anthropic` providers alongside Google and Ollama, using pydantic-ai's native `openai:<model>` / `anthropic:<model>` strings (not langchain). Custom base URLs honoured via `SEEKNAL_ASK_OPENAI_BASE_URL` and `SEEKNAL_ASK_ANTHROPIC_BASE_URL`, bridged to pydantic-ai's `OPENAI_BASE_URL` / `ANTHROPIC_BASE_URL` at factory time. Enables Azure OpenAI, Together, Groq, vLLM, LM Studio, enterprise Anthropic proxies, MiniMax — anything API-compatible.
- **Pre-execution query safety hooks** (#27) — new `preview_query` tool runs four cheap diagnostic probes against a SELECT before `execute_sql` touches data:
  - **Row count pre-check** — warns at ≥10k rows, blocks at ≥100k
  - **Column count pre-check** — warns when projection exceeds 50 columns
  - **Fan-out detection** — after a JOIN, warns when result rows exceed the primary table by ≥3×
  - **Reachability dry-run** — surfaces unreachable tables with the set of accessible schemas before the real query runs

  Pure aggregations and `LIMIT 1` queries auto-skip the probes. Callers can pass `run_hooks=False` for known-small queries.
- **Context files + durable preferences** (#24, #25) — three new tools:
  - `list_context_files` — scans `{project}/context/` for `.md / .yml / .yaml / .txt` files with first-line hints so the agent can pick what to load via `read_project_file`
  - `write_project_file` — path-safe writes into `context/` with traversal/absolute-path/extension guards and a 100 KB per-file cap
  - `save_preference` — appends durable one-line rules to `{project}/preferences.yml`; deduplicates, escapes YAML specials, and caps at 500 chars per entry

  Preferences are auto-loaded and injected into the system prompt on every session start (see `load_preferences` in `agent.py`).

### Changed

- **`execute_sql` context-window guards** (#26) — results are now capped to prevent LLM context bloat:
  - Row count hard cap at 500 regardless of caller's `limit` argument
  - Column count hard cap at 50
  - Per-cell length cap at 200 chars (with `…` ellipsis)
  - 50 KB markdown byte budget — trailing rows dropped to fit
  - Pipe and newline characters in cell values are now escaped so table rows stay aligned

  Every truncation emits a `⚠ Result truncated:` notice explaining the cause and suggested remediation (WHERE, GROUP BY, narrower SELECT). When the hard cap clamps, a COUNT(*) round-trip populates the accurate total in the footer (`12 of 4,823 rows shown`).

### Tests

65 new tests across four files. 874/874 passed in 59s (up from 809/809).
- `test_providers.py` — 21 tests (4 providers × env var resolution × happy path × failures)
- `test_execute_sql.py` — 11 tests (hard caps, truncation notices, caller-limit pagination, SQL error passthrough)
- `test_preview_query.py` — 12 tests (OK / WARN / BLOCK transitions, fan-out, reachability error, identifier guard)
- `test_context_preferences.py` — 21 tests (context discovery, path-traversal / oversize / extension rejection, preferences dedup + YAML escape round-trip, malformed-file tolerance)

## [2.7.1] - 2026-04-18

Combined parallel session work with 2.7.0 — additive merge, two aligned conflicts.

### Added

- **Gateway pairing** — `FilePairingStore`, `TelegramLinkStore`, `PublicSessionStore` wired into the app lifespan. New Telegram `/pair` command redeems admin-generated one-time codes; expired-code cleanup runs on the eviction loop.
- **`execute_uv_script`** — Ask tool to run uv-managed Python scripts from the agent.
- **Pipeline runtime helpers**:
  - `ctx.llm` — Ask-aligned text/JSON generation inside `@transform` functions
  - `ctx.state` — lightweight per-node persistent state keyed to the run
- **Config discovery** — `find_agent_config_path()` locates `seeknal_agent.yml` under both project root and `seeknal/`.
- **Draft normalisation** — `normalize_python_deps()` preserves user order, deduplicates, and ensures `seeknal` is present first so generated Python drafts can import the local package when executed via `uv run`.
- Tests: `test_gateway_pairing`, `test_telegram_channel`, `test_execute_uv_script`, `test_apply_draft`, `test_draft_node`, `test_session_cli`, `test_pipeline_runtime_helpers`, `test_python_executor_runtime_context`.

### Changed

- Gateway startup banner prints plain-string endpoints (no unnecessary f-strings).
- `test_create_worker` consolidates both fix styles: compact `return_value=` patch plus full-kwargs assertions against `task_queue`, `workflows`, and `activities`.

## [2.7.0] - 2026-04-18

### Added

- **Conversational data ingestion** — drop an `.xlsx / .csv / .tsv / .json` file or a direct-download URL into chat and the `data-ingest` built-in skill walks the user through schema preview → business key → append-or-create → reusable SKILL.md. Re-runs surface a self-defending drift/dedup report before any append write.
  - New tools: `read_tabular`, `write_ingested_table`, `save_ingestion_skill`, `check_ingestion_drift`
  - Every write emits a provenance JSON sidecar with SHA-256 of the source, row counts, and drift decisions
- **Record entry from text or image** — `/record fitra, 1 mie ayam, 1 mineral water` or a BCA/Mandiri/GoPay/OVO/DANA fund-transfer screenshot becomes one row in a kind-appropriate `ingest_*` table. The `record-entry` skill classifies across `transfer`, `order`, `expense`, `activity`, `meeting`, `health`, `note`, and custom kinds, and asks for clarification until every required field is resolved.
  - New tools: `parse_record` (kind-aware hint extractor), `extract_from_image` (Gemini vision via pydantic-ai `BinaryContent`), `propose_record_table` (table-aware schema router with per-kind templates + agent-supplied `_columns` override)
- **Gateway endpoints**
  - `POST /upload` — accepts tabular and image uploads with a shared body, returns `kind` (`"tabular"` | `"image"`) and a `next` hint. Tenant-scoped staging under `target/ask_ingest/_staging/{tenant}/{uuid}/`
  - `POST /record` — runs the record-entry skill on free-form text via the streaming runner
- **Telegram channel**
  - `filters.Document.ALL` handler routes file uploads into `data-ingest`
  - `filters.PHOTO` handler routes screenshots into `record-entry` with live status edits
- **REPL Phase 1c** — on startup, `target/ask_ingest/*.parquet` files auto-register as `ingest_{stem}` DuckDB views (non-recursive `glob` deliberately excludes `_staging/` subdirectories)
- **Built-in skills**: `data-ingest`, `record-entry`
- **Dependency**: `openpyxl>=3.1` added to `seeknal[ask]` extras for `.xlsx` parsing

### Changed

- `WRITABLE_DIRS` in the ask agent's write-security allowlist now includes `target/` (canonical pipeline output scope)
- `ToolContext` gained `last_read_staging_path` for tool-to-tool handoff of parsed file locations
- Gateway startup banner now lists `/upload` and `/record` alongside the existing endpoints

### Security

- **SSRF** — `read_tabular` blocks private / loopback / link-local / reserved IP ranges (incl. `169.254.169.254` cloud metadata) and disables HTTP redirect following so a public URL cannot bounce into an internal host
- **Content-Disposition traversal** — attacker-controlled filenames from HTTP responses normalised via `Path(...).name` before joining to the staging root
- **Tenant isolation** — `POST /upload` resolves tenant via `X-Tenant-ID` and stages under per-tenant UUID directories
- **fs_lock** — `write_ingested_table`'s parquet swap + provenance write are now serialised against sibling filesystem tools (`draft_node`, `apply_draft`, `save_ingestion_skill`)
- **Self-defending write** — `write_ingested_table(mode='append', user_confirmed=False)` returns the drift report without touching disk, eliminating any reliance on the per-turn `reset_report_approval()` flag reset

### Fixed

- `TestTemporalWorkerFactory::test_create_worker` now patches `Worker` at the call site instead of passing a `MagicMock` to the constructor (newer `temporalio` versions enforce that the client argument is a real bridge client)

## [2.6.0] - 2026-04-16

### Added
- **Thin tools + fat skills architecture** for the ask agent: 16 thin tools provide fast schema discovery and execution; 11 built-in skills deliver multi-step workflows (report generation, pipeline building, data profiling, semantic modeling, metric codification, Python analysis, publishing) via progressive disclosure — skills are loaded on demand, keeping the agent's context lean
- **Seeknal Report Server** (`seeknal report-server start`): self-hosted server for publishing and sharing Evidence.dev reports via unique URLs with token-based authentication
- **Report publishing**: publish reports from the chat TUI menu or via the `publish_to_seeknal_report` agent tool — both paths write to the Report Server and return a shareable URL
- **Chat session options**: `--style` (concise, explanatory, formal, conversational), `--budget` (max USD per session), `--web` (enable DuckDuckGo web search), `--session` (resume named session), `--name` (create named session)
- **Gateway improvements**: `seeknal gateway backend` for cloud-only mode (no local project), `seeknal gateway worker` for standalone Temporal workers, `--redis` for multi-replica scaling, split topology support with `--callback-url` and `--worker-project-path`
- **Auto-load project `.env`**: `seeknal ask --project <path>` now loads `<path>/.env` automatically — no manual `source .env` needed
- **Network error classification**: chat loop errors are classified into DNS, connection, timeout, and unknown categories with actionable hints instead of raw tracebacks
- **Error log persistence**: chat errors are saved to `~/.seeknal/logs/` with full tracebacks for debugging
- **Background task messaging**: backgrounded tool messages no longer leak internal `task_id` values
- **`execute_python` sandbox guard**: detects `duckdb.connect()` calls that shadow the sandbox connection and returns a helpful hint
- **Built-in skills**: `profile-data`, `report-generation`, `build-pipeline-node`, `execute-python-analysis`, `query-metric`, `save-metric`, `save-report-exposure`, `bootstrap-semantic-model`, `publish-memo-to-proof`, `publish-to-seeknal-report`, `edit-proof-document`

### Changed
- Ask agent tool count reduced from 34 to 16 thin tools; complex multi-step workflows moved to 11 progressively-disclosed skills
- System prompt streamlined — workflow instructions moved from prompt to skills, reducing baseline token usage
- `request_limit` moved from module-level global to per-session `ToolContext` for concurrent session safety

### Fixed
- `seeknal ask --project <path>` now resolves `.env` from the project directory, not the shell's cwd
- Chat loop no longer dumps raw `OSError` / `gaierror` — shows classified error with provider/DNS hints
- `generate_report` overwrite warning only shown when target directory already contains built files
- `ask()` and `_quality_gate()` gracefully handle missing `ToolContext` in unit tests (fallback to default request limit)
- `pytest-asyncio` added to dev dependencies for async temporal integration tests

## [2.5.0] - 2026-04-06

### Added
- **Interactive chat mode** (`seeknal ask chat`) with multi-turn conversation, streaming UI, and auto-backgrounding for long tasks
- **Session management** (`seeknal session list/show/delete`) with persistent message history and named sessions
- **Gateway server** with WebSocket, SSE, and REST endpoints for web clients; optional Telegram bot integration
- **ask_user tool** for interactive arrow-key menus that clarify user intent before analysis
- **open_in_browser tool** to open generated reports directly in the browser
- **UI components**: animated fox mascot, interactive menus, ask spinner, keyreader

### Changed
- Iceberg source executor and materialization now support no-auth Lakekeeper catalogs (`AUTHORIZATION_TYPE 'none'` fallback when OAuth2 credentials are absent)

## [2.1.0] - 2026-02-10

### Added - RUN Command Parity with SQLMesh

This release brings comprehensive workflow capabilities for production data pipelines, achieving feature parity with SQLMesh's RUN command functionality.

#### Interval Tracking
- **IntervalCalculator**: Calculate time intervals for incremental processing with cron-based scheduling
  - Support for cron expressions (`0 2 * * *`) and shorthand (`@daily`, `@hourly`)
  - Track completed intervals to prevent duplicate processing
  - Query pending intervals for backfill operations
  - 44 tests passing

#### Change Detection
- **SQL-aware diffing**: Automatically categorize changes as breaking or non-breaking
  - Column-level lineage tracking for impact analysis
  - SQLGlot integration for AST-based SQL comparison
  - Automatic downstream impact calculation
  - 266+ tests passing across all DAG modules
- **Change categories**:
  - `BREAKING` - Schema/logic changes requiring downstream rebuild
  - `NON_BREAKING` - Changes affecting only this node
  - `METADATA` - Description/format changes with no rebuild needed

#### Plan/Apply Workflow
- **Environment Manager**: Safe deployments with isolated testing environments
  - Create plans showing categorized changes before execution
  - Apply changes in isolated dev/staging environments
  - Atomic promotion from dev to production
  - Virtual environments reference production outputs for unchanged nodes
  - TTL-based automatic cleanup (default 7 days)
  - 33 tests passing

#### State Backends
- **Pluggable state backend protocol**: Support for distributed execution
  - `FileBackend` - JSON file storage (default, single-node)
  - `DatabaseBackend` - SQLite/Turso for concurrent access
  - Transactional integrity with atomic updates
  - Optimistic locking for concurrent execution
  - Migration CLI: `seeknal migrate-state --backend database`
  - 25 tests passing

#### Distributed Execution
- **Prefect Integration**: Scheduled pipeline runs with horizontal scaling
  - `seeknal_run_flow()` - Execute full pipeline as Prefect flow
  - `seeknal_backfill_flow()` - Backfill historical data across intervals
  - `create_prefect_deployment()` - Deploy flows for cron-scheduled execution
  - Built-in retry logic and flow monitoring
  - CLI: `prefect worker work-queue` for distributed execution

#### CLI Commands
- Interval tracking:
  - `seeknal intervals show` - Show completed intervals
  - `seeknal intervals pending --schedule @daily` - List pending intervals
  - `seeknal intervals complete --interval "2024-01-01"` - Mark interval complete
- Plan/Apply workflow:
  - `seeknal plan dev` - Create plan for environment
  - `seeknal env apply dev` - Execute plan in environment
  - `seeknal env promote dev prod` - Promote to production
  - `seeknal env list` - List all environments
  - `seeknal env delete dev` - Delete environment
  - `seeknal env cleanup` - Remove expired environments
- State migration:
  - `seeknal migrate-state --backend database` - Migrate state (dry-run)
  - `seeknal migrate-state --backend database --no-dry-run` - Execute migration

#### Documentation
- **[Interval Tracking Guide](docs/guides/interval-tracking.md)** - Time-series incremental processing
- **[Change Detection Guide](docs/guides/change-detection.md)** - SQL-aware change detection
- **[Plan/Apply Workflow Guide](docs/guides/plan-apply-workflow.md)** - Safe deployments
- **[State Backends Guide](docs/guides/state-backends.md)** - Pluggable state storage
- **[Distributed Execution Guide](docs/guides/distributed-execution.md)** - Prefect integration
- **[Workflow API Reference](docs/api/workflow.md)** - Complete API documentation
- Updated Getting Started Guide with workflow feature references

#### Dependencies
- `sqlglot>=20.0.0` - SQL parsing and AST analysis
- `prefect>=3.0.0` (optional) - Distributed execution

### Migration Guide

#### Upgrading from 2.0.0

No breaking changes - all existing APIs remain compatible.

To enable new features:

```bash
# Install new dependencies
pip install sqlglot

# For distributed execution (optional)
pip install prefect

# Migrate state to database backend for production
seeknal migrate-state --backend database --no-dry-run
```

#### Using New Features

```python
# Interval tracking
from seeknal.workflow.intervals import IntervalCalculator

calculator = IntervalCalculator()
intervals = calculator.calculate_intervals(
    start_date="2024-01-01",
    end_date="2024-01-31",
    schedule="@daily"
)

# Change detection
from seeknal.dag.diff import ManifestDiff

diff = ManifestDiff.compare(old_manifest, new_manifest)
to_rebuild = diff.get_nodes_to_rebuild(new_manifest)

# Plan/Apply workflow
from seeknal.workflow.environment import EnvironmentManager

manager = EnvironmentManager(target_path="target")
plan = manager.plan("dev", manifest)
result = manager.apply("dev")
manager.promote("dev", "prod")

# State backend
from seeknal.state.backend import create_state_backend

backend = create_state_backend("database", db_path="target/state.db")

# Prefect integration
from seeknal.workflow.prefect_integration import seeknal_run_flow

results = seeknal_run_flow(project_path="/path/to/project", parallel=True)
```

## [2.0.0] - 2026-01-14

### Breaking Changes
- **SparkEngineTask**: Scala-based implementation replaced with pure PySpark
  - No JVM or Scala installation required
  - Same public API - user code unchanged
  - All transformers ported to PySpark
  - Internal directory structure changed: `pyspark/` → `py_impl/` to avoid namespace collision

### Removed
- Scala spark-engine wrapper code (~3600 lines)
- `findspark` dependency (no longer needed)
- Old transformer implementations using JavaWrapper

### Added
- Pure PySpark transformer implementations:
  - Column operations: ColumnRenamed, FilterByExpr, AddColumnByExpr
  - Joins: JoinById, JoinByExpr
  - SQL: SQL transformer
  - Special: AddEntropy, AddLatLongDistance (with UDFs)
- PySpark aggregator: FunctionAggregator
- PySpark extractors: FileSource, GenericSource
- PySpark loaders: ParquetWriter
- Comprehensive test suite: 22 tests (20 unit tests + 2 integration tests)
- PySpark base classes for transformers, aggregators, extractors, loaders

### Changed
- SparkEngineTask now orchestrates PySpark transformations instead of Scala wrappers
- All transformers use PySpark DataFrame API directly
- Removed dependency on external Scala/Java compilation

### Migration
- Users: Update to v2.0.0 via pip
- No code changes required for existing users - API remains compatible
- See updated CLAUDE.md for PySpark-specific notes

## [1.0.0] - Previous Release
- Initial release with Scala-based Spark engine
