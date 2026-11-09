# Atlas Runtime Governed Seeknal Ask

In Atlas Runtime (P2-S1), Seeknal Ask runs in a sandbox pod **as the user** and holds **no engine or storage credentials**. Every data tool fetches rows strictly through the Atlas Catalog Ask API, aligning Ask execution with the same governed access control and policy evaluation as Superset.

## Environment Contract

Governed mode is activated by setting:

```bash
ATLAS_ASK_MODE=governed
```

When `ATLAS_ASK_MODE` is not set, Seeknal Ask runs in standard mode, retaining full backward compatibility.

### Required Configuration

When governed mode is active, the runtime requires:

- `ATLAS_CATALOG_URL`: The base URL of the Atlas Catalog service.
  - Must use the `https://` protocol.
  - Plain `http://` is strictly prohibited unless `ATLAS_ASK_ALLOW_HTTP=true` is explicitly configured.
- `ATLAS_ASK_SESSION_TOKEN_FILE`: Absolute path to a file containing the short-lived bearer session token.
  - File permissions must be strict: mode `0600` (refuses any group- or world-readable bits).
  - The token is re-read on every HTTP request so the external pod controller can refresh credentials seamlessly without process restarts.

### Prohibited Credentials

To enforce the zero-credential invariant, the runtime refuses to start if any static or engine credential environment variables are set in the pod:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `KEYCLOAK_CLIENT_SECRET`
- `LAKEKEEPER_TOKEN`
- `ATLAS_API_TOKEN`
- `IBA_ENGINE_API_KEY`
- `IBA_STORAGE_API_KEY`
- Any PostgreSQL credential matching `SEEKNAL_PG_*PASSWORD*`

If any prohibited variable is present in the environment, `GovernedAskContext.from_env()` raises a configuration error naming the offending variable.

## Tool Allowlist

Governed mode replaces open-ended tool discovery with an explicit, strict allowlist. Only the following 17 tools are permitted:

### 1. Database Analysis Tools
- `execute_sql`: Sends queries to `POST /api/ask/v1/query` with `maxRows`. Does not perform local unhardened DuckDB execution or secondary row counting.
- `preview_query`: Dry-run query diagnostic tool (inspects schema and limits without engine attachments).
- `list_tables`: Discovers available tables via progressive `GET /api/ask/v1/schema`.
- `describe_table`: Resolves table schemas and column types via `GET /api/ask/v1/schema`.

### 2. Visualization
- `visualize_chart`: Chat chart generator operating purely over in-memory row data already retrieved from the Catalog.

### 3. Read-Only Context Tools
- `list_source_context` / `read_source_context`: Read generated documentation and context notes.
- `list_sql_pairs` / `read_sql_pair` / `execute_sql_pair`: Read and execute pre-approved SQL examples.
- `list_ask_tests` / `read_ask_test` / `run_ask_test`: QA regression test inspection and execution.
- `list_ask_test_results` / `read_ask_test_result`: QA regression test result inspection.

### 4. Memory Read Tools
- `list_context_files`: List project-local context files.
- `read_project_file`: Read project-local context notes.

### Excluded Tools

Any tool not explicitly listed in the allowlist is excluded. Specifically:
- `execute_python`: Excluded because its sandbox rebuilds a credentialed REPL.
- Write tools (`write_project_file`, `save_preference`, `write_ingested_table`, `save_metric`, `save_ingestion_skill`, `save_report_exposure`): Excluded; the sandbox pod has no local or remote write authority.
- Pipeline and transformation tools (`run_pipeline`, `plan_pipeline`, `draft_node`, `edit_node`, `dry_run_draft`, `apply_draft`): Excluded.
- Export and upload tools (`upload_to_s3`): Excluded.
- External predictive tools (`run_forecast`, `detect_anomaly`): Excluded; these make direct external IBA HTTP calls.
- Web tools (`web_search`, `web_scrape`, DuckDuckGo): Excluded.
- Proof and report publishing tools (`publish_to_proof`, `publish_to_seeknal_report`, `edit_proof_document`): Excluded.

Any newly added tool is blocked by default unless explicitly added to `GOVERNED_TOOL_ALLOWLIST`.

## Guarantees

1. **No Engine Credentials in Process:**
   The Ask agent never holds database passwords, S3 keys, or lakehouse tokens. It does not load `profiles.yml` or invoke DuckDB `ATTACH`.

2. **Every Row Governed by Atlas Catalog:**
   Data rows are returned exclusively by `POST /api/ask/v1/query` and `GET /api/ask/v1/schema`. Column masking, row-level filtering, and access permissions are enforced upstream by the Catalog.

3. **Scratch DuckDB Lockdown:**
   The scratch DuckDB instance used for in-memory operations is initialized via `seeknal.serving.duckdb_policy.connect_hardened(":memory:", read_only=False)` with:
   - `enable_external_access = false` (filesystem operations and `ATTACH` blocked).
   - `autoinstall_known_extensions = false` and `autoload_known_extensions = false`.
   - `memory_limit = '512MB'` and `threads = 2`.
   - `lock_configuration = true`.

4. **Credential Leak Prevention:**
   The bearer session token is never included in exceptions, logs, or error payloads.

## What is Deferred

- **Write Approvals:** Human-in-the-loop or automated approval workflows for write actions (pipeline execution, report creation, preference saving) are deferred to subsequent Atlas Runtime milestones.
- **Model Gateway and Egress Tiers:** Network egress filtering and LLM model gateway routing are managed by the surrounding Kubernetes pod network policies and Atlas egress proxies.
