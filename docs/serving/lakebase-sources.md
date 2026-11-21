# Seeknal Serving: Atlas Lakebase Sources (`lakebase_endpoint.py`)

## 1. Overview

Seeknal Serving distinguishes two Postgres-wire source kinds:

- **`postgres`:** Plain PostgreSQL on local or attached block storage (`storage_class: local` by default). Used for on-premises operational databases and PII replicas.
- **`lakebase`:** Atlas Lakebase — PostgreSQL compute backed by Neon pageservers and safekeepers that persist WAL and page layers to object storage (`storage_class: object_storage`, fixed), organized by projects, branches, and endpoints.

Both kinds share the Postgres-wire adapter, connection pool, workload accounts, and session hardening (`postgres_*`). `src/seeknal/serving/lakebase_endpoint.py` implements the Lakebase-specific layer:

1. Validated `lakebase:` endpoint, `cold_start:`, and `freshness:` configuration (`parse_lakebase_block`).
2. Mandatory `verify-full` TLS with a site CA bundle (`validate_lakebase_tls`).
3. Engine `storage_class` derivation across all serving engine kinds (`effective_storage_class`).
4. Pool-startup role guard rejecting `rolsuper`, `rolbypassrls`, and `neon_superuser` membership (`role_guard_sql`, `check_role_guard`).
5. Single-flight control-plane endpoint wake and bounded connect retry (`ColdStartConnector`, `EngineStartingError`).
6. Frozen-snapshot age measurement for `read_only` Lakebase endpoints (`snapshot_freshness`).

---

## 2. Configuration Reference

```yaml
secrets_env_file: /etc/seeknal/secrets/serving.env  # mode 0600

connections:
  lb_ops:
    kind: lakebase                    # storage_class is fixed to 'object_storage'
    dsn_env: CBN_LB_OPS_DSN           # SQL-created least-privilege role (never an API role)
    pool_max: 4
    session:
      statement_timeout_ms: 15000
      lock_timeout_ms: 2000
      idle_in_transaction_session_timeout_ms: 10000
      search_path: pg_catalog
    tls:
      sslmode: verify-full            # Mandatory for Lakebase; weaker modes are rejected
      sslrootcert_file: /etc/seeknal/tls/lakebase-ca.pem
    lakebase:
      project_id: prj_xxxxxxxxxxxxxxxxxxxx
      branch_id: br_xxxxxxxxxxxxxxxxxxxx
      endpoint_id: ep_xxxxxxxxxxxxxxxxxxxx
      endpoint_type: read_write       # 'read_write' (default) | 'read_only'
      cu: 1                           # Optional positive CU; max_connections = max(25, round(cu * 100))
      cold_start:
        connect_timeout_seconds: 5    # Default: 5.0
        wake: api                     # Default: 'api' when cold_start is present; 'none' when omitted
        api_url: https://lakebase-cp.internal.example
        api_token_env: LB_OPS_PROJECT_TOKEN
        max_query_wait_seconds: 10    # Per-request wait cap (never exceeds query deadline)
        max_wake_seconds: 60          # Background single-flight wake budget
        retry_backoff_ms: [500, 1000, 2000, 4000]
      freshness:
        sync_state_table: public._lakebase_sync_state  # Default ON for lakebase; null disables
        freshness_sql: null           # Optional single-value SQL freshness fallback
```

### Validation Rules

- **Endpoint Identifiers:** `project_id`, `branch_id`, and `endpoint_id` must match `^prj_[a-z0-9]{20}$`, `^br_[a-z0-9]{20}$`, and `^ep_[a-z0-9]{20}$` respectively. Uppercase characters, wrong prefixes, and invalid lengths are rejected.
- **Compute Size (`cu`):** Optional positive number (`int` or `float`, never `bool`). Exposes `LakebaseEndpointConfig.max_connections = max(25, round(cu * 100))` for startup pool-budget checks (`instances * pool_max <= max_connections`).
- **Fixed Storage Class:** Setting `storage_class` inside `lakebase:` or on a `kind: lakebase` connection that declares `lakebase:` raises a configuration error (`Lakebase storage_class is fixed to 'object_storage' and cannot be set`).
- **Mandatory `verify-full` TLS:** `validate_lakebase_tls` requires `tls.sslmode == 'verify-full'` and a non-empty `tls.sslrootcert_file`. Plaintext and weaker modes (`disable`, `prefer`, `require`, `verify-ca`) are refused.
- **Secret Hygiene:** Literal credential keys (`api_token`, `token`, `secret`, `password`, `dsn`, `api_key`) and URLs containing userinfo (`https://user:pass@...`) are rejected at parse time. `api_token_env` must match `^[A-Za-z_][A-Za-z0-9_]{0,127}$`. Unknown keys in `lakebase`, `cold_start`, or `freshness` are rejected.

---

## 3. Storage Class & PII Placement (`effective_storage_class`)

Every connection resolves to a deterministic storage class:

| Connection Kind & Profile | Effective `storage_class` | Notes |
|---|---|---|
| `postgres` | `local` (default) or declared `local` / `object_storage` | Plain PostgreSQL on local or attached block storage. |
| `lakebase` (with `lakebase:` block) | `object_storage` (fixed) | WAL and page layers persist to object storage; setting `storage_class` is an error. |
| `lakebase` (legacy, no `lakebase:` block) | Declared `local` / `object_storage`, else `unknown` | `unknown` fails closed as object storage under PII placement rules. |
| `duckdb` (`profile: iceberg_direct`) | `object_storage` (fixed) | Reads Iceberg files from object storage; `storage_class: local` is rejected. |
| `duckdb` (`profile: local_file`, default) | `local` (default) or declared `local` / `object_storage` | Local read-only DuckDB file or acceleration cache. |
| `starrocks` | Declared `local` / `object_storage`, else `object_storage` | Fails closed to `object_storage` when omitted. |
| `cube` | Declared `local` / `object_storage`, else `object_storage` | Fails closed to `object_storage` when omitted. |

Tables marked `pii: true` may only be served from `local` engines unless explicitly allowlisted in `placement.policy.pii_object_storage_engines` (or marked `tokenized: true`).

### Legacy `kind: lakebase` Migration

Existing configurations that specify `kind: lakebase` **without** a `lakebase:` block remain accepted as *legacy Lakebase* for backward compatibility (`storage_class: unknown` unless declared) and emit a deprecation warning. Plain PostgreSQL databases should be relabelled `kind: postgres` (adding `mode: read_write` if write routing is required); true Atlas Lakebase endpoints should add the `lakebase:` block.

---

## 4. Least-Privilege Role Guard (`role_guard_sql`, `check_role_guard`)

Roles provisioned via the Lakebase control-plane API are members of `neon_superuser` and hold `BYPASSRLS`, which would bypass PostgreSQL RLS policies assumed by Serving's row filters.

- Serving must connect as a role **created in SQL** (`NOSUPERUSER NOBYPASSRLS`, not granted `neon_superuser`).
- At pool startup, `check_role_guard(conn, kind)` runs `role_guard_sql(kind)`:
  - `postgres`: checks `rolsuper` and `rolbypassrls` on `pg_catalog.pg_roles` for `current_user`.
  - `lakebase`: additionally checks `pg_has_role(current_user, 'neon_superuser', 'MEMBER')` guarded by `CASE WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'neon_superuser')`.
- If any check is `True` (or the query fails), the connection is immediately closed and `EngineError` is raised. On `lakebase`, the role guard cannot be disabled.

---

## 5. Cold-Start Wake & Single-Flight Coordination (`ColdStartConnector`)

Lakebase endpoints do not wake on TCP/PostgreSQL connect; suspended or restarting endpoints refuse connections until started via the control plane.

When `connect_fn` fails on `ColdStartConnector.connect(deadline=...)`:

1. **Single-Flight Background Wake:** Concurrent callers share a single `_WakeOperation` per endpoint:
   - When `wake: api`, issues **one** `POST {api_url}/v1/projects/{project_id}/endpoints/{endpoint_id}/start` with `Authorization: Bearer <token>`, then polls `GET {api_url}/v1/projects/{project_id}/endpoints/{endpoint_id}` using `retry_backoff_ms` until `state == "running"`.
   - When `wake: none`, skips control-plane HTTP calls and runs single-flight bounded `connect_fn` retries with `retry_backoff_ms` up to `max_wake_seconds`.
2. **Bounded Per-Query Wait (`engine_starting`):** Each caller waits at most `min(max_query_wait_seconds, deadline - now)`. If that budget expires while the wake is still within `max_wake_seconds`, the caller receives `EngineStartingError` (`code="engine_starting"`, HTTP `503`, `Retry-After` header) while the background wake continues for subsequent requests.
3. **Failure & Outage Handling:** If the control-plane API fails or `max_wake_seconds` expires without establishing a connection, the connector transitions to `state = "unavailable"` and raises `EngineError("Engine connection failed")` (`engine_unavailable`). The API token is registered with the secret scrubber and never appears in logs, exceptions, or metrics.

### Readiness & Metrics

- **Connector state (`.state`):** `'ok'`, `'starting'`, or `'unavailable'`.
- **Metrics hooks:**
  - `serving_lakebase_wake_total{engine,outcome}` (`ok` | `error` | `timeout`)
  - `serving_lakebase_connect_retries_total{engine}`
  - `serving_lakebase_cold_start_seconds{engine}`
  - `serving_lakebase_state{engine,state}`

---

## 6. Freshness & Read-Only Snapshot Semantics (`snapshot_freshness`)

Lakebase `read_only` endpoints currently serve a frozen snapshot captured when compute starts rather than streaming WAL continuously:

1. **Sync State (`public._lakebase_sync_state`):** Enabled by default on `lakebase` (`LakebaseFreshnessConfig.sync_state_table`) using schema-qualified reads compatible with `search_path = pg_catalog`.
2. **Custom SQL (`freshness_sql`):** Optional single-value read-only query marker.
3. **Read-Only Snapshot Age (`snapshot_freshness`):** When `endpoint_type == "read_only"` and no data-level marker overrides it, the adapter queries `SELECT pg_catalog.pg_postmaster_start_time()` and reports `max(0.0, now - postmaster_start_time)` in seconds. Returns `None` for `read_write` endpoints.
