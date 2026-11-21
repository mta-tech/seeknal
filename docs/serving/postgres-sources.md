# Seeknal Serving: Shared PostgreSQL-Wire Sources (`postgres` and `lakebase`)

## 1. Overview

Seeknal Serving supports two source kinds over one shared PostgreSQL-wire execution, pooling, and hardening layer (`postgres_session.py`, `adapters.py`, `server/config.py`, `server/app.py`):

- **`postgres`**: Plain PostgreSQL on local or attached block storage (`storage_class: local` by default; `mode: read_only` by default). Suitable for operational and PII report databases.
- **`lakebase`**: Atlas Lakebase — PostgreSQL compute backed by Neon pageserver/safekeepers persisting to object storage (`storage_class: object_storage`, fixed; `endpoint_type: read_write` by default). Adds endpoint metadata, mandatory `verify-full` TLS with a site CA, `neon_superuser` role-guard enforcement, and bounded cold-start wake/retry.

Both kinds share `PostgresPool` (`LakebasePool` remains as an import alias), `DBAPIAdapter`, workload-scoped `accounts`, TLS verification, role guards, session hardening, and `pg_catalog` discovery wiring.

---

## 2. Configuration Reference (`connections.<name>`)

### Plain PostgreSQL (`kind: postgres`)

```yaml
secrets_env_file: /etc/seeknal/secrets/serving.env   # mode 0600

connections:
  pg_report:
    kind: postgres
    mode: read_only                                  # read_only (default) | read_write
    storage_class: local                             # local (default) | object_storage
    accounts:
      interactive: {dsn_env: PG_REPORT_DSN_BI}
      bi:          {dsn_env: PG_REPORT_DSN_BI}       # identical credential shares one PostgresPool
      report:      {dsn_env: PG_REPORT_DSN_BI}
      ask:         {dsn_file: /etc/seeknal/secrets/pg-report-ask.dsn}  # mode 0600
    pool_max: 12
    pool_min: 2
    connect_timeout_seconds: 5
    acquire_timeout_seconds: 5
    max_idle_seconds: 300
    max_lifetime_seconds: 1800
    role_guard: true                                 # default true
    session:
      statement_timeout_ms: 30000
      lock_timeout_ms: 2000
      idle_in_transaction_session_timeout_ms: 10000
      search_path: pg_catalog
      application_name: seeknal-serving
      startup_options: true                          # set false behind transaction-mode poolers
      connection_limit: 30
    tls:
      sslmode: verify-full                           # defaults to verify-full on kind: postgres; weaker modes require allow_insecure_tls: true
      sslrootcert_file: /etc/seeknal/tls/pg-ca.pem
    freshness:
      sync_state_table: public._lakebase_sync_state
      freshness_sql: "SELECT max(as_of_date) FROM stg.load_control"
```

### Lakebase (`kind: lakebase` with `lakebase:` endpoint block)

```yaml
connections:
  lb_ops:
    kind: lakebase                                   # storage_class is fixed to object_storage
    dsn_env: LB_OPS_DSN                              # SQL-created role (never API neon_superuser)
    pool_max: 4
    session:
      statement_timeout_ms: 15000
      lock_timeout_ms: 2000
      idle_in_transaction_session_timeout_ms: 10000
      search_path: pg_catalog
    tls:
      sslmode: verify-full                           # mandatory on lakebase
      sslrootcert_file: /etc/seeknal/tls/lakebase-ca.pem
    lakebase:
      project_id: prj_xxxxxxxxxxxxxxxxxxxx
      branch_id:  br_xxxxxxxxxxxxxxxxxxxx
      endpoint_id: ep_xxxxxxxxxxxxxxxxxxxx
      endpoint_type: read_write                      # read_write (default) | read_only
      cu: 1
      cold_start:
        wake: api                                    # api | none
        api_url: https://lakebase-cp.internal.example
        api_token_env: LB_OPS_PROJECT_TOKEN
        connect_timeout_seconds: 5
        max_query_wait_seconds: 10
        max_wake_seconds: 60
```

---

## 3. Credentials, Accounts, and Pooling

- **No Literal Secrets:** Literal credential keys (`dsn`, `password`, `passwd`, `secret`, `token`, `api_token`, `uri`, `url`, `connection_string`) are rejected at the connection, account, and `lakebase.cold_start` levels without echoing secret values.
- **Exclusive Credential Source:** Specify **exactly one** of top-level `dsn_env`, top-level `dsn_file`, or `accounts`.
- **Secret Files:** Every `dsn_file` is verified not to be group- or world-readable (`0o044` bits must be clear), and resolved secrets are registered with the log/error scrubber so DSNs and passwords never appear in logs, responses, metrics, or `/readyz`.
- **Per-Workload Accounts & Pool Sharing:**
  - `accounts` accepts workload keys from `{"interactive", "bi", "report", "ask", "job"}` and must include `"interactive"`.
  - Requests for unconfigured workloads are refused with `EngineConfigError`.
  - Workloads referencing the **same** credential (`dsn_env` or `dsn_file`) share a single `PostgresPool` instance while preserving per-workload `account` attribution.

---

## 4. Session Hardening, Role Guard, and Per-Query Transactions

### Connection Startup (`build_connect_kwargs` & `verify_and_harden_session`)
1. **Role Guard (`role_guard: true` by default):**
   - Queries `pg_catalog.pg_roles` on every newly opened connection and refuses any role with `rolsuper` or `rolbypassrls`.
   - On `kind: lakebase`, also refuses membership in `neon_superuser` (preventing accidental use of control-plane API roles that carry `BYPASSRLS`).
   - `role_guard: false` is refused on `lakebase`; on `postgres` it is permitted with a configuration warning.
2. **Hardening Parameters (`session:`):**
   - When `startup_options: true` (default), passes libpq `options`:
     `-c default_transaction_read_only=on` (for read-only sources), `-c statement_timeout=<ms>`, `-c lock_timeout=<ms>`, `-c idle_in_transaction_session_timeout=<ms>`, `-c search_path=<search_path>`, plus `application_name`.
   - Behind poolers (such as PgBouncer in transaction mode) that drop startup options, set `startup_options: false` (backed by `ALTER ROLE ... SET` in PostgreSQL); `verify_and_harden_session` applies session `SET` statements after connect.
   - Runs `SHOW` checks for `default_transaction_read_only` (when read-only), `statement_timeout`, `lock_timeout`, `idle_in_transaction_session_timeout`, and `search_path`. Any error or mismatch closes the connection immediately and raises `EngineError` (fail closed).

### Per-Query Preamble (`begin_query_transaction`)
- Write statements targeting a `read_only` source are rejected before any connection is acquired.
- Every hardened or read-only query executes:
  1. `BEGIN` followed immediately by `SET TRANSACTION READ ONLY` (whenever `read_only` or executing a row-limited `SELECT`)
  2. `SET LOCAL statement_timeout = <min(request_ms, session_cap_ms)>`
  3. `SELECT set_config('application_name', %s, true)` bound to `seeknal:<workload>:<audit_id>` as a driver parameter.

---

## 5. Least-Privilege PostgreSQL Role Setup

Create Serving roles in SQL (never via the Lakebase control-plane API):

```sql
CREATE ROLE seeknal_read_rpt NOLOGIN;
CREATE ROLE seeknal_bi LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
       CONNECTION LIMIT 30 IN ROLE seeknal_read_rpt;
ALTER ROLE seeknal_bi SET default_transaction_read_only = on;
ALTER ROLE seeknal_bi SET statement_timeout = '30s';
ALTER ROLE seeknal_bi SET lock_timeout = '2s';
ALTER ROLE seeknal_bi SET idle_in_transaction_session_timeout = '10s';
ALTER ROLE seeknal_bi SET search_path = pg_catalog;
```

---

## 6. Backward Compatibility & Migration from Legacy `kind: lakebase`

Existing configurations declaring `kind: lakebase` **without** a `lakebase:` endpoint block continue to parse and execute identically to previous releases (`is_legacy == True`), while emitting a one-time deprecation warning:

- **Plain PostgreSQL database:** Relabel the connection to `kind: postgres`. Because `kind: postgres` defaults to `mode: read_only` and `storage_class: local`, add `mode: read_write` only if write statements are still required. Physical table names and placement engine names do not change.
- **Atlas Lakebase endpoint:** Add the `lakebase:` endpoint block (`project_id`, `branch_id`, `endpoint_id`, `endpoint_type`) and `tls: {sslmode: verify-full, sslrootcert_file: ...}`.
