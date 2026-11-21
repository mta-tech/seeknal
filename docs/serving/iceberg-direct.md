# Seeknal Serving: DuckDB `iceberg_direct` Engine Profile (Spike Stage 2)

## 1. Overview & Architectural Goal

Seeknal Serving's default `duckdb` engine profile (`profile: local_file`) opens a single local read-only DuckDB database file and requires that `ATTACH`, `INSTALL`, and `LOAD` are all refused.

The **`iceberg_direct`** engine profile (`profile: iceberg_direct`) enables an in-process `duckdb` engine to read Iceberg tables (`<alias>.<namespace>.<table>`) directly from a **Lakekeeper REST catalog** in read-only mode while preserving the security lockdown:
- An in-memory DuckDB instance (`:memory:`) is created per generation (no local cache file).
- Pre-installed extensions (`httpfs`, `iceberg`), temporary S3/OAuth2 secrets, and read-only Iceberg REST catalog attachments are configured **once** during generation initialization before `SET lock_configuration = true`.
- **No Postgres attach is permitted in this profile.** PII and transactional workloads remain exclusively on the `lakebase` (PostgreSQL) engine; any `postgres`, `attach`, or `lakebase` keys on a `duckdb` connection are rejected at configuration load time.
- The existing `local_file` profile remains the default and behaves identically to Stage 1 when `profile: iceberg_direct` is not configured.

---

## 2. Configuration Reference (`server/config.py`)

A `duckdb` connection in `serving.yaml` selects the profile via `profile: iceberg_direct` (default `local_file`):

```yaml
secrets_env_file: /etc/seeknal/secrets/iceberg-direct.env  # mode 0600

connections:
  duck_ice:
    kind: duckdb
    profile: iceberg_direct
    extension_directory: /opt/duckdb/extensions  # Absolute, normalized path; pre-installed extensions only
    extensions:
      - httpfs
      - iceberg
    allowed_s3_prefixes:
      - s3://atlas-warehouse/analytics/
      - s3://atlas-warehouse/curated/
    recycle_seconds: 240
    token_lifetime_seconds: 300  # Optional: when set, recycle_seconds must be strictly < token_lifetime_seconds
    external_access_after_attach: off  # 'off' (default) | 'on' (explicit opt-in surfaced in probe report)
    temp_directory: /tmpfs/duckdb-spill
    max_temp_directory_size: 4GB
    memory_limit: 2GB
    threads: 4
    max_concurrent_cursors: 32
    iceberg_catalogs:
      lake:  # Alias must match ^[a-z][a-z0-9_]{0,31}$ and not be system/temp/memory/main
        endpoint: https://lakekeeper.internal.example/catalog
        warehouse: atlas_main
        token_uri: https://keycloak.internal.example/realms/atlas/protocol/openid-connect/token
        client_id_env: LAKEKEEPER_OAUTH_CLIENT_ID
        client_secret_env: LAKEKEEPER_OAUTH_CLIENT_SECRET
        scope: catalog:read  # Optional
        access_delegation: vended  # 'vended' | 'none'
        token_lifetime_seconds: 300  # Optional per-catalog token lifetime bound
        s3:
          endpoint: s3.internal.example:9000
          region: ap-southeast-1
          url_style: path  # 'path' (default) | 'vhost'
          key_id_env: ICEBERG_S3_KEY_ID
          secret_env: ICEBERG_S3_SECRET
          use_ssl: true
```

### Validation Rules
- **No YAML Literal Secrets:** Secret values are resolved strictly from environment variables (or `secrets_env_file` with mode `0600`) referenced by `client_id_env`, `client_secret_env`, `key_id_env`, and `secret_env`. Literal secret keys (`client_secret`, `secret`, `password`, `token`, `key_id`) and non-identifier values are rejected with secret-safe errors.
- **Extension Allowlist:** `extensions` must be a non-empty subset of `{"iceberg", "httpfs"}`. `extension_directory` must be an absolute, normalized path; `INSTALL` is never executed.
- **Catalog Alias Safety:** Every `<alias>` under `iceberg_catalogs` must match `^[a-z][a-z0-9_]{0,31}$` and cannot be `system`, `temp`, `memory`, or `main`.
- **S3 Prefix Confinement:** `allowed_s3_prefixes` must be a non-empty list of normalized `s3://<bucket>/<prefix>/` URIs ending with `/`.
- **Token Lifetime & Generation Recycle:** `recycle_seconds` must be `> 0` and strictly less than any configured `token_lifetime_seconds` (at the connection or catalog level).
- **No Postgres Attach:** Any key matching `postgres*`, `postgresql`, `pg`, `lakebase`, or `attach` is rejected.

---

## 3. Hardening Order (`duckdb_policy.py`)

`plan_iceberg_direct(cfg) -> list[Step]` is a pure function that plans the exact hardening sequence, and `harden_iceberg_direct(conn, cfg)` executes it against an in-memory connection in strict phase order `(a)` through `(g)`:

1. **`(a)` Path & Prefix Confinement:**
   - `SET allowed_directories = ['<extension_directory>/', '<temp_directory>/', 's3://<bucket>/<prefix>/', ...]`
   - `SET allowed_paths = []`
   - DuckDB 2.0.0 accepts `s3://bucket/prefix/` entries in `allowed_directories` and enforces prefix confinement when `enable_external_access = false`. If a runtime DuckDB build cannot express S3 prefix confinement in `allowed_directories`, `harden_iceberg_direct` records capability `s3_prefix_confinement = False` rather than faking it.
2. **`(b)` Offline Extension Loading & Persistent Secret Disabling:**
   - `SET autoinstall_known_extensions = false`
   - `SET autoload_known_extensions = false`
   - `SET allow_persistent_secrets = false`
   - `SET extension_directory = '<extension_directory>'`
   - `LOAD <ext>` for each allowlisted extension (never `INSTALL`).
3. **`(c)` Ephemeral Secret Creation:**
   - `CREATE TEMPORARY SECRET s3_<alias> (TYPE S3, ...)` scoped to `allowed_s3_prefixes`.
   - `CREATE TEMPORARY SECRET iceberg_<alias> (TYPE ICEBERG, CLIENT_ID ..., CLIENT_SECRET ..., OAUTH2_SERVER_URI ...)`.
   - Each `Step` carries a `redacted_sql` (`safe_sql`) representation where credential literals are replaced with `'<redacted>'`.
4. **`(d)` Read-Only Iceberg Catalog Attachment:**
   - `ATTACH '<warehouse>' AS <alias> (TYPE ICEBERG, ENDPOINT '...', SECRET iceberg_<alias>, ACCESS_DELEGATION_MODE '...', READ_ONLY)` for each configured catalog.
5. **`(e)` Disable External Access (When `external_access_after_attach: off`):**
   - `SET enable_external_access = false` (default `off`).
   - Note on DuckDB 2.0.0: because DuckDB 2.0.0 forbids modifying `temp_directory` after `enable_external_access = false` (`PermissionException: Modifying the temp_directory has been disabled by configuration`), `harden_iceberg_direct` applies `temp_directory` immediately prior to `SET enable_external_access = false` and verifies it in phase `(f)`.
   - If `external_access_after_attach: on` is explicitly configured, phase `(e)` is skipped and `external_access_after_attach: "on"` is surfaced in the probe report.
6. **`(f)` Resource & Spill Limits:**
   - `SET memory_limit = ...`, `SET threads = ...`, `SET temp_directory = ...`, `SET max_temp_directory_size = ...`.
7. **`(g)` Configuration Lock:**
   - `SET lock_configuration = true`.

Any failure during `(a)`–`(g)` immediately closes the connection and raises `IcebergDirectHardeningError` naming **only** the failed step name (never SQL literals or secret values).

---

## 4. Conformance Probe Profile (`probe_iceberg_direct`)

Alongside the unchanged local-file `probe(conn)`, `probe_iceberg_direct(conn, cfg)` verifies:

| Check Name | Condition Verified |
|:---|:---|
| `external_access_disabled` | `current_setting('enable_external_access')` is `false` (or `external_access_after_attach: "on"` is surfaced when explicitly configured) |
| `config_locked` | `SET threads = 1` is refused (`InvalidInputException` / `PermissionException`) |
| `allow_persistent_secrets_disabled` | `current_setting('allow_persistent_secrets')` is `false` |
| `autoinstall_autoload_disabled` | `autoinstall_known_extensions` and `autoload_known_extensions` are both `false` |
| `custom_extension_repository_locked` | `custom_extension_repository` is locked to `/nonexistent_extension_repository` |
| `read_text_outside_refused` | `SELECT * FROM read_text('/etc/hostname')` (outside allowed dirs) is refused with `PermissionException` |
| `copy_to_refused` | `COPY (SELECT 1) TO '/tmp/...'` is refused with `PermissionException` |
| `install_refused` | `INSTALL httpfs` is refused with `PermissionException` |
| `new_attach_refused` | Attaching `:memory:`, an outside file, or a file inside `allowed_directories` after lock is refused with `PermissionException` |
| `detach_refused` | `DETACH <alias>` (including quoted, comment-prefixed, and multi-statement forms) after lock is refused with `PermissionException` |
| `create_secret_refused` | `CREATE PERSISTENT/TEMPORARY/TEMP SECRET` and `DROP SECRET` (including comment-prefixed and multi-statement forms) after lock are refused |
| `catalog_set_exact` | `duckdb_databases()` contains only the allowlisted read-only catalog aliases plus `system`/`temp`/`memory` |
| `load_unlisted_refused` | `LOAD <unlisted_extension>` is refused with `PermissionException` |
| `secrets_temporary_only` | Every entry in `duckdb_secrets()` has `persistent == false` |
| `writes_refused` | DDL/DML writes (`CREATE TABLE/VIEW/MACRO`, `INSERT`, `DELETE`) on each attached catalog and the in-memory default catalog are refused |
| `s3_outside_prefix_refused` | Reading an `s3://` URI outside `allowed_s3_prefixes` is refused with `PermissionException` (or reported `"unsupported"` if the runtime capability is `False`) |
| `select_ok` | `SELECT 42` returns `42` |

Serving refuses to serve from any generation whose probe fails (`probe_iceberg_direct_ok` fails closed with no fallback).

---

## 5. Generation Recycling, Discovery & Snapshot Pinning

### In-Memory Generation Recycling (`DuckDBHolder.get_iceberg_cursor`)
- Each generation is an isolated in-memory DuckDB instance hardened and probed at creation.
- When `now - created_at >= recycle_seconds` (or upon a probe failure), `DuckDBHolder` opens, hardens, and probes a new generation, atomically switches incoming queries to the new generation, marks the old generation retired, allows in-flight queries on the old generation to finish, and closes the old generation once its `in_flight` count reaches zero.
- All Stage 1 adapter guarantees (bounded concurrent cursor semaphore, single deadline across queue wait and execution, pre-wait cancel never touching DuckDB, first-wins outcome latching, and deterministic cursor cleanup) are preserved.

### Discovery & Snapshot Pinning (`discovery/`, `placement.py`, `router.py`)
- In discovery (`placement.discovery.engines.<name>`), a `duckdb` engine with `profile: iceberg_direct` (and optional `alias`, defaulting to `catalog` or `default_catalog`) emits a serve for each discovered Lakekeeper Iceberg table with:
  - `name`: `<alias>.<namespace>.<table>`
  - `max_lag_seconds`: `(now - current_snapshot_timestamp) + refresh_interval_seconds`
  - Optional `snapshot_id` when `pin_current_snapshot: true` or when pinned in static placement (`serve.<engine>.snapshot_id`).
- When a table serve specifies `snapshot_id: <id>` (a positive signed 64-bit integer), `Router.rewrite()` renders DuckDB's Iceberg time-travel syntax with the table alias placed before `AT (VERSION => <id>)`:
  ```sql
  <alias>.<namespace>.<table> AS <table_alias> AT (VERSION => <id>)
  ```

---

## 6. What Is Proven Offline (Stage 2) vs. What Requires Stage 3 Live Gates

### Proven Offline in Stage 2
- Strict configuration validation, env-only secret resolution (`secrets_env_file`), and rejection of unknown or Postgres/attach keys.
- Pure step-plan ordering `(a)`–`(g)`, SQL quoting, alias validation, and redaction of secrets in diagnostic strings.
- Real DuckDB 2.0.0 execution of the hardening sequence and probe profile using offline stand-ins (built-in `json`/`parquet` extensions, `READ_ONLY` attached stand-in catalog, and `s3://` prefix confinement in `allowed_directories`).
- Zero-downtime generation recycling under 8-thread concurrent query load with a fake clock, draining in-flight cursors without leaks.
- Discovery lag calculation from Lakekeeper snapshot timestamps and router SQL rewriting for `AT (VERSION => <id>)` including nested namespaces and quoted identifiers.

### Not Proven Offline — Decided by Stage 3 Live Gates (`tests/serving/live/test_iceberg_direct_live.py`)
- Live DuckDB `iceberg` + `httpfs` extension interaction with a real Lakekeeper REST catalog and S3/MinIO object storage after `SET enable_external_access = false` (Gate **S2**; specifically whether DuckDB's `iceberg` extension requires `external_access_after_attach: on` to fetch remote manifest/data files on S3 after `ATTACH`).
- Live OAuth2 token refresh and vended S3 credential rotation across generation recycles under concurrency `c8` (Gate **S5**).
- Live p50/p95 query latency, cold vs. warm metadata overhead, and memory RSS stability on real Iceberg datasets (Gates **S8**, **S9**, **S11**, **S13**).

**Stage 3 live gate results decide production adoption of the `iceberg_direct` profile.**
