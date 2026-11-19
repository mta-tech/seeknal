# Deploying Seeknal Serving

Seeknal Serving is a secure, high-performance internal execution engine and SQL routing gateway positioned directly behind the **Atlas Data Catalog**. It enforces fine-grained access policies (row filters, column masking, column allowlists), selects the optimal execution engine (StarRocks, Lakebase PostgreSQL, Cube SQL API, or DuckDB), and streams policy-compliant result rows back to the Catalog.

Clients, business intelligence tools (e.g., Apache Superset), and end users never contact Seeknal Serving directly. All inbound traffic must originate from the Catalog over mutual TLS (mTLS) and authenticate via short-lived, signed ES256 grant tokens.

---

## 1. Complete Configuration Reference

Seeknal Serving is driven by a YAML configuration file passed via `--config PATH`.

```yaml
# /etc/seeknal/serving.yaml

listen:
  host: 0.0.0.0
  port: 8765

tls:
  cert_file: /etc/seeknal/tls/server.crt
  key_file: /etc/seeknal/tls/server.key
  client_ca_file: /etc/seeknal/tls/catalog-ca.crt
  require_client_cert: true

grant_keys:
  jwks_file: /etc/seeknal/keys/catalog-grants.jwks.json
  # Alternatively, use pem_files:
  # pem_files:
  #   atlas-2026-11: /etc/seeknal/keys/atlas-2026-11.pub.pem

secrets_env_file: /etc/seeknal/secrets/starrocks-workloads.env

logging:
  level: INFO
  format: json
  file: null  # null emits to stderr; string path writes to log file

require_grant_typ: true

grant_max_lifetime_seconds: 60  # Maximum allowable grant token lifetime in seconds (default 60, max 300)
replay_cache_max_entries: 100000  # Size limit for the grant replay cache (default 100000)
compilation_cache_max_entries: 256  # Bounded LRU of raw parsed SQLGlot AST templates for named operations (default 256, max 1024, 0 disables; parse/compilation reuse only, never caches grants, policies, routes, DB physical plans, or query results)

default_catalog: lakekeeper

cube_mode: refuse  # refuse | jwt

placement:
  discovery:
    default_catalog: lakekeeper
    refresh_interval_seconds: 60.0
    timeout_seconds: 10.0
    lakekeeper:
      url: https://lakekeeper.internal.net:8181
      warehouse: main
      # Option A: OAuth2 client credentials (recommended for Keycloak)
      oauth:
        token_url: https://keycloak.internal.net/realms/atlas/protocol/openid-connect/token
        client_id: seeknal-serving-discovery
        client_secret_env: LAKEKEEPER_CLIENT_SECRET
        # Alternatively: client_secret_file: /etc/seeknal/secrets/lk-oauth.secret
        scope: catalog:read
        audience: lakekeeper
      # Option B: Static token (mutually exclusive with oauth)
      # token_env: LAKEKEEPER_TOKEN
    engines:
      sr:
        kind: starrocks
        dialect: starrocks
        catalog: lakekeeper
      lb:
        kind: lakebase
        dialect: postgres
      cube:
        kind: cube
        dialect: postgres
        url: https://cube.internal.net:4000/cubejs-api/v1
        # Option A: Minted HS256 JWT using CUBEJS_API_SECRET
        api_secret_env: CUBEJS_API_SECRET
        # Alternatively: api_secret_file: /etc/seeknal/secrets/cube-api.secret
        api_claims: {role: seeknal-discovery}
        # Option B: Static token (mutually exclusive with api_secret_env/file)
        # token_env: CUBE_API_TOKEN
    lakebase:
      engine: lb
      synced_tables:
        lakekeeper.analytics.users:
          target: users_sync
          primary_key: [id]
          pii: false  # Optional boolean; when true, enforces single-serve PII placement rules
    # Optional discovery-level PII lists:
    # pii_tables: [lakekeeper.analytics.patient_identity]
    # pii_engines: []  # Engine names of kind 'duckdb' explicitly permitted to serve pii tables

  # Alternatively, static placement:
  # static:
  #   placement_file: /etc/seeknal/placement.yaml
  #   schemas_file: /etc/seeknal/schemas.json

connections:
  sr:
    kind: starrocks
    accounts:
      interactive:
        host: starrocks-fe.internal.net
        port: 9030
        user: seeknal_demo_query
        password_env: SEEKNAL_DEMO_QUERY_PASSWORD
      bi:
        host: starrocks-fe.internal.net
        port: 9030
        user: atlas_bi
        password_env: ATLAS_BI_PASSWORD
      report:
        host: starrocks-fe.internal.net
        port: 9030
        user: atlas_reports
        password_file: /run/secrets/atlas_reports_password
  lb:
    kind: lakebase
    dsn_env: LAKEBASE_DSN
    pool_max: 8  # Optional: max pooled connections (default 8, 1..64)
    pool_min: 0  # Optional: min idle connections (default 0)
    connect_timeout_seconds: 10  # Optional: connection timeout in seconds (default 10)
    max_idle_seconds: 300  # Optional: max idle seconds before close (default 300)
    max_lifetime_seconds: 1800  # Optional: max connection lifetime in seconds (default 1800)
    acquire_timeout_seconds: 5  # Optional: acquire wait timeout before 503 error (default 5)
  cube:
    kind: cube
    host: cube-sql.internal.net
    port: 15432
    dbname: cube
    sql_user: seeknal
    # secret_env is required when cube_mode is 'jwt' and forbidden when 'refuse':
    # secret_env: CUBE_SQL_SECRET
    connect_timeout_seconds: 10
  duck:
    kind: duckdb
    path: /data/serving/dev.duckdb
    allowed_directories:
      - /data/serving/
    memory_limit: 4GB
    threads: 4
    temp_directory: /tmpfs/duckdb-temp  # Optional: writable directory outside allowed_directories
    max_temp_directory_size: 10GB  # Optional: max disk space for spills
    max_concurrent_cursors: 32  # Optional: max concurrent query cursors (default 32)

limits:
  timeout_seconds: 30.0
  max_body_bytes: 1000000
  max_result_bytes: 67108864
```

---

## 2. Secrets & Environment Variables

### Invariants
1. **Secrets never live in configuration files:** Only environment variable names (`*_env`) or file paths (`*_file`) are permitted.
2. **Secrets never leak:** Validation errors, application logs, diagnostic outputs, and HTTP error payloads never expose secret values or environment variable contents. If an env var is missing, the error identifies the variable name only.

### `secrets_env_file`
The optional `secrets_env_file` allows loading key-value secrets from a dotenv-formatted file (`KEY=VALUE`, `# comments`).
- **File Permissions:** The file must have filesystem permissions of `0600` or stricter (no group or other read/write/execute permissions). Group- or world-readable files raise `ServerConfigError` immediately upon loading.
- **Lookup Order:** Variable lookups check `secrets_env_file` before falling back to the process environment (`os.environ`).
- **No Shell Expansion:** Values are read literally without shell variable substitution.

### `password_file` / `*_file`
Secret files specified by `password_file`, `client_secret_file`, or `api_secret_file` (such as Kubernetes secret volumes):
- Are read at connection/discovery time, stripping exactly one trailing newline.
- Must not be group- or world-readable (`0o044` permissions raise `ServerConfigError` or `DiscoveryConfigError`).

### Discovery Credentials & Minting

#### Lakekeeper OAuth2 Client Credentials
When Lakekeeper is deployed behind Keycloak or another OAuth2 identity provider, access tokens expire quickly. Rather than relying on a static token that will expire and silently halt catalog refreshes:
- Configure `placement.discovery.lakekeeper.oauth` with `token_url`, `client_id`, and `client_secret_env` (e.g. `LAKEKEEPER_CLIENT_SECRET`) or `client_secret_file`.
- A dedicated, read-only Keycloak client is recommended (for example `seeknal-serving-discovery`).
- Tokens are requested via POST `grant_type=client_credentials` and cached until `expires_at − 30 s` (or 60 s if `expires_in` is missing).
- **401 Retry**: Upon receiving an HTTP 401 from Lakekeeper, discovery invalidates the cached token and immediately retries the request once with a freshly fetched token. A second 401 raises `DiscoveryError`.

#### Minted Cube Meta Token vs. Cube SQL Context Secret
- **Cube REST Discovery (`placement.discovery.engines.<cube>.api_secret_env` | `api_secret_file`)**:
  - Holds Cube's `CUBEJS_API_SECRET` (e.g. via environment variable `CUBEJS_API_SECRET`).
  - Discovery automatically mints a compact HS256 JWT per `discover()` call with claims `iat`, `exp = iat + 300`, and optional `api_claims`. This token is sent as `Authorization: <token>` to Cube's HTTP `/cubejs-api/v1/meta` endpoint to discover cubes, measures, and dimensions.
  - The token is never exposed in logs or exception messages.
- **Cube SQL API Secret (`connections.cube.secret_env`)**:
  - In `cube_mode: jwt`, `connections.cube.secret_env` (e.g. `CUBE_SQL_SECRET`) is used to sign database-level HMAC tokens passed during client SQL connection handshake over Cube's Postgres wire protocol (port 15432). Those tokens carry tenant row filters and column masking rules for query execution, completely distinct from discovery metadata API access.

### Summary of Environment Variables
| Variable Name | Purpose | Configuration Location |
|:---|:---|:---|
| `SEEKNAL_DEMO_QUERY_PASSWORD` | StarRocks password for interactive queries | `connections.sr.accounts.interactive.password_env` |
| `ATLAS_BI_PASSWORD` | StarRocks password for BI workloads | `connections.sr.accounts.bi.password_env` |
| `LAKEBASE_DSN` | PostgreSQL connection string for Lakebase | `connections.lb.dsn_env` |
| `CUBE_SQL_SECRET` | Shared HMAC secret for Cube SQL JWT query context (mode `jwt`) | `connections.cube.secret_env` |
| `LAKEKEEPER_TOKEN` | Static bearer token for Iceberg REST catalog discovery | `placement.discovery.lakekeeper.token_env` |
| `LAKEKEEPER_CLIENT_SECRET` | OAuth2 client secret for Lakekeeper discovery | `placement.discovery.lakekeeper.oauth.client_secret_env` |
| `CUBE_API_TOKEN` | Static bearer token for Cube REST meta discovery | `placement.discovery.engines.cube.token_env` |
| `CUBEJS_API_SECRET` | Secret for minting Cube REST `/meta` HS256 JWT tokens | `placement.discovery.engines.cube.api_secret_env` |

---

## 3. Cryptographic Key Management (EC P-256)

All query executions require a grant token signed with **ECDSA using the P-256 (SECP256R1) curve and SHA-256 (ES256)**.

### Asymmetric Key Boundary
- **Atlas Data Catalog (Signer):** Retains the private key (`catalog.key.pem`). The Catalog uses this key to sign grant and cancel tokens for authenticated requests.
- **Seeknal Serving (Verifier):** Retains **only public keys** (`catalog-grants.jwks.json` or public PEM files). Any attempt to configure a private key (such as a JWK containing the private coordinate `d`) is rejected with `ServerConfigError`.

### Generating Keys
Generate a P-256 private key and extract the public key:

```bash
# 1. Generate private key (Catalog side only)
openssl ecparam -name prime256v1 -genkey -noout -out catalog.key.pem
chmod 0600 catalog.key.pem

# 2. Extract public key (delivered to Serving)
openssl ec -in catalog.key.pem -pubout -out catalog.pub.pem
```

### JWKS Format
When `grant_keys.jwks_file` is used, the JSON Web Key Set file must contain public P-256 EC keys with non-empty `kid` attributes:

```json
{
  "keys": [
    {
      "kty": "EC",
      "crv": "P-256",
      "kid": "atlas-2026-11",
      "x": "us9_4dO31g_Vj2vO9lqD4gT6_...",
      "y": "g4W2H6mD1m3r4k1q9n8p3v..."
    }
  ]
}
```

### `require_grant_typ: true`
Serving defaults to `require_grant_typ: true`. This enforces that incoming grant tokens must contain the claim `"typ": "grant"`. As of the Catalog integration release, the Catalog includes this claim on all issued grant tokens. Cancel tokens (`POST /v1/cancel`) must contain `"typ": "cancel"`.

---

## 4. Mutual TLS (mTLS) Between Catalog and Serving

Because Seeknal Serving executes arbitrary routed SQL on internal analytical backends, network isolation is paramount. Mutual TLS guarantees that **only the Atlas Data Catalog can establish a TCP connection** to Serving.

### Setup Steps
1. **Server Certificate:** Provisioned at `/etc/seeknal/tls/server.crt` with private key `/etc/seeknal/tls/server.key`.
2. **Client CA:** Install the Catalog's Certificate Authority root certificate at `/etc/seeknal/tls/catalog-ca.crt`.
3. **Enforcement:** Setting `require_client_cert: true` configures `ssl.CERT_REQUIRED` in the underlying ASGI server (`uvicorn`). Any connection attempt that fails to present a valid client certificate signed by `catalog-ca.crt` is terminated during the TLS handshake.

---

## 5. Cube Semantic Analytics & `cube_mode`

Cube SQL API routes queries through Cube's semantic layer. Serving supports two modes via `cube_mode`:

### `refuse` (Default)
In `refuse` mode, Serving fails closed:
- Queries referencing semantic cubes with masking or row filters are immediately rejected with `EnforcementError`.
- No shared secret is configured: `connections.cube.secret_env` is forbidden and raises `ServerConfigError` if set.

### `jwt` (Opt-in)
In `jwt` mode, Serving passes signed HMAC-SHA256 tokens in the database connection password containing column masks and mandatory row filters:
- `connections.cube.secret_env` is required.
- Masked members may only be projected at the top level and cannot be probed via `WHERE`, `JOIN`, or `GROUP BY` clauses.

### Production Pre-flight Checklist (`cube.md`)
Do not enable `cube_mode: jwt` in production until all six verification criteria from `docs/serving/cube.md` have been validated in a staging environment:
1. [ ] **Masking:** Masked members in bare top-level projections return masked values.
2. [ ] **Row filtering:** Row filters are applied even with `MEASURE()` aggregations.
3. [ ] **Probe refusal:** Probing masked columns in predicates or expressions is refused.
4. [ ] **Token integrity:** Expired, forged, or invalid tokens are rejected by Cube's `checkSqlAuth`.
5. [ ] **Watchdog deadline:** Queries exceeding the execution timeout trigger `conn.cancel` and raise `EngineTimeout`.
6. [ ] **Client cancel:** Client cancel requests invoke `conn.cancel` via the cancel handle.

---

## 6. Health and Readiness Probes

Seeknal Serving exposes two distinct probe endpoints:

| Endpoint | HTTP Method | Intended Probe | Success Condition | Failure Response |
|:---|:---:|:---|:---|:---|
| `/healthz` | `GET` | Kubernetes Liveness | Service process is alive | Connection refused / 500 |
| `/readyz` | `GET` | Kubernetes Readiness | Catalog placement is active | `503 {"ready": false, "reason": "catalog_not_loaded"}` |

### Detailed Behavior of `/readyz`
- **Static Placement:** Returns `200 {"ready": true}` immediately upon server startup once static placement and schemas files are loaded.
- **Discovery Placement:** The server starts with an empty catalog. `/readyz` returns `503 {"ready": false, "reason": "catalog_not_loaded"}` until the background `PlacementRefresher` successfully completes at least one discovery cycle.
- **Fail-safe:** If discovery fails on startup or during refresh, `/readyz` remains 503 or retains its last successful state. It **never leaks error messages or stack traces** to caller probes.

---

## 7. Operational Deployment

### Fail-Closed Startup Validation (`validate` CLI Subcommand)
In addition to `--check`, Seeknal Serving provides comprehensive fail-closed startup validation via `validate_startup()` (`src/seeknal/serving/startup_validation.py`) and the `validate` subcommand:

```bash
python -m seeknal.serving validate --config /etc/seeknal/serving.yaml
```

- **Exit codes & output:**
  - Exits with code `0` and prints `ok` to `stdout` when all checks pass (in `discovery` mode, also prints a note recording `skipped: live_catalog_discovery` for checks that require live catalog polling).
  - Exits with code `2` and prints one clear, secret-safe `Error: ...` line per validation problem to `stderr` when any check fails—never leaking DSNs, passwords, tokens, or secret file contents.
- **Comprehensive fail-closed checks collected in a single pass:**
  1. **Unknown engine & kind mismatches:** Verifies every engine in `placement.engines` (or `placement.discovery.engines` and `placement.discovery.lakebase.engine`) has a same-name, same-kind entry in `connections`; verifies every table `serve` engine, cube `engine`, and `policy.pii_engines` entry exists in both `placement` and `connections`; and verifies engine kinds in `policy.small_scan_order` / `policy.analytics_order`.
  2. **Unknown table:** Verifies exact case-insensitive and lowercase alignment between static `schemas.json` and `placement.tables` / `placement.cubes` (rejecting missing or extra/stale schema keys) as well as any table references in `pii_tables` or policy table mappings.
  3. **Unknown column:** Verifies that every column in `table.keys`, `cube.measures`, `cube.dimensions`, `cube.mask_variants`, and any static table/policy `masks` or `row_filters` exists in `schemas.json`.
  4. **Unknown role:** Seeknal Serving authorizes requests via per-request signed ES256 grant tokens from Atlas Data Catalog rather than static server roles (`ValidationReport` records `no_static_role_references` when none exist, and rejects any unbacked static role references if present in config or placement policy).
  5. **PII placement & query isolation rules:**
     - Any table marked `pii: true` (in static `placement.yaml` or carried through discovery via `lakebase.synced_tables.<table_name>.pii: true` or `pii_tables`) must have **exactly one** `serve` (`len(serves) == 1`).
     - A `pii: true` table cannot be served on a `duckdb` engine (whether `local_file` or `iceberg_direct`) unless that engine name is explicitly listed in `policy.pii_engines`.
     - At query time, `check_pii_query_mixing()` rejects any statement that joins or combines a `pii: true` table with a table not served on that PII table's sole engine—or supplies an engine hint targeting another engine—with typed error code `pii_cross_engine` (**HTTP 422**) before any engine is called.

### Pre-flight Configuration & Runtime-Wiring Check (`--check`)
Before launching the service (in CI pipelines, container entrypoints, or `systemd` `ExecStartPre`), run the offline preflight check:

```bash
python -m seeknal.serving --config /etc/seeknal/serving.yaml --check
```

`--check` shares the exact cross-artifact offline validation (`ServerConfig.validate_runtime_wiring()`) executed at the beginning of `build_server()` before any file logger, refresher, or database adapter is constructed:

- **Exit codes & output:**
  - Exits with code `0` and prints `config ok` to `stdout` when valid.
  - Exits with code `2` and prints a secret-safe, actionable `Error: ...` diagnostic to `stderr` (never stack traces, raw YAML/JSON snippets, DSNs, or secret values) when invalid.
- **What `--check` validates offline:**
  1. **Server config & TLS/limits/Cube mode:** Structural validation of `serving.yaml`, including `cube_mode` (`refuse` vs. `jwt` `secret_env` rules) and `grant_max_lifetime_seconds`.
  2. **Grant public keys:** Loads and validates EC P-256 public keys (`jwks_file` or `pem_files`) and rejects private key material.
  3. **Environment variables & secret file permissions:** Verifies that every referenced `*_env` variable (such as `dsn_env`, `password_env`, `url_env`, `secret_env`, `token_env`, `client_secret_env`, `api_secret_env`) exists in `secrets_env_file` or `os.environ`, and that `secrets_env_file` (`0600`) and all `*_file` secret paths (not group/world-readable) exist with safe permissions.
  4. **Static placement & engine-connection wiring (`placement.static`):**
     - Parses `placement_file` with `Placement` validation.
     - Verifies that **every** engine declared in `placement.engines` (even if not referenced by a table or when `cube_mode: refuse` is set, since bare `SELECT`, hints, or fallback routing can select declared engines) has an exact same-name entry in `connections` with a matching `kind` (`duckdb`, `lakebase`, `starrocks`, or `cube`). Extra connections in `connections` are allowed.
  5. **Static schemas alignment (`placement.static.schemas_file`):**
     - Parses `schemas_file` as a JSON object, rejecting malformed JSON and exact duplicate JSON keys.
     - Verifies that the case-insensitive (`casefold`) set of schema keys equals the logical names of `placement.tables` union `placement.cubes`—rejecting missing schema entries, stale/extra schema keys, and case-insensitive key collisions.
     - Verifies that each schema value is a JSON list of unique (case-insensitively), non-empty column/member strings (empty lists `[]` are allowed), while preserving original logical-name and column casing/order.
  6. **Discovery engine-connection wiring (`placement.discovery`):**
     - Verifies that every engine declared in `placement.discovery.engines` has an exact same-name entry in `connections` with a matching `kind`, and validates `placement.discovery.lakebase.engine` against `connections`.
- **Strictly offline:** Performs no network I/O, DNS lookups, OAuth token exchanges, discovery HTTP requests, database connections, secret generation, or file writes.
- **Single-process reuse vs. multi-process boundary:** Within `build_server()`, the parsed static `Placement` and validated `schemas` returned by `validate_runtime_wiring()` are reused directly so static files are read only once per process startup. Note that running `--check` in a separate `ExecStartPre` process and then starting the server in `ExecStart` are two OS processes; `build_server()` re-runs the same offline preflight at startup to fail fast before `/readyz` can ever report ready.

#### Valid Matching Static Configuration Example

`serving.yaml`:
```yaml
listen:
  host: 127.0.0.1
  port: 8765

grant_keys:
  jwks_file: /etc/seeknal/keys/catalog-grants.jwks.json

placement:
  static:
    placement_file: /etc/seeknal/placement.yaml
    schemas_file: /etc/seeknal/schemas.json

connections:
  duck:
    kind: duckdb
    path: /data/serving/analytics.duckdb
    allowed_directories:
      - /data/serving/
  lb:
    kind: lakebase
    dsn_env: LAKEBASE_DSN
```

`placement.yaml`:
```yaml
engines:
  duck:
    kind: duckdb
    dialect: duckdb
  lb:
    kind: lakebase
    dialect: postgres
tables:
  lakekeeper.analytics.users:
    keys: [id]
    row_count: 5000
    serve:
      lb: public.users
      duck: main.users
cubes: {}
```

`schemas.json`:
```json
{
  "lakekeeper.analytics.users": ["id", "email", "created_at"]
}
```

#### Actionable Error Examples (Placeholders Only)

- **Missing runtime connection for a declared placement engine:**
  ```text
  Error: Static placement file '/etc/seeknal/placement.yaml' declares engine 'duck' (kind 'duckdb') in 'placement.engines', but no matching entry 'connections.duck' is configured — add 'connections.duck' with kind 'duckdb' or remove the unused engine from placement
  ```
- **Engine `kind` mismatch between placement/discovery and `connections`:**
  ```text
  Error: Engine 'lb' kind mismatch between static placement ('/etc/seeknal/placement.yaml', kind 'lakebase') and 'connections.lb' (kind 'duckdb') — align both to 'lakebase'
  ```
- **Missing or stale schema keys in `schemas.json`:**
  ```text
  Error: Static schemas file '/etc/seeknal/schemas.json' is missing schema entry for placement table/cube: 'lakekeeper.analytics.users' — add matching column list(s) to '/etc/seeknal/schemas.json' or remove the table/cube from placement
  ```
- **Malformed YAML or JSON (reports path and line/column, never raw file snippets or secrets):**
  ```text
  Error: Invalid JSON in static schemas file '/etc/seeknal/schemas.json' at line 4, column 12 (Expecting ',' delimiter) — fix JSON syntax
  ```
- **Missing environment variable:**
  ```text
  Error: Missing required environment variable '<ENV_VAR_NAME>'
  ```

#### Migration Note: Unused Engine Declarations & Mismatched Names
Earlier configurations that left unused engine declarations in `placement.yaml` (for example, declaring `sr`, `cube`, and `duck` in `placement.engines` while only configuring `connections.duck` or `connections.lb`) previously passed `--check` and only failed at query time with `503 engine_unavailable`. Both `--check` and `build_server()` now reject any declared placement or discovery engine that lacks a same-name, same-kind entry in `connections`. When upgrading:
1. Remove unused engine entries from `placement.engines` (or `placement.discovery.engines`), **or** add the corresponding runtime connection under `connections.<engine_name>`.
2. Ensure engine names and `kind` values match identically between `placement` and `connections`.
3. Remove stale table/cube keys from `schemas.json` and ensure every table and cube in `placement.yaml` has a matching key in `schemas.json`.

#### Configuration Validity vs. Runtime Backend Health
- **Configuration & Wiring Validity (`--check` and startup preflight):** Guarantees that all configuration files, keys, environment variables, static placement/schemas, and engine-to-adapter mappings are structurally consistent offline before the server starts.
- **Runtime Backend Health (`/readyz` and query execution):** Neither `--check` nor `/readyz` opens live TCP connections to StarRocks, Lakebase, or Cube to probe backend database health. In static mode, `/readyz` reports `200 {"ready": true}` once the validated static catalog is loaded; in discovery mode, `/readyz` reports `200` after at least one discovery refresh succeeds. Live backend unreachability during query execution is handled by per-query timeouts and circuit/error handling (`503` / timeout errors).

#### Why UI, Scaffolding, and JSON CLI Output Are Deferred
Seeknal Serving is an internal service-to-service (S2S) SQL execution gateway deployed behind the Atlas Data Catalog over mTLS—not an end-user or browser-facing application. The existing `python -m seeknal.serving --config PATH --check` CLI and `build_server()` startup preflight are the primary operator and CI/CD developer-experience surfaces; web UIs, project scaffolding subcommands, and machine-readable JSON CLI flags are intentionally deferred to keep the serving surface minimal and hardened.

### Systemd Service Unit Example
Create `/etc/systemd/system/seeknal-serving.service`:

```ini
[Unit]
Description=Seeknal Serving Query Gateway
After=network.target

[Service]
Type=simple
User=seeknal
Group=seeknal
WorkingDirectory=/etc/seeknal
EnvironmentFile=/etc/seeknal/secrets/starrocks-workloads.env
ExecStartPre=/usr/local/bin/python3 -m seeknal.serving --config /etc/seeknal/serving.yaml --check
ExecStart=/usr/local/bin/python3 -m seeknal.serving --config /etc/seeknal/serving.yaml
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536

# Security hardening
ProtectSystem=strict
ProtectHome=true
ReadOnlyPaths=/etc/seeknal/tls /etc/seeknal/keys
ReadWritePaths=/var/log/seeknal

[Install]
WantedBy=multi-user.target
```

### Docker Run Example
Run the container mounting TLS certificates and secrets:

```bash
docker run -d \
  --name seeknal-serving \
  --restart unless-stopped \
  -p 8765:8765 \
  -v /etc/seeknal/serving.yaml:/etc/seeknal/serving.yaml:ro \
  -v /etc/seeknal/tls:/etc/seeknal/tls:ro \
  -v /etc/seeknal/keys:/etc/seeknal/keys:ro \
  -v /etc/seeknal/secrets/starrocks-workloads.env:/etc/seeknal/secrets/starrocks-workloads.env:ro \
  --env-file /etc/seeknal/secrets/starrocks-workloads.env \
  seeknal-serving:latest \
  python -m seeknal.serving --config /etc/seeknal/serving.yaml
```

---

## 8. DuckDB Engine Hardening & Conformance Probe

When using the embedded DuckDB engine (`kind: duckdb`), Seeknal Serving locks down database connections to prevent external access, unauthorized file reads, extension downloads, and configuration tampering.

### Owned Per-Query Cursor Lifecycle, Bounded Concurrency & Operational Limits
- **Shared Hardened Instance & Owned Per-Query Cursor:** A process-wide, thread-safe `DuckDBHolder` maintains a single hardened database instance per resolved database file. Hardening policy enforcement and conformance probing execute once when each database file generation is loaded. Each query acquires a bounded concurrency slot (`max_concurrent_cursors`, default `32`) and its own isolated `cursor()`, cancels and joins its started watchdog timer, and deterministically closes the cursor and releases the slot on every path.
- **Deadline-Aware Slot Acquisition & Pre-Wait Cancellation:**
  - Request cancellation handles are registered before waiting for a cursor slot.
  - A single monotonic deadline (`timeout_seconds`) spans both queue wait and query execution (not reset after acquiring a slot); queued requests that are cancelled or expire before acquiring a slot fail fast (`499 query_cancelled` or `504 engine_timeout`) without touching DuckDB or executing SQL.
  - Terminal outcomes (`success`, `cancel`, `timeout`, `error`) are latched first-wins per request, and connection/cursor cleanup failures never mask primary errors (sole close failures raise secret-safe `EngineError("Engine connection cleanup failed")`).

The probe command verifies that a DuckDB database connection complies with the locked-down runtime policy:

```bash
python -m seeknal.serving.duckdb_policy probe --path /path/to/database.duckdb [--allowed-dir /data/serving/ ...]
```

The probe outputs a JSON report with conformance check results and exits with code `0` if all checks pass, or `1` if any check fails:

```json
{
  "external_access_disabled": true,
  "config_locked": true,
  "read_text_outside_refused": true,
  "copy_to_refused": true,
  "attach_refused": true,
  "install_refused": true,
  "load_refused": true,
  "select_ok": true
}
```

### DuckDB Connection Reuse, Symlink File Swap, and Temp Directory
- **Connection Reuse:** A process-wide, thread-safe holder maintains a single hardened database instance per resolved database file. Hardening policy enforcement and conformance probing execute once when the database is loaded. Each incoming query operates on its own isolated `cursor()` taken from that connection, allowing per-query interruption and timeout isolation without interference across concurrent queries.
- **Zero-Downtime Symlink File Swap:** The configured database `path` may point to a symlink that is atomically updated to point to a new database file after an offline data reload. Only atomic symlink swaps are supported (in-place rewrites of active database files are not supported and will be refused). On every query, the holder cheaply checks the path's resolved target and inode. When a swap is detected:
  - If the path points back to an already open instance, that instance is reused without re-hardening.
  - Otherwise, the new file is opened, hardened, and probed immediately for incoming queries.
  - In-flight queries on superseded instances complete normally.
  - A superseded database connection is automatically closed once all in-flight queries finish and it is no longer current (or reaped after `max_idle_seconds` 300s). No queries fail during swaps and no server restart is needed.
- **`temp_directory` and `max_temp_directory_size`:**
  - `temp_directory`: An optional absolute path where DuckDB may spill data for memory-intensive operations (such as large sorts or group-by aggregations). It **has to be a dedicated per-instance writable tmpfs or volume outside the data directory** (DuckDB allows arbitrary reads and writes inside its temp directory) and **must NOT be inside any `allowed_directories` entry or the data directory**.
  - `max_temp_directory_size`: An optional disk limit string (e.g., `10GB`).
  - When `temp_directory` is unset, DuckDB is configured so that it does not spill into the database file directory, and queries exceeding memory limits fail with a clear `resource_exhausted` error instead of an internal server error.
- **Graceful Shutdown:** All open DuckDB database instances and cursors are explicitly closed during application lifespan shutdown.

### DuckDB `iceberg_direct` Profile (Experimental / Spike Stage 2)
A `duckdb` connection may optionally set `profile: iceberg_direct` (default `local_file`) to attach a read-only Lakekeeper Iceberg REST catalog (`<alias>.<namespace>.<table>`) in an in-memory DuckDB generation using pre-installed `httpfs` and `iceberg` extensions, temporary S3/OAuth2 secrets from `secrets_env_file`, periodic generation recycling (`recycle_seconds`), and post-attach configuration lock (`lock_configuration = true`). No Postgres attach is allowed in this profile. See [`docs/serving/iceberg-direct.md`](iceberg-direct.md) for the complete configuration reference, hardening step order, conformance probe checks, and Stage 3 live adoption gates.

---

## 9. Lakebase Connection Pool & Sizing Guidance

When querying Lakebase (PostgreSQL), Seeknal Serving manages a thread-safe connection pool per Lakebase endpoint to eliminate connection establishment overhead.

### Connection Configuration Keys
| Key | Type | Default | Description |
|:---|:---|:---|:---|
| `pool_max` | integer | `8` | Maximum concurrent connections in the pool (allowed range: `1` to `64`). |
| `pool_min` | integer | `0` | Minimum idle connections pre-warmed in the pool. |
| `connect_timeout_seconds` | number | `10.0` | Connection timeout in seconds, applied to all driver connection attempts even if not in DSN. |
| `max_idle_seconds` | number | `300.0` | Maximum time in seconds an idle connection remains in the pool before being closed and replaced. |
| `max_lifetime_seconds` | number | `1800.0` | Maximum lifetime in seconds of any pooled connection before being closed and replaced. |
| `acquire_timeout_seconds` | number | `5.0` | Maximum wait time in seconds to check out a pooled connection. If exhausted, fails with a `503` error. |

### Sizing Guidance Against Database `max_connections`
When planning pool sizes, calculate the total potential connections across all running Seeknal Serving instances against PostgreSQL's `max_connections` setting:
$$\text{Total Connections} = (\text{Number of Serving Replicas}) \times \text{pool\_max}$$
Ensure that this total connection count leaves sufficient margin for:
1. `superuser_reserved_connections` (typically 3–5 connections reserved for Postgres superusers).
2. Direct admin tools, catalog discovery workers, and operational migration tasks.
For example, if PostgreSQL has `max_connections = 100` and you run 4 Serving replicas, setting `pool_max: 16` requires at most $4 \times 16 = 64$ connections, leaving 36 connections for administrative operations and catalog sync.

### Session Isolation & Endpoint Resilience
- **Strict Session Isolation:** No connection is ever shared between concurrent queries. Each query executes within its own transaction (`BEGIN`, `SET TRANSACTION READ ONLY`, `SET LOCAL statement_timeout`).
- **Safe Return:** A connection is returned to the pool only after a successful `ROLLBACK` or `COMMIT` leaving no modified session state. Network I/O (rollback and close) is performed outside the pool lock to prevent contention.
- **Fail-Closed on Errors:** Any query error, cancellation, or statement timeout causes the connection to be immediately closed and discarded—never reused. Late cancellations cannot affect subsequent queries on reused connections.
- **Idle Expiry via Background Reaper:** A lightweight daemon thread periodically reaps connections idle longer than `max_idle_seconds` (default 300s). Expired connections are also closed on checkout.
- **Dead Connection & Restart Recovery:** Connections found dead on checkout are transparently replaced. If the database endpoint restarts and an existing pooled connection is severed, the connection attempt is transparently retried once before starting query execution, preventing spurious errors to callers.
