# Seeknal Serving: Placement Discovery and Live Refresh

## 1. Overview & Purpose

Seeknal Serving routes incoming SQL queries using a `Placement` model and a `schemas` mapping (`logical table name → column names`). In earlier versions, both structures were hand-written. The discovery subsystem (`seeknal.serving.discovery`) automates this process by discovering tables, physical bindings, schemas, and semantic cubes directly from live upstream systems:

- **Lakekeeper (Iceberg REST catalog)**: Discovers canonical tables, schemas, primary keys, snapshot IDs, and row counts.
- **Lakebase (`_lakebase_sync_state`)**: Discovers replication sync state, timestamps, and target tables for transactional point-lookup serving.
- **Cube REST API (`/cubejs-api/v1/meta`)**: Discovers semantic cubes, measures, and dimensions.

Discovery generates valid `Placement` configurations and schema dictionaries, calculates replication lag guarantees, and supports live periodic background catalog refreshes without query downtime.

---

## 2. Discovery Sources

### Lakekeeper (`LakekeeperSource`)
Discovers Iceberg tables using the standard Iceberg REST API over HTTP GET:
1. `GET {url}/v1/config?warehouse=<warehouse>`: Resolves catalog prefix overrides or defaults.
2. `GET {url}/v1/{prefix}/namespaces`: Discovers namespaces. Single-level namespaces matching the optional allowlist are retained; multi-level namespaces are skipped with diagnostics warnings.
3. `GET {url}/v1/{prefix}/namespaces/{ns}/tables`: Lists tables in each retained namespace.
4. `GET {url}/v1/{prefix}/namespaces/{ns}/tables/{table}`: Reads table metadata:
   - **`columns`**: Top-level field names of the schema where `schema-id` matches `current-schema-id`.
   - **`keys`**: Field names corresponding to `identifier-field-ids` in the current schema.
   - **`current_snapshot_id`**: Current snapshot identifier (-1 or null indicates none).
   - **`row_count`**: Total record count (`total-records`) parsed from snapshot summary.
   - **`snapshot_ms`**: Timestamp in milliseconds of the current snapshot.

Tables whose metadata fails to load or parse are skipped with a warning so discovery continues uninterrupted. Failures in namespace listing or catalog config raise `DiscoveryError`.

#### Lakekeeper Authentication (OAuth2 Client Credentials & Static Token)
- **OAuth2 Client Credentials (`lakekeeper.oauth`)**: When Lakekeeper runs behind Keycloak or another OAuth2 identity provider where bearer tokens expire in minutes, configure `oauth` with `token_url`, `client_id`, `client_secret_env` (e.g., `LAKEKEEPER_CLIENT_SECRET`) or `client_secret_file`, and optional `scope` / `audience`.
  - A dedicated read-only Keycloak client is recommended (for example `seeknal-serving-discovery`).
  - Tokens are fetched via POST `application/x-www-form-urlencoded` to `token_url` with `grant_type=client_credentials`. This POST request to `token_url` is the **only** non-GET request discovery may make; all Lakekeeper catalog calls remain strictly HTTP GET.
  - The token is cached until `expires_at − 30 s` (or for 60 s if `expires_in` is omitted in the response).
  - **401 Retry**: If Lakekeeper responds with an HTTP 401 Unauthorized, `LakekeeperSource` invalidates the cached token and immediately retries the request once with a fresh token. If the second attempt also returns 401, a `DiscoveryError` is raised.
- **Static Bearer Token (`lakekeeper.token_env`)**: For legacy deployments with long-lived tokens, `token_env` specifies the environment variable containing the static token. `token_env` and `oauth` are mutually exclusive.

### Lakebase (`LakebaseSyncSource`)
Queries the Lakebase replication metadata table:
```sql
SELECT target_table, snapshot_id, synced_at FROM _lakebase_sync_state
```
- `synced_at` supports standard Python datetimes or ISO-8601 formatted timestamp strings. Naive datetimes are interpreted as UTC.
- Maps `target_table` to `SyncState(snapshot_id, synced_at)`.
- Database query failures raise `DiscoveryError`.

### Cube (`CubeSource`)
Discovers semantic cubes and views using:
```http
GET {url}/cubejs-api/v1/meta
Authorization: <token>
```
- All cubes and views are parsed.
- Member names have the leading cube name prefix stripped (e.g., `orders.revenue` becomes `revenue`).
- Produces `CubeMeta(name, measures, dimensions)`.
- Endpoint or JSON parsing failures raise `DiscoveryError`.

#### Cube Authentication (Minted Token & Static Token)
- **Minted Meta Token (`api_secret_env` | `api_secret_file`)**: Cube `/cubejs-api/v1/meta` can be authenticated by minting an HS256 JWT using Cube's `CUBEJS_API_SECRET` (configured via `api_secret_env`, e.g., `CUBEJS_API_SECRET`, or `api_secret_file`).
  - `CubeSource` mints a compact HS256 JWT per `discover()` call using the Python standard library.
  - JWT claims include `iat` (current timestamp) and `exp = iat + 300` (valid for 5 minutes).
  - Optional `api_claims: {..}` can supply additional claims (such as `role`), but cannot override `iat` or `exp`.
  - The token is sent in the `Authorization: <token>` header without leaking to logs or error messages.
  - **Difference from Cube SQL `secret_env`**: Discovery `api_secret_env` authenticates against Cube's HTTP REST metadata endpoint (`/cubejs-api/v1/meta`). In contrast, `connections.cube.secret_env` (used in `cube_mode: jwt`) signs database authorization context tokens containing row filters and column masking for SQL execution over Cube's Postgres wire protocol.
- **Static Token (`token_env`)**: Transmits a static token from the specified environment variable. Mutually exclusive with `api_secret_env`/`api_secret_file`.

---

## 3. Configuration Model (`config.py`)

Discovery is configured via YAML or dictionaries:

```yaml
default_catalog: lakekeeper
refresh_interval_seconds: 60
timeout_seconds: 10.0

lakekeeper:
  url: https://lakekeeper.example/catalog   # Iceberg REST base (without /v1)
  warehouse: atlas
  # Option A: OAuth2 Client Credentials (recommended for Keycloak)
  oauth:
    token_url: https://keycloak.example/realms/atlas/protocol/openid-connect/token
    client_id: seeknal-serving-discovery
    client_secret_env: LAKEKEEPER_CLIENT_SECRET
    # Alternatively: client_secret_file: /etc/seeknal/secrets/lk-oauth.secret
    scope: catalog:read
    audience: lakekeeper
  # Option B: Static token (mutually exclusive with oauth)
  # token_env: LAKEKEEPER_TOKEN
  namespaces: [claims, analytics]           # Optional allowlist

engines:
  sr:   {kind: starrocks, dialect: starrocks, catalog: lakekeeper}
  lb:   {kind: lakebase,  dialect: postgres}
  cube:
    kind: cube
    dialect: postgres
    url: http://cube:4000
    # Option A: Minted HS256 JWT using CUBEJS_API_SECRET
    api_secret_env: CUBEJS_API_SECRET
    # Alternatively: api_secret_file: /etc/seeknal/secrets/cube-api.secret
    api_claims: {role: seeknal-discovery}
    # Option B: Static token (mutually exclusive with api_secret_env/file)
    # token_env: CUBE_API_TOKEN
  duck: {kind: duckdb,    dialect: duckdb}

lakebase:
  engine: lb
  synced_tables:
    lakekeeper.claims.claim: {target: public.claim, primary_key: [claim_id]}
```

### Validation Rules
- `default_catalog`: Required non-empty string. Canonical logical names follow `<default_catalog>.<namespace>.<table>`.
- `refresh_interval_seconds`: Refresh period in seconds (default: 60.0). Must be positive.
- `timeout_seconds`: HTTP request timeout in seconds (default: 10.0). Must be positive.
- Engines: Only one `starrocks` engine and at most one `cube` engine are allowed. `duckdb` engines are copied as-is into placement.
- `lakebase.engine` must reference a defined engine with kind `lakebase`.
- Tokens come strictly from environment variables named by `token_env` / `*_env` or safe secret files named by `*_file`. Secret files must not be group- or world-readable (`0600` or stricter required).
- Validation failures raise `DiscoveryConfigError` without leaking credentials.

---

## 4. Lag Calculation Rule

For synced tables served by Lakebase, the router needs an upper bound on replication lag to guarantee freshness (`max_staleness_seconds` checks).

### The Rule
$$\text{max\_lag\_seconds} = \text{observed\_lag} + \text{refresh\_interval\_seconds}$$

Where:
- If the Lakebase synced `snapshot_id` matches the Iceberg `current_snapshot_id`:
  $$\text{observed\_lag} = 0$$
- If the snapshot IDs differ (or Lakebase has not caught up to the current snapshot):
  $$\text{observed\_lag} = \max(0, \text{now} - \text{synced\_at})$$

### Why the Interval Is Added
Adding `refresh_interval_seconds` is deliberate. Catalog discovery executes periodically at interval $T$. A table whose observed lag at discovery time $t$ is $L$ will experience up to $L + T$ seconds of staleness immediately before the next refresh at $t + T$. Adding the refresh interval ensures the calculated lag remains a safe, valid upper bound across the entire refresh window until the next check.

### Diagnostics & Serve Fallbacks
- A synced table missing from Iceberg, or lacking an entry in `_lakebase_sync_state`, triggers a warning and receives no Lakebase serve.
- Keys default to Iceberg identifier fields, falling back to Lakebase `primary_key`, or `[]`.
- Tables with no available engine serve (e.g., unsynced tables when only Lakebase is configured) are omitted from placement with a warning.

---

## 5. Live Refresh and Pipeline Atomic Swap

### Atomic Pipeline Swap (`service.py`)
`ServingPipeline.update_catalog(placement, schemas)` provides hot-swapping without restarting the serving process:
- Placement, schemas, and router are updated atomically under an internal lock.
- `ServingPipeline.prepare(...)` snapshots `(placement, schemas, router)` once under the lock at the start of preparation. Concurrent swaps never mix old schemas with new placements or intermediate router states.

### Refresher Lifecycle (`PlacementRefresher`)
`PlacementRefresher(discover_fn, pipeline, interval_seconds=..., clock=time.monotonic)` manages periodic refresh:
- `refresh_once() -> bool`: Executes `discover_fn()` and swaps the pipeline catalog via `update_catalog()`.
- **Fault Tolerance & Redaction**: If an error occurs during discovery, the refresher keeps the last good catalog in service, increments `consecutive_failures`, records `last_error` (`<ExceptionClassName>` only), and logs a warning formatted as `Catalog refresh failed: <ExceptionClassName>` with no error message and no traceback.
- `start()`: Spawns a background daemon thread that calls `refresh_once()` every interval.
- `stop()`: Signals thread shutdown and joins cleanly within $2 \times \text{interval\_seconds}$.

---

## 6. CLI Usage (`__main__.py`)

Discovery can be executed as a one-shot CLI command:

```bash
python -m seeknal.serving.discovery \
  --config cfg.yaml \
  --placement-out placement.yaml \
  --schemas-out schemas.json
```

- Emits discovery diagnostics and warnings to `stderr`.
- Writes valid placement YAML (loadable with `Placement.from_yaml`) and schemas JSON.
- **Exit Codes**:
  - `0`: Discovery succeeded.
  - `2`: `DiscoveryError` or `DiscoveryConfigError` encountered. Clean error message printed to `stderr` with no traceback and no credential leaks.

For programmatic testing, `main(argv, *, http_client=None, lakebase_connect=None, env=None)` is exposed directly.

---

## 7. Security Architecture

1. **HTTP GET Only**: All discovery network interactions use HTTP GET exclusively. No state mutations occur upstream.
2. **Environment-Only Tokens**: Credentials are never specified in configuration files. Config blocks define `token_env`, which names the environment variable containing the secret.
3. **No Credential Leakage**:
   - Token values are never logged, formatted into exception messages, or echoed in CLI error output.
   - Bearer headers are sanitized across error strings, tracebacks, and warnings.
