# Seeknal Serving: Placement Discovery and Live Refresh

## 1. Overview & Purpose

Seeknal Serving routes incoming SQL queries using a `Placement` model and a `schemas` mapping (`logical table name → column names`). In earlier versions, both structures were hand-written. The discovery subsystem (`seeknal.serving.discovery`) automates this process by discovering tables, physical bindings, schemas, and semantic cubes directly from live upstream systems:

- **Lakekeeper (Iceberg REST catalog)**: Discovers canonical tables, schemas, primary keys, snapshot IDs, and row counts.
- **Postgres & Lakebase (`pg_catalog` introspection via `postgres_sources`)**: Discovers allowlisted schemas, tables, views, materialized views, columns, indexes, derived `access` metadata, and freshness lag across both plain PostgreSQL (`kind: postgres`) and Atlas Lakebase (`kind: lakebase`).
- **Lakebase (`public._lakebase_sync_state`)**: Discovers replication sync state, timestamps, and target tables (schema-qualified by default).
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

### Lakebase Sync State (`LakebaseSyncSource`)
Queries the schema-qualified replication metadata table (`public._lakebase_sync_state` by default, configurable via `sync_state_table` matching `^[a-z_][a-z0-9_]{0,62}(\.[a-z_][a-z0-9_]{0,62})?$`):
```sql
SELECT target_table, snapshot_id, synced_at FROM public._lakebase_sync_state
```
- `synced_at` supports Python `datetime`, `date`, or ISO-8601 formatted timestamp strings. Naive datetimes are interpreted as UTC.
- Maps `target_table` to `SyncState(snapshot_id, synced_at)`.
- Database query failures raise `DiscoveryError` (invalid `sync_state_table` identifiers raise `DiscoveryConfigError`).

### Postgres & Lakebase `pg_catalog` Introspection (`postgres_introspect.py`)
For engines configured under `postgres_sources` (`kind: postgres` or `kind: lakebase`), discovery runs four read-only `pg_catalog` queries parameterised by the allowlisted physical schemas (`n.nspname = ANY(%s)`):
1. **Server facts**: `SHOW server_version_num` and `SHOW default_transaction_read_only`. Read-only sources (`kind: postgres` with `mode: read_only`, or `kind: lakebase` with `endpoint_type: read_only`) fail closed with `DiscoveryError` unless `default_transaction_read_only = on`.
2. **Relations**: `pg_class` + `pg_namespace` (`relkind IN ('r','p','v','m')`) with `SELECT` privilege. Unanalyzed tables (`reltuples < 0`) and views (`relkind = 'v'`) yield `row_count: None`.
3. **Columns**: `pg_attribute` (`attnum > 0`, not dropped, `has_column_privilege(..., 'SELECT')`), `format_type`, and `attnotnull`.
4. **Indexes**: valid and ready `pg_index` + `pg_am` (`btree`, `hash`, `brin`, etc.), `indisprimary`, `indisunique`, key columns, opclasses, collations, expression flags, and partial predicates.

#### Deny-by-Default & Access Metadata Derivation
- Only schemas listed in `postgres_sources.<engine>.schemas`, tables passing `include`/`exclude` globs and `^[a-z_][a-z0-9_]{0,62}$` with `SELECT` privilege, and columns matching `^[a-z_][a-z0-9_]{0,62}$` with column `SELECT` privilege are exposed.
- Physical name is `<physical_schema>.<table>`; logical name is `<logical_catalog>.<mapped_schema>.<table>`.
- Partial, expression, and invalid/unready indexes are ignored for automatic derivation:
  - `keys`: Primary key, else narrowest unique non-partial non-expression index with all `NOT NULL` columns.
  - `filterable`: Leading column of valid non-partial non-expression `btree` or `hash` index, plus `keys`.
  - `range_filterable`: Leading column of valid non-partial non-expression `btree` index.
  - `prefix_filterable`: Leading `btree` column with `text_pattern_ops` / `varchar_pattern_ops` / `bpchar_pattern_ops` or `COLLATE "C"`.
  - `sortable`: `NOT NULL` columns forming a leading prefix of any valid non-partial non-expression `btree` index, plus `keys`.
  - `pattern`: For `pii` tables, `point` when `keys` exist else `filter`. Otherwise `scan` when `relkind != 'v'` and `row_count < require_filter_min_rows`, else `filter`.
  - `require_filter`: `True` for `point` and `filter`, `False` for `scan`.
  - `max_rows`: `50` for `pii` tables, else `None`.
- Per-table overrides (`postgres_sources.<engine>.tables`) win over derived fields. Missing tables, unknown/unprivileged columns, `point` pattern without `keys`, or nullable `sortable` columns raise `DiscoveryError`. Declaring an unindexed column in `filterable` / `range_filterable` / `prefix_filterable` / `sortable` emits a warning.

### Cube (`CubeSource`)
Discovers semantic cubes and views using:
```http
GET {url}/cubejs-api/v1/meta
Authorization: <token>
```
- The engine `url` is configured as the bare base (e.g. `http://cube:4000`), because `/cubejs-api/v1/meta` is appended automatically.
- All cubes and views are parsed.
- Member names have the leading cube name prefix stripped (e.g., `orders.revenue` becomes `revenue`).
- **Twins and Mask Variants:** Dimensions are inspected for twin masking dimensions `<base>__<method>` where `<method>` is one of `redact`, `null`, `year`, `last4` and `<base>` is another (non-twin) dimension of the same cube. Valid twins are recorded in `mask_variants` and excluded from `dimensions` and `schemas`.
- **Reserved Suffixes & Member Omission:** Suffixes `__redact`, `__null`, `__last4`, `__year`, and `__hash` are reserved for masking. Any member (dimension or measure) whose name ends in a reserved suffix and is not a valid twin dimension is omitted everywhere: from `dimensions`, `measures`, `schemas`, and `mask_variants`. A warning names each omitted member as `<cube>.<member>` (e.g. `x__hash`, `product__hash`, `orphan__last4`, or a measure `total__null`). Names with `__` but a non-reserved suffix (e.g. `nik__weird`) stay ordinary members.
- **Placement and Schemas:** `placement["cubes"][name]` receives `measures`, ordinary `dimensions`, and `mask_variants`. `schemas[name]` is populated with ordinary dimensions + measures (no twins) so cube queries resolve without table-not-found errors.
- **Cube Failure Isolation:** While `CubeSource.discover()` raises `DiscoveryError` upon HTTP or parsing failures, `discover()` isolates Cube failures so that discovery continues: `placement["cubes"]` is empty, no cube appears in `schemas`, and a warning is appended to `DiscoveryResult.warnings` mentioning that Cube metadata was unavailable and that cubes were omitted. The warehouse catalog (Iceberg/Lakekeeper tables and Lakebase synced tables) still loads successfully.
- Produces `CubeMeta(name, measures, dimensions, mask_variants)`.

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
  sr:        {kind: starrocks, dialect: starrocks, catalog: lakekeeper}
  pg_report: {kind: postgres,  dialect: postgres}
  lb:        {kind: lakebase,  dialect: postgres}
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

postgres_sources:
  pg_report:
    account: interactive
    logical_catalog: lakekeeper
    freshness:
      sync_state_table: public._lakebase_sync_state
      freshness_sql: "select max(as_of_date) from stg.load_control"
    schemas:
      rpt:     {include: ["rpt_*"], exclude: ["rpt_*_stg"]}
      src_pii: {include: [customer], pii: true}
    tables:
      lakekeeper.src_pii.customer:
        access: {pattern: point, keys: [customer_id], filterable: [customer_id], max_rows: 50}

policy:
  require_filter_min_rows: 100000
  max_in_list: 1000
  max_offset: 10000
  guard_mode: enforce
  pii_engines: []
  pii_object_storage_engines: []

# Legacy sync-state-only block remains supported:
lakebase:
  engine: lb
  synced_tables:
    lakekeeper.claims.claim: {target: public.claim, primary_key: [claim_id]}
```

### Validation Rules
- `default_catalog`: Required non-empty string. Canonical logical names follow `<default_catalog>.<namespace>.<table>`.
- `refresh_interval_seconds`: Refresh period in seconds (default: 60.0). Must be positive.
- `timeout_seconds`: HTTP request timeout in seconds (default: 10.0). Must be positive.
- Engines: Allowed kinds are `starrocks`, `cube`, `lakebase`, `postgres`, and `duckdb`. Engines in `placement.discovery.engines` cannot declare `storage_class` (declare `storage_class` on `connections.<name>` instead). Only one `starrocks` engine and at most one `cube` engine are allowed. `duckdb` engines default to `profile: local_file`; setting `profile: iceberg_direct` emits `<alias>.<namespace>.<table>` serves with `max_lag_seconds` derived from the Lakekeeper current-snapshot timestamp age plus `refresh_interval_seconds` (see [`docs/serving/iceberg-direct.md`](iceberg-direct.md)).
- `postgres_sources`: Keys must reference engines of kind `postgres` or `lakebase`, with valid `account` (`interactive`, `bi`, `report`, `ask`, `job`), `schemas`, optional `schema_map`, `freshness`, and `tables` overrides.
- **Storage-Class PII Rule**: Plaintext `pii: true` tables (where `tokenized` is not `true`) must be served on an engine with effective `storage_class: local` (such as `kind: postgres` by default), unless the engine is explicitly listed in `policy.pii_object_storage_engines`.
- **Lakekeeper Merge Rules**:
  - `pii: true` tables never merge: lake serves (`duckdb` / `starrocks`) are dropped and only the Postgres-wire serve is kept.
  - Non-PII tables with identical column lists merge into one table entry carrying both lake and Postgres-wire serves plus the Postgres-wire `access` metadata and `keys`.
  - Non-PII tables with differing column lists drop the Postgres-wire serve and emit a warning.
- **Freshness Cascade**: For `postgres_sources`, `max_lag_seconds` tries in order: (1) `sync_state_table` row for `<physical_schema>.<table>` (default ON for `lakebase`, opt-in for `postgres`), (2) `freshness_sql` executed inside `BEGIN READ ONLY ... ROLLBACK` (must return 1 non-null timestamp/date), (3) `SELECT pg_catalog.pg_postmaster_start_time()` on `read_only` `lakebase` endpoints, (4) `None`.
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

Discovery can be executed as a one-shot CLI command against either a standalone discovery YAML or a full `serving.yaml` (`placement.discovery`):

```bash
# Emit reviewable static placement YAML directly (go-live fallback):
python -m seeknal.serving.discovery \
  --config serving.yaml \
  --emit placement.yaml

# Or emit both placement YAML and schemas JSON:
python -m seeknal.serving.discovery \
  --config cfg.yaml \
  --placement-out placement.yaml \
  --schemas-out schemas.json
```

- Emits discovery diagnostics and warnings to `stderr`.
- Writes valid placement YAML (loadable with `Placement.from_yaml`) and optional schemas JSON.
- **Exit Codes**:
  - `0`: Discovery succeeded.
  - `2`: `DiscoveryError` or `DiscoveryConfigError` encountered. Clean error message printed to `stderr` with no traceback and no credential leaks.

For programmatic testing, `main(argv, *, http_client=None, lakebase_connect=None, postgres_connects=None, storage_classes=None, env=None)` is exposed directly.

---

## 7. Security Architecture

1. **HTTP GET Only**: All discovery network interactions use HTTP GET exclusively. No state mutations occur upstream.
2. **Environment-Only Tokens**: Credentials are never specified in configuration files. Config blocks define `token_env`, which names the environment variable containing the secret.
3. **No Credential Leakage**:
   - Token values are never logged, formatted into exception messages, or echoed in CLI error output.
   - Bearer headers are sanitized across error strings, tracebacks, and warnings.
