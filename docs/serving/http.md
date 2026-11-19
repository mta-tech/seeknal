# Seeknal Serving HTTP API

## Security Notice: Strict Service-to-Service Gateway

**Only the Atlas Data Catalog may reach this API.**

Seeknal Serving is an internal, non-public gateway located directly behind the Atlas Data Catalog. Clients, BI tools (such as Superset), and users never call this API directly. All requests to Seeknal Serving must originate from the Catalog and must include an ES256 grant token signed by the Catalog's private key.

To enforce this boundary in production, the service must run behind mutual TLS (mTLS) requiring client certificates issued exclusively to the Catalog.

---

## 1. Endpoints

### 1.1 `GET /healthz`

Service health probe endpoint. Requires no authentication.

**Response (200 OK):**
```json
{
  "status": "ok"
}
```

---

### 1.2 `POST /v1/query`

Executes a prepared and policy-enforced query on the routed engine.

#### Request Headers
- `Authorization: Bearer <grant token>` (required): Compact ES256 JWS issued by the Catalog.

#### Grant Token Contract v3 & Audit Logging
The Catalog signs every grant token presented in the `Authorization` header. In contract v3, the grant supports workloads `ask` and `job` (in addition to `interactive`, `bi`, and `report`) and optional audit-only claims:
- `workload` (`str`): Target execution account workload (`"interactive"`, `"bi"`, `"report"`, `"ask"`, `"job"`). Defaults to `"interactive"`.
- `act` (`dict`): Actor claim `{"sub": str, "typ": "ask_session" | "run"}` representing an acting session or job run.
- `azp` (`str`): Authorized party client ID (non-empty string or absent).
- `origin` (`str`): Request origin; one of `"superset"`, `"ask"`, `"job"`, `"portal"`, or absent.

##### Consistency Rules
- Workload `ask` **requires** `act.typ == "ask_session"` and `origin == "ask"`.
- Workload `job` **requires** `act.typ == "run"` and `origin == "job"`.
- `act` supplied with any other workload is refused (`403 enforcement_error`).

#### Signed Parameter Digest (`seeknal-params-v1`) & Named Operations
Grants may include three optional signed claims for governed named operations and parameter integrity:
- `params_sha256` (`str`): Lower-case 64-character hexadecimal SHA-256 digest of the positional `params` array encoded under `seeknal-params-v1`.
- `operation_id` (`str`): Server-owned operation identifier matching `^[a-z][a-z0-9_.-]{0,63}$` (for example, `"order_by_id"` or `"orders_by_month"`).
- `operation_version` (`int`): Positive safe integer (`1..9007199254740991`).

##### Co-presence & Verification Invariants
- **Legacy callers:** Requests whose grant omits all three claims retain existing uncached behavior.
- **Claimed digest without operation claims:** If `params_sha256` is present without `operation_id`/`operation_version`, the parameter digest is still verified against `params`, though the query is not eligible for the compilation cache.
- **Named operations require all three claims:** If either `operation_id` or `operation_version` is present in the grant, all three (`operation_id`, `operation_version`, `params_sha256`) must be present and valid; omitting `params_sha256` is rejected (`403 enforcement_error`).
- **Pre-cache verification:** Both `sql_sha256` and `params_sha256` are verified after JWS signature verification and **before** any compilation-cache lookup.
- **Wire encoding (`seeknal-params-v1`):**
  - Header: ASCII `seeknal-params-v1\n`, decimal parameter count, `\n`.
  - Scalars concatenated in array order:
    - `null`: `n;`
    - `boolean`: `t;` or `f;`
    - safe integer (`[-9007199254740991, 9007199254740991]`, `-0` normalized to `0`): `i<base10>;`
    - UTF-8 string: `s<byte_length>:<utf8_bytes>;`
  - Limits: at most 32 parameters, at most 16,384 UTF-8 bytes per string, and at most 65,536 total frame bytes (including header and framing). Floats, unsafe integers, unpaired surrogates, binary values, arrays, and objects are rejected without echoing parameter values in error messages.

#### Parse-Template Compilation Cache
For requests carrying complete valid named-operation claims (`operation_id`, `operation_version`, `params_sha256`) and SQL $\le 32\text{ KiB}$, `ServingPipeline` reuses raw parsed SQLGlot AST templates across requests via a bounded thread-safe LRU cache (`compilation_cache_max_entries`, default `256`, max `1024`, `0` disables):
- **Parse/compilation reuse only:** The cache stores **only** raw parsed logical SQLGlot ASTs keyed by `(sql_sha256, sql_dialect, default_catalog, catalog_generation)`. It is **not** a database physical execution plan cache or a query result cache.
- **Fresh per-request governance:** Every request verifies its own grant token, clones a fresh copy of the raw AST, and runs default-catalog canonicalization, routing, table/column/mask/row-filter enforcement, physical table rewriting, and driver parameter binding from scratch.
- **Catalog refresh fencing:** `ServingPipeline.update_catalog()` atomically increments `catalog_generation` and clears cached AST entries; any concurrent parse begun under an older generation is fenced from repopulating the new generation.
- **Read-only numeric diagnostics:** `pipeline.compilation_cache_info()` returns only `{"hits", "misses", "evictions", "size", "max_entries"}` and never exposes SQL text, cache keys, subjects, grants, or parameter values.

##### Audit Log Record
Each request emits a per-request audit log line. When present in the token/grant, `workload`, `azp`, `origin`, `act_sub`, and `act_typ` are included in the audit log line (e.g., `workload=ask azp=seeknal-client origin=ask act_sub=session-01 act_typ=ask_session`). These claims are strictly audit-only and do not alter table permissions, row limits, or masking policies.

#### Request Body
JSON object with the following fields:
- `sql` (string, required): The client SQL statement using standard `$1..$n` parameter placeholders.
- `params` (list, optional): Positional parameter values corresponding to `$1..$n`.
- `hint` (string, optional): Routing override hint (e.g., target engine name).
- `max_staleness_seconds` (number, optional): Maximum allowable replication lag in seconds.
- `timeout_s` (number, optional): Positive finite execution timeout in seconds. Effective timeout is `min(server timeout, timeout_s)`.
- `max_rows` (integer, optional): Positive integer lowering the result row cap. Effective cap is `min(grant.max_rows, max_rows)`.
- `max_result_bytes` (integer, optional): Positive integer upper bound on the JSON-encoded `rows` size. Effective cap is `min(server max_result_bytes, max_result_bytes)`. Exceeding this returns `413 result_too_large`.

*Note: Any unrecognized fields in the JSON body will trigger a `400 Bad Request`.*

#### Example Request
```http
POST /v1/query HTTP/1.1
Host: serving.internal.atlas.net:8000
Authorization: Bearer eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImNhdGFsb2ctMjAyNiJ9...
Content-Type: application/json

{
  "sql": "SELECT id, name, salary FROM users WHERE salary >= $1",
  "params": [60000],
  "timeout_s": 15.0,
  "max_rows": 500,
  "max_result_bytes": 10485760
}
```

#### Response (200 OK)
Returns a JSON object with:
- `columns` (`list[str]`): Output column names in projection order.
- `column_types` (`list[str]`): Output column data types in projection order, matching the length of `columns`. Normalized to contract v2 types: `boolean, smallint, integer, bigint, float, double, decimal, decimal(p,s), varchar, date, timestamp, timestamptz, time, binary, json, unknown`. Bare `decimal` denotes an unconstrained decimal with unknown or arbitrary precision.
- `rows` (`list[list]`): Query result rows converted to JSON-safe representations:
  - `datetime`, `date`, `time`: ISO 8601 formatted strings (e.g., `"2026-10-31T12:00:00"`).
  - `Decimal`: Exact numeric string representation (e.g., `"123.45"`).
  - `bytes`: Base64 encoded string.
  - `float` `NaN`, `+Infinity`, `-Infinity`: Encoded as `null`.
- `truncated` (`bool`): `true` if results exceeded the effective row cap.
- `notices` (`list[str]`): Machine-readable execution notices; contains `"row_cap_hit"` when `truncated` is `true`, otherwise `[]`.
- `engine` (`str`): Identifier of the engine where the query executed.
- `rule` (`str`): Placement routing rule selected (e.g., `analytics`, `point_lookup`, `small_scan`).
- `reason` (`str`): Diagnostic rationale for engine selection.
- `masked_columns` (`list[str]`): List of columns transformed by masking policies.
- `audit_id` (`str`): JTI audit identifier from the grant token.
- `redacted_sql` (`str`): AST-redacted SQL where all literals and placeholders are replaced with `?`.
- `elapsed_ms` (`float`): Query execution time in milliseconds.

#### Example Response
```json
{
  "columns": ["id", "name", "salary"],
  "column_types": ["integer", "varchar", "integer"],
  "rows": [
    [2, "****", 80000]
  ],
  "truncated": false,
  "notices": [],
  "engine": "duck",
  "rule": "small_scan",
  "reason": "small table scan without key filter",
  "masked_columns": ["users.name"],
  "audit_id": "jti-claim-9012",
  "redacted_sql": "SELECT id, name, salary FROM users WHERE salary >= $?",
  "elapsed_ms": 3.75
}
```

---

### 1.3 `POST /v1/explain`

Prepares the query, resolves routing, validates grants, applies masks, rewrites physical tables, and binds parameters, but **does not execute** the query against any engine.

#### Request
Identical headers and JSON body schema as `POST /v1/query`.

#### Response (200 OK)
Returns a JSON object with:
- `engine` (`str`): Selected target engine.
- `rule` (`str`): Selected routing rule.
- `reason` (`str`): Rationale for the routing decision.
- `sql` (`str`): Engine-ready SQL statement rendered in the target engine's dialect with parameter markers compiled.
- `paramstyle` (`str`): Driver parameter style (`"format"` for `%s` or `"qmark"` for `?`).
- `masked_columns` (`list[str]`): Masked column paths.
- `row_limit` (`int | null`): Upper limit enforced (`max_rows + 1`), or `null` for write queries.
- `redacted_sql` (`str`): AST-redacted SQL for audit logs.

#### Example Response
```json
{
  "engine": "sr",
  "rule": "analytics",
  "reason": "table users row count 50000000 >= threshold 1000000",
  "sql": "SELECT id, name, salary FROM lakekeeper.users AS users WHERE salary >= %s LIMIT 1001",
  "paramstyle": "format",
  "masked_columns": ["users.name"],
  "row_limit": 1001,
  "redacted_sql": "SELECT id, name, salary FROM users WHERE salary >= $?"
}
```

---

### 1.4 `POST /v1/cancel`

Cancels an in-flight query (actively executing or queued waiting for an embedded DuckDB cursor slot) identified by `jti` in the cancel token. For embedded DuckDB (`DuckDBAdapter`), the cancellation handle is registered before waiting for a cursor slot and the request's monotonic timeout deadline (`timeout_s` / server `timeout_seconds`) includes both queue wait and SQL execution; cancelling a queued request aborts it (`499 query_cancelled`) without touching DuckDB or executing SQL.

#### Request Headers
- `Authorization: Bearer <cancel token>` (required): Compact ES256 JWS cancel token issued by the Catalog.

#### Request Body
Empty body or `{}`.

#### Cancel Token Format
- ES256 compact JWS using the same trusted `kid` public keys as grant tokens.
- Claims:
  - `typ` (string, required): Must be `"cancel"`.
  - `iss` (string, required): Expected Catalog issuer (`"atlas-catalog"`).
  - `aud` (string or list, required): Must match or contain `"seeknal-serving"`.
  - `iat`, `exp` (number, required): Token lifetime `exp - iat <= 60` seconds.
  - `jti` (string, required): Non-empty query audit ID matching the executing query.
  - `sub` (string, required): Non-empty user subject matching the executing query.

#### Response (202 Accepted)
```json
{
  "cancelled": true,
  "query_id": "jti-claim-9012"
}
```

#### Status Codes
- **202 Accepted:** Query matched and cancel signal sent to the engine adapter.
- **401 Unauthorized:** Invalid or expired cancel token, missing `typ: "cancel"`, or bad signature.
- **404 Not Found:** Query ID not currently running, or running query's subject differs from `sub` (never reveals which).
- **400 Bad Request:** Malformed request body (non-empty and not `{}`).
- **413 Payload Too Large:** Request body exceeds `max_body_bytes` (`code: "payload_too_large"`).

---

## 2. Error Handling, Typed Error Catalogue & Privacy Guarantees

All error responses have an HTTP status code $\ge 400$ and return a consistent typed JSON payload:

```json
{
  "error": "<category>",
  "code": "<catalogue_code>",
  "message": "<short safe diagnostic text>",
  "next_step": "<user-facing actionable guidance>",
  "audit_id": "<verified jti when known>"
}
```

- `error` (`str`, always present): Backwards-compatible error category string.
- `code` (`str`, always present): Stable typed error code from `seeknal.serving.errors.ERROR_CATALOGUE`.
- `message` (`str`, always present): Short, secret-safe user-facing summary.
- `next_step` (`str`, always present): Actionable user-facing guidance sentence from `ERROR_CATALOGUE`.
- `audit_id` (`str`, optional): Included whenever the request's `jti` has been cryptographically verified (omitted when missing or before token verification).

### Privacy & Leakage Invariants
- **No Grant Tokens or Unverified Headers:** Raw grant/cancel tokens, signature data, and unverified JWS header fields (`kid`, `alg`, `typ`) are never echoed in error messages.
- **No SQL Literals or Sensitive Parameters:** Client SQL expressions, string literals, exact quoted bound-parameter tokens (`'<p>'`), and non-numeric parameter values (`len >= 3`) are never included in error messages or logs, while ordinary numeric diagnostics (such as `"61.0s"`) remain readable.
- **No Driver Internals, Config Secrets, or Log Forging:** Database driver connection strings, DSNs (including `/` in userinfo), quoted/escaped credentials (`''`, `""`, `\'`, `\"`), DuckDB `SECRET '...'` clauses, secrets loaded from `ServerConfig` / secret files / environment variables, raw driver error text, and Python tracebacks are never returned to the HTTP client; operational failure details (`engine`, `code`, `error` class, secret-scrubbed single-line `detail` with all control/non-printable characters escaped and capped at 2048 characters, and `audit_id`) are logged server-side at `ERROR` and never included in HTTP responses.

### Typed Error Code Catalogue (`ERROR_CATALOGUE`)

| `code` | Status | `error` Category | `message` | `next_step` |
|:---|:---:|:---|:---|:---|
| `grant_expired` | **401** | `grant_token_error` | Grant token has expired. | Sign in again or refresh the dashboard to get a new grant. |
| `grant_invalid` | **401** | `grant_token_error` | Grant token is missing, malformed, or invalid. | Sign in again or request a valid grant token from the catalog. |
| `grant_replayed` | **401** | `grant_replayed` | Grant token has already been used. | Request a fresh grant token for each query execution. |
| `access_denied` | **403** | `enforcement_error` | Query violates data access policy or grant permissions. | Request access to the required tables and columns in the catalog. |
| `data_stale` | **422** | `routing_error` | No engine meets the requested data freshness constraint. | Relax max_staleness_seconds or wait for replication to catch up. |
| `engine_unavailable` | **503** | `engine_unavailable` | The target execution engine is currently unavailable. | Retry shortly or contact the platform operator if the outage persists. |
| `engine_timeout` | **504** | `engine_timeout` | Query execution timed out. | Simplify the query, add selective filters, or increase timeout_s. |
| `query_spans_engines` | **422** | `routing_error` | Query references tables or cubes across incompatible engines. | Query tables served by a single engine or split the query. |
| `pii_cross_engine` | **422** | `routing_error` | PII table cannot be queried together with tables not served on its engine. | Query the PII table separately on its designated engine. |
| `row_cap_hit` | **422** | `enforcement_error` | Query exceeded the maximum permitted row cap. | Add a stricter LIMIT or narrow your query filters. |
| `result_too_large` | **413** | `result_too_large` | Query result size exceeds the maximum byte limit. | Reduce the number of projected columns or rows returned. |
| `invalid_sql` | **400** | `bad_request` | The SQL statement or request payload is invalid. | Check the SQL syntax and request fields and try again. |
| `payload_too_large` | **413** | `payload_too_large` | Request body exceeds the maximum size limit. | Shorten the SQL statement or reduce the request payload size. |
| `query_cancelled` | **499** | `query_cancelled` | Engine query execution cancelled | Re-run the query if the cancellation was unintentional. |
| `audit_unavailable` | **503** | `audit_unavailable` | Audit logging destination is unavailable; query refused. | Retry later or contact the platform operator to restore the audit sink. |
| `internal_error` | **500** | `internal_error` | Internal server error | Retry the request or contact support with the audit_id. |
| `not_found` | **404** | `not_found` | Query not found | Verify the query_id (jti) and that the query is still running. |
| `duplicate_query_id` | **409** | `duplicate_query_id` | A query with this audit_id is already running. | Wait for the in-flight query to complete or issue a new grant. |

### Category Compatibility Notes
- **Row Cap Truncation vs Refusal:** In default truncation mode (`on_row_cap="truncate"`), queries exceeding the effective row cap succeed with `200 OK`, `"truncated": true`, and `"notices": ["row_cap_hit"]`. When configured to refuse (`on_row_cap="error"`) or when a query supplies an invalid `LIMIT`/`FETCH` clause, Serving returns `422` with `error="enforcement_error"` and `code="row_cap_hit"`.
- **Engine Driver Errors (`502 engine_error`):** When an adapter raises `EngineError` during connection or query execution, Serving preserves HTTP `502` and `error="engine_error"` with `code="engine_unavailable"`, a whitelisted safe message (`"Engine connection failed"` or `"Engine query execution failed"`), and `next_step` from `engine_unavailable`.
- **Workload Account & Replay Cache (`503`):** Missing workload accounts (`workload_account_missing`) and full grant replay caches (`replay_cache_full`) preserve their existing `error` category and `503` status while setting `code="engine_unavailable"`.

---

## 3. Deployment Adapter Wiring

Serving adapters implement the `EngineAdapter` protocol, executing `PreparedQuery` instances against target backends.

Below is an example showing how a deployment wires adapters for StarRocks, Lakebase, Cube, and DuckDB:

```python
import pymysql
import psycopg2
import duckdb
from seeknal.serving import (
    Placement,
    ServingPipeline,
    DuckDBAdapter,
    DBAPIAdapter,
    build_adapter,
    create_app,
)
from seeknal.serving.duckdb_policy import connect_hardened

# 1. StarRocks with PyMySQL
def connect_starrocks():
    return pymysql.connect(
        host="starrocks-fe.internal",
        port=9030,
        user="serving_user",
        password="secret_password",
        database="lakekeeper",
        autocommit=True,
    )

sr_adapter = DBAPIAdapter(connect_starrocks, kind="starrocks")

# 2. Lakebase with Psycopg2 and Lakebase SDK ensure_running
def lakebase_ensure_running():
    # Call Lakebase SDK to ensure compute instance is active before connecting
    # e.g., lakebase_client.clusters.ensure_running(cluster_id="lb-01")
    pass

def connect_lakebase():
    return psycopg2.connect(
        host="lakebase.internal",
        port=5432,
        user="serving_svc",
        password="lakebase_secret",
        dbname="lakebase_db",
    )

lb_adapter = DBAPIAdapter(
    connect_lakebase,
    kind="lakebase",
    before_connect=lakebase_ensure_running,
)

# 3. Cube SQL API with Psycopg2
# The deployment's connect callable signs the prepared query's cube_security_context
# into the authentication payload (such as username or password token).
def connect_cube():
    return psycopg2.connect(
        host="cube-sql.internal",
        port=15432,
        user="cube_user",
        password="cube_signed_token",
        dbname="cube",
    )

cube_adapter = DBAPIAdapter(connect_cube, kind="cube")

# 4. In-process DuckDB fallback (fresh owned connection per acquired query)
def connect_duckdb():
    return connect_hardened(
        "/data/serving/analytics.duckdb",
        read_only=True,
        allowed_directories=["/data/serving/"],
        memory_limit="4GB",
        threads=4,
    )

duck_adapter = DuckDBAdapter(connect_duckdb)

# 5. Assemble Serving Application
adapters = {
    "sr": sr_adapter,
    "lb": lb_adapter,
    "cube": cube_adapter,
    "duck": duck_adapter,
}

app = create_app(
    pipeline=serving_pipeline,
    adapters=adapters,
    timeout_seconds=30.0,
    max_body_bytes=1_000_000,
)
```

### 3.1 Embedded DuckDB Lifecycle, Bounded Concurrency, Queueing & Symlink Swaps

- **Shared Hardened Instance & Owned Per-Query Cursor:** In production (`DuckDBAdapter(config=conn_cfg)`), a process-wide thread-safe `DuckDBHolder` maintains a single hardened database instance per resolved database file (`connect_hardened` + conformance `probe`). Each query acquires a bounded concurrency slot (`max_concurrent_cursors`, default `32`) and its own isolated `cursor()`, executes the query, stops and joins any started watchdog timer, and deterministically closes the cursor and releases the slot. When constructed with a custom factory (`DuckDBAdapter(connect)`), each acquired slot calls `connect()` once and closes the returned connection/cursor before releasing the slot.
  - Connection-factory / holder errors map to secret-safe `EngineError` (`502 engine_error`), and sole connection/cursor-close failures raise secret-safe `EngineError("Engine connection cleanup failed")` (`502 engine_error`) after releasing the slot, without exposing file paths, DSNs, SQL, parameters, or driver internals.
  - *In-memory test fixtures:* Do not pass `lambda: parent` when retaining an in-memory parent connection across queries, as the adapter closes each returned object. Instead, pass `lambda: parent.cursor()` so each query owns and closes a child cursor while the caller-owned `parent` fixture stays inspectable until the caller explicitly calls `parent.close()`.
- **Pre-Wait Cancellation & Shared Monotonic Deadline:**
  - `DuckDBAdapter.execute()` registers its per-request cancellation handle via `on_cancel_handle` *before* waiting for a cursor slot.
  - A single monotonic deadline (`timeout_seconds`) covers both queue wait and SQL execution; the timeout budget is **not** reset after acquiring a slot, and the execution watchdog timer receives only the remaining budget after the cursor is acquired.
  - If a queued request is cancelled or its deadline expires while waiting for a slot, it raises `EngineCancelled` (`499 query_cancelled`) or `EngineTimeout` (`504 engine_timeout`) without touching DuckDB or executing SQL. If cursor acquisition finishes after the deadline, the acquired cursor is closed immediately without executing SQL.
- **Zero-Downtime Symlink Swaps:** `DuckDBHolder` checks the configured path's resolved target and `(st_dev, st_ino)` on each query. When an atomic symlink swap points to a new database file, incoming queries open, harden, and probe the new generation while in-flight queries finish on the old generation; the superseded generation closes once drained (or via the idle reaper after `max_idle_seconds` 300s).

---

## 4. Production Deployment with mTLS

To ensure strict network-level isolation so that **only the Atlas Data Catalog** can communicate with Seeknal Serving, deploy the Starlette application with `uvicorn` configured for mutual TLS (mTLS):

```bash
uvicorn my_serving_app:app \
  --host 0.0.0.0 \
  --port 8443 \
  --ssl-keyfile /etc/serving/certs/server.key \
  --ssl-certfile /etc/serving/certs/server.crt \
  --ssl-ca-certs /etc/serving/certs/catalog-ca.crt \
  --ssl-cert-reqs 2
```

### Parameter Breakdown
- `--ssl-certfile`: The server's public X.509 TLS certificate.
- `--ssl-keyfile`: The server's private key.
- `--ssl-ca-certs`: The trusted internal Certificate Authority (CA) that signs client certificates for the Atlas Data Catalog.
- `--ssl-cert-reqs 2`: Configures `ssl.CERT_REQUIRED`, enforcing that any inbound TCP connection must present a valid client certificate signed by `catalog-ca.crt`. Connections without valid client certificates are dropped immediately at the TLS handshake level.
