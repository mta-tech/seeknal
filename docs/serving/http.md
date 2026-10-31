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

#### Request Body
JSON object with the following fields:
- `sql` (string, required): The client SQL statement using standard `$1..$n` parameter placeholders.
- `params` (list, optional): Positional parameter values corresponding to `$1..$n`.
- `hint` (string, optional): Routing override hint (e.g., target engine name).
- `max_staleness_seconds` (number, optional): Maximum allowable replication lag in seconds.

*Note: Any unrecognized fields in the JSON body will trigger a `400 Bad Request`.*

#### Example Request
```http
POST /v1/query HTTP/1.1
Host: serving.internal.atlas.net:8000
Authorization: Bearer eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImNhdGFsb2ctMjAyNiJ9...
Content-Type: application/json

{
  "sql": "SELECT id, name, salary FROM users WHERE salary >= $1",
  "params": [60000]
}
```

#### Response (200 OK)
Returns a JSON object with:
- `columns` (`list[str]`): Output column names in projection order.
- `rows` (`list[list]`): Query result rows converted to JSON-safe representations:
  - `datetime`, `date`, `time`: ISO 8601 formatted strings (e.g., `"2026-10-31T12:00:00"`).
  - `Decimal`: Exact numeric string representation (e.g., `"123.45"`).
  - `bytes`: Base64 encoded string.
  - `float` `NaN`, `+Infinity`, `-Infinity`: Encoded as `null`.
- `truncated` (`bool`): `true` if results exceeded the grant's `max_rows` cap.
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
  "rows": [
    [2, "****", 80000]
  ],
  "truncated": false,
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

## 2. Error Handling & Privacy Guarantees

All error responses have an HTTP status code $\ge 400$ and return a consistent JSON payload:

```json
{
  "error": "<category>",
  "message": "<short safe diagnostic text>"
}
```

### Privacy & Leakage Invariants
- **No Grant Tokens:** Raw grant tokens and signature data are never included in error messages.
- **No SQL Literals or Parameters:** Client SQL expressions, literals, and supplied parameter values are scrubbed and redacted from error text.
- **No Driver Internals:** Database driver connection strings, internal stack traces, and data fragments are logged server-side and never returned to the HTTP client.

### Error Status Mapping

| Condition | Status | Error Category | Description |
|:---|:---:|:---|:---|
| `GrantTokenError` | **401** | `grant_token_error` | Missing/malformed Authorization header, invalid signature, expired token, or invalid claims. |
| `EnforcementError` | **403** | `enforcement_error` | Query policy violation: ungranted table, ungranted column, disallowed SQL function, or write violation. |
| `RoutingError` | **422** | `routing_error` | No feasible engine available to satisfy query constraints (e.g. invalid hint, unserved table). |
| Malformed JSON or invalid types | **400** | `bad_request` | Non-JSON body, missing `sql`, invalid field types, or unrecognized extra fields. |
| Request body exceeds size limit | **413** | `payload_too_large` | Body exceeds `max_body_bytes` (checked prior to JSON parsing). |
| No adapter for chosen engine | **503** | `engine_unavailable` | Placement routed to an engine that is not registered in the application's adapters mapping. |
| `EngineTimeout` | **504** | `engine_timeout` | Query execution exceeded the allotted timeout threshold. |
| `EngineError` | **502** | `engine_error` | Database engine driver failed during connection or query execution. |
| Unhandled server exception | **500** | `internal_error` | Generic server-side fault. |

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

# 4. In-process DuckDB fallback
duck_conn = duckdb.connect()
duck_adapter = DuckDBAdapter(lambda: duck_conn)

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
