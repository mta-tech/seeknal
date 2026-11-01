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
      token_env: LAKEKEEPER_TOKEN
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
        token_env: CUBE_API_TOKEN
    lakebase:
      engine: lb
      synced_tables:
        lakekeeper.analytics.users:
          target: users_sync
          primary_key: [id]

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
Secret files specified by `password_file` (such as Kubernetes secret volumes):
- Are read at connection time, stripping exactly one trailing newline.
- Must not be group- or world-readable (`0o044` permissions raise `ServerConfigError`).

### Summary of Environment Variables
| Variable Name | Purpose | Configuration Location |
|:---|:---|:---|
| `SEEKNAL_DEMO_QUERY_PASSWORD` | StarRocks password for interactive queries | `connections.sr.accounts.interactive.password_env` |
| `ATLAS_BI_PASSWORD` | StarRocks password for BI workloads | `connections.sr.accounts.bi.password_env` |
| `LAKEBASE_DSN` | PostgreSQL connection string for Lakebase | `connections.lb.dsn_env` |
| `CUBE_SQL_SECRET` | Shared HMAC secret for Cube JWT context (mode `jwt`) | `connections.cube.secret_env` |
| `LAKEKEEPER_TOKEN` | Bearer token for Iceberg REST catalog discovery | `placement.discovery.lakekeeper.token_env` |
| `CUBE_API_TOKEN` | Bearer token for Cube REST meta discovery | `placement.discovery.engines.cube.token_env` |

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

### Pre-flight Configuration Check (`--check`)
Before launching the service, validate the configuration, grant keys, and environment variables:

```bash
python -m seeknal.serving --config /etc/seeknal/serving.yaml --check
```
- Exits with code `0` and outputs `config ok` if valid.
- Exits with code `2` and outputs a safe diagnostic error message (without stack traces or secret leakage) if invalid.
- Performs no network I/O or database connections.

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
