# Seeknal Serving Operations: Health, Readiness, Build Info, and Prometheus Metrics

Seeknal Serving separates tenant-facing query traffic from operational telemetry and cluster probes. By default, `/metrics` and `/buildinfo` are never exposed on the public query port; instead, an isolated operations listener binds to `127.0.0.1:9090` (or an internal cluster management interface) with optional bearer-token authentication.

---

## Listener Separation and Defaults

| Listener | Default Bind | Default Endpoints | Purpose |
| :--- | :--- | :--- | :--- |
| **Public Serving App** | `0.0.0.0:8765` | `POST /v1/query`, `POST /v1/explain`, `POST /v1/cancel`, `GET /healthz`, `GET /readyz` | Policy-enforced query execution for grant-bearing callers and load-balancer health checks |
| **Operations App (`ops`)** | `127.0.0.1:9090` | `GET /healthz`, `GET /readyz`, `GET /metrics`, `GET /buildinfo` | Prometheus scraping, deep readiness inspection, and build provenance |

- When the `ops:` section is omitted (or `null`) in `serving.yaml`, `ServerConfig.ops` is `None` (the operations listener is off, `/readyz` remains enabled on the public port, and `/metrics` and `/buildinfo` remain disabled on the public port).
- When an `ops:` section is present in `serving.yaml`, `enabled` defaults to `True` and `host` defaults to `127.0.0.1`.

---

## Configuration (`ops:`)

```yaml
ops:
  enabled: true
  host: "127.0.0.1"
  port: 9090
  auth_token_env: "SEEKNAL_OPS_BEARER_TOKEN"
  readyz_timeout_seconds: 2.0
  readyz_cache_seconds: 5.0
  public_metrics: false
  public_buildinfo: false
  public_readyz: true
```

### Configuration Fields

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `enabled` | `bool` | `true` when `ops:` is present (`false` when absent) | Enables the dedicated operations listener. |
| `host` | `str` | `"127.0.0.1"` | Bind address for the operations listener. Keep bound to loopback or a cluster-internal interface. |
| `port` | `int` | `9090` | TCP port (`1..65535`) for the operations listener. |
| `auth_token_env` | `str \| null` | `null` | Name of an environment variable (`^[A-Za-z_][A-Za-z0-9_]{0,127}$`) holding the required bearer token for ops endpoints. |
| `readyz_timeout_seconds` | `float` | `2.0` | Positive per-engine wall-clock timeout in seconds for `/readyz` health probes. |
| `readyz_cache_seconds` | `float` | `5.0` | Non-negative TTL in seconds for caching per-engine `/readyz` probe outcomes so frequent probes cannot overload downstream engines. |
| `public_metrics` | `bool` | `false` | Whether to expose `GET /metrics` on the public query listener. Disabled by default. |
| `public_buildinfo` | `bool` | `false` | Whether to expose `GET /buildinfo` on the public query listener. Disabled by default. |
| `public_readyz` | `bool` | `true` | Whether to expose `GET /readyz` on the public query listener for ingress/load-balancer readiness checks. |

### Secret Hygiene and Startup Guarantees

- Unknown keys in `ops:` are rejected at startup.
- Literal secret keys (`token`, `auth_token`, `bearer_token`, `secret`, `password`, `api_key`) are strictly rejected; always configure `auth_token_env` with an environment variable name.
- When `auth_token_env` is configured, every endpoint on the operations listener requires `Authorization: Bearer <token>`, verified in constant time via `hmac.compare_digest`. Missing, malformed, or mismatched tokens return HTTP `401` (`{"error": "unauthorized", ...}`).
- Binding `ops.host` to a non-loopback address (anything other than `127.0.0.1`, `::1`, or `localhost`) without `ops.auth_token_env` configured emits a startup `WARNING` log.
- `start_ops_server` pre-binds and verifies the operations listener socket and waits until it is actively serving before main query traffic starts; bind conflicts (e.g. `EADDRINUSE`) or startup failures raise `RuntimeError` immediately rather than silently running without operational endpoints.

---

## Operational Endpoints

### `GET /healthz` — Process Liveness

Lightweight liveness probe confirming that the ASGI event loop is responsive without touching downstream databases.

```json
{"status": "ok"}
```

### `GET /readyz` — Catalog and Engine Readiness

Verifies both catalog availability and downstream engine reachability out-of-band without competing with tenant query concurrency limits:

1. **Catalog check:** Ready when static placement is loaded or when live discovery has completed at least one refresh (`refresher.last_success_at is not None`).
2. **Out-of-band engine probes with single-flight guard:** Probes each configured engine within `readyz_timeout_seconds` and caches each engine's status for `readyz_cache_seconds`:
   - **DuckDB (`DuckDBAdapter`):** Opens a short-lived probe cursor directly and executes `SELECT 1` under bounded timeout without acquiring `_slot_semaphore` or touching `in_flight_cursors` / `queue_depth`, so saturated pods remain `Ready` under query load.
   - **Lakebase / StarRocks / DB-API (`DBAPIAdapter`):** Opens a dedicated short-lived probe connection directly with `connect_timeout` (never acquiring from `LakebasePool`), executes `SELECT 1` on a cursor, and closes both cursor and connection immediately so exhausted query pools do not cause cascading `NotReady` transitions.
   - **Cube (`CubeAdapter`):** Connects with a short-lived probe security context and verifies SQL API reachability.
   - **Workload-routed engines (`WorkloadRoutedAdapter`):** Probes the `"interactive"` workload adapter (or the first configured account adapter).
   - **Single-flight timeout protection:** At most one probe worker thread may be in flight per engine at any time. If a probe hangs past `readyz_timeout_seconds`, subsequent `/readyz` checks never spawn additional worker threads for that engine while the hung thread is still running; they return a fresh cached status if available or `"unavailable"` immediately until the in-flight probe exits.

#### Public Listener vs. Dedicated Operations Listener Payloads

Both listeners execute the full catalog and engine reachability checks (returning HTTP `200` when ready and HTTP `503` when not ready), but **public `/readyz` omits engine names** (`include_engines=False`) so unauthenticated ingress callers cannot enumerate configured backend engines:

- **Public port (`GET /readyz`):**
  - HTTP `200`: `{"ready": true}`
  - HTTP `503` (catalog not yet loaded): `{"ready": false, "reason": "catalog_not_loaded"}`
  - HTTP `503` (engine unavailable): `{"ready": false}`
- **Operations listener (`GET /readyz`):**
  - **HTTP 200 (all engines healthy and catalog loaded):**

```json
{
  "ready": true,
  "engines": {
    "duck": "ok",
    "lakebase": "ok"
  }
}
```

  - **HTTP 503 (discovery catalog not yet loaded):**

```json
{
  "ready": false,
  "reason": "catalog_not_loaded",
  "engines": {
    "duck": "ok",
    "lakebase": "ok"
  }
}
```

  - **HTTP 503 (one or more engines unreachable or timed out):**

```json
{
  "ready": false,
  "engines": {
    "duck": "ok",
    "lakebase": "unavailable"
  }
}
```

`/readyz` responses never contain exception messages, database DSNs, credentials, or internal hostnames.

### `GET /buildinfo` — Build Provenance

Returns runtime and artifact build metadata as JSON:

```json
{
  "version": "2.14.0",
  "git_sha": "abc1234",
  "build_time": "2026-01-15T00:00:00Z",
  "duckdb_version": "1.4.4",
  "lock_sha256": "..."
}
```

### `GET /metrics` — Prometheus Text Exposition (`0.0.4`)

Emits `text/plain; version=0.0.4; charset=utf-8` using standard library formatting with zero additional runtime dependencies.

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `seeknal_serving_requests_total` | `counter` | `route`, `engine`, `status` | Total HTTP requests handled by route, engine, and 3-digit HTTP status. |
| `seeknal_serving_request_duration_seconds` | `histogram` | `route`, `engine`, `le` | Request latency distribution with buckets `(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)` plus `+Inf`, `_sum`, and `_count`. |
| `seeknal_serving_errors_total` | `counter` | `route`, `engine`, `code` | Total errors categorized by bounded typed error code. |
| `seeknal_serving_duckdb_queue_depth` | `gauge` | `engine` | Number of requests waiting to acquire a DuckDB cursor slot. |
| `seeknal_serving_duckdb_in_flight_cursors` | `gauge` | `engine` | Number of currently active DuckDB cursor slots. |
| `seeknal_serving_duckdb_generation` | `gauge` | `engine` | Current DuckDB holder instance generation (read non-destructively without opening a connection). |
| `seeknal_serving_lakebase_pool_in_use` | `gauge` | `engine` | Borrowed connections currently in use in the Lakebase pool. |
| `seeknal_serving_lakebase_pool_idle` | `gauge` | `engine` | Available idle connections in the Lakebase pool. |
| `seeknal_serving_grants_rejected_total` | `counter` | `reason` | Total rejected grant/cancel tokens by bounded reason (`expired`, `invalid_signature`, `malformed`, `replayed`, `untrusted_kid`, `invalid_claims`, `lifetime_exceeded`, `other`). |
| `seeknal_serving_audit_write_failures_total` | `counter` | *(none)* | Total audit log write failures. |

---

## Cardinality and Tenant-Data Isolation Guarantees

`MetricsRegistry` enforces strict allowlists on every label before recording:

- **`route`:** Must be one of `("/v1/query", "/v1/explain", "/v1/cancel", "/healthz", "/readyz", "/metrics", "/buildinfo")`; anything else maps to `"other"`.
- **`engine`:** Must match a configured safe engine identifier; unknown or unrouted requests map to `"none"`.
- **`status`:** Must be an HTTP status code in `100..599`; invalid values map to `"unknown"`.
- **`code`:** Must belong to the bounded typed error code allowlist; unmapped values map to `"internal_error"`.
- **`reason`:** Must belong to `("expired", "invalid_signature", "malformed", "replayed", "untrusted_kid", "invalid_claims", "lifetime_exceeded", "other")`; unmapped values map to `"other"`.

Subjects (`sub`), JWT strings, table names, SQL text, query literals, and arbitrary user inputs can never enter metric names or labels.

---

## Kubernetes Probe and Scrape Example

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8765
  periodSeconds: 10
  timeoutSeconds: 2
readinessProbe:
  httpGet:
    path: /readyz
    port: 8765
  periodSeconds: 5
  timeoutSeconds: 3
```

When scraping the dedicated operations port with bearer authentication enabled:

```yaml
scrape_configs:
  - job_name: seeknal-serving
    metrics_path: /metrics
    authorization:
      type: Bearer
      credentials_file: /var/run/secrets/seeknal/ops-token
    static_configs:
      - targets: ["127.0.0.1:9090"]
```
