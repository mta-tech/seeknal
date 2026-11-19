# Seeknal Serving Durable Audit Logging

Seeknal Serving supports optional durable, structured JSON-lines audit logging across `/v1/query`, `/v1/select`, `/v1/explain`, and `/v1/cancel` (including rejected requests).

When the `audit:` section is **absent** from `serving.yaml`, Seeknal Serving behaves identically to existing deployments—no new configuration is required.

---

## 1. Configuration (`serving.yaml`)

Add an optional top-level `audit:` mapping to `serving.yaml`. Unknown keys in `audit:` are rejected at startup with a configuration error.

### 1.1 File Sink (Default)

```yaml
audit:
  sink: file                       # "file" (default), "stdout", or "syslog"
  on_failure: refuse               # "refuse" (default) or "log"
  path: /var/log/seeknal/audit.jsonl
  mode: "0600"                     # "0600" (default) or "0640"
  max_bytes: 104857600             # optional size-based rotation (100 MiB); alias: rotate_size_bytes
  rotate_interval_seconds: 3600    # optional time-based rotation (1 hour); alias: rotate_seconds
  max_files: 14                    # optional rotated file count cap; alias: retention_count
  max_age_seconds: 604800          # optional rotated file age cap (7 days); alias: retention_age_seconds
```

### 1.2 Stdout Sink

```yaml
audit:
  sink: stdout
  on_failure: refuse
```

### 1.3 Syslog Sink (RFC 5424)

```yaml
audit:
  sink: syslog
  on_failure: refuse
  host: 127.0.0.1                  # default "127.0.0.1"
  port: 514                        # 1..65535 (default 514)
  transport: tcp                   # "udp" (default) or "tcp"; alias: protocol
  facility: 16                     # 0..23 (default 16 / local0)
  app_name: seeknal-serving        # 1..48 printable ASCII chars without spaces
  timeout_seconds: 2.0             # positive float (default 2.0)
```

### 1.4 Configuration Reference

| Key | Alias | Type | Default | Description |
| --- | --- | --- | --- | --- |
| `sink` | — | `"file" \| "stdout" \| "syslog"` | `"file"` | Destination sink type. |
| `on_failure` | — | `"refuse" \| "log"` | `"refuse"` | Behavior when startup verification or runtime audit writes fail. |
| `path` | — | `str` | *required for `file`* | Target JSON-lines audit log path. Parent directory must exist and be writable. |
| `mode` | — | `"0600" \| "0640" \| 0o600 \| 0o640` | `0o600` | Strict POSIX file mode. Group-writable, world-readable, or world-writable modes are rejected. |
| `max_bytes` | `rotate_size_bytes` | `int > 0` | `null` | Rotate active file before a write would cause size to exceed `max_bytes`. |
| `rotate_interval_seconds` | `rotate_seconds` | `float > 0` | `null` | Rotate non-empty active file once open duration reaches `rotate_interval_seconds`. |
| `max_files` | `retention_count` | `int > 0` | `null` | Maximum number of rotated archive files retained alongside the active file. |
| `max_age_seconds` | `retention_age_seconds` | `float > 0` | `null` | Prune rotated archive files older than `max_age_seconds`. |
| `host` | — | `str` | `"127.0.0.1"` | Syslog collector hostname or IP address. |
| `port` | — | `int` (`1..65535`) | `514` | Syslog collector port. |
| `transport` | `protocol` | `"udp" \| "tcp"` | `"udp"` | Syslog transport protocol. |
| `facility` | — | `int` (`0..23`) | `16` | RFC 5424 facility code (`16` = `local0`). |
| `app_name` | — | `str` | `"seeknal-serving"` | RFC 5424 `APP-NAME` header token. |
| `timeout_seconds` | — | `float > 0` | `2.0` | Total per-write audit deadline in seconds across all sinks (and socket connect/send timeout for syslog). |

Specifying both a primary option and its alias simultaneously (for example, both `max_bytes` and `rotate_size_bytes`) is rejected as a configuration error.

---

## 2. Audit Record Schema & Strict Allowlist

One single-line JSON object is emitted per request to `/v1/query`, `/v1/select`, `/v1/explain`, and `/v1/cancel` (both successful and rejected requests).

### 2.1 Allowlisted Fields

Only the following 32 fields may ever appear in a serialized audit record (unset fields default to `null`):

| Field | Type | Description |
| --- | --- | --- |
| `timestamp` | `str` | UTC ISO-8601 timestamp ending in `Z` (e.g. `"2026-11-19T09:00:00Z"`). |
| `audit_id` | `str \| null` | Grant/cancel token `jti` claim, or `null` if unavailable. |
| `subject` | `str \| null` | Grant/cancel token `sub` claim, or `null` if unavailable. |
| `act_sub` | `str \| null` | Actor subject from `act.sub` claim, or `null`. |
| `act_typ` | `str \| null` | Actor type from `act.typ` (`"ask_session"` or `"run"`), or `null`. |
| `azp` | `str \| null` | Authorized party (`azp`) claim, or `null`. |
| `workload` | `str \| null` | Workload claim (`"interactive"`, `"bi"`, `"report"`, `"ask"`, `"job"`), or `null`. |
| `origin` | `str \| null` | Request origin claim (`"superset"`, `"ask"`, `"job"`, `"portal"`), or `null`. |
| `route` | `str` | Endpoint path: `"/v1/query"`, `"/v1/select"`, `"/v1/explain"`, or `"/v1/cancel"`. |
| `api` | `str \| null` | API contract identifier (e.g. `"select.v1"` for `POST /v1/select`), or `null`. |
| `engine` | `str \| null` | Routed execution engine name, or `null` when unrouted/cancelled. |
| `engine_kind` | `str \| null` | Configured engine kind (`"postgres"`, `"lakebase"`, `"duckdb"`, `"starrocks"`, `"cube"`), or `null`. |
| `storage_class` | `str \| null` | Effective storage class (`"local"`, `"object_storage"`, `"unknown"`), or `null`. |
| `account` | `str \| null` | Workload credential account used on the target source, or `null`. |
| `lakebase_endpoint` | `dict[str, str] \| str \| null` | Lakebase endpoint identifiers (`project_id`, `branch_id`, `endpoint_id`), or `null`. |
| `cold_start` | `bool \| number \| dict \| null` | Lakebase cold-start/wake indicator or timing metadata, or `null`. |
| `rule` | `str \| null` | Placement routing rule name, or `null` when unrouted/cancelled. |
| `access_pattern` | `str \| null` | Effective table access pattern (`"point"`, `"filter"`, `"scan"`), or `null`. |
| `guard` | `str \| null` | Guardrail evaluation mode or outcome (`"enforce"`, `"warn"`, or rejection code), or `null`. |
| `filter_columns` | `list[str] \| null` | Scrubbed sargable filter column names only (never predicate values), or `null`. |
| `effective_max_rows` | `int \| null` | Effective row cap applied across grant, request, and table `access.max_rows`, or `null`. |
| `paged` | `bool \| null` | Whether cursor pagination (`after` / `next`) was used on `/v1/select`, or `null`. |
| `tables` | `list[str]` | Logical table or cube names referenced by the request (defaults to `[]`). |
| `status` | `int` | HTTP response status code (`100..599`). |
| `code` | `str \| null` | Typed error code when `status >= 400` (e.g. `"grant_token_error"`, `"filter_required"`), else `null`. |
| `elapsed_ms` | `float` | Request or execution duration in milliseconds rounded to 2 decimal places. |
| `row_count` | `int \| null` | Returned row count on `200 /v1/query` or `/v1/select`, or `null`. |
| `truncated` | `bool \| null` | Whether query output hit the effective row cap, or `null`. |
| `publication_id` | `str \| null` | Catalog binding `publication_id` claim, or `null`. |
| `revision` | `int \| str \| null` | Catalog binding `revision` claim, or `null`. |
| `digest` | `str \| null` | Catalog binding `digest` claim, or `null`. |
| `sql_sha256` | `str \| null` | Lowercase 64-character hex SHA-256 of UTF-8 `sql` (or exact `/v1/select` body bytes), else `null`. |

### 2.2 Example Record

```json
{"timestamp":"2026-11-19T09:00:00Z","audit_id":"019344a0-88b1-7000-8123-abcdef012345","subject":"user:analyst","act_sub":"session-42","act_typ":"ask_session","azp":"atlas-ask","workload":"ask","origin":"ask","route":"/v1/query","engine":"duck","rule":"analytics","tables":["orders","customers"],"status":200,"code":null,"elapsed_ms":14.28,"row_count":25,"truncated":false,"publication_id":"pub-sales-v1","revision":7,"digest":"sha256:9f86d081884c7d65","sql_sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
```

### 2.3 Redaction & Never-Logged Material

Audit serialization enforces a **strict schema allowlist** plus value-level secret scrubbing:

- **Never logged:** Raw SQL statements, redacted SQL templates, SQL string/numeric literals, positional query `params`, result `rows` or column values, compact JWS grant/cancel tokens, `Authorization` headers, engine DSNs/connection URIs, passwords, or API secrets.
- **SQL correlation without leakage:** Only `sql_sha256` (`SHA-256(sql.encode("utf-8", errors="replace"))`) is recorded, enabling correlation against Catalog grant issuance logs without exposing query predicates or literals and without failing on lone UTF-16 surrogates.
- **Table & surrogate sanitization:** Blank/whitespace-only table names are dropped, lone UTF-16 surrogates (`U+D800..U+DFFF`) in string fields and table names are replaced with `\ufffd`, table names are truncated to 256 characters, deduplicated in order, and capped at 64 entries. `build_minimal_audit_record()` provides a minimal fallback record **only** if full record construction or JSON serialization (`ValueError`, `UnicodeError`, `TypeError`) fails (logging an `ERROR` with scrubbed `detail` and `audit_id`); write or I/O errors from the audit sink never fall back to a minimal record.
- **Defense in depth:** Any non-allowlisted keys passed to `build_audit_record()` or `serialize_audit_record()` are stripped before serialization, and any accidental Bearer token, compact JWT (`eyJ...`), or database DSN/credential URI in string fields is scrubbed to `[REDACTED]`.

---

## 3. Sinks, Rotation & Retention

### 3.1 `FileAuditSink` (`sink: file`)

- **Symlink, hardlink, UID, `fchmod` & cached-fd re-verification:** Never follows symlinks (`O_NOFOLLOW` + `os.lstat`) and never calls path-based `os.chmod`. Rejects paths that are symlinks, non-regular files, hard-linked (`st_nlink != 1`), or not owned by `os.getuid()` during offline validation (`verify_audit_config_offline`), during `os.open` (`os.fstat`), and **before every write** by comparing `os.lstat(path)` against `os.fstat(fd)`. If the active path was unlinked or replaced, the stale descriptor is closed and safely reopened (or fails closed with `AuditWriteError` / `503 audit_unavailable` in `refuse` mode if reopening is unsafe or impossible). Sets permissions (`0600` or `0640`) exclusively via `os.fchmod(fd, mode)` on the verified file descriptor, independent of process `umask`.
- **Partial-write & `fsync` crash safety:** Closes the file descriptor on any `os.write` or `os.fsync` failure and never appends a duplicate minimal record once bytes may have been persisted. On `(re)open`, if the existing file is non-empty and does not end with `\n`, appends a healing `\n` (and `os.fsync` when enabled) before writing the next JSON line.
- **Unbounded growth warning:** When `max_bytes`, `max_files`, and `max_age_seconds` are all unset (`null`), logs a startup `WARNING` indicating that the audit log may grow unbounded.
- Flushes writes with `os.fsync` and serializes concurrent writes within a process using a `threading.Lock()`.
- **Rotation:** Before writing a new record to a non-empty active file, verifies `os.lstat` on both the active path and the target archive path, applies `os.fchmod(fd, mode)` before closing the descriptor, and atomically renames `audit.jsonl` to `audit.jsonl.<UTC_TIMESTAMP>.<SEQ>` via `os.replace` if:
  - `max_bytes` is set and `current_size + record_bytes > max_bytes`, or
  - `rotate_interval_seconds` is set and `now - window_opened_at >= rotate_interval_seconds`.
  Rotated archive files retain the configured `0600` or `0640` permissions.
- **Retention:** Prunes rotated archive files older than `max_age_seconds` and deletes the oldest remaining archive files when rotated file count exceeds `max_files`.

### 3.2 `StdoutAuditSink` (`sink: stdout`)

- Writes one UTF-8 JSON line (`\n`-terminated) per record to `sys.stdout` under a `threading.Lock()` and flushes immediately. Ideal for containerized deployments with log shipping sidecars.

### 3.3 `SyslogAuditSink` (`sink: syslog`)

- Formats each audit record as an RFC 5424 message:
  ```text
  <PRI>1 TIMESTAMP HOSTNAME APP-NAME PROCID audit - JSON
  ```
  where `PRI = facility * 8 + 6` (Informational severity `6`).
- **TCP (`transport: tcp`):** Frames each message using RFC 6587 octet-counting (`"<byte_len> <rfc5424_bytes>"`), detects peer half-close, and reconnects once if an idle connection was closed.
- **UDP (`transport: udp`):** Sends one UDP datagram per RFC 5424 message.

---

## 4. Failure Semantics (`refuse` vs `log`)

Audit records are always emitted on a **dedicated bounded audit worker pool** (`seeknal-audit-io-*`, isolated from the shared AnyIO threadpool used by `pipeline.prepare` and engine execution) **before** the HTTP response is returned or side effects (such as `/v1/cancel` query cancellation) are committed:

- **Dedicated bounded audit I/O, deadlines, fail-fast & `/readyz` integration:** Each audit write enforces a total monotonic deadline (`timeout_seconds`, default `2.0s`). If the sink stalls or goes down, in-flight query preparation and `/healthz` remain unstarved, subsequent audit writes fail fast without piling up threads, and `GET /readyz` reports HTTP `503` with `{"ready": false, "reason": "audit_sink", ...}` while a `refuse`-mode sink is down or stalled. SQL table extraction runs only after token signature/issuer/audience verification (`claims is not None`), skips statements over 64 KiB, and stops at 64 unique tables.
- **Pre-action `/v1/cancel` auditing:** `/v1/cancel` emits the `202` audit record before cancelling the in-flight query; if audit emission fails under `on_failure: refuse`, HTTP `503` `audit_unavailable` is returned and the query is not cancelled.
- **Rejected method & disconnect coverage:** Disallowed HTTP methods (`405 method_not_allowed`, `code: "invalid_sql"`) and client disconnects during request body reads (`499 query_cancelled`) on `/v1/query`, `/v1/explain`, and `/v1/cancel` return typed JSON payloads and emit audit records.

| Phase | `on_failure: refuse` (Default) | `on_failure: log` |
| --- | --- | --- |
| **Startup (`build_server` / `--check`)** | Unwritable file path or unreachable TCP syslog destination raises `AuditStartupError` (`ServerConfigError` & `AuditWriteError`) with a secret-safe message and **refuses startup with exit code `2`**. | Logs an `ERROR` diagnostic (with scrubbed `detail=` and `audit_id=`), increments the failure callback if wired, and allows the server to start. |
| **Runtime (`create_app` / `emit_audit_record`)** | Logs `ERROR` with scrubbed `detail=` and `audit_id=`, returns HTTP `503` with typed error payload (`error: "audit_unavailable"`, `code: "audit_unavailable"`, `message`, `next_step`, and `audit_id` when known), and **returns NO query rows** (never weakening the record to a minimal fallback). | Logs loudly at `ERROR` (including scrubbed `detail=` and `audit_id=`), invokes `on_write_failure()` (incrementing `seeknal_serving_audit_write_failures_total`), and **allows the HTTP response to proceed**. |
