# Seeknal Serving: DuckDB Acceleration Cache Loader (`seeknal.serving.accel`)

`seeknal.serving.accel` materializes analytical tables from either **PostgreSQL (`LakebaseSource`)** or a **Lakekeeper Iceberg REST catalog (`IcebergSource`)** into a local, read-only DuckDB database file (`mart.duckdb`) consumed by Seeknal Serving's hardened `local_file` DuckDB profile.

---

## Overview & Lifecycle Guarantees

Each `python -m seeknal.serving.accel load` run executes an atomic **load -> verify -> swap -> freshness -> prune** sequence inside `--cache-dir`:

1. **Exclusive Process Lock:** Acquires a non-blocking `fcntl.flock` lock on `<cache_dir>/.loader.lock` for the duration of the run. Concurrent runs fail fast with exit code `3`.
2. **DuckDB Version Pin Check:** Refuses to run (exit code `2`) before touching disk if the runtime `duckdb.__version__` does not match the expected Serving DuckDB version (`2.0.0` by default).
3. **Isolated Staging File:** Writes tables into a new versioned file `mart-<YYYYMMDDTHHMMSSZ>-<8hex>.duckdb` and runs `CHECKPOINT`.
4. **PII Value Exclusion:** Applies `--pii-mode null` (default; casts hashed PII columns to typed `NULL`) or `--pii-mode exclude` (omits PII columns from both `SELECT` and the target table schema). Raw PII values never enter the output `.duckdb` file in either mode.
5. **Snapshot-Consistent Row-Count Verification:** Verifies `SELECT COUNT(*)` in the newly materialized target table against the source table (pinned to the exact `AT (VERSION => <snapshot_id>)` when `snapshot_id` is configured). Any mismatch aborts the run, deletes the new file, and leaves the existing `mart.duckdb` symlink untouched.
6. **Atomic Symlink Swap:** Closes the target file, creates a temporary relative symlink `.mart.duckdb.tmp.<hex> -> mart-<ts>-<hex>.duckdb`, and atomically replaces `mart.duckdb` via `os.replace()`. Seeknal Serving's `DuckDBHolder` detects the `(st_dev, st_ino)` change on the next query, opens and hardens the new file, and drains in-flight cursors on the previous file without downtime.
7. **`freshness.json` Publication:** Atomically writes `freshness.json` **after** the symlink swap with row counts (`tables`), Iceberg snapshot IDs (`snapshots`), `file_name`, `as_of_date`, `loaded_at`, `loader_version`, and `duckdb_version`.
8. **Old File Pruning:** Retains the newest `--keep` (default `3`) cache files matching `mart-*.duckdb`, cleans up orphan `.wal` files from interrupted runs, and **never** unlinks the active `mart.duckdb` symlink target.
9. **Secret Scrubbing:** Credentials resolved from libpq env vars, Iceberg OAuth2/S3 env vars, or `secrets_env_file` are scrubbed from all JSON stderr log lines and error messages, and redacted as `'<redacted>'` in planned SQL diagnostics.

---

## When to use this instead of iceberg_direct

Seeknal Serving offers two ways to serve Iceberg tables with DuckDB:

1. **`profile: iceberg_direct` (in-process live Iceberg reads):** Serving attaches the Lakekeeper REST catalog directly inside an in-memory DuckDB generation and reads remote Parquet/manifest objects from S3 at query time.
2. **`seeknal.serving.accel` + `profile: local_file` (pre-materialized local DuckDB cache — the fallback path):** An out-of-band loader job materializes tables from Lakekeeper/S3 (or Postgres) into a local `mart.duckdb` file, and Serving opens `mart.duckdb` with `enable_external_access = false`, no extensions loaded, no network access, and no runtime credentials.

Use `seeknal.serving.accel` instead of `iceberg_direct` when any of the following apply:

- **Strict runtime air-gap / zero external access in Serving:** DuckDB's `iceberg` + `httpfs` extensions may require `external_access_after_attach: on` to fetch S3 manifest and Parquet files after `ATTACH` (decided by Stage 3 live gate S2). With `accel`, Serving runs `profile: local_file` where `enable_external_access = false`, `INSTALL`/`LOAD`/`ATTACH` are completely disabled, and Serving holds **zero** S3 or Lakekeeper OAuth2 credentials.
- **Predictable low-latency local NVMe scans:** Queries scan pre-checkpointed native DuckDB columnar storage on local disk/tmpfs without S3 network round-trips, cold Iceberg metadata/manifest parsing, or OAuth2/vended-credential token refresh latency.
- **Decoupled availability from Lakekeeper & S3:** Even if Lakekeeper, the identity provider, or object storage experiences transient outages, Serving continues answering queries from the last verified `mart.duckdb` snapshot; failed loader runs leave the active symlink intact.
- **Deterministic point-in-time snapshot bundles:** Pinning `snapshot_id` per table materializes an immutable, row-count-verified multi-table snapshot bundle recorded in `freshness.json`.

Prefer `iceberg_direct` when sub-minute Iceberg freshness is required without periodic batch materialization or when dataset volume exceeds local node storage budgets.

---

## DuckDB Version Rule (`2.0.0`)

**The DuckDB binary storage format written by `seeknal.serving.accel` MUST match the DuckDB version running inside Seeknal Serving (`2.0.0`).**

- By default, `seeknal.serving.accel` imports `EXPECTED_DUCKDB_VERSION` from `seeknal.serving.buildinfo` (falling back to `"2.0.0"`), guaranteeing a single source of truth between the loader and Serving.
- Before creating directories or database files, `check_duckdb_version()` verifies `duckdb.__version__ == expected_version` and exits with code `2` on mismatch.
- Every `freshness.json` records `"duckdb_version"` alongside `"file_name"` for operational auditing.
- Only pass `--allow-version-mismatch` during local development testing when storage format compatibility is explicitly known.

---

## Iceberg Source Hardening & Setup Order

When `--source-type iceberg` is selected, `IcebergSource` and the target DuckDB connection execute setup statements in strict order **before** any `ATTACH`:

1. `SET autoinstall_known_extensions = false`
2. `SET autoload_known_extensions = false`
3. `SET allow_persistent_secrets = false`
4. `SET extension_directory = '<extension_dir>'`
5. `SET custom_extension_repository = '/nonexistent_extension_repository'`
6. `LOAD httpfs` and `LOAD iceberg` (**never** `INSTALL`)
7. `CREATE TEMPORARY SECRET s3_<alias> (TYPE S3, ...)` and `CREATE TEMPORARY SECRET iceberg_<alias> (TYPE ICEBERG, ...)` using credentials resolved strictly from environment variables or a mode-`0600` `secrets_env_file`
8. `ATTACH '<warehouse>' AS <alias> (TYPE ICEBERG, ENDPOINT '...', SECRET iceberg_<alias>, ACCESS_DELEGATION_MODE '...', READ_ONLY)`

---

## Configuration Reference (`--source-config`)

`--source-config` accepts either YAML (`.yaml`/`.yml`) or JSON (`.json`). Literal secrets (`client_id`, `client_secret`, `key_id`, `secret`, `password`, `token`) and Postgres/attach keys are rejected at parse time.

```yaml
secrets_env_file: /etc/seeknal/secrets/iceberg-accel.env  # Optional; must be mode 0600 or stricter
extension_directory: /opt/duckdb/extensions               # Absolute normalized path with pre-staged httpfs & iceberg
extensions:                                               # Optional; subset of {httpfs, iceberg}
  - httpfs
  - iceberg
allowed_s3_prefixes:                                      # Optional S3 secret SCOPE confinement
  - s3://warehouse-bucket/analytics/

memory_limit: 2GB     # Optional (overrides ACCEL_MEMORY_LIMIT)
threads: 4            # Optional 1..256 (overrides ACCEL_THREADS)
pii_mode: exclude     # Optional: 'null' (default) or 'exclude' (overridden by CLI --pii-mode)
target_schema: rpt    # Optional target schema inside mart.duckdb (default: rpt)

default_catalog: lake        # Optional default catalog alias when using --columns/--registry
default_namespace: analytics # Optional default Iceberg namespace when using --columns/--registry

iceberg_catalogs:
  lake:  # Must match ^[a-z][a-z0-9_]{0,31}$ and not be system/temp/memory/main
    endpoint: https://lakekeeper.internal.example/catalog
    warehouse: analytics_wh
    token_uri: https://idp.internal.example/realms/main/protocol/openid-connect/token
    client_id_env: LAKEKEEPER_CLIENT_ID
    client_secret_env: LAKEKEEPER_CLIENT_SECRET
    scope: catalog:read         # Optional OAuth2 scope
    access_delegation: vended   # 'vended' (default -> vended-credentials) | 'none'
    s3:
      endpoint: s3.internal.example:9000
      region: ap-southeast-1
      url_style: path           # 'path' (default) | 'vhost'
      key_id_env: ICEBERG_S3_KEY_ID
      secret_env: ICEBERG_S3_SECRET
      use_ssl: true

pii_columns:
  customer_email:
    source_form: sha256
    mask: redact

tables:
  - view: daily_orders
    source: lake.analytics.daily_orders
    snapshot_id: 8877665544332211  # Optional positive int64 snapshot pin
    keys: [order_id]
    columns:
      - name: order_id
        type: bigint
      - name: order_date
        type: date
      - name: net_amount
        type: decimal(18,2)
      - name: customer_email
        type: varchar
        pii: customer_email
```

### Key Reference

| Key | Required | Description |
|:---|:---|:---|
| `secrets_env_file` | No | Path to `KEY=VALUE` file (mode `0600` or stricter) consulted before `os.environ`. |
| `extension_directory` | Yes* | Absolute, normalized path containing pre-staged `httpfs` and `iceberg` extensions (`*`can also be set via `--extension-dir` or `ACCEL_EXTENSION_DIR`). |
| `extensions` | No | Non-empty subset of `["httpfs", "iceberg"]` (defaults to `["httpfs", "iceberg"]`). |
| `allowed_s3_prefixes` | No | List of `s3://<bucket>/<prefix>/` URIs applied to `SCOPE` on `s3_<alias>` temporary secret. |
| `iceberg_catalogs.<alias>.endpoint` | Yes | `http://` or `https://` Lakekeeper REST catalog endpoint. |
| `iceberg_catalogs.<alias>.warehouse` | Yes | Non-empty Lakekeeper warehouse name. |
| `iceberg_catalogs.<alias>.token_uri` | Yes | `http://` or `https://` OAuth2 token endpoint. |
| `iceberg_catalogs.<alias>.client_id_env` | Yes | Environment variable name holding OAuth2 client ID. |
| `iceberg_catalogs.<alias>.client_secret_env` | Yes | Environment variable name holding OAuth2 client secret. |
| `iceberg_catalogs.<alias>.access_delegation` | No | `'vended'` (default, maps to `'vended-credentials'`) or `'none'`. |
| `iceberg_catalogs.<alias>.s3.*` | Yes | `endpoint`, `region`, `key_id_env`, `secret_env`, `url_style` (`'path'`/`'vhost'`), `use_ssl` (`bool`). |
| `pii_mode` | No | `'null'` (default; `CAST(NULL AS <type>) AS <col>`) or `'exclude'` (omits PII columns from `SELECT` and target table). |
| `tables[].snapshot_id` | No | Positive signed 64-bit integer (`1..9223372036854775807`). Appends `AT (VERSION => <snapshot_id>)` to both CTAS load and `SELECT COUNT(*)` verification, and records the pin under `snapshots` in `freshness.json`. |

---

## CLI Examples

### 1. Dry-Run Plan Inspection (Redacted Secrets)

```bash
python -m seeknal.serving.accel load \
  --source-type iceberg \
  --source-config /etc/seeknal/iceberg-accel.yaml \
  --cache-dir /data/serving \
  --pii-mode exclude \
  --dry-run
```

### 2. Materialize Iceberg Tables into `mart.duckdb`

```bash
python -m seeknal.serving.accel load \
  --source-type iceberg \
  --source-config /etc/seeknal/iceberg-accel.yaml \
  --cache-dir /data/serving \
  --pii-mode null \
  --keep 3 \
  --as-of-date 2026-11-19
```

### 3. Materialize Iceberg Tables Using External `columns.json` / `pii_columns.json` / `registry.json`

```bash
python -m seeknal.serving.accel load \
  --source-type iceberg \
  --source-config /etc/seeknal/iceberg-accel.yaml \
  --columns /etc/seeknal/columns.json \
  --pii /etc/seeknal/pii_columns.json \
  --registry /etc/seeknal/registry.json \
  --cache-dir /data/serving \
  --pii-mode exclude
```

### 4. Materialize PostgreSQL (`LakebaseSource`) Tables (Default `--source-type postgres`)

```bash
PGHOST=lakebase.internal.example PGDATABASE=analytics PGUSER=reader PGPASSWORD=... \
python -m seeknal.serving.accel load \
  --columns /etc/seeknal/columns.json \
  --pii /etc/seeknal/pii_columns.json \
  --registry /etc/seeknal/registry.json \
  --cache-dir /data/serving \
  --extension-dir /opt/duckdb/extensions
```

### Exit Codes

- `0`: Success (or `--dry-run` plan printed).
- `2`: Invalid CLI arguments, invalid configuration, missing env var/file, or DuckDB version mismatch (nothing on disk modified).
- `3`: Runtime load, row-count verification, or lock contention failure (any staged `.duckdb` file is cleaned up; active `mart.duckdb` symlink remains intact).

---

## `freshness.json` Format

After atomic symlink replacement of `/data/serving/mart.duckdb`, `accel` writes `/data/serving/freshness.json`:

```json
{
  "file_name": "mart-20261119T093000Z-a1b2c3d4.duckdb",
  "tables": {
    "rpt.daily_orders": 125400
  },
  "snapshots": {
    "rpt.daily_orders": 8877665544332211
  },
  "as_of_date": "2026-11-19",
  "loaded_at": "2026-11-19T09:30:00Z",
  "loader_version": "1.0.0",
  "duckdb_version": "2.0.0"
}
```

Seeknal Serving then serves `/data/serving/mart.duckdb` using the standard `local_file` connection profile:

```yaml
connections:
  duck:
    kind: duckdb
    profile: local_file
    path: /data/serving/mart.duckdb
    allowed_directories:
      - /data/serving/
    memory_limit: 2GB
    threads: 4
```
