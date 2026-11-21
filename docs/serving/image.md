# Atlas Runtime Serving Container Image (`atlas-rt-serving`)

The `atlas-rt-serving` container image provides the minimal, hardened, air-gap-ready runtime environment for Seeknal Serving. Serving acts as the policy enforcement gateway between the Atlas Data Catalog and downstream execution engines (StarRocks, Lakebase PostgreSQL, Cube SQL API, and DuckDB).

Because Seeknal Serving operates as a critical policy enforcement boundary, the `atlas-rt-serving` image is strictly isolated, reproducible, and lock-verified: every Python wheel and DuckDB binary extension is verified by SHA-256 digest at build time.

---

## What the Image Contains and Excludes

### Included Dependencies

The image installs the `[dependency-groups].serving` group from `pyproject.toml` (mirrored in `docker/serving-requirements.txt`) strictly from `uv.lock` with `--locked` and `--require-hashes`:

- **Core serving framework:** `starlette`, `uvicorn` (ASGI HTTP server and routing)
- **SQL parsing and dialect translation:** `sqlglot`
- **Embedded query engine:** `duckdb` (pinned to `2.0.0`, enforced via four-way agreement across `duckdb.__version__`, `seeknal.serving.buildinfo.EXPECTED_DUCKDB_VERSION`, `seeknal.serving.accel.settings.DEFAULT_EXPECTED_DUCKDB_VERSION`, and `uv.lock`)
- **Baked DuckDB extensions (`/opt/seeknal/duckdb_extensions`):** `httpfs`, `avro`, and `iceberg` pre-unpacked at `/opt/seeknal/duckdb_extensions/v2.0.0/<platform>/<name>.duckdb_extension` (`linux_amd64` / `linux_arm64`), verified against compressed and decompressed SHA-256 digests in `docker/serving-image/extensions.lock.json` (`avro` is ordered and loaded before `iceberg` because DuckDB 2.0.0's `iceberg` extension requires `avro`)
- **Engine database drivers:** `pymysql` (StarRocks MySQL protocol), `psycopg2-binary` (Lakebase PostgreSQL, Cube SQL API)
- **Discovery and catalog HTTP clients:** `httpx`
- **Configuration parsing:** `pyyaml`
- **Security and token cryptography:** `cryptography` (ES256 ECDSA grant token validation)
- **Support libraries:** `attrs`, `pygments`, `python-multipart`, `rich`, `sniffio`
- **Transitive runtime dependencies:** `anyio`, `click`, `idna`, `typing-extensions`, `certifi`, `h11`, `httpcore`, `cffi`, `pycparser`, `markdown-it-py`, `mdurl`

The `seeknal` package itself is installed from the copied `src/` directory with `--no-deps`, ensuring that optional extras and heavy non-serving dependencies are never pulled into the runtime environment and that `/opt/seeknal/buildinfo.json` (`/buildinfo`) only ever reports a `git_sha` matching the installed `src/` files.

### Excluded Dependencies

The full Seeknal repository includes libraries for analysis, orchestration, and machine learning that are explicitly excluded from `atlas-rt-serving` and rejected at build time by `docker/serving-image/check_serving_env.py`:

- **LLM SDKs and Agent frameworks:** `pydantic-ai-slim`, `pydantic-deep`, `openai`, `anthropic`, `google-genai`
- **Workflow orchestrators:** `prefect`, `temporalio`
- **Data science and ML packages:** `scikit-learn`, `scipy`, `pyspark`
- **External channels and tools:** `python-telegram-bot`, `redis`, `ddgs`
- **Development and interactive runtimes:** `ipykernel`, `pytest`, `black`

---

## Build Modes

The build context must be the root of the repository. `docker/Dockerfile.serving` uses a two-stage multi-stage build (`FROM ${BASE_IMAGE} AS builder` followed by `FROM ${BASE_IMAGE} AS runtime`) and omits `# syntax=` directives so air-gapped hosts never attempt a remote BuildKit frontend pull. The base image defaults to `ghcr.io/astral-sh/uv:0.9.18-python3.11-bookworm-slim`.

### 1. Online Lock-Only Build

In the `builder` stage, `Dockerfile.serving` exports hash-pinned requirements from `uv.lock`, installs with `--require-hashes`, installs `seeknal` from `src/` with `--no-deps`, downloads and verifies DuckDB `2.0.0` extensions (`httpfs`, `avro`, `iceberg`) against `docker/serving-image/extensions.lock.json`, and writes `/opt/seeknal/buildinfo.json`. The final `runtime` stage copies only `/usr/local/lib/python3.11/site-packages`, `/opt/seeknal/duckdb_extensions`, and `/opt/seeknal/buildinfo.json` via `COPY --from=builder`, copies `uv.lock`, `docker/serving-image/extensions.lock.json`, and `docker/serving-image/check_serving_env.py`, runs `check_serving_env.py` in the final stage, and removes those lock/check files via `rm -rf`:

```bash
docker build -f docker/Dockerfile.serving \
  --build-arg SEEKNAL_COMMIT="$(git rev-parse HEAD)" \
  -t atlas-rt-serving:local .
```

### 2. Air-Gapped (`--network=none`) Build

For disconnected or regulated build environments where container builds run with `--network=none`:

1. **Prefetch on a connected host:** Download and SHA-256-verify all serving wheels and the locked `hatchling` build backend (`hatchling`, `packaging`, `pathspec`, `pluggy`, `trove-classifiers`) from `uv.lock`, plus DuckDB `2.0.0` extensions (`httpfs`, `avro`, `iceberg`) from `docker/serving-image/extensions.lock.json`:
   ```bash
   python3 docker/serving-image/prefetch.py \
     --vendor-dir docker/serving-image/vendor
   ```
   This populates `docker/serving-image/vendor/{requirements.txt,wheels,extensions}` (no prebuilt unchecked `seeknal` wheel) and fails closed on any digest mismatch.

2. **Build with `--network=none` and `OFFLINE=1`:** Run Docker build offline (optionally pointing `BASE_IMAGE` to an internal registry mirror). The offline path is taken strictly when `--build-arg OFFLINE=1`:
   ```bash
   docker build --network=none -f docker/Dockerfile.serving \
     --build-arg OFFLINE=1 \
     --build-arg BASE_IMAGE=ghcr.io/astral-sh/uv:0.9.18-python3.11-bookworm-slim \
     --build-arg SEEKNAL_COMMIT="$(git rev-parse HEAD)" \
     -t atlas-rt-serving:airgapped .
   ```
   In the `builder` stage, the offline path re-verifies vendor wheel and extension SHA-256 hashes (`prefetch.py verify-vendor`), installs serving and locked build-backend wheels via `uv pip install --system --offline --no-index --require-hashes --find-links docker/serving-image/vendor/wheels -r docker/serving-image/vendor/requirements.txt`, builds and installs `seeknal` from the copied `src/` via `uv pip install --system --offline --no-index --no-deps --no-build-isolation .`, unpacks verified extensions into `/opt/seeknal/duckdb_extensions`, writes `/opt/seeknal/buildinfo.json`, and cleans up `docker/serving-image/vendor`. The final `runtime` stage uses `COPY --from=builder` for only installed `site-packages`, `/opt/seeknal/duckdb_extensions`, and `/opt/seeknal/buildinfo.json` (never copying `docker/serving-image/` or `docker/serving-image/vendor`), verifies `LOAD httpfs`, `LOAD avro`, and `LOAD iceberg` offline in `check_serving_env.py`, and cleans up temporary lock/check files via `rm -rf`.

> **Multi-stage layer isolation:** Because `docker/serving-image/` (including `vendor/`) is copied only into the `builder` stage and never copied into the final `runtime` stage, vendored wheels and compressed extension archives never remain in any image layer of the final image.

### Build Arguments

- **`BASE_IMAGE`**: Defaults to `ghcr.io/astral-sh/uv:0.9.18-python3.11-bookworm-slim`. Pin to an immutable `@sha256:...` digest or internal mirror in release pipelines.
- **`SEEKNAL_COMMIT`**: Defaults to `unknown`. Sets `org.opencontainers.image.revision` and `git_sha` in `/opt/seeknal/buildinfo.json`.
- **`BUILD_TIME`**: Optional UTC ISO-8601 build timestamp override recorded in `/opt/seeknal/buildinfo.json` (defaults to current UTC timestamp at build time).
- **`OFFLINE`**: Set to `1` (`--build-arg OFFLINE=1`) to take the air-gapped vendor path; online builds (`OFFLINE=0`) never implicitly consume `docker/serving-image/vendor`.

---

## Baked DuckDB Extensions (`/opt/seeknal/duckdb_extensions`) and Storage Compatibility

DuckDB extensions are baked at `/opt/seeknal/duckdb_extensions/v2.0.0/<platform>/<name>.duckdb_extension` (`httpfs`, `avro`, and `iceberg` for `linux_amd64` and `linux_arm64`) with world-readable `0644` permissions for non-root UID `10001`. In DuckDB `2.0.0`, the `iceberg` extension depends on `avro`, so `avro` must be loaded before `iceberg`.

In `/etc/seeknal/serving.yaml`, configure DuckDB `iceberg_direct` connections to point `extension_directory` to `/opt/seeknal/duckdb_extensions` and include `extensions: [httpfs, avro, iceberg]`:

```yaml
connections:
  iceberg_wh:
    kind: duckdb
    profile: iceberg_direct
    extension_directory: /opt/seeknal/duckdb_extensions
    extensions: [httpfs, avro, iceberg]
    temp_directory: /tmp/seeknal-duckdb
```

Runtime policy disables `autoinstall_known_extensions` and `autoload_known_extensions`, loading extensions strictly from `/opt/seeknal/duckdb_extensions` via `LOAD`.

**DuckDB 2.0 `.duckdb` file compatibility:** `.duckdb` cache files written by DuckDB `2.0` cannot be read by pre-2.0 DuckDB tools. Ensure all acceleration loaders and inspection tools reading or writing local `.duckdb` files run DuckDB `2.0.0`.

---

## CLI Entrypoint, `--version`, and `validate` (`python -m seeknal.serving`)

The image defines:
- **Port:** `EXPOSE 8765`
- **Entrypoint:** `ENTRYPOINT ["python", "-m", "seeknal.serving"]`
- **Default Command:** `CMD ["--config", "/etc/seeknal/serving.yaml"]`
- **User:** Non-root numeric user `10001:10001`

Every image writes `/opt/seeknal/buildinfo.json` containing `version`, `git_sha`, `build_time`, `duckdb_version`, and `lock_sha256`. All CLI modes run via `python -m seeknal.serving`:

```bash
# Query build metadata without supplying --config (python -m seeknal.serving --version)
docker run --rm atlas-rt-serving:local --version
docker run --rm atlas-rt-serving:local --version --json

# Offline configuration validation (python -m seeknal.serving validate --config /etc/seeknal/serving.yaml)
docker run --rm \
  -v /path/to/production/serving.yaml:/etc/seeknal/serving.yaml:ro \
  atlas-rt-serving:local validate --config /etc/seeknal/serving.yaml

# Run the server (python -m seeknal.serving --config /etc/seeknal/serving.yaml)
docker run --rm \
  -p 8765:8765 \
  -v /path/to/production/serving.yaml:/etc/seeknal/serving.yaml:ro \
  atlas-rt-serving:local --config /etc/seeknal/serving.yaml
```

Equivalent direct commands inside an interactive shell or Kubernetes `command` override:

```bash
python -m seeknal.serving --version
python -m seeknal.serving validate --config /etc/seeknal/serving.yaml
python -m seeknal.serving --config /etc/seeknal/serving.yaml
```

---

## TLS and Key Mounts (Path Only)

In compliance with Atlas Runtime security mandates, secret material (tokens, passwords, private keys) must never be passed via container environment variables (`ENV`), and no image layers contain credentials.

All sensitive files are mounted as read-only volumes and referenced strictly by path inside `/etc/seeknal/serving.yaml`:

### 1. Mutual TLS (mTLS)

```yaml
tls:
  cert_file: /etc/seeknal/tls/server.crt
  key_file: /etc/seeknal/tls/server.key
  client_ca_file: /etc/seeknal/tls/catalog-ca.crt
  require_client_cert: true
```

### 2. Catalog Grant Keys

```yaml
grant_keys:
  jwks_file: /etc/seeknal/keys/catalog-grants.jwks.json
```

### 3. Downstream Engine Secrets

```yaml
secrets_env_file: /etc/seeknal/secrets/starrocks.env
```

Ensure file permissions allow UID `10001` read access (e.g. `chmod 0644`).

---

## Release Pipeline: Signing and Digest Pinning

The automated release CI/CD pipeline:
1. Pins `BASE_IMAGE` (`ghcr.io/astral-sh/uv:0.9.18-python3.11-bookworm-slim`) to its immutable `@sha256:...` digest.
2. Passes `SEEKNAL_COMMIT` matching the Git commit SHA.
3. Builds and verifies the image (executing `check_serving_env.py`).
4. Signs the resulting image artifact with Cosign and publishes provenance attestations to the container registry.

---

## Release notes

- seeknal works with DuckDB 1.4.x and 2.0.x; the Serving release pins 2.0.0.
- `atlas-rt-serving` pins DuckDB `2.0.0` across `uv.lock`, `seeknal.serving.buildinfo.EXPECTED_DUCKDB_VERSION`, `seeknal.serving.accel.settings.DEFAULT_EXPECTED_DUCKDB_VERSION`, and `docker/serving-image/extensions.lock.json` (`httpfs`, `avro`, `iceberg`).
- DuckDB advises waiting for `2.0.1` before mission-critical use; the pin will move to `2.0.1` when it ships.
- `.duckdb` cache files written by DuckDB `2.0` cannot be read by pre-2.0 DuckDB tools.
- Known limitations (backlog):
  - a discovery refresh fails as a whole (the previous catalog is kept) when one PII table loses its only safe serve, or when two Lakekeeper tables differ only by case; planned fix: omit just that table and add a metric for consecutive failures;
  - in discovery mode, cubes cannot be marked PII; PII cubes are static-only;
  - validate cannot detect a typo in discovery pii_tables offline; start refuses it, because the first refresh fails.
