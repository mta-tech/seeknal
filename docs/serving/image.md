# Atlas Runtime Serving Container Image (`atlas-rt-serving`)

The `atlas-rt-serving` container image provides the minimal, hardened, air-gap-ready runtime environment for Seeknal Serving. Serving acts as the policy enforcement gateway between the Atlas Data Catalog and downstream execution engines (StarRocks, Lakebase PostgreSQL, Cube SQL API, and DuckDB).

Because Seeknal Serving operates as a critical policy enforcement boundary, the `atlas-rt-serving` image is strictly isolated, reproducible, and lock-verified: every Python wheel and DuckDB binary extension is verified by SHA-256 digest at build time.

---

## What the Image Contains and Excludes

### Included Dependencies

The image installs the `[dependency-groups].serving` group from `pyproject.toml` (mirrored in `docker/serving-requirements.txt`) strictly from `uv.lock` with `--locked` and `--require-hashes`:

- **Core serving framework:** `starlette`, `uvicorn` (ASGI HTTP server and routing)
- **SQL parsing and dialect translation:** `sqlglot`
- **Embedded query engine:** `duckdb` (pinned to `2.0.0`, enforced via `EXPECTED_DUCKDB_VERSION` in `seeknal.serving.buildinfo`)
- **Baked DuckDB extensions (`/opt/seeknal/duckdb_extensions`):** `httpfs` and `iceberg` pre-unpacked at `/opt/seeknal/duckdb_extensions/v2.0.0/<platform>/<name>.duckdb_extension` (`linux_amd64` / `linux_arm64`), verified against compressed and decompressed SHA-256 digests in `docker/serving-image/extensions.lock.json`
- **Engine database drivers:** `pymysql` (StarRocks MySQL protocol), `psycopg2-binary` (Lakebase PostgreSQL, Cube SQL API)
- **Discovery and catalog HTTP clients:** `httpx`
- **Configuration parsing:** `pyyaml`
- **Security and token cryptography:** `cryptography` (ES256 ECDSA grant token validation)
- **Support libraries:** `attrs`, `pygments`, `python-multipart`, `rich`, `sniffio`
- **Transitive runtime dependencies:** `anyio`, `click`, `idna`, `typing-extensions`, `certifi`, `h11`, `httpcore`, `cffi`, `pycparser`, `markdown-it-py`, `mdurl`

The `seeknal` package itself is installed with `--no-deps`, ensuring that optional extras and heavy non-serving dependencies are never pulled into the runtime environment.

### Excluded Dependencies

The full Seeknal repository includes libraries for analysis, orchestration, and machine learning that are explicitly excluded from `atlas-rt-serving` and rejected at build time by `docker/serving-image/check_serving_env.py`:

- **LLM SDKs and Agent frameworks:** `pydantic-ai-slim`, `pydantic-deep`, `openai`, `anthropic`, `google-genai`
- **Workflow orchestrators:** `prefect`, `temporalio`
- **Data science and ML packages:** `scikit-learn`, `scipy`, `pyspark`
- **External channels and tools:** `python-telegram-bot`, `redis`, `ddgs`
- **Development and interactive runtimes:** `ipykernel`, `pytest`, `black`

---

## Build Modes

The build context must be the root of the repository. The base image defaults to `ghcr.io/astral-sh/uv:0.9.18-python3.11-bookworm-slim`.

### 1. Online Lock-Only Build

Exports hash-pinned requirements from `uv.lock`, installs with `--require-hashes`, downloads and verifies DuckDB `2.0.0` extensions (`httpfs`, `iceberg`) against `docker/serving-image/extensions.lock.json`, writes `/opt/seeknal/buildinfo.json`, and runs `docker/serving-image/check_serving_env.py`:

```bash
docker build -f docker/Dockerfile.serving \
  --build-arg SEEKNAL_COMMIT="$(git rev-parse HEAD)" \
  -t atlas-rt-serving:local .
```

### 2. Air-Gapped (`--network=none`) Build

For disconnected or regulated build environments where container builds run with `--network=none`:

1. **Prefetch on a connected host:** Download and SHA-256-verify all serving wheels (from `uv.lock`), the built `seeknal` wheel, and DuckDB `2.0.0` extensions (from `docker/serving-image/extensions.lock.json`):
   ```bash
   python3 docker/serving-image/prefetch.py \
     --vendor-dir docker/serving-image/vendor
   ```
   This populates `docker/serving-image/vendor/{requirements.txt,wheels,project,extensions}` and fails closed on any digest mismatch.

2. **Build with `--network=none`:** Run Docker build offline (optionally pointing `BASE_IMAGE` to an internal registry mirror):
   ```bash
   docker build --network=none -f docker/Dockerfile.serving \
     --build-arg OFFLINE=1 \
     --build-arg BASE_IMAGE=ghcr.io/astral-sh/uv:0.9.18-python3.11-bookworm-slim \
     --build-arg SEEKNAL_COMMIT="$(git rev-parse HEAD)" \
     -t atlas-rt-serving:airgapped .
   ```
   The offline stage re-verifies vendor wheel and extension SHA-256 hashes before installing via `uv pip install --system --offline --no-index --require-hashes --find-links ...`, unpacks verified extensions into `/opt/seeknal/duckdb_extensions`, verifies `LOAD httpfs` and `LOAD iceberg` offline in `check_serving_env.py`, and removes `docker/serving-image/vendor` before finalizing the image.

### Build Arguments

- **`BASE_IMAGE`**: Defaults to `ghcr.io/astral-sh/uv:0.9.18-python3.11-bookworm-slim`. Pin to an immutable `@sha256:...` digest or internal mirror in release pipelines.
- **`SEEKNAL_COMMIT`**: Defaults to `unknown`. Sets `org.opencontainers.image.revision` and `git_sha` in `/opt/seeknal/buildinfo.json`.
- **`BUILD_TIME`**: Optional UTC ISO-8601 build timestamp override recorded in `/opt/seeknal/buildinfo.json` (defaults to current UTC timestamp at build time).
- **`OFFLINE`**: Set to `1` to force air-gapped vendor mode and fail closed if `docker/serving-image/vendor` is absent.

---

## Baked DuckDB Extensions (`/opt/seeknal/duckdb_extensions`)

DuckDB extensions are baked at `/opt/seeknal/duckdb_extensions/v2.0.0/<platform>/<name>.duckdb_extension` (`httpfs` and `iceberg` for `linux_amd64` and `linux_arm64`) with world-readable `0644` permissions for non-root UID `10001`.

In `/etc/seeknal/serving.yaml`, configure DuckDB `iceberg_direct` connections to point `extension_directory` to `/opt/seeknal/duckdb_extensions`:

```yaml
connections:
  iceberg_wh:
    kind: duckdb
    mode: iceberg_direct
    extension_directory: /opt/seeknal/duckdb_extensions
    temp_directory: /tmp/seeknal-duckdb
```

Runtime policy disables `autoinstall_known_extensions` and `autoload_known_extensions`, loading extensions strictly from `/opt/seeknal/duckdb_extensions` via `LOAD`.

---

## Build Metadata and `--version`

Every image writes `/opt/seeknal/buildinfo.json` containing `version`, `git_sha`, `build_time`, `duckdb_version`, and `lock_sha256`. You can query build metadata without supplying `--config`:

```bash
docker run --rm atlas-rt-serving:local --version
docker run --rm atlas-rt-serving:local --version --json
```

---

## Configuration Mount (`/etc/seeknal/serving.yaml`)

The image defines:
- **Port:** `EXPOSE 8765`
- **Entrypoint:** `ENTRYPOINT ["python", "-m", "seeknal.serving"]`
- **Default Command:** `CMD ["--config", "/etc/seeknal/serving.yaml"]`
- **User:** Non-root numeric user `10001:10001`

Production deployments mount the server configuration YAML file into `/etc/seeknal/serving.yaml` as read-only:

```bash
docker run --rm \
  -p 8765:8765 \
  -v /path/to/production/serving.yaml:/etc/seeknal/serving.yaml:ro \
  atlas-rt-serving:local
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
