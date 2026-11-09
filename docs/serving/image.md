# Atlas Runtime Serving Container Image (`atlas-rt-serving`)

The `atlas-rt-serving` container image provides the minimal, hardened runtime environment for Seeknal Serving. Serving acts as the policy enforcement gateway between the Atlas Data Catalog and downstream execution engines (StarRocks, Lakebase PostgreSQL, Cube SQL API, and DuckDB).

Because Seeknal Serving operates as a critical policy enforcement boundary, the `atlas-rt-serving` image is strictly isolated and carries only the minimal dependency closure necessary for serving queries, validating grant tokens, and communicating with catalogs and engines.

---

## What the Image Contains and Excludes

### Included Dependencies

The image is built from the unified `uv.lock` lockfile without modifying `pyproject.toml`. Dependencies are filtered through `docker/serving-requirements.txt` and pinned to lockfile versions using constraints exported from `uv.lock`:

- **Core serving framework:** `starlette`, `uvicorn` (ASGI HTTP server and routing)
- **SQL parsing and dialect translation:** `sqlglot`
- **Embedded query engine:** `duckdb`
- **Engine database drivers:** `pymysql` (StarRocks MySQL protocol), `psycopg2-binary` (Lakebase PostgreSQL, Cube SQL API)
- **Discovery and catalog HTTP clients:** `httpx`
- **Configuration parsing:** `pyyaml`
- **Security and token cryptography:** `cryptography` (ES256 ECDSA grant token validation)
- **Support libraries:** `attrs`, `pygments`, `python-multipart`, `rich`, `sniffio`
- **Transitive runtime dependencies:** `anyio`, `click`, `idna`, `typing-extensions`, `certifi`, `h11`, `httpcore`

The `seeknal` package itself is installed with `--no-deps`, ensuring that optional extras and heavy engine packages are not pulled into the runtime environment.

### Excluded Dependencies

The full Seeknal repository wheel includes numerous libraries for analysis, orchestration, and machine learning that are explicitly excluded from `atlas-rt-serving`:

- **LLM SDKs and Agent frameworks:** `pydantic-ai-slim`, `pydantic-deep`, `openai`, `anthropic`, `google-genai`
- **Workflow orchestrators:** `prefect`, `temporalio`
- **Data science and ML packages:** `scikit-learn`, `scipy`, `pyspark`
- **External channels and tools:** `python-telegram-bot`, `redis`, `ddgs`
- **Development and interactive runtimes:** `ipykernel`, `pytest`, `black`

---

## Build Command

The build context must be the root of the repository.

### Standard Local Build

```bash
docker build -f docker/Dockerfile.serving -t atlas-rt-serving:local .
```

### Passing Build Arguments

The Dockerfile supports two build arguments:

1. **`BASE_IMAGE`**: Defaults to `ghcr.io/astral-sh/uv:python3.11-bookworm-slim`. Used to pin specific image digests or alternate registry mirrors:
   ```bash
   docker build -f docker/Dockerfile.serving \
     --build-arg BASE_IMAGE=ghcr.io/astral-sh/uv:python3.11-bookworm-slim@sha256:d8b74f762dd8a56f6c91e1d2c178875323a6abac3a0ab8a08d248b1111e13fae \
     -t atlas-rt-serving:local .
   ```

2. **`SEEKNAL_COMMIT`**: Defaults to `unknown`. Sets the OCI revision label (`org.opencontainers.image.revision`):
   ```bash
   docker build -f docker/Dockerfile.serving \
     --build-arg SEEKNAL_COMMIT=$(git rev-parse HEAD) \
     -t atlas-rt-serving:local .
   ```

Both build arguments can be combined:

```bash
docker build -f docker/Dockerfile.serving \
  --build-arg BASE_IMAGE=ghcr.io/astral-sh/uv:python3.11-bookworm-slim \
  --build-arg SEEKNAL_COMMIT=$(git rev-parse HEAD) \
  -t atlas-rt-serving:$(git rev-parse --short HEAD) .
```

---

## Configuration Mount (`/etc/seeknal/serving.yaml`)

The image defines:
- **Port:** `EXPOSE 8765`
- **Entrypoint:** `ENTRYPOINT ["python", "-m", "seeknal.serving"]`
- **Default Command:** `CMD ["--config", "/etc/seeknal/serving.yaml"]`
- **User:** Non-root numeric user `10001:10001`

The directory `/etc/seeknal` is owned by root and readable by non-root users. Production deployments mount the server configuration YAML file into this path as read-only:

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
# /etc/seeknal/serving.yaml
tls:
  cert_file: /etc/seeknal/tls/server.crt
  key_file: /etc/seeknal/tls/server.key
  client_ca_file: /etc/seeknal/tls/catalog-ca.crt
  require_client_cert: true
```

Mount directories:
```bash
  -v /path/to/tls/server.crt:/etc/seeknal/tls/server.crt:ro \
  -v /path/to/tls/server.key:/etc/seeknal/tls/server.key:ro \
  -v /path/to/tls/catalog-ca.crt:/etc/seeknal/tls/catalog-ca.crt:ro
```

### 2. Catalog Grant Keys

Grant validation tokens are verified against trusted ECDSA public keys provided as JWKS or PEM files:

```yaml
# /etc/seeknal/serving.yaml
grant_keys:
  jwks_file: /etc/seeknal/keys/catalog-grants.jwks.json
  # Alternatively, use pem_files:
  # pem_files:
  #   atlas-catalog: /etc/seeknal/keys/atlas-catalog.pub.pem
```

Mount directories:
```bash
  -v /path/to/keys/catalog-grants.jwks.json:/etc/seeknal/keys/catalog-grants.jwks.json:ro
```

### 3. Downstream Engine Secrets

Database connection passwords and tokens are loaded from secret files mounted into the container:

```yaml
# /etc/seeknal/serving.yaml
secrets_env_file: /etc/seeknal/secrets/starrocks.env
```

Mount secrets:
```bash
  -v /path/to/secrets/starrocks.env:/etc/seeknal/secrets/starrocks.env:ro
```

Ensure file permissions allow UID `10001` read access (e.g. `chmod 0644`).

---

## Release Pipeline: Signing and Digest Pinning

Release signing (e.g., Cosign keyless signatures, in-toto attestations, SBOM generation) and upstream base image digest pinning are managed by the automated release CI/CD pipeline, not directly in this repository file or Dockerfile.

The release pipeline:
1. Pins `BASE_IMAGE` to the exact immutable digest in `ghcr.io/astral-sh/uv`.
2. Passes `SEEKNAL_COMMIT` matching the Git commit SHA.
3. Builds and tests the image.
4. Signs the resulting image artifact with Cosign and publishes provenance attestations to the container registry.
