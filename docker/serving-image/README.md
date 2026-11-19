# Serving Release Image Tooling (`docker/serving-image/`)

This directory contains lock manifests, prefetch utilities, and build-time verification scripts for the `atlas-rt-serving` container image (`docker/Dockerfile.serving`).

## Files

- `extensions.lock.json`: Pins DuckDB `2.0.0` extensions (`httpfs`, `iceberg`) for `linux_amd64` and `linux_arm64`, including compressed byte counts, compressed `.gz` SHA-256 digests (`gz_sha256` / `sha256`), and decompressed `.duckdb_extension` SHA-256 digests (`raw_sha256` / `decompressed_sha256`).
- `prefetch.py`: Prefetches hash-verified Python wheels from `uv.lock`, builds the local `seeknal` wheel, and downloads/verifies pinned DuckDB extension archives into a vendor directory for air-gapped `--network=none` builds. Also installs extensions into `/opt/seeknal/duckdb_extensions/v2.0.0/<platform>/<name>.duckdb_extension` during image build.
- `check_serving_env.py`: Build-time fail-closed verification script run inside `Dockerfile.serving`. Verifies:
  1. Installed versions of key serving packages match `uv.lock` and no forbidden non-serving packages (`pydantic-ai-slim`, `prefect`, `temporalio`, `pyspark`, `scikit-learn`, `openai`, etc.) are present.
  2. `duckdb.__version__ == EXPECTED_DUCKDB_VERSION` (`"2.0.0"` from `seeknal.serving.buildinfo`, plus agreement with `seeknal.serving.accel.settings.DEFAULT_EXPECTED_DUCKDB_VERSION` when present).
  3. Baked extensions exist at `/opt/seeknal/duckdb_extensions/v2.0.0/<platform>/<name>.duckdb_extension`, match `raw_sha256`, and `LOAD` in DuckDB with `autoinstall_known_extensions=false`, `autoload_known_extensions=false`, and `custom_extension_repository='/nonexistent_extension_repository'`.
  4. `uv pip check` reports zero dependency conflicts.

## Online Lock-Only Build

From the repository root:

```bash
docker build -f docker/Dockerfile.serving \
  --build-arg SEEKNAL_COMMIT="$(git rev-parse HEAD)" \
  -t atlas-rt-serving:local .
```

## Air-Gapped (`--network=none`) Build

1. On a network-connected workstation or CI stage, prefetch and verify all artifacts against `uv.lock` and `extensions.lock.json`:

   ```bash
   python3 docker/serving-image/prefetch.py \
     --vendor-dir docker/serving-image/vendor
   ```

   To prefetch only a single target architecture (e.g., `linux_amd64`):

   ```bash
   python3 docker/serving-image/prefetch.py \
     --vendor-dir docker/serving-image/vendor \
     --platform linux_amd64
   ```

2. Transfer the repository checkout (including `docker/serving-image/vendor/`) to the air-gapped builder and build with network completely disabled:

   ```bash
   docker build --network=none -f docker/Dockerfile.serving \
     --build-arg OFFLINE=1 \
     --build-arg BASE_IMAGE=mirror.internal/astral-sh/uv:0.9.18-python3.11-bookworm-slim \
     --build-arg SEEKNAL_COMMIT="$(git rev-parse HEAD)" \
     -t atlas-rt-serving:airgapped .
   ```

   During the build, `Dockerfile.serving` re-verifies all vendor wheel and extension SHA-256 digests against `uv.lock` and `extensions.lock.json`, installs packages via `--offline --no-index --require-hashes --find-links`, installs baked extensions into `/opt/seeknal/duckdb_extensions`, writes `/opt/seeknal/buildinfo.json`, runs `check_serving_env.py`, and deletes `docker/serving-image/vendor` from the final image layer.

## Inspecting Build Metadata

Run `--version` (or `--version --json`) without needing `--config`:

```bash
docker run --rm atlas-rt-serving:local --version
docker run --rm atlas-rt-serving:local --version --json
```
