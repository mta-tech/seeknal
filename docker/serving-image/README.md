# Serving Release Image Tooling (`docker/serving-image/`)

This directory contains lock manifests, prefetch utilities, and build-time verification scripts for the `atlas-rt-serving` container image (`docker/Dockerfile.serving`).

## Files

- `extensions.lock.json`: Pins DuckDB `2.0.0` extensions (`httpfs`, `avro`, `iceberg`, with `avro` ordered before `iceberg` because DuckDB 2.0.0's `iceberg` extension requires `avro`) for `linux_amd64` and `linux_arm64`, including compressed byte counts, compressed `.gz` SHA-256 digests (`gz_sha256` / `sha256`), and decompressed `.duckdb_extension` SHA-256 digests (`raw_sha256` / `decompressed_sha256`).
- `prefetch.py`: Prefetches hash-verified Python wheels from `uv.lock`—both the `serving` dependency group and the locked `hatchling` build backend (`hatchling`, `packaging`, `pathspec`, `pluggy`, `trove-classifiers`)—into `vendor/requirements.txt` and `vendor/wheels`, and downloads/verifies pinned DuckDB extension archives into `vendor/extensions` for air-gapped `--network=none` builds. Never builds or installs an unchecked `seeknal` wheel; offline builds compile `seeknal` directly from the copied `src/` with `--no-build-isolation`. Also unpacks verified extensions into `/opt/seeknal/duckdb_extensions/v2.0.0/<platform>/<name>.duckdb_extension` during image build.
- `check_serving_env.py`: Build-time fail-closed verification script run inside `Dockerfile.serving`. Verifies:
  1. Installed versions of key serving packages match `uv.lock` and no forbidden non-serving packages (`pydantic-ai-slim`, `prefect`, `temporalio`, `pyspark`, `scikit-learn`, `openai`, etc.) are present.
  2. Four-way DuckDB version pin agreement: `duckdb.__version__ == seeknal.serving.buildinfo.EXPECTED_DUCKDB_VERSION == seeknal.serving.accel.settings.DEFAULT_EXPECTED_DUCKDB_VERSION == uv.lock["duckdb"]` (`"2.0.0"`).
  3. Baked extensions (`httpfs`, `avro`, `iceberg`, ordered so `avro` is always verified and `LOAD`ed before `iceberg`) exist under the resolved extension directory (`--extension-directory`, then `SEEKNAL_EXTENSION_DIRECTORY`, then `extensions.lock.json`), match `raw_sha256`, and `LOAD` in DuckDB with `autoinstall_known_extensions=false`, `autoload_known_extensions=false`, and `custom_extension_repository='/nonexistent_extension_repository'`.
  4. `uv pip check` reports zero dependency conflicts.

## Online Lock-Only Build

From the repository root (note that `Dockerfile.serving` intentionally omits `# syntax=` so BuildKit never pulls an external frontend image, and only takes the offline vendor path when `--build-arg OFFLINE=1`):

```bash
docker build -f docker/Dockerfile.serving \
  --build-arg SEEKNAL_COMMIT="$(git rev-parse HEAD)" \
  -t atlas-rt-serving:local .
```

## Air-Gapped (`--network=none`) Build

1. On a network-connected workstation or CI stage, prefetch and verify all serving wheels, locked build-backend wheels (`hatchling`, `packaging`, `pathspec`, `pluggy`, `trove-classifiers`), and DuckDB extensions (`httpfs`, `avro`, `iceberg`) against `uv.lock` and `extensions.lock.json`:

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

2. Transfer the repository checkout (including `docker/serving-image/vendor/`) to the air-gapped builder and build with `--network=none` and `--build-arg OFFLINE=1`:

   ```bash
   docker build --network=none -f docker/Dockerfile.serving \
     --build-arg OFFLINE=1 \
     --build-arg BASE_IMAGE=mirror.internal/astral-sh/uv:0.9.18-python3.11-bookworm-slim \
     --build-arg SEEKNAL_COMMIT="$(git rev-parse HEAD)" \
     -t atlas-rt-serving:airgapped .
   ```

   During the offline build (`OFFLINE=1`), `Dockerfile.serving` re-verifies all vendor wheel and extension SHA-256 digests against `uv.lock` and `extensions.lock.json`, installs serving and build-backend wheels via `uv pip install --system --offline --no-index --require-hashes --find-links docker/serving-image/vendor/wheels -r docker/serving-image/vendor/requirements.txt`, installs `seeknal` from the copied `src/` via `uv pip install --system --offline --no-index --no-deps --no-build-isolation .`, unpacks verified extensions into `/opt/seeknal/duckdb_extensions`, verifies installed source hashes before writing `/opt/seeknal/buildinfo.json`, runs `check_serving_env.py`, and removes `docker/serving-image/vendor` from the final filesystem view.

   > **Layer size note:** `COPY`ing `docker/serving-image/` when `vendor/` is populated for an air-gapped build retains the vendored files in the `COPY` layer even after `rm -rf docker/serving-image/vendor`, whereas online builds keep `vendor/` empty/absent (or operators with BuildKit locally can bind-mount the vendor dir).

## DuckDB Storage & Extension Notes

- For `iceberg_direct` connections, configure `extensions: [httpfs, avro, iceberg]` (`avro` must be loaded before `iceberg` in DuckDB 2.0.0).
- `.duckdb` cache files written by DuckDB 2.0 cannot be read by pre-2.0 DuckDB tools.

## CLI Entrypoint & Examples (`python -m seeknal.serving`)

The image sets `ENTRYPOINT ["python", "-m", "seeknal.serving"]` and `CMD ["--config", "/etc/seeknal/serving.yaml"]`:

```bash
# Print build metadata (equivalent to: python -m seeknal.serving --version [--json])
docker run --rm --entrypoint python atlas-rt-serving:local -m seeknal.serving --version
docker run --rm --entrypoint python atlas-rt-serving:local -m seeknal.serving --version --json

# Offline configuration validation (python -m seeknal.serving validate --config ...)
docker run --rm \
  -v /path/to/serving.yaml:/etc/seeknal/serving.yaml:ro \
  --entrypoint python atlas-rt-serving:local \
  -m seeknal.serving validate --config /etc/seeknal/serving.yaml

# Start the Serving gateway (python -m seeknal.serving --config ...)
docker run --rm -p 8765:8765 \
  -v /path/to/serving.yaml:/etc/seeknal/serving.yaml:ro \
  --entrypoint python atlas-rt-serving:local \
  -m seeknal.serving --config /etc/seeknal/serving.yaml
```

Because `ENTRYPOINT` is `["python", "-m", "seeknal.serving"]`, passing arguments directly (`docker run --rm atlas-rt-serving:local --version`, `validate --config /etc/seeknal/serving.yaml`, or `--config /etc/seeknal/serving.yaml`) invokes the same module entrypoint.

## Release notes

- This image pins DuckDB `2.0.0` across `uv.lock`, `seeknal.serving.buildinfo.EXPECTED_DUCKDB_VERSION`, `seeknal.serving.accel.settings.DEFAULT_EXPECTED_DUCKDB_VERSION`, and `docker/serving-image/extensions.lock.json`.
- DuckDB advises waiting for `2.0.1` before mission-critical use; the DuckDB pin in `atlas-rt-serving` will move to `2.0.1` when it ships.
- `.duckdb` cache files written by DuckDB `2.0` use the 2.0 storage format and cannot be read by pre-2.0 DuckDB tools.
