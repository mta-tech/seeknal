# Atlas Runtime Release Pipeline

## 1. Overview

An Atlas Runtime release (`atlas-rt/X.Y.Z`) is a signed manifest over an image family. Every image in the family is built from a single Seeknal Git commit and a single `uv.lock` dependency lockfile. All images are pushed to GitHub Container Registry (GHCR), referenced **strictly by cryptographic digest** (`@sha256:...`), and signed using Cosign with keyless OpenID Connect (OIDC) provenance.

Projects pin a runtime line or exact release version in `seeknal_project.yml`, and the Atlas Catalog validates the manifest's `contracts` before dispatching compute workloads.

---

## 2. Image Family

The runtime consists of three purpose-built images:

| Image | Dockerfile | Purpose | Base & Permissions |
|---|---|---|---|
| `atlas-rt` | `docker/Dockerfile.rt` | Job and interactive session execution | Non-root `10001:10001`, read-only root friendly (`/work`, `/tmp`) |
| `atlas-rt-serving` | `docker/Dockerfile.serving` | Governed online feature and model serving | Non-root `10001:10001`, minimal serving dependencies |
| `atlas-rt-agent` | `docker/Dockerfile.agent` | Governed Seeknal Ask gateway & worker | Non-root `10001:10001`, `ATLAS_ASK_MODE=governed` |

### DuckDB Extension Pre-Installation

The `atlas-rt` image pre-installs DuckDB extensions (`iceberg` and `httpfs`) at container build time so that workloads never download extensions over the network during runtime:

```dockerfile
ENV DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb/extensions
RUN python -c "import duckdb; con = duckdb.connect(); con.execute(\"SET extension_directory='/opt/duckdb/extensions'; INSTALL iceberg; INSTALL httpfs;\")"
```

The runtime environment variable `DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb/extensions` points DuckDB directly to this directory.

### Governed Agent Configuration

The `atlas-rt-agent` image has `ENV ATLAS_ASK_MODE=governed` baked into the image. It contains no Spark runtime, no Node.js or npm toolchain, no secrets, and no `.env` files.

---

## 3. Release Pipeline Flow

The release workflow is defined in `.github/workflows/atlas-rt-release.yml` and is triggered when a tag matching `atlas-rt/v*` is pushed or via `workflow_dispatch`.

```
Git Tag (atlas-rt/vX.Y.Z)
  │
  ├──► Buildx Build & Push (atlas-rt, atlas-rt-serving, atlas-rt-agent)
  │      └── push: true, provenance: true, sbom: true
  │      └── Output: Image SHA-256 Digests
  │
  ├──► Keyless Cosign Image Signing (OIDC)
  │      └── cosign sign --yes ghcr.io/<owner>/<image>@<digest>
  │
  ├──► Licence Scanning
  │      └── pip-licenses check against uv.lock constraints (fails on AGPL/SSPL/RSAL)
  │
  ├──► Manifest Generation (scripts/release/build_manifest.py)
  │      └── Validates schema, embeds digests, contracts & ACU weights
  │      └── Output: dist/manifest.yaml & dist/manifest.yaml.sha256
  │
  ├──► Keyless Cosign Manifest Signing
  │      └── cosign sign-blob --yes --bundle dist/manifest.yaml.bundle dist/manifest.yaml
  │
  └──► GitHub Release Assets Upload
         └── manifest.yaml, manifest.yaml.sha256, manifest.yaml.bundle
```

### No Inline Secrets

All steps run keyless using GitHub Actions OIDC (`id-token: write`) and the standard `GITHUB_TOKEN` for package and release asset publication. No long-lived credentials or signing keys are stored in the repository.

---

## 4. Consumer Verification

Consumers and cells verify the authenticity and integrity of images and manifests using Cosign.

### Verifying Container Images by Digest

To verify container image signatures against the GitHub Actions release workflow:

```bash
cosign verify \
  --certificate-identity-regexp "^https://github\.com/mta-tech/seeknal/\.github/workflows/atlas-rt-release\.yml@" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  ghcr.io/mta-tech/atlas-rt@sha256:<DIGEST>
```

Replace `ghcr.io/mta-tech/atlas-rt@sha256:<DIGEST>` with the exact digest recorded in the release manifest.

### Verifying the Release Manifest Blob

Download `manifest.yaml`, `manifest.yaml.sha256`, and `manifest.yaml.bundle` from the release assets:

1. Check file integrity:
```bash
sha256sum -c manifest.yaml.sha256
```

2. Verify Cosign keyless blob signature:
```bash
cosign verify-blob \
  --certificate-identity-regexp "^https://github\.com/mta-tech/seeknal/\.github/workflows/atlas-rt-release\.yml@" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  --bundle manifest.yaml.bundle \
  manifest.yaml
```

---

## 5. How a Cell Pins by Digest

Projects configure their desired runtime release in `seeknal_project.yml`:

```yaml
# Default project-wide runtime pin (line pin)
runtime: atlas-rt/1.0

environments:
  dev:
    principal: auto
  prod:
    # Exact pin override for production stability
    runtime: atlas-rt/1.0.3
```

When deploying or dispatching tasks:

1. The cell resolves the project pin against available verified release manifests:
   ```bash
   python -m seeknal.runtime resolve \
     --project /path/to/project \
     --releases /path/to/manifests \
     --env prod \
     --registry ghcr.io/mta-tech
   ```
   Output:
   ```
   atlas-rt/1.0.3 ghcr.io/mta-tech/atlas-rt@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
   ```

2. The execution environment starts the container **by digest only**. Image tags are never used at runtime, preventing tag-mutability attacks and ensuring completely reproducible execution.
