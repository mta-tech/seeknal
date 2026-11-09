# Atlas Runtime Release Manifest, Validator, and Project Pinning

## 1. Overview

Atlas Runtime is a signed release (`atlas-rt/X.Y.Z`) defining a manifest over a family of container images, all built from a single Seeknal commit and `uv.lock`.

Two parties consume this release manifest:
1. **Atlas Catalog**: Reads the manifest's `contracts` before dispatching work. Any runtime that does not satisfy the contract versions signed by Catalog is refused, ensuring version skew fails closed.
2. **Seeknal Projects**: A project pins a runtime line or exact release version in `seeknal_project.yml`.

The `seeknal.runtime` package provides data models, a strict schema validator, pin resolution logic, release ID calculation, and a command-line interface.

---

## 2. Manifest Schema (`apiVersion: atlas.io/v1`, `kind: RuntimeRelease`)

The manifest is authored in YAML format:

```yaml
apiVersion: atlas.io/v1
kind: RuntimeRelease
metadata:
  name: atlas-rt/1.0.3
  channel: stable
seeknal:
  version: 2.15.2
  commit: 0123456789abcdef0123456789abcdef01234567
  lock_sha256: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
python: 3.11.9
images:
  atlas-rt:
    digest: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    sbom: "https://atlas.io/sbom/atlas-rt/1.0.3"
    signature: cosign
  atlas-rt-serving:
    digest: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  atlas-rt-agent:
    digest: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  atlas-rt-sql:
    digest: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    starrocks: 4.0.2
engines:
  duckdb: 1.4.4
  duckdb_extensions:
    iceberg: 0123456789abcdef
    httpfs: 0123456789abcdef
contracts:
  grant:
    - v2
    - v3
  run_grant:
    - v1
  engine_api:
    - v1
  lineage:
    - openlineage-1
  cube_mode:
    - refuse
    - jwt
serving:
  grant_max_lifetime_seconds: 60
acu_weights:
  job: 1.0
  session: 1.5
  warehouse: 2.0
  sandbox: 1.0
licence_scan: clean
```

---

## 3. Validation Rules

All validation failures raise `seeknal.runtime.release.ManifestError`. The validator collects all validation errors across all fields and reports them together. Each error message names the dotted field path:

1. **Top-level envelope**:
   - `apiVersion`: Must be exactly `atlas.io/v1`.
   - `kind`: Must be exactly `RuntimeRelease`.
   - Unknown top-level keys are rejected.
2. **Metadata**:
   - `metadata.name`: Required, must match `^atlas-rt/(\d+)\.(\d+)\.(\d+)$`.
   - `metadata.channel`: Required, must be one of `stable`, `lts`, `preview`.
3. **Seeknal information**:
   - `seeknal.version`: Required semver `X.Y.Z`.
   - `seeknal.commit`: Required 40 lowercase hexadecimal characters (`^[0-9a-f]{40}$`).
   - `seeknal.lock_sha256`: Required 64 lowercase hexadecimal characters (`^[0-9a-f]{64}$`).
4. **Python runtime**:
   - `python`: Required, must match `^3\.\d+\.\d+$`.
5. **Container Images (`images`)**:
   - Must contain required images `atlas-rt` and `atlas-rt-serving`.
   - `atlas-rt-agent` and `atlas-rt-sql` are optional. Any other image key must match `^atlas-rt-[a-z0-9-]+$`.
   - Every image must specify `digest` matching `^sha256:[0-9a-f]{64}$`.
   - Image tags (e.g. `:latest` or `tag` fields) are rejected; digest-only pinning is strictly enforced.
   - Optional `sbom`: string URI.
   - Optional `signature`: string, only `cosign` is accepted.
   - Optional `starrocks`: semver `X.Y.Z`, only allowed on the `atlas-rt-sql` image.
6. **Engines (`engines`)**:
   - `engines.duckdb`: Required semver `X.Y.Z`.
   - `engines.duckdb_extensions`: Optional mapping of extension names to hexadecimal strings between 8 and 64 characters (`^[0-9a-fA-F]{8,64}$`).
7. **Contracts (`contracts`)**:
   - Keys must be chosen from `{grant, run_grant, engine_api, lineage, cube_mode}`.
   - Values must be non-empty lists of unique strings.
   - `contracts.grant`: Must be a subset of `{v1, v2, v3}`.
   - `contracts.cube_mode`: Must be a subset of `{refuse, jwt}`.
8. **Serving (`serving`)**:
   - `serving.grant_max_lifetime_seconds`: Required number in `(0, 60]`. Caps grant lifetime at 60 seconds.
9. **ACU Weights (`acu_weights`)**:
   - Must contain weights for `job`, `session`, `warehouse`, and `sandbox`.
   - All weights must be finite positive numbers up to 10 (`0 < weight <= 10.0`). Additional custom keys are permitted under the same rule.
10. **Licence Scan (`licence_scan`)**:
    - Must be exactly `clean`. Dirty licence scans fail closed.

---

## 4. Project Pin Semantics

In `seeknal_project.yml`, projects specify runtime release requirements:

```yaml
runtime: atlas-rt/1.0
environments:
  dev:
    principal: auto
  prod:
    principal: auto
    runtime: atlas-rt/1.0.3
```

- **Pin Formats**:
  - **Line pin**: `atlas-rt/X.Y` (e.g. `atlas-rt/1.0`).
  - **Exact pin**: `atlas-rt/X.Y.Z` (e.g. `atlas-rt/1.0.3`).
- **Environment Overrides**:
  - `read_runtime_pin(project_dir, environment=None)` checks `environments.<env>.runtime`. If set, it overrides top-level `runtime`. If neither is configured, returns `None`.
  - Invalid pin strings raise `PinError`. Other project keys are tolerated.
- **Resolution Rules (`resolve_pin`)**:
  - An **exact pin** (`atlas-rt/X.Y.Z`) matches a release by name across any channel (`stable`, `lts`, `preview`).
  - A **line pin** (`atlas-rt/X.Y`) resolves to the highest patch version (`Z`) matching `X.Y` on the specified channel (default: `stable`).
  - If no suitable release is found, `PinError` is raised.

---

## 5. Deterministic Project Release ID

The function `project_release_id(project_id, commit_sha, release, manifest_sha256)` generates a deterministic tracking string combining the project identity, code commit, runtime release, container digest, and manifest fingerprint:

$$\text{Format: } \texttt{<project\_id>@<commit\_sha>+<release.name>@<atlas-rt digest>\#<manifest\_sha256[:16]>}$$

Example:
`my-project@9fce3a1b+atlas-rt/1.0.0@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa#0123456789abcdef`

---

## 6. CLI Usage

The package can be executed via `python -m seeknal.runtime`:

### Validate Manifests

Validates one or more manifest files against the schema.
```bash
python -m seeknal.runtime validate path/to/manifest.yaml [path/to/another.yaml ...]
```
- If valid: prints `OK <release.name> <manifest_sha256>`. Exits 0 if all manifests are valid.
- If invalid: prints error list naming dotted field paths. Exits 1.

### Resolve Project Pin

Resolves a project pin against a directory of release manifests:
```bash
python -m seeknal.runtime resolve --project path/to/project --releases path/to/releases [--env prod] [--registry ghcr.io/mta-tech]
```
- Loads all `*.yaml` files in `--releases` (reporting and skipping invalid manifests).
- Reads the pin from `seeknal_project.yml` (applying environment overrides if `--env` is supplied).
- Resolves the pin and prints: `<release.name> <registry>/atlas-rt@sha256:...`.
- Exits 0 on success.
- Exits 2 if the project does not have a runtime pin configured.
- Exits 1 on resolution error or invalid configuration.

---

## 7. Python API

```python
from seeknal.runtime import (
    load_manifest,
    parse_manifest,
    manifest_sha256,
    supports,
    require_contracts,
    image_reference,
    read_runtime_pin,
    resolve_pin,
    project_release_id,
    RuntimeRelease,
    ImageRef,
    ManifestError,
    ContractMismatch,
    PinError,
)
```

- `load_manifest(path: str | Path) -> RuntimeRelease`
- `parse_manifest(data: Mapping[str, Any]) -> RuntimeRelease`
- `manifest_sha256(path: str | Path) -> str`
- `supports(release: RuntimeRelease, contract: str, version: str) -> bool`
- `require_contracts(release: RuntimeRelease, required: Mapping[str, str]) -> None`
- `image_reference(release: RuntimeRelease, image: str, registry: str) -> str`
- `read_runtime_pin(project_dir: str | Path, environment: str | None = None) -> str | None`
- `resolve_pin(pin: str, releases: Iterable[RuntimeRelease], *, channel: str = "stable") -> RuntimeRelease`
- `project_release_id(project_id: str, commit_sha: str, release: RuntimeRelease, manifest_sha256: str) -> str`
