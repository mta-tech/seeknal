# Dependency security validation — 20 October 2026

Based on current main `9211d60` (v2.13.0). Open PRs #112, #106 and #92 were
checked before starting; none implements these dependency fixes. The primary Mac
checkout's unrelated changes were preserved in place. Work uses a separate worktree.

## Verified advisories and resolution

| Package | Main lock | Candidate lock | Advisory / applicability |
| --- | --- | --- | --- |
| pydantic-ai-slim | 1.87.0 | 1.107.4 | [GHSA-h4xc-3qfq-jf93](https://github.com/pydantic/pydantic-ai/security/advisories/GHSA-h4xc-3qfq-jf93): web UI `Agent.to_web()` / `clai web`; no repository sink found |
| fsspec | 2026.1.0 | 2026.6.0 | [GHSA-27vj-qcqg-25rc](https://github.com/fsspec/filesystem_spec/security/advisories/GHSA-27vj-qcqg-25rc): untrusted ReferenceFileSystem metadata; no repository sink found |
| aiohttp | 3.13.5 | 3.14.4 | [GHSA-2g87-pp6c-29x5](https://github.com/aio-libs/aiohttp/security/advisories/GHSA-2g87-pp6c-29x5): resource exhaustion; repository uses HTTP client callbacks; this GHSA lists no known CVE |
| multidict | 6.7.0 | 6.9.1 | [GHSA-54p9-h82j-f925](https://github.com/aio-libs/multidict/security/advisories/GHSA-54p9-h82j-f925): C items-view union/subtraction reference leak |
| Mako | 1.3.10 | 1.4.2 | [GHSA-5639-2j2p-m4mx](https://github.com/sqlalchemy/mako/security/advisories/GHSA-5639-2j2p-m4mx): Windows drive-letter template traversal; installed through Alembic |
| Apprise (Prefect extra) | 1.9.7 | 1.11.0 | [GHSA-856c-92hv-3vxx](https://github.com/caronc/apprise/security/advisories/GHSA-856c-92hv-3vxx): redirect/header controls |

Apprise still follows redirects by default after upgrading. Operators using
notification destinations they do not fully trust must choose `redirect=no` or
`AppriseAsset(http_redirects=False)` as described upstream. This change does not
alter runtime notification behavior or claim the upgrade disables redirects.

s3fs also moves from 2026.1.0 to 2026.6.0 because the older release pins vulnerable
fsspec exactly. Its new metadata permits `fsspec>=2026.6.0,<2026.6.1`. The lock
retains unrelated versions where possible. Required supporting changes are
pydantic-graph 1.107.4, genai-prices 0.0.62 (and its httpx2/httpcore2/truststore
requirements), google-genai 1.70.0, google-auth 2.49.0, and yarl 1.25.1.
The existing pydantic-deep 0.3.19 and Prefect 3.6.13 remain unchanged.

Both core and backwards-compatible Ask declarations enforce the AI security
floor. Core transitive floors also protect wheel/manifest installs. Prefect's
Apprise floor remains optional. Gateway and worker Dockerfiles enforce the same
aiohttp minimum. The gateway additionally needs `COPY docs/cli/ ./docs/cli/` to
satisfy the existing Hatch forced include; the actual build exposed this omission.
PR #112 addresses a separate Prefect Dockerfile.

## Public real-data validation

[Palmer Penguins](https://allisonhorst.github.io/palmerpenguins/) contains observed
measurements collected by Kristen Gorman and Palmer Station LTER. We use the
[Seaborn seven-column CSV](https://github.com/mwaskom/seaborn-data/blob/master/penguins.csv):
344 rows, 7 columns, 13,478 bytes. SHA-256:
`e07636bd8af74260099ea2f8678e2eabbf35def579940cc76f67061ee16c06c1`.
Missing measurements are retained. No private datasets or production resources
were used, no dataset was sent to a model provider, and raw rows are not committed.

Disposable infrastructure: SeaweedFS 3.85 S3 bound to `127.0.0.1:19240`, authenticated
loopback Iceberg REST backed by a temporary PyIceberg SQL catalog, and Temporal CLI
1.3.0 / server 1.27.1 bound to `127.0.0.1:19243`. MinIO's requested image was
unavailable. SeaweedFS needs test-only `AWS_REQUEST_CHECKSUM_CALCULATION=when_required`
and `AWS_RESPONSE_CHECKSUM_VALIDATION=when_required` because its older server does
not decode newer AWS streaming checksums. The script sets these locally and strips
ambient deployment endpoints/credentials. No production configuration changes.

`tools/validate_dependency_real_data.py` uploads, downloads and parses the public
CSV through s3fs, then writes actual S3 Iceberg metadata and Parquet using
`pyiceberg.io.fsspec.FsspecFileIO`. Surrogate row IDs and batch-version columns are
**test metadata**: these are not claims of naturally observed revision events.
All seven observed columns remain unchanged. The workload asserts:

- 300 initial inserts; a 344-row replay inserts 44 new keys and skips 300 equal versions.
- 20 higher-version updates; 20 older-version attempts skip, retaining the snapshot.
- Exact 344-row equality after normalized Arrow types through both PyIceberg and
  a separate DuckDB `iceberg_scan` reader, including all observed columns and nulls.
- Actual Seeknal deep-agent construction and `execute_sql` against the Parquet
  project data, yielding Adelie 152, Chinstrap 68 and Gentoo 124. A deterministic
  local `FunctionModel` drives the tool call; no hosted model quality is claimed.
- A real Temporal workflow and activity deliver two authenticated aiohttp HTTP
  callbacks (`answer`, `done`) containing the SQL answer. The activity's stream
  boundary is deterministic; this is not an external LLM end-to-end test.

The workload passed before and after the dependency upgrade with identical
functional assertions and aggregates. The [receipt](dependency-security-2026-10-20.json)
records both version sets. Each run uses a unique S3 prefix and removes its objects.

Reproduce after launching disposable S3 and Temporal services:

```sh
curl -fL https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv -o /tmp/penguins.csv
uv sync --frozen --dev --extra ask --extra temporal --extra prefect
PYTHONPATH=src:. .venv/bin/python tools/validate_dependency_real_data.py \
  --csv /tmp/penguins.csv --output /tmp/dependency-real-data.json
```

## Checks and limits

| Check | Result |
| --- | --- |
| Targeted lock resolution; `uv lock --check` | Passed |
| `uv sync --frozen --dev --extra ask --extra temporal --extra prefect` | Passed |
| `uv pip check` | All installed packages compatible |
| `uv build` wheel and source distribution | Passed |
| Ask suite + Iceberg mutation/config/REST regressions | 1,419 passed, 67 skipped, 22 warnings |
| Optional Prefect regression suite | 32 passed, 3 skipped |
| Public real-data workload on main and candidate | Passed with identical assertions |
| Mac CI suite excluding the native-crashing module | 5,009 passed, 75 skipped, 23 warnings (450.50 seconds) |
| Full Mac CI command / large synthetic conditional-upsert test | Native illegal-instruction exit 132; reproduced on unchanged main |
| Gateway Docker build and runtime imports | Passed; installs patched versions independently of the lock |
| Worker Docker build | Passed |
| Bounded multidict reference-count probe (1,000 operands) | Old union/subtraction leak 1,000 references each; patched versions leak zero, intersection control zero |
| Non-executing fsspec template introspection | Old version exposes a module name; patched version does not generate that reference |

The Mac native crash is an existing limitation, not a passing test. The four-test
synthetic end-to-end module remains part of remote Linux CI. A local Linux test
container and worker runtime container stayed in Docker `Created` state with no
process started; those local executions are unrun, not passed. Both image builds
completed; gateway runtime imports/version inspection completed. Spark-specific tests
and external-provider QA are skipped when their optional infrastructure is absent.
No live Lakekeeper, production cloud IAM, real provider inference, or Windows
execution is claimed. The bounded dataset verifies correctness, not throughput.
