# Iceberg write modes: real-stack validation

**PASS — 16 September 2026.** The public Seeknal `write_to_iceberg` API was exercised against an actual PostgreSQL source, Lakekeeper REST catalog, and SeaweedFS S3-compatible storage. All source records were deterministic synthetic BPOM-style data; original BPOM data was not used or restored.

The [machine-readable execution receipt](iceberg-write-modes-real-stack-2026-09-16.json) is copied byte-for-byte from the completed run. Its SHA-256 is `3ae83891f8bead0f9b34e0606231a90747248b069768aebf94002ecb42910aed`.

## Tested code and environment

- Feature implementation: `77328f7d64118b5c29956430f1e50594550e9904`.
- Packaged source: `6f5730148cb9fbb5bc9b7dc9d36d85d9b61678ab` (the additional change is the visual documentation).
- Wheel SHA-256: `2aa2da82a540fd469437300cc02175af15c2501f2b3e9562ec429d39f05737f7`.
- Seeknal 2.11.1, Python 3.11.16, DuckDB 1.4.3, PyIceberg 0.10.0, PyArrow 23.0.0.
- Deployment inventory recorded PostgreSQL 17.11, Lakekeeper v0.13.5, SeaweedFS 4.36, Docker Engine 29.8.1, and Docker Compose 5.5.1. Catalog migration exited successfully and four services were healthy at verification time. Those infrastructure checks were recorded separately from the execution receipt.
- Services were isolated to host-loopback ports and a private Docker network. The client ran inside that network. No mocks replaced the writer, catalog, database, or object store.

## Data and results

The source database contained 32 balai, 12 commodity, 64 location, and 90 date dimension rows; two fact tables contained 25,000 and 12,000 records. Prepared update/replacement inputs supplied controlled changes for the two modes.

| Check | Observed result |
| --- | --- |
| Initial dimension creation | 32 rows; contents compared with the PostgreSQL input |
| Keyed upsert | 8 updates and 8 inserts; exact final contents contained 40 rows |
| Identical upsert replay | No-op; snapshot and contents unchanged |
| Initial daily mart | 1,440 aggregate rows partitioned by date |
| Partition replacement | Two date partitions replaced with 8 input rows; exact final contents contained 1,416 rows |
| Unaffected partitions | Every unaffected row compared with the pre-write contents |
| Empty batch | No-op; snapshot and digest unchanged |
| Duplicate/null keys | Rejected as `preflight_failed`; snapshot and row digest unchanged |
| Actual S3 objects | 93 current data files and 2 metadata files verified to exist |
| Independent readback | A separate DuckDB connection read Iceberg metadata directly from S3; counts and SHA-256 row digests matched the PyIceberg results |
| Source protection | Both read sessions were read-only; before/after counts matched for all eight checked objects, and content checksums matched for the dimension, delta, and replacement inputs |

The read-only PostgreSQL role was additionally checked to permit SELECT and deny INSERT, UPDATE, and DELETE. Full content checksums were not collected for the fact tables.

The run started at `2026-09-16T09:05:39.219440Z` and finished at `2026-09-16T09:05:48.283585Z`. Elapsed time was 9.064 seconds, including extension loading and readback. This is functional-test timing, not a throughput or scalability benchmark. The namespace and tables were retained for inspection.

## Coverage boundaries

This extends the local REST/FileIO fixtures with real remote PostgreSQL, Lakekeeper, and S3 storage evidence. It does not establish production OIDC/TLS behavior or execution of the original BPOM project's complete CLI/YAML pipeline. The earlier automated suite covers CLI/profile propagation, failure-state reporting, and suppression of retries after conflicts or unknown commit outcomes; those scenarios are not claimed as repeated by this remote run.

The feature's compatibility and usage limits remain unchanged: complete incoming identity partitions, explicit upsert keys, no SCD Type 2/CDC delete support, and no automatic schema or partition evolution. Failed writes can leave unreferenced files; the documented orphan-retention procedure still applies.

Existing regression evidence at the implementation commit remains **402 passed, 4 pre-existing skips**, with **77 focused QA tests passed**. Legacy lint/typecheck findings remain outside this change. The added report and receipt do not modify runtime code.
