# Conditional Iceberg upsert: implementation and verification

**PASS — 15 October 2026.** Implemented on `feat/iceberg-conditional-upsert`,
starting from Seeknal `v2.12.1` (`094ba65c2329d01aa6ba230e84d167fec5038aa6`).
The dataset is deterministic synthetic data, as requested; no customer or
production data was used.

## Implemented behavior

- Optional `when_matched_update_if`, `compare_columns`, and
  `when_not_matched: insert|skip` propagate through profiles, YAML targets,
  the legacy YAML helper, decorator configuration, and Python Iceberg output.
- A true predicate accepts a matched row; false or NULL preserves its target.
  New keys insert by default and may be skipped independently.
- Target comparison columns share the existing uniqueness scan. Predicate
  binding runs before staging, including for empty inputs and missing tables.
- A missing table with `skip` and `create_table: true` is a noop without
  namespace/table creation. `create_table: false` still fails if it is missing.
- Results distinguish original input, accepted rows, inserts, updates,
  condition skips, and skipped new keys. Dispatcher logs and persisted CLI
  run state expose the skip counts. Accepted identical rows can remain noops.
- PyIceberg 0.12 internal commit retries previously dropped the original
  snapshot assertion. Advanced writes now disable those retries on a private
  client table copy, without modifying persisted table properties. A concurrent
  writer produces a conflict; a fresh invocation re-evaluates the condition.
- Whole-record predicates see public schemas only. Internal row numbering does
  not change equality/hash semantics, and target projection order matches binding.

The optional `version_column` shorthand and full SQL MERGE are not implemented.
Python `IcebergStoreOutput` uses `None` to inherit defaults; YAML can explicitly
clear an inherited condition with null and an empty comparison list.

## Verification results

Primary environment: Python 3.12.14, DuckDB 1.5.0, PyArrow 23.0.1,
PyIceberg 0.12.0. This differs from the proposal's standalone DuckDB/PyArrow
versions; it is not a claim to have reproduced those exact versions.

| Verification | Result |
| --- | --- |
| Relevant configuration, profile, dispatcher, routing, mutation, DAG normalization, pipeline config, REST, and all three new E2E tests | **264 passed, 4 skipped** |
| Mutation and REST suite in a separate PyIceberg 0.10.0 environment | **36 passed** |
| Compile check and `git diff --check` | Passed |
| Independent read-only correctness review | Whole-record helper-field defect found, reproduced, corrected, and regression-tested |

The four skips are existing `test_materialization.py` schema compatibility
tests marked as requiring connection mocking. The primary suite emitted 85
warnings; passing does not mean warning-free. This is a focused regression
suite, not the complete repository test suite.

The REST endpoint is the repository's authenticated loopback fixture backed by
a real PyIceberg SQL catalog and local Iceberg metadata/Parquet. No mocks replace
the writer, catalog client, CLI pipeline, or readback. This is not a live
Lakekeeper, S3, production authentication, or distributed multi-process test.

### Complex synthetic workload

Two principal tables each start with 2,400 rows, composite tenant/id keys,
timestamps (including NULL), status, amount, and extract/load metadata.

| Case | Observed result |
| --- | --- |
| Version comparison, including newer/equal/older/NULL values and 300 new keys | 2,700 input; 1,500 accepted (1,200 updates and 300 inserts); 1,200 condition skips |
| Old window extracted later, same window extracted later, next window, and new-key skip | 2,700 input; 1,200 updates; 1,200 condition skips; 300 new keys skipped |
| All matched rows rejected | 2,000 skipped; snapshot and contents unchanged |
| Business columns unchanged, load timestamp changed | 2,400 skipped; snapshot and contents unchanged |
| Duplicate source keys | Rejected before publication; snapshot unchanged |
| Externally introduced duplicate target keys | Rejected before publication; snapshot unchanged |
| Missing target with skip | 12 skipped; table not created |

Successful table contents were compared exactly with an independently
constructed expected result using both PyIceberg and a separate DuckDB
`iceberg_scan` connection. The CLI receipt includes SHA-256 row digests.

The [large-workload receipt](iceberg-conditional-upsert-2026-10-15.json) records
an initial standalone CLI run of the seven-case workload in 210.03 seconds
under shared CPU contention. The same workload also passed as part of the
final pytest run. The earlier attempt before extension dependencies were
installed failed; it is not counted as a passing run. These are functional
checks, not throughput benchmarks.

### Actual Seeknal CLI pipeline

`seeknal run --full --profile …` read a synthetic Parquet source, built its
manifest from YAML, executed source and SQL transform nodes, removed duplicate
keys with `QUALIFY ROW_NUMBER()`, and materialized through conditional upsert
to REST. The test inspected persisted `run_state.json`, then compared exact
Iceberg contents using both readers.

- First run: 60 source/transform rows, 60 inserted.
- Second run: 78 raw source rows, 72 after SQL deduplication; 20 updated,
  12 inserted, and 40 condition skips. Final table: 72 rows.
- Writer input was correctly reported as 72, independently of raw source count.

See the [pipeline receipt](iceberg-conditional-upsert-2026-10-15-pipeline.json)
and the smaller [YAML/profile/dispatcher receipt](iceberg-conditional-upsert-2026-10-15-yaml.json).
The actual CLI has its own execution loop; this does not claim to exercise the
separate `DAGRunner` class in that scenario. The `--start-date`/`--end-date`
options were not invoked; window-order semantics were checked with explicit
synthetic window timestamps in the large workload.

Separate REST regressions inject commit failures and a newer same-key winner
between preflight and commit. They assert one commit attempt, a conflict result,
preservation of the winner and the stored retry property, and a noop when a
fresh invocation compares against that winner.

## Reproduce from the checkout

Install the project's runtime and test dependencies and signed DuckDB `iceberg`
extension (including its `avro` and `httpfs` dependencies). Then run:

```bash
PYTHONPATH=src:. python tools/validate_conditional_upsert.py --output workload.json
PYTHONPATH=src:. python tools/validate_conditional_upsert.py --yaml-only --output yaml-smoke.json
PYTHONPATH=src:. python tools/validate_conditional_upsert.py --pipeline-only --output pipeline-smoke.json
```

The new integration test file exercises all three workloads. The temporary
catalogs/data are removed after the standalone tool exits; JSON receipts remain.
Projected comparisons add I/O, memory, and join work. PyIceberg still performs
its own upsert scan; these tests establish correctness, not a zero-cost scan
claim or a production-scale performance guarantee.
