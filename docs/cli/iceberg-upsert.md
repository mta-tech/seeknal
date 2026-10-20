---
summary: Iceberg upsert with conditional updates, version ordering, and skipped-row counts
read_when: You need to protect Iceberg rows from older backfills or avoid unnecessary updates
related:
  - run
  - docs
---

# Conditional Iceberg upsert

These conditional options are part of the unreleased 2.13.0 integration
candidate in [draft PR #111](https://github.com/mta-tech/seeknal/pull/111).
See [upgrade guidance](upgrade.md) before moving an existing 2.12.1 project.

Use `mode: upsert` with explicit `unique_keys` to replace matched rows and
insert new keys. Add `when_matched_update_if` to accept a matched incoming row
only when its SQL predicate is true. An omitted predicate preserves the usual
upsert behavior. The incoming batch must contain complete rows, with unique,
non-null keys; deduplicate within the node's SQL before materialization.

Read this configuration topic with `seeknal docs iceberg-upsert`.

## Configure a target

Keep the REST catalog and credentials in your existing `profiles.yml` and
environment. Set the conditional options on a transform or feature group:

```yaml
# seeknal/transforms/silver_customers.yml
kind: transform
name: silver_customers
inputs:
  - ref: source.customers
transform: |
  SELECT * FROM input_0
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id ORDER BY updated_at DESC NULLS LAST
  ) = 1
materialization:
  - type: iceberg
    table: lake.analytics.silver_customers
    mode: upsert
    unique_keys: [customer_id]
    when_matched_update_if: |
      target.updated_at IS NULL OR source.updated_at > target.updated_at
    compare_columns: [updated_at]
    when_not_matched: insert
```

Run the pipeline using the selected profile:

```bash
seeknal run --full --profile profiles.yml
```

`--full` runs the DAG regardless of cache, subject to `--exclude-tags`; the
target still uses the configured upsert mode. It does not turn the write into
a table overwrite. The SQL above chooses
one row per incoming key. If equal timestamps can carry different values, add
a deterministic tie-breaker to that SQL and to the update predicate as needed.

## Options and predicate scope

| Option | Default | Behavior |
| --- | --- | --- |
| `when_matched_update_if` | Absent | DuckDB SQL predicate over incoming `source` and existing `target` rows |
| `compare_columns` | `[]` | Additional target columns exposed to the predicate; requires a predicate |
| `when_not_matched` | `insert` | Insert new keys, or use `skip` to leave them out |

These settings apply only to `upsert`. `source` exposes the complete incoming
row. `target` exposes the unique keys plus `compare_columns`. Every other
target column is unavailable. Source-only predicates and predicates using only
target keys can leave `compare_columns` empty.

A matched row is accepted only when the predicate is true. False and NULL
preserve the existing target row. New keys do not depend on the predicate;
`when_not_matched` controls them separately.

For the timestamp predicate above:

| Incoming timestamp | Target timestamp | Matched-row decision |
| --- | --- | --- |
| Newer | Older | Accept |
| Equal | Equal | Skip |
| Older | Newer | Skip |
| NULL | Non-NULL | Skip |
| Any, including NULL | NULL | Accept |

Predicates are trusted project SQL, with the same trust expectation as
transform SQL. Seeknal binds them against empty public schemas before staging
a write, even for an empty batch or a missing table. Undeclared target columns
fail preflight. Value-dependent expression failures also stop the write before
staging. This is whole-row replacement, without per-column SET or DELETE rules.

## Order by window, then extraction time

For data without a business update timestamp, a fresh extraction of an older
window must not outrank a later window. Use a stable window timestamp first:

```yaml
when_matched_update_if: |
  source._partition_timestamp > target._partition_timestamp
  OR (source._partition_timestamp = target._partition_timestamp
      AND source._extract_timestamp > target._extract_timestamp)
compare_columns: [_partition_timestamp, _extract_timestamp]
```

The window value must describe the source window, rather than the time at
which the rerun happens. Add explicit NULL handling if either timestamp may be
missing; SQL comparisons with NULL otherwise do not pass the condition.

## Ignore changes to load metadata

Use NULL-safe comparisons of business columns to preserve rows when only an
extraction or load timestamp changed:

```yaml
when_matched_update_if: |
  source.status IS DISTINCT FROM target.status
  OR source.amount IS DISTINCT FROM target.amount
compare_columns: [status, amount]
```

## Profile defaults and overrides

These options may also be defaults in the existing profile:

```yaml
# Merge into profiles.yml; retain your materialization.catalog settings.
materialization:
  default_mode: upsert
  unique_keys: [customer_id]
  when_matched_update_if: source.updated_at > target.updated_at
  compare_columns: [updated_at]
  when_not_matched: insert
```

Node settings override profile defaults. An omitted setting inherits its
profile value. To clear an inherited predicate in node YAML, clear its target
projection too:

```yaml
when_matched_update_if: null
compare_columns: []
```

Set `when_not_matched: insert` explicitly to reset an inherited `skip` policy.
In Python's typed pipeline `MaterializationConfig` and `IcebergStoreOutput`,
`None` means inheritance, rather than the explicit predicate clearing
supported by node YAML target dictionaries. There is no
`version_column` shorthand; express timestamp and tie-breaker rules explicitly.

## Python pipeline configuration

The public typed configuration preserves the same conditional options through
decorator registration and DAG discovery. This example assumes the upstream
source already has one complete row per non-null `customer_id`:

```python
from seeknal.pipeline import transform
from seeknal.pipeline.materialization_config import MaterializationConfig

@transform(
    name="silver_customers",
    inputs=["source.customers"],
    materialization=MaterializationConfig(
        enabled=True,
        table="lake.analytics.silver_customers",
        mode="upsert",
        unique_keys=["customer_id"],
        when_matched_update_if="source.updated_at > target.updated_at",
        compare_columns=["updated_at"],
        when_not_matched="skip",
    ),
)
def silver_customers(ctx):
    return ctx.ref("source.customers")
```

Here an incoming NULL or a target NULL does not pass the strict comparison.
Use the explicit NULL-handling predicate from the YAML example if a target
NULL should accept an incoming value. Stackable `@materialize` target
dictionaries accept these options too; specify `mode="upsert"` there because
the stackable decorator's omitted mode defaults to append.

## Noops, counts, and conflicts

- With `when_not_matched: skip` and `create_table: true`, a missing target is a
  noop: no table or namespace is created. `create_table: false` still fails for
  a missing target.
- If every incoming row is rejected, an existing table retains its snapshot
  and no commit is attempted. Conditions do not bypass schema checks or
  duplicate-key validation.
- `input_row_count` is the original node-output batch size; `row_count` is the
  number accepted by the filter. Source rows removed by transform SQL are not
  part of this input count.
- `skipped_by_condition` counts rejected matched rows. `skipped_not_matched`
  counts new keys excluded by `skip`. Both appear in dispatcher logs and the
  materialization write result in `target/run_state.json`.
- Accepted rows identical to their targets can remain PyIceberg noops, so
  accepted rows need not equal inserted plus updated rows.
- A target snapshot change between preflight and commit fails with
  `failure_category: conflict`. Advanced writes disable PyIceberg's internal
  snapshot-rebase retry for that commit, without changing stored properties.
  Reconcile the concurrent write and invoke the pipeline again to evaluate the
  condition against the current target. Generic pipeline retry does not retry
  conflict or unknown-commit outcomes automatically.

Comparison columns share the existing target uniqueness scan. Reading them
adds I/O, memory, and join work; PyIceberg still performs its own upsert scan.
Key filtering does not guarantee file or partition pruning. `max_batch_bytes`
limits the original Arrow input, not total join/writer working memory.

Advanced writes require PyIceberg 0.10+ at runtime. The large macOS ARM
composite-key scan crash is still unresolved although the same workload
passes Linux CI. See the [candidate verification notes](../releases/2.13.0.md#known-limits)
for the tested versions and platform boundary.

## See also

- [Iceberg materialization guide](../iceberg-materialization.md#conditional-updates)
- [seeknal run](run.md)
- [Implementation and synthetic end-to-end verification](../evaluations/iceberg-conditional-upsert-2026-10-15.md)
