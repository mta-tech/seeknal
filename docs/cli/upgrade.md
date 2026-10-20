---
summary: Upgrade to Seeknal 2.13.x and migrate existing pipeline configuration
read_when: You are upgrading a 2.12.1 project or checking conditional Iceberg backfill behavior
related:
  - run
  - iceberg-upsert
  - docs
---

# Upgrading to 2.13.x

2.13.0 was [released on 2026-10-20](https://github.com/mta-tech/seeknal/releases/tag/v2.13.0),
based on 2.12.1. This branch prepares the unreleased 2.13.1 patch, which changes
version metadata and release documentation without runtime or dependency changes.
No additional configuration migration is required from 2.13.0 to this patch.
`pip install seeknal` continues to select a published package.

Read this guide from an installation with `seeknal docs upgrade`.
This topic does not add an upgrade command.

## 1. Align catalog environment variables

Prefer one catalog URI name:

```bash
export LAKEKEEPER_URI="https://catalog.example.com"
```

`LAKEKEEPER_URL` continues to work when URI is unset, empty, or whitespace.
If both contain different values, URI wins across source, preview, REPL,
snapshot metadata, materialization and credential fallback paths. Review an
existing project that relied on the two names selecting different catalogs.
Explicit node/profile catalog settings keep their precedence over environment
fallbacks. Retain existing warehouse and authentication configuration.

## 2. Check date and parameter behavior

```bash
seeknal run --date 2026-10-12 --params '{"region": "US"}' --profile profiles.yml
```

`--date` sets `today`, `date`, and `run_date`. For this invocation,
`{{ today(-1) }}` and `{{ yesterday }}` resolve to `2026-10-11`,
`{{ month_start }}` to `2026-10-01`, and `{{ year_start }}` to `2026-01-01`.
`--date` and a `today` supplied through `--params` are validated against the
CLI date contract (use YYYY-MM-DD, years 2000–2100 and at most 365 days in the
future). Dedicated flags override the same keys in `--params`.

- YAML `params`, transform SQL and source `table` paths are resolved during
  DAG construction, including a transform without a `params` block.
- Python decorator parameters reach `ctx.params`; CLI overrides replace
  declared keys. Non-template Python values retain their types.
- Unset `{{ env:VAR }}` with no default now fails construction instead of
  surviving as literal SQL. Set the variable or use `{{ env:VAR|default }}`.
- Python decorator parameters and feature-group/SOA parameters now affect
  fingerprints. Expect a one-time rerun; subsequent functional parameter
  changes invalidate the affected nodes.

`--start-date`/`--end-date` supply template values; `--start`/`--end` drive
interval execution. They are different options. `--full` ignores the cache
for the DAG, subject to `--exclude-tags`, and cannot be combined with `--tags`,
`--nodes`, or `--types`; it does not change an Iceberg target's configured write mode.

## 3. Choose an explicit upsert policy

Existing `mode: upsert` remains unconditional when no predicate is configured
or inherited. To protect newer stored rows:

```yaml
# Within a transform's materializations list
materializations:
  - type: iceberg
    table: lake.analytics.customers
    mode: upsert
    unique_keys: [customer_id]
    when_matched_update_if: |
      target.updated_at IS NULL OR source.updated_at > target.updated_at
    compare_columns: [updated_at]
    when_not_matched: insert
```

Only a true predicate accepts a matched row. False and NULL preserve it; new
keys insert independently unless `when_not_matched: skip` is set. Source rows
must be complete, with non-null unique keys. Deduplicate within transform SQL
before writing. A predicate is not a deduplication or schema-evolution policy.

For older-window backfills, compare a stable source window before extraction
time; a later load time alone must not make old data win. Define NULL and
equal-version tie behavior explicitly. See `seeknal docs iceberg-upsert` for
window ordering, NULL-safe business comparisons and complete examples.

Omitted node values inherit profile defaults. To clear an inherited predicate
in a YAML target dictionary, set `when_matched_update_if: null` together with
`compare_columns: []`; reset inherited skip with `when_not_matched: insert`.
`None` in typed pipeline configuration and `IcebergStoreOutput` means
inheritance, so it does not explicitly clear a predicate.

## 4. Interpret empty results and write outcomes

A SQL SELECT with zero rows publishes a schema-preserving empty intermediate
parquet, replacing the previous run's output. Downstream nodes see the empty
batch. A failed publication fails the node; it does not continue with stale
data. A DDL-only transform with no SELECT has no such output to publish.

For conditional upsert, `input_row_count` counts rows after transform SQL;
`row_count` counts accepted rows. `skipped_by_condition` counts rejected
matched rows and `skipped_not_matched` counts skipped new keys. Accepted
identical rows can be noops, so accepted need not equal inserted plus updated.
Inspect write results under node metadata in `target/run_state.json`.

An existing target keeps its snapshot if no rows survive the upsert filter.
For a missing target, `when_not_matched: skip` with `create_table: true` does
not create a table or namespace; `create_table: false` still fails.

Snapshot conflicts fail rather than internally rebasing a stale predicate
decision. Reconcile the concurrent write and invoke the pipeline again.
For an unknown commit outcome, inspect the target before retrying. Generic
pipeline retries do not automatically retry conflict or unknown outcomes.

## 5. Check custom session identifiers and installed docs

Custom tenant/session names must be 1–128 ASCII letters, digits, dots,
underscores or hyphens, excluding `.` and `..`. Absolute paths, separators,
and symlinks resolving to or outside the storage base are rejected. Existing
projects using malformed custom names need valid identifiers; there is no
automatic identifier migration.

CLI topics are bundled in wheel/sdist. Installed topics take precedence over
a current project's `docs/cli`; source checkouts use repository topics when
bundled topics are absent. Exact topics and `--list` work without ripgrep;
full-text search requires `rg`.

```bash
seeknal docs --list
seeknal docs --json iceberg-upsert
seeknal docs --json upgrade
```

Advanced Iceberg writes require PyIceberg 0.10+ at runtime. Conditional joins
can use more memory than the input batch cap. The 2,400-row macOS ARM native
crash remains unresolved although that case passes Linux CI. Consult the
[2.13.0 release notes](../releases/2.13.0.md) for tested versions and validation limits.
