# Chapter 10: Iceberg Incremental Processing

> **Duration:** 30 minutes | **Difficulty:** Intermediate | **Format:** YAML & CLI

Learn how Seeknal detects Iceberg data changes via snapshot comparison, performs partition-pruned incremental reads with watermark tracking, and cascades re-execution through mixed-source pipelines.

---

## What You'll Build

An incremental pipeline that loads Iceberg data efficiently:

```
Iceberg (events)  ──→  transform.event_summary  ──→  transform.enriched_events
                         (aggregation)                   (JOIN with CSV)
CSV (categories)  ─────────────────────────────────────┘
                         ▲                           ▲
                    watermark tracked            selective cascade
                    in run_state.json            (only changed branches)
```

**After this chapter, you'll have:**

- Snapshot-based change detection that skips unchanged Iceberg sources
- Partition-pruned incremental reads using `freshness.time_column`
- Watermark tracking that advances automatically after each run
- NULL-safe incremental filters that never silently drop rows
- Mixed-source cascade where only affected branches re-execute
- Full refresh override with `--full` to reset incremental state

---

## Prerequisites

Before starting, ensure you have:

- [ ] [Chapter 9: Database & External Sources](9-database-sources.md) — Iceberg source basics and profiles
- [ ] Access to a Lakekeeper REST catalog with at least one Iceberg table
- [ ] S3/MinIO and OAuth2 environment variables configured (see Chapter 9, Part 4)

---

## Part 1: Snapshot-Based Change Detection (10 minutes)

### How Seeknal Detects Iceberg Changes

Unlike file-based sources (where Seeknal hashes file size and modification time), Iceberg tables have **snapshot IDs** — immutable identifiers that change whenever data is written. Seeknal uses these for change detection:

1. Before executing an Iceberg source, the runner queries the REST catalog API for the current snapshot ID
2. Compares it with the snapshot ID stored in `run_state.json` from the last successful run
3. If the IDs match, the source is marked **CACHED** and skipped
4. If they differ (or the catalog is unreachable), the source re-executes

This happens automatically — no configuration needed. Every Iceberg source gets snapshot-based caching for free.

### Create an Iceberg Pipeline

Start with a source and a downstream transform:

```bash
seeknal draft source events
seeknal draft transform event_summary
```

Edit `draft_source_events.yml`:

```yaml
kind: source
name: events
description: "Signal events from Iceberg lakehouse"
source: iceberg
table: atlas.signals.events
```

!!! info "source_defaults"
    If your profile has `source_defaults.iceberg` configured (see Chapter 9), you don't need `catalog_uri` or `warehouse` in the source YAML.

Edit `draft_transform_event_summary.yml`:

```yaml
kind: transform
name: event_summary
description: "Daily event counts by category"
inputs:
  - ref: source.events
transform: |
  SELECT
      category,
      CAST(event_time AS DATE) AS event_date,
      CAST(COUNT(*) AS BIGINT) AS event_count,
      CAST(SUM(value) AS DOUBLE) AS total_value
  FROM ref('source.events')
  GROUP BY category, CAST(event_time AS DATE)
```

!!! warning "HUGEINT Casting"
    Always `CAST(COUNT(*) AS BIGINT)` and `CAST(SUM(...) AS DOUBLE)` in transforms that read from Iceberg. DuckDB's default `HUGEINT` type is not compatible with Iceberg writes.

Apply both nodes:

```bash
seeknal apply draft_source_events.yml
seeknal apply draft_transform_event_summary.yml
```

### Run 1: First Execution

```bash
seeknal run --profile profiles.yml --full
```

On the first run, `--full` ensures all nodes execute regardless of state. After completion, inspect the stored state:

```bash
cat target/run_state.json | python -m json.tool
```

Look for the Iceberg-specific fields in the source node:

```json
{
  "nodes": {
    "source.events": {
      "status": "success",
      "iceberg_snapshot_id": "4837265091827364",
      "iceberg_table_ref": "atlas.signals.events",
      "last_watermark": null
    }
  }
}
```

The `iceberg_snapshot_id` is now stored. Future runs will compare against this value.

### Run 2: No Data Changes

```bash
seeknal run --profile profiles.yml
```

Expected output:

```
1/2: Skipping events (cached)
2/2: Skipping event_summary (cached)
No changes detected. Nothing to run.
```

Both nodes are cached because the Iceberg snapshot ID hasn't changed and the transform YAML hasn't changed.

### Run 3: After Data Changes

Insert new rows into your Iceberg table (using your preferred tool — Spark, Trino, or direct API). Then run again:

```bash
seeknal run --profile profiles.yml
```

Expected output:

```
1/2: Executing events (iceberg data changed)
  events completed in 1.23s
2/2: Executing event_summary (downstream of changed node)
  event_summary completed in 0.45s
```

Seeknal detected the new snapshot ID, re-executed the source, and cascaded to the downstream transform.

**Checkpoint:** Pipeline correctly caches when data is unchanged and re-executes when the Iceberg snapshot changes.

---

## Part 2: Partition-Pruned Incremental Reads (10 minutes)

### Enable Watermark Tracking

Without `freshness.time_column`, every re-execution loads the **entire** Iceberg table. For large tables, this is wasteful. Add a `freshness` block to enable incremental reads:

```yaml
kind: source
name: events
description: "Signal events from Iceberg lakehouse"
source: iceberg
table: atlas.signals.events
freshness:
  time_column: event_time
```

The `freshness.time_column` tells Seeknal which column tracks event time. When a watermark exists from a previous run, Seeknal injects a WHERE clause:

```sql
SELECT * FROM atlas.signals.events
WHERE "event_time" > CAST('2026-01-15 10:30:00' AS TIMESTAMP)
   OR "event_time" IS NULL
```

This filters at the Iceberg scan level, leveraging partition pruning for large tables.

### Run the Incremental Pipeline

**Run 1 (full scan):** On the first run (or after `--full`), there is no stored watermark, so Seeknal loads all rows:

```bash
seeknal run --profile profiles.yml --full
```

After completion, check the watermark:

```bash
cat target/run_state.json | python -m json.tool | grep last_watermark
```

```json
"last_watermark": "2026-01-15 10:30:00"
```

The watermark is set to `MAX(event_time)` from the loaded data.

**Insert new rows** with timestamps after the watermark (e.g., `2026-01-16 08:00:00`).

**Run 2 (incremental):**

```bash
seeknal run --profile profiles.yml
```

Expected output:

```
1/2: Executing events (iceberg data changed)
  events completed in 0.31s    ← faster: only new rows loaded
2/2: Executing event_summary (downstream of changed node)
  event_summary completed in 0.22s
```

Only rows with `event_time > '2026-01-15 10:30:00'` were loaded. The watermark advances to the new `MAX(event_time)`.

**Checkpoint:** Incremental reads load only new rows, and the watermark advances after each run.

### NULL Handling

The incremental WHERE clause always includes `OR "event_time" IS NULL`:

```sql
WHERE "event_time" > CAST('2026-01-15 10:30:00' AS TIMESTAMP)
   OR "event_time" IS NULL
```

This ensures rows with NULL timestamps are never silently excluded from incremental batches. If your Iceberg table has rows where the time column is NULL, they will be included in every incremental run.

Insert rows with NULL event_time into your Iceberg table, then run:

```bash
seeknal run --profile profiles.yml
```

The NULL rows appear in the incremental batch alongside any rows with timestamps after the watermark.

**Checkpoint:** NULL rows are never silently excluded from incremental reads.

---

## Part 3: Mixed Sources & Cascade (5 minutes)

### Add a CSV Source

Real pipelines often combine Iceberg tables with static reference data. Add a CSV source and a JOIN transform:

```bash
seeknal draft source categories
seeknal draft transform enriched_events
```

Edit `draft_source_categories.yml`:

```yaml
kind: source
name: categories
description: "Category lookup table (static CSV)"
source: csv
table: data/categories.csv
columns:
  category_id: "Category identifier"
  category_name: "Human-readable category name"
```

Edit `draft_transform_enriched_events.yml`:

```yaml
kind: transform
name: enriched_events
description: "Events enriched with category names"
inputs:
  - ref: source.events
  - ref: source.categories
transform: |
  SELECT
      e.*,
      c.category_name
  FROM ref('source.events') e
  LEFT JOIN ref('source.categories') c
      ON e.category = c.category_id
```

Apply both:

```bash
seeknal apply draft_source_categories.yml
seeknal apply draft_transform_enriched_events.yml
```

### Observe Selective Cascade

Run the full pipeline once to establish state:

```bash
seeknal run --profile profiles.yml --full
```

Now insert new rows into the Iceberg table (but don't change the CSV). Run again:

```bash
seeknal run --profile profiles.yml
```

Expected output:

```
1/3: Executing events (iceberg data changed)
  events completed in 0.31s
2/3: Skipping categories (cached)
3/3: Executing enriched_events (downstream of changed node)
  enriched_events completed in 0.18s
```

The CSV source stays cached while the Iceberg branch cascades. Only `source.events` and its downstream dependents re-execute.

**Checkpoint:** Only affected branches re-execute; unchanged sources stay cached.

---

## Part 4: Full Refresh Override (5 minutes)

### When to Use --full

The `--full` flag forces a complete re-execution of all nodes:

1. **Clears all watermarks** — the next Iceberg read loads all rows
2. **Ignores cached state** — every node runs regardless of snapshot changes
3. **Recalculates watermarks** — sets new `MAX(time_column)` from the full scan

Use `--full` when:

- You suspect watermark drift or data corrections in historical partitions
- Schema changes require reprocessing all data
- You want to rebuild the pipeline from scratch

```bash
seeknal run --profile profiles.yml --full
```

After a full refresh, the watermark resets to the current `MAX(event_time)`. The next incremental run resumes from this new watermark:

```bash
# Full refresh → loads all rows, watermark = MAX(event_time)
seeknal run --profile profiles.yml --full

# Next run → incremental from the new watermark
seeknal run --profile profiles.yml
```

**Checkpoint:** `--full` correctly resets incremental state, and subsequent runs resume incrementally.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. HUGEINT type error with Iceberg**

    - Symptom: `HUGEINT is not a valid Iceberg type`
    - Fix: Always use `CAST(COUNT(*) AS BIGINT)` and `CAST(SUM(...) AS DOUBLE)` in transforms that materialize to Iceberg.

    **2. Watermark not advancing**

    - Symptom: Same rows loaded on every incremental run
    - Fix: Ensure `freshness.time_column` points to a non-NULL timestamp column. Check `last_watermark` in `target/run_state.json`.

    **3. Custom query disables watermarking**

    - Symptom: No incremental filtering despite `freshness.time_column` being set
    - Fix: `params.query` overrides the generated WHERE clause. Remove the custom query to use automatic watermarking, or implement the filter in your query manually.

    **4. OAuth2 token expired**

    - Symptom: `401 Unauthorized` from Lakekeeper
    - Fix: Verify Keycloak credentials are current. Check `KEYCLOAK_TOKEN_URL`, `KEYCLOAK_CLIENT_ID`, and `KEYCLOAK_CLIENT_SECRET` environment variables.

    **5. Lakekeeper unreachable during change detection**

    - Symptom: Source re-executes even though data hasn't changed
    - Fix: This is expected — when the catalog is unreachable, Seeknal re-executes to be safe (fail-open). The pipeline still runs correctly; it just can't skip the cache check.

---

## Summary

In this chapter, you learned:

- [x] **Snapshot-based change detection** — Seeknal compares Iceberg snapshot IDs via REST API before execution
- [x] **Partition-pruned incremental reads** — `freshness.time_column` enables WHERE clause injection with watermark
- [x] **Watermark tracking** — `MAX(time_column)` stored in `run_state.json`, advances after each run
- [x] **NULL event_time handling** — `OR time_column IS NULL` always included in incremental filter
- [x] **Mixed source cascade** — Only branches with changed upstream sources re-execute
- [x] **Full refresh override** — `--full` clears watermarks and forces full scan

**Processing Mode Comparison:**

| Mode | Trigger | Rows Loaded | Watermark |
|------|---------|-------------|-----------|
| **Full Scan** | First run (no state) | All rows | Set to MAX(time_column) |
| **Incremental** | Snapshot changed + watermark exists | Only new rows | Advances to new MAX |
| **Cached** | Snapshot unchanged | None (skipped) | Unchanged |
| **Full Refresh** | `--full` flag | All rows | Reset to MAX(time_column) |

**Key Commands:**

```bash
seeknal run --profile profiles.yml                # Incremental (default)
seeknal run --profile profiles.yml --full         # Full refresh
seeknal run --profile profiles.yml --verbose      # Show change detection details
cat target/run_state.json | python -m json.tool   # Inspect watermarks and snapshots
```

---

## What's Next?

You've completed the Advanced Guide! Explore other paths or dive into reference documentation:

- **[Python Pipelines Guide](../../guides/python-pipelines.md)** — Full decorator reference
- **[Entity Consolidation Guide](../../guides/entity-consolidation.md)** — Cross-FG retrieval and materialization
- **[CLI Reference](../../reference/cli.md)** — All commands and flags
- **[YAML Schema Reference](../../reference/yaml-schema.md)** — Source, transform, and profile schemas

---

## See Also

- **[Chapter 9: Database & External Sources](9-database-sources.md)** — Iceberg source basics, profiles, and OAuth2 setup
- **[Data Engineer Path](../data-engineer-path/)** — ELT pipelines with incremental models
- **[API Reference: Change Detection](../../reference/change-detection.md)** — Fingerprint and snapshot comparison internals

---

*Last updated: March 2026 | Seeknal Documentation*
