# Chapter 3: Deploy to Production Environments

> **Duration:** 20 minutes | **Difficulty:** Intermediate | **Format:** YAML

Learn to use Seeknal's virtual environments to safely test pipeline changes in isolation before promoting to production.

---

## What You'll Build

A safe deployment workflow using virtual environments:

```
seeknal run              (default — "production" outputs)
seeknal env plan dev     (preview changes in isolation)
seeknal env apply dev    (execute in dev namespace)
seeknal env promote dev  (promote tested changes to production)
```

**After this chapter, you'll have:**
- A `dev` environment with isolated namespace prefixing
- A per-environment profile (`profiles-dev.yml`) for different connections
- Understanding of plan → apply → promote workflow
- Confidence to test breaking changes without affecting production outputs

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: Build ELT Pipeline](1-elt-pipeline.md) — Working pipeline with `raw_orders` and `orders_cleaned`
- [ ] [Chapter 2: Add Incremental Models](2-incremental-models.md) — Multi-step DAG with merge and aggregation

Your DAG from Chapter 2 should look like:

```
raw_orders (CSV) ──→ orders_cleaned ──┐
                                      ├──→ orders_merged ──→ daily_revenue
orders_updates (CSV) ─────────────────┘
```

---

## Part 1: Understand Virtual Environments (5 minutes)

### Why Environments?

When you run `seeknal run`, outputs go to `target/intermediate/` — this is your default ("production") output. But what if you want to test a schema change without overwriting those outputs?

Virtual environments solve this:

| | Default (Production) | Virtual Environment |
|---|---|---|
| **Outputs** | `target/intermediate/` | `target/environments/dev/` |
| **Namespace** | `schema.table` | `dev_schema.table` |
| **State** | `target/run_state.json` | `target/environments/dev/run_state.json` |
| **Risk** | Changes affect production | Changes are isolated |

### How It Works

Seeknal's environment system:

1. **`env plan`** — Compares your current YAML definitions against the production manifest. Categorizes each change as breaking, non-breaking, or metadata-only.
2. **`env apply`** — Executes only changed nodes in an isolated directory. Unchanged nodes reference production outputs (zero-copy).
3. **`env promote`** — Copies tested environment outputs to production, replacing the manifest.

---

## Part 2: Create a Dev Environment (8 minutes)

### Run a Baseline

First, make sure your pipeline has a clean production run from Chapter 2:

```bash
seeknal run
```

You should see all nodes cached (or successfully executed). This establishes the production manifest at `target/manifest.json`.

### Plan a Dev Environment

```bash
seeknal env plan dev
```

**Expected output:**
```
ℹ Planning environment 'dev'...

ℹ Environment Plan: dev
============================================================
✓ No changes detected.
```

No changes — because your YAML files match the production manifest. Let's make a change to test.

### Make a Change to Test

Add a `customer_segment` column to the daily revenue aggregation. Open `seeknal/transforms/daily_revenue.yml` and add the column:

```yaml
transform: |
  SELECT
    order_date,
    COUNT(*) as total_orders,
    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_orders,
    COUNT(CASE WHEN status = 'PENDING' THEN 1 END) as pending_orders,
    COALESCE(SUM(revenue), 0) as total_revenue,
    ROUND(COALESCE(AVG(revenue), 0), 2) as avg_order_value,
    MAX(revenue) as max_order_value,
    SUM(items) as total_items,
    COUNT(CASE WHEN quality_flag = 1 THEN 1 END) as flagged_orders,
    COUNT(DISTINCT customer_id) as unique_customers              -- add this line

  FROM ref('transform.orders_merged')
  WHERE status != 'CANCELLED'
  GROUP BY order_date
  ORDER BY order_date
```

Now plan again:

```bash
seeknal env plan dev
```

**Expected output:**
```
ℹ Planning environment 'dev'...

ℹ Environment Plan: dev
============================================================
  Non-breaking changes: 1
  Total nodes to execute: 1
✓ Plan saved to target/environments/dev/plan.json
```

Seeknal detects one non-breaking change (adding a column) and plans to re-execute only `daily_revenue`.

### Preview with Dry Run (Optional)

Before executing, you can preview what would run:

```bash
seeknal env apply dev --dry-run
```

This shows which nodes will execute without actually running them — useful for reviewing the execution plan before committing resources.

### Apply the Dev Environment

```bash
seeknal env apply dev
```

This executes the changed node in isolation — outputs go to `target/environments/dev/`, not `target/intermediate/`. Your production data is untouched.

### Verify with REPL

After applying, use the REPL to query your dev environment outputs:

```bash
seeknal repl --env dev
```

The REPL loads data from `target/environments/dev/` for changed nodes and falls back to production for unchanged nodes. The prompt shows which environment you're in:

```
Seeknal SQL REPL (DuckDB + StarRocks)
Project: my-project [env: dev]
  Intermediate outputs: 4 tables registered

seeknal[dev]> SELECT * FROM transform_daily_revenue LIMIT 5;
```

You should see the `unique_customers` column in `daily_revenue`. Compare with production by running the REPL without `--env`:

```bash
seeknal repl
```

```
seeknal> SELECT * FROM transform_daily_revenue LIMIT 5;
```

Production still has the original schema — no `unique_customers` column. Your change is safely isolated in `dev`.

!!! tip "Don't run `seeknal run` yet"
    Running `seeknal run` would execute the pipeline with your current YAML definitions (including `unique_customers`) and update production outputs. Use `seeknal env promote` instead to safely move tested changes to production.

---

## Part 3: Use Per-Environment Profiles (5 minutes)

### Why Profiles?

In real projects, dev and production use different database connections. Seeknal supports per-environment profiles that automatically resolve based on the environment name.

### Create a Dev Profile

Create `profiles-dev.yml` in your project root:

```yaml
connections:
  local_pg:
    type: postgresql
    host: localhost
    port: 5432
    database: ecommerce_dev
    user: dev_user
    password: dev_pass

source_defaults:
  postgresql:
    connection: local_pg
```

### Profile Resolution Order

When you run `seeknal env plan dev --profile profiles-dev.yml`, the profile is explicitly set. But Seeknal also auto-discovers profiles by convention:

1. **Explicit `--profile`** flag (highest priority)
2. **`profiles-dev.yml`** in project root (convention: `profiles-{env}.yml`)
3. **`~/.seeknal/profiles-dev.yml`** (global fallback)
4. **Default `profiles.yml`** (lowest priority)

So with `profiles-dev.yml` in your project root, this just works:

```bash
seeknal env plan dev
```

Seeknal finds `profiles-dev.yml` automatically because the filename matches the `profiles-{env}.yml` convention.

---

## Part 4: Promote to Production (2 minutes)

### Promote the Dev Environment

Once you've tested the change in `dev`, promote it to production:

```bash
seeknal env promote dev
```

This copies the tested outputs from `target/environments/dev/` to production, updating the manifest. The `unique_customers` column is now part of your production pipeline.

### Preview Before Promoting

If you want to see what will be promoted without executing:

```bash
seeknal env promote dev --dry-run
```

This shows the changes that would be promoted without actually applying them.

### Clean Up

After promoting, delete the dev environment:

```bash
seeknal env delete dev
```

Verify it's gone:

```bash
seeknal env list
```

```
ℹ No environments found.
```

---

## Namespace Prefixing (Advanced)

When your pipeline writes to external targets (PostgreSQL, Iceberg), environments automatically prefix namespaces to prevent collisions:

| Target Type | Production | Dev Environment |
|---|---|---|
| PostgreSQL | `analytics.orders` | `dev_analytics.orders` |
| Iceberg | `catalog.warehouse.orders` | `catalog.dev_warehouse.orders` |

The prefix applies to the **schema/namespace only**, not the catalog or table name. This means each environment gets its own isolated schema without needing separate databases.

To trigger re-materialization with production credentials after promoting:

```bash
seeknal env promote dev --rematerialize --profile profiles.yml
```

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. Stale Plan**

    - Symptom: `seeknal env apply dev` warns plan is stale
    - Fix: Re-run `seeknal env plan dev` to refresh after YAML changes

    **2. Missing Production Manifest**

    - Symptom: `env plan` treats everything as new
    - Fix: Run `seeknal plan` and `seeknal run` first to establish baseline

    **3. Profile Not Found**

    - Symptom: Connection errors in dev environment
    - Fix: Ensure `profiles-dev.yml` exists in project root or use `--profile` flag

    **4. Forgot to Promote**

    - Symptom: Production still uses old schema after testing in dev
    - Fix: Run `seeknal env promote dev` after verifying changes

---

## Summary

In this chapter, you learned:

- [x] **Virtual Environments** — Isolate changes in `target/environments/<name>/`
- [x] **Plan → Apply → Promote** — Safe workflow for testing changes
- [x] **Per-Environment Profiles** — Different connections per environment via `profiles-{env}.yml`
- [x] **Namespace Prefixing** — Automatic schema isolation for PostgreSQL and Iceberg
- [x] **Change Categorization** — Seeknal classifies changes as breaking, non-breaking, or metadata-only

**Key Commands:**
```bash
seeknal env plan <name>              # Preview changes in environment
seeknal env apply <name>             # Execute changes in isolation
seeknal env apply <name> --dry-run   # Preview execution plan without running
seeknal env promote <from> [<to>]    # Promote to production
seeknal env list                     # List all environments
seeknal env delete <name>            # Clean up environment
seeknal repl --env <name>            # Query environment outputs in REPL
```

---

## Path Complete!

**Congratulations!** You've completed the Data Engineer Path:

- **Chapter 1**: Built an ELT pipeline with CSV sources and DuckDB transforms
- **Chapter 2**: Added incremental processing with merge logic and aggregation
- **Chapter 3**: Deployed safely with virtual environments and promotion workflows

**What you can do now:**
- Build production-grade data pipelines
- Handle data corrections with merge transforms
- Test changes safely in isolated environments
- Promote verified changes to production

**Next Steps:**
- Explore [Analytics Engineer Path](../analytics-engineer-path/) for semantic modeling
- Explore [ML Engineer Path](../ml-engineer-path/) for feature stores
- Read [Production Best Practices](../../guides/production.md)

---

## See Also

- **[Virtual Environments](../../concepts/virtual-environments.md)** — Deep dive on environment isolation
- **[Change Categorization](../../concepts/change-categorization.md)** — Understanding change impact
- **[YAML Schema Reference](../../reference/yaml-schema.md)** — All supported source types and fields
