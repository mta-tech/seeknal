# Phase 2 ML Engineering: Parallel Execution at Scale

> **Tutorial for:** ML Engineers | **Duration:** 45 minutes | **Level:** Intermediate

Learn how to build high-throughput feature pipelines using Seeknal's parallel execution engine. This tutorial demonstrates building a fraud detection system with 8 independent data sources that execute simultaneously, achieving up to 8x speedup.

---

## Table of Contents

- [Overview](#overview)
- [What You'll Build](#what-youll-build)
- [Sequential Baseline](#sequential-baseline)
- [Parallel Execution](#parallel-execution)
- [Understanding Layers](#understanding-layers)
- [Feature Experimentation](#feature-experimentation)
- [Breaking Change Awareness](#breaking-change-awareness)
- [Max Workers Tuning](#max-workers-tuning)
- [Environment Workflow](#environment-workflow)
- [Production Workflow](#production-workflow)

---

## Overview

Traditional ML pipelines execute sequentially, wasting CPU cycles when data sources have no dependencies. Seeknal's parallel execution engine groups nodes into **topological layers** and executes independent nodes concurrently.

### Key Benefits

- **8x faster** for wide DAGs with independent sources
- **Automatic layer detection** via topological sort
- **Thread-safe execution** with per-worker DuckDB connections
- **Smart dependency handling** prevents data races

### Prerequisites

- Seeknal installed (`pip install seeknal`)
- Understanding of YAML pipelines (see [YAML Pipeline Tutorial](yaml-pipeline-tutorial.md))
- Basic fraud detection knowledge

---

## What You'll Build

A fraud detection feature pipeline for a fintech company:

**Architecture:**
```
Layer 0 (8 sources - parallel):
├── transactions
├── user_profiles
├── device_fingerprints
├── ip_geolocation
├── merchant_data
├── card_data
├── fraud_labels
└── realtime_alerts

Layer 1 (4 transforms - parallel):
├── txn_enriched (uses: transactions, users, devices, ip)
├── merchant_risk (uses: merchant_data, fraud_labels)
├── card_velocity (uses: card_data, transactions)
└── alert_features (uses: realtime_alerts, transactions)

Layer 2 (2 feature groups - parallel):
├── txn_features (uses: txn_enriched, merchant_risk)
└── user_risk (uses: card_velocity, alert_features)

Layer 3 (1 exposure):
└── training_data (uses: txn_features, user_risk)
```

**Total:** 15 nodes, 4 layers, 14 nodes run in parallel

---

## Sequential Baseline

### 1. Get the Example Files

```bash
# Clone or download the tutorial files
mkdir -p ~/fraud-detection
cd ~/fraud-detection

# Copy example files (adjust path to your Seeknal installation)
cp -r <seeknal-install>/examples/phase2-ml-eng/ .
cd phase2-ml-eng/
```

### 2. Generate Manifest

Validate the pipeline structure:

```bash
seeknal plan
```

Expected output:
```
Parsing project: phase2-ml-eng
  Path: ~/fraud-detection/phase2-ml-eng
✓ Manifest generated: target/manifest.json
  Nodes: 15
  Edges: 22
```

> **Note:** `seeknal parse` also works as a backward-compatible alias.

### 3. Run Sequential Execution

```bash
time seeknal run
```

Expected output:
```
Seeknal Pipeline Execution
============================================================
  Project: phase2-ml-eng
  Mode: Incremental

Execution
============================================================
1/15: transactions [RUNNING]
  SUCCESS in 0.15s
  Rows: 10000

2/15: user_profiles [RUNNING]
  SUCCESS in 0.12s
  Rows: 5000

3/15: device_fingerprints [RUNNING]
  SUCCESS in 0.10s
  Rows: 8000

4/15: ip_geolocation [RUNNING]
  SUCCESS in 0.08s
  Rows: 3000

5/15: merchant_data [RUNNING]
  SUCCESS in 0.11s
  Rows: 1500

6/15: card_data [RUNNING]
  SUCCESS in 0.09s
  Rows: 6000

7/15: fraud_labels [RUNNING]
  SUCCESS in 0.07s
  Rows: 2000

8/15: realtime_alerts [RUNNING]
  SUCCESS in 0.06s
  Rows: 1000

9/15: txn_enriched [RUNNING]
  SUCCESS in 0.25s
  Rows: 10000

10/15: merchant_risk [RUNNING]
  SUCCESS in 0.18s
  Rows: 1500

11/15: card_velocity [RUNNING]
  SUCCESS in 0.20s
  Rows: 6000

12/15: alert_features [RUNNING]
  SUCCESS in 0.15s
  Rows: 1000

13/15: txn_features [RUNNING]
  SUCCESS in 0.22s

14/15: user_risk [RUNNING]
  SUCCESS in 0.19s

15/15: training_data [RUNNING]
  SUCCESS in 0.10s

✓ State saved

Execution Summary
============================================================
  Total nodes:    15
  Executed:       15
  Duration:       2.07s
============================================================
```

**Sequential Duration:** ~2.07s (8 sources @ 0.10s avg = 0.80s sequential overhead)

---

## Parallel Execution

### Run with Parallel Flag

```bash
time seeknal run --parallel --full
```

Expected output:
```
Seeknal Parallel Execution
============================================================
  Project: phase2-ml-eng
  Mode: Full (ignore cache)
  Max Workers: 8

[Layer 1/4] Running 8 node(s) in parallel...
  transactions completed in 0.15s
  user_profiles completed in 0.12s
  device_fingerprints completed in 0.10s
  ip_geolocation completed in 0.08s
  merchant_data completed in 0.11s
  card_data completed in 0.09s
  fraud_labels completed in 0.07s
  realtime_alerts completed in 0.06s

[Layer 2/4] Running 4 node(s) in parallel...
  txn_enriched completed in 0.25s
  merchant_risk completed in 0.18s
  card_velocity completed in 0.20s
  alert_features completed in 0.15s

[Layer 3/4] Running 2 node(s) in parallel...
  txn_features completed in 0.22s
  user_risk completed in 0.19s

[Layer 4/4] Running 1 node(s) in parallel...
  training_data completed in 0.10s

✓ State saved

Parallel Execution Summary
============================================================
  Total nodes:     15
  Layers:          4
  Executed:        15
  Successful:      15
  Duration:        0.72s
============================================================
```

**Parallel Duration:** ~0.72s

**Speedup:** 2.07s / 0.72s = **2.87x faster**

---

## Understanding Layers

Seeknal groups nodes into **topological layers** where:
- Nodes in the same layer have **no dependencies** on each other
- Layer N can only start after Layer N-1 completes
- Execution is thread-safe (per-thread DuckDB connections)

### Why Layers Matter

**Layer 0 (8 sources):**
```
All sources are independent - no source depends on another source.
Result: 8 nodes run simultaneously on 8 threads.
```

**Layer 1 (4 transforms):**
```
Each transform depends on Layer 0 sources, but not on other transforms.
Result: 4 nodes run simultaneously.
```

**Layer 2 (2 feature groups):**
```
Feature groups depend on Layer 1 transforms, but not on each other.
Result: 2 nodes run simultaneously.
```

**Layer 3 (1 exposure):**
```
Exposure depends on Layer 2 feature groups.
Result: 1 node runs alone.
```

### Visualize Layers

```bash
seeknal run --parallel --show-plan
```

Output:
```
Execution Plan (Parallel):
============================================================
Layer 1: 8 nodes (0 dependencies)
  - transactions
  - user_profiles
  - device_fingerprints
  - ip_geolocation
  - merchant_data
  - card_data
  - fraud_labels
  - realtime_alerts

Layer 2: 4 nodes (depends on Layer 1)
  - txn_enriched → [transactions, user_profiles, device_fingerprints, ip_geolocation]
  - merchant_risk → [merchant_data, fraud_labels]
  - card_velocity → [card_data, transactions]
  - alert_features → [realtime_alerts, transactions]

Layer 3: 2 nodes (depends on Layer 2)
  - txn_features → [txn_enriched, merchant_risk]
  - user_risk → [card_velocity, alert_features]

Layer 4: 1 node (depends on Layer 3)
  - training_data → [txn_features, user_risk]
============================================================
```

---

## Feature Experimentation

Environments provide isolated workspaces to test feature changes without affecting production.

### 1. Create Experiment Environment

Plan a new experimental environment:

```bash
seeknal plan experiment
```

> **Note:** `seeknal env plan experiment` also works as a backward-compatible alias.

Output:
```
Planning environment: experiment
  Production manifest: target/manifest.json (15 nodes)
  New manifest: seeknal/ (15 nodes)

No changes detected.

Environment Plan:
  Name: experiment
  Created: 2026-02-09T10:30:00
  Changes: 0 nodes to execute
  Added: 0
  Removed: 0
```

### 2. Add a New Feature

Edit `13_feature_group_txn_features.yml`:

```yaml
features:
  customer_id:
    dtype: integer
  txn_amount:
    dtype: float
  merchant_risk_score:
    dtype: float
  # NEW FEATURE - add IP reputation
  ip_reputation_score:
    dtype: float
```

### 3. Re-Plan

```bash
seeknal plan experiment
```

> **Note:** `seeknal env plan experiment` also works as a backward-compatible alias.

Output:
```
Planning environment: experiment
  Production manifest: target/manifest.json (15 nodes)
  New manifest: seeknal/ (15 nodes)

Changes Detected:
============================================================
  NON_BREAKING (1):
    - feature_group.txn_features (new feature: ip_reputation_score)

  DEPENDENT (1):
    - exposure.training_data (upstream changed)

Environment Plan:
  Name: experiment
  Created: 2026-02-09T10:35:00
  Changes: 2 nodes to execute
  Added: 0
  Removed: 0
============================================================
```

**Key insight:** Only 2 nodes need to run (txn_features + training_data). The other 13 nodes reference production cache.

### 4. Apply Experiment

Execute the experiment with parallel processing:

```bash
seeknal run --env experiment --parallel
```

> **Note:** `seeknal env apply experiment --parallel` also works as a backward-compatible alias.

Output:
```
Applying environment: experiment
  Plan created: 2026-02-09T10:35:00
  Nodes to execute: 2

[Layer 3/4] Running 1 node(s) in parallel...
  txn_features completed in 0.22s

[Layer 4/4] Running 1 node(s) in parallel...
  training_data completed in 0.10s

Parallel Execution Summary
============================================================
  Total nodes:     15
  Executed:        2
  Cached (from production): 13
  Duration:        0.32s
============================================================

✓ Environment 'experiment' applied
  Outputs: target/environments/experiment/
```

---

## Breaking Change Awareness

Seeknal classifies changes as:
- **METADATA** - Documentation only (no execution)
- **NON_BREAKING** - New features, safe changes
- **BREAKING** - Removed features, schema changes

### 1. Remove a Feature

Edit `13_feature_group_txn_features.yml`:

```yaml
features:
  customer_id:
    dtype: integer
  txn_amount:
    dtype: float
  # REMOVED: merchant_risk_score
  ip_reputation_score:
    dtype: float
```

### 2. Plan Shows BREAKING

```bash
seeknal plan breaking-test
```

> **Note:** `seeknal env plan breaking-test` also works as a backward-compatible alias.

Output:
```
Planning environment: breaking-test

Changes Detected:
============================================================
  BREAKING (1):
    - feature_group.txn_features (removed feature: merchant_risk_score)

  DEPENDENT (1):
    - exposure.training_data (upstream BREAKING change)

⚠ WARNING: BREAKING changes detected.
  Downstream consumers may fail if they depend on removed features.
  Review carefully before applying.

Environment Plan:
  Name: breaking-test
  Created: 2026-02-09T10:40:00
  Changes: 2 nodes to execute (1 BREAKING)
  Added: 0
  Removed: 0
============================================================
```

**Key insight:** Seeknal warns about breaking changes and marks downstream dependents.

---

## Max Workers Tuning

The `--max-workers` flag controls parallelism.

### Small Machine (4 cores)

```bash
seeknal run --parallel --max-workers 4 --full
```

Output:
```
[Layer 1/4] Running 8 node(s) in parallel...
  Max workers: 4 (limited by --max-workers)
  # 8 nodes execute in 2 batches: 4 + 4
  Duration: ~0.30s (vs 0.15s with 8 workers)
```

### Large Machine (16 cores)

```bash
seeknal run --parallel --max-workers 16 --full
```

Output:
```
[Layer 1/4] Running 8 node(s) in parallel...
  Max workers: 8 (limited by node count)
  # All 8 nodes execute simultaneously
  Duration: ~0.15s
```

### Auto (default)

```bash
seeknal run --parallel --full
```

Uses `min(cpu_count(), 8)` by default. On an 8-core machine, uses 8 workers.

---

## Environment Workflow

Complete ML experiment lifecycle:

### 1. Plan (Preview Changes)

```bash
seeknal plan dev-model-v2
```

Analyzes changes, creates execution plan.

> **Note:** `seeknal env plan dev-model-v2` also works as a backward-compatible alias.

### 2. Apply (Execute)

```bash
seeknal run --env dev-model-v2 --parallel
```

Executes changed nodes, references production for unchanged nodes.

> **Note:** `seeknal env apply dev-model-v2 --parallel` also works as a backward-compatible alias.

### 3. Validate Results

```bash
# Query environment outputs
python -c "
import duckdb
con = duckdb.connect()
df = con.execute(\"
  SELECT * FROM read_parquet('target/environments/dev-model-v2/cache/exposure/training_data.parquet')
  LIMIT 5
\").df()
print(df)
"
```

### 4. Promote to Production

```bash
seeknal promote dev-model-v2
```

Atomically copies environment outputs to production cache.

> **Note:** `seeknal env promote dev-model-v2 prod` also works as a backward-compatible alias.

### 5. List Environments

```bash
seeknal env list
```

Output:
```
Environments:
  Name              Created              Last Accessed        Status
  experiment        2026-02-09 10:35     2026-02-09 10:36     Applied
  breaking-test     2026-02-09 10:40     2026-02-09 10:40     Planned
  dev-model-v2      2026-02-09 11:00     2026-02-09 11:05     Applied
```

---

## Production Workflow

Best practices for deploying feature pipelines.

### 1. Development

```bash
# Create feature branch
git checkout -b feature/new-fraud-signal

# Edit YAML files
vim seeknal/feature_groups/13_feature_group_txn_features.yml

# Plan in dev environment
seeknal plan dev

# Apply and test
seeknal run --env dev --parallel
```

### 2. Staging

```bash
# Promote to staging
seeknal promote dev staging

# Run full pipeline in staging
seeknal run --env staging --parallel --full
```

### 3. Production

```bash
# Merge to main
git checkout main
git merge feature/new-fraud-signal

# Promote to production
seeknal promote staging

# Run production pipeline
seeknal run --parallel
```

### 4. CI/CD Integration

```yaml
# .github/workflows/seeknal-pipeline.yml
name: Seeknal Feature Pipeline

on:
  pull_request:
    paths:
      - 'seeknal/**'

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install seeknal
      - run: seeknal plan
      - run: seeknal plan ci-test
      - run: seeknal run --env ci-test --parallel --dry-run

  deploy:
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
      - run: pip install seeknal
      - run: seeknal run --parallel
```

---

## Summary

You've learned:

✅ **Parallel execution** achieves 2-8x speedup for wide DAGs
✅ **Topological layers** enable safe concurrent execution
✅ **Environments** provide isolated feature experimentation
✅ **Breaking change detection** prevents downstream breakage
✅ **Max workers tuning** optimizes for different machine sizes
✅ **Production workflow** for safe deployments

### Key Commands

| Command | Purpose |
|---------|---------|
| `seeknal run --parallel` | Execute with parallel engine |
| `seeknal run --parallel --max-workers N` | Limit parallelism |
| `seeknal plan <name>` | Create environment plan |
| `seeknal run --env <name> --parallel` | Execute environment |
| `seeknal promote <from>` | Promote environment |
| `seeknal env list` | Show all environments |

**Backward-compatible aliases:**
- `seeknal env plan <name>` → `seeknal plan <name>`
- `seeknal env apply <name>` → `seeknal run --env <name>`
- `seeknal env promote <from> <to>` → `seeknal promote <from>`

### Performance Guidelines

**When parallel execution helps most:**
- Wide DAGs with many independent sources
- I/O-bound operations (reading CSV/Parquet)
- Machines with 4+ cores

**When sequential is fine:**
- Linear DAGs (chain of transforms)
- Small datasets (<1000 rows)
- Single-core machines

---

**Next Steps:**
- Try parallel execution on your own pipelines
- Experiment with environments for safe development
- Measure speedup with different `--max-workers` values
- Integrate into CI/CD workflows

**Tutorial Complete!** 🚀
