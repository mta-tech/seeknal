# ML Engineer Path

**Duration:** ~110 minutes | **Format:** Python Pipeline | **Prerequisites:** Python, [DE Path Chapter 1](../data-engineer-path/1-elt-pipeline.md) completed

Build production feature stores and ML models using Python pipeline decorators (`@source`, `@feature_group`, `@transform`) and Seeknal's declarative YAML SOA engine.

---

## What You'll Learn

The ML Engineer path teaches you to build production-grade feature stores and ML models with Seeknal's Python pipeline API. You'll learn to:

1. **Build Feature Stores** — Create feature groups with `@feature_group`, evolve schemas iteratively
2. **Second-Order Aggregations** — Generate hierarchical features with the YAML SOA engine (basic, window, ratio)
3. **Point-in-Time Joins & Training-Serving Parity** — Build PIT-correct training data with `HistoricalFeaturesDuckDB`, temporal SOA features, and online serving
4. **Entity Consolidation** — Merge feature groups into per-entity views with struct columns

---

## Prerequisites

Before starting this path, ensure you have:

- **[DE Path Chapter 1](../data-engineer-path/1-elt-pipeline.md)** completed — You'll use the e-commerce data
- Python 3.11+ and `uv` installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- Understanding of ML features and training data

---

## Chapters

### Chapter 1: Build a Feature Store (~30 minutes)

Create feature groups using Python decorators:

```
source.transactions (Python) ──→ feature_group.customer_features (Python)
                                          ↓
                                    REPL Exploration
```

**You'll build:**
- Python sources with `@source` decorator and PEP 723 dependencies
- Feature groups with `@feature_group` and `ctx.ref()`
- Schema evolution workflow for iterating on features

**[Start Chapter 1 →](1-feature-store.md)**

---

### Chapter 2: Second-Order Aggregations (~30 minutes)

Generate hierarchical features from raw transactions:

```
source.transactions (Ch.1) → feature_group.customer_daily_agg → second_order_aggregation.region_metrics
         (Python @feature_group)              (YAML SOA engine)
         ├── SUM, COUNT per day               ├── basic: sum, mean, max, stddev
         └── application_date                 ├── window: recent 7-day totals
                                              └── ratio: recent vs past spending
```

**You'll build:**
- Feature groups with `@feature_group` and `ctx.duckdb.sql()`
- YAML SOA with declarative `features:` spec (basic, window, ratio)
- Time-window features using `application_date_col`

**[Start Chapter 2 →](2-second-order-aggregation.md)**

---

### Chapter 3: Point-in-Time Joins & Training-Serving Parity (~35 minutes)

Build a production ML pipeline with temporal correctness:

```
source.churn_labels (spine with application_date)
         ↓
@transform: pit_training_data
  PIT-joins customer_daily_agg via HistoricalFeaturesDuckDB
         ↓
SOA: customer_training_features (per-customer temporal features)
         ↓
@transform: churn_model (scikit-learn)
         ↓
REPL: Online serving demo (OnlineFeaturesDuckDB)
```

**You'll learn:**
- Point-in-time joins with `HistoricalFeaturesDuckDB` to prevent data leakage
- Per-customer SOA temporal features (spending trends, recency)
- Training scikit-learn models inside `@transform` nodes
- Online serving with `OnlineFeaturesDuckDB.get_features()` for training-serving parity

**[Start Chapter 3 →](3-training-serving-parity.md)**

---

### Chapter 4: Entity Consolidation (~15 minutes)

Consolidate multiple feature groups into unified entity views:

```
feature_group.customer_features ──┐
                                  ├──→ Entity Consolidation ──→ entity_customer
feature_group.product_preferences ┘         (automatic)              ↓
                                                              REPL Exploration
                                                                     ↓
                                                          seeknal entity list/show
```

**You'll build:**
- A second feature group (`product_preferences`) for the customer entity
- Automatic consolidation with struct-namespaced columns
- CLI commands to inspect consolidated entities

**[Start Chapter 4 →](4-entity-consolidation.md)**

---

## What You'll Build

By the end of this path, you'll have a complete ML pipeline:

| Component | Decorator / Tool | Purpose |
|-----------|------------------|---------|
| **Sources** | `@source` | Declare data ingestion (CSV, Parquet, DB) |
| **Feature Groups** | `@feature_group` | Compute and version ML features |
| **Transforms** | `@transform` | Data prep, PIT joins, model training |
| **SOA** | YAML `features:` spec | Hierarchical meta-features (basic, window, ratio) |
| **PIT Joins** | `HistoricalFeaturesDuckDB` | Temporally correct training data |
| **Online Serving** | `OnlineFeaturesDuckDB` | Training-serving parity for inference |
| **Entity Consolidation** | CLI + REPL | Unified per-entity views with struct columns |

---

## Key Commands You'll Learn

```bash
# Python pipeline templates
seeknal draft source <name> --python --deps pandas
seeknal draft feature-group <name> --python --deps pandas,duckdb
seeknal draft transform <name> --python --deps pandas,scikit-learn
seeknal draft second-order-aggregation <name>

# Preview and apply
seeknal dry-run <draft_file>.py
seeknal apply <draft_file>.py
seeknal apply <draft_file>.yml

# Pipeline execution
seeknal plan
seeknal run

# Feature management
seeknal validate-features <fg_name> --mode fail
seeknal lineage <node> --ascii

# Entity consolidation
seeknal entity list
seeknal entity show <entity_name>
seeknal consolidate

# Interactive verification
seeknal repl
```

---

## Resources

### Reference
- [Python Pipelines Guide](../../guides/python-pipelines.md) — Full decorator reference and patterns
- [Entity Consolidation Guide](../../guides/entity-consolidation.md) — Cross-FG retrieval and materialization
- [CLI Reference](../../reference/cli.md) — All commands and flags
- [YAML Schema Reference](../../reference/yaml-schema.md) — Feature group and SOA schemas

### Related Paths
- [Data Engineer Path](../data-engineer-path/) — ELT pipelines (prerequisite)
- [Analytics Engineer Path](../analytics-engineer-path/) — Semantic layers and metrics
- [Advanced Guide: Python Pipelines](../advanced/8-python-pipelines.md) — Mixed YAML + Python

---

*Last updated: February 2026 | Seeknal Documentation*
