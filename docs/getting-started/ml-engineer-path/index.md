# ML Engineer Path

> **Time Commitment:** 3 chapters × 25-35 minutes | **Prerequisites:** [Quick Start](../../quick-start/) completed

Build production feature stores that prevent data leakage and ensure training-serving parity for ML models.

---

## Who This Path Is For

This path is ideal for:

- **ML Engineers** building feature stores for production ML
- **Data Scientists** transitioning models to production
- **ML Platform Engineers** creating feature infrastructure
- **Applied Scientists** needing robust feature engineering

**You'll love this path if you:**
- Need to prevent data leakage in your features
- Want training-serving parity without duplicate code
- Care about feature versioning and reproducibility
- Need low-latency online feature serving

---

## What You'll Learn

By completing this path, you will:

1. **Build Feature Stores** — Create feature groups with entities and point-in-time joins
2. **Second-Order Aggregations** — Create multi-level features from aggregations
3. **Training-to-Serving Parity** — Ensure consistency between offline training and online serving

| Chapter | Focus | Duration |
|---------|-------|----------|
| [Chapter 1](1-feature-store.md) | Build Feature Stores | 30 min |
| [Chapter 2](2-second-order-aggregation.md) | Second-Order Aggregations | 35 min |
| [Chapter 3](3-training-to-serving-parity.md) | Training-to-Serving Parity | 30 min |

**Total Time:** ~95 minutes

---

## Prerequisites

Before starting this path, ensure you've completed:

- [ ] [Quick Start](../../quick-start/) — Understand the pipeline builder workflow
- [ ] Python 3.11+ installed
- [ ] Seeknal CLI installed
- [ ] Basic Python knowledge (functions, classes, decorators)
- [ ] Understanding of ML concepts (features, training, inference)

!!! tip "New to Seeknal?"
    Start with the [Quick Start](../../quick-start/) if you haven't already. It takes 10 minutes and teaches the fundamentals.

---

## Chapter Overview

### Chapter 1: Build Feature Stores (30 minutes)

**Use Case:** Predict customer churn with temporal features

You'll create a feature store that:
- Defines entities (customers, products)
- Creates feature groups with point-in-time joins
- Implements historical feature retrieval
- Manages feature versioning and rollbacks

**What You'll Learn:**
- Feature store architecture and concepts
- Entity definitions and join keys
- Point-in-time joins to prevent data leakage
- Feature group versioning for reproducibility

**Start Chapter 1 →** [1-feature-store.md](1-feature-store.md)

---

### Chapter 2: Second-Order Aggregations (35 minutes)

**Use Case:** Customer behavior features from transaction data

You'll build features that:
- Aggregate events (transactions, clicks, sessions)
- Create multi-level rollups (user → category → global)
- Handle time-based windows (7-day, 30-day, 90-day)
- Optimize computation for production

**What You'll Learn:**
- Second-order aggregation concept and patterns
- Hierarchical feature engineering
- Window-based aggregations
- Performance optimization techniques

**Start Chapter 2 →** [2-second-order-aggregation.md](2-second-order-aggregation.md)

---

### Chapter 3: Training-to-Serving Parity (30 minutes)

**Use Case:** Deploy churn prediction model with consistent features

You'll learn to:
- Materialize offline features for model training
- Create online serving tables for inference
- Ensure feature consistency between offline and online
- Implement feature refresh schedules

**What You'll Learn:**
- Offline and online store architecture
- Materialization strategies
- Online serving with low latency
- Feature refresh and consistency guarantees

**Start Chapter 3 →** [3-training-to-serving-parity.md](3-training-to-serving-parity.md)

---

## What Makes This Path Different

### ML Engineer Focus

Unlike generic feature engineering tutorials, this path teaches production feature store concepts:

```python
# Real-world feature group with point-in-time joins
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
    HistoricalFeaturesDuckDB,
    OnlineFeaturesDuckDB,
)
from seeknal.entity import Entity
from datetime import datetime

# Define entity and feature group
entity = Entity(name="customer", join_keys=["customer_id"])
fg = FeatureGroupDuckDB(
    name="customer_features",
    entity=entity,
    description="Customer behavior features",
)
fg.set_dataframe(df)
fg.set_features()

# Write to offline store with time travel
fg.write(feature_start_time=datetime(2024, 1, 1))

# Point-in-time correct historical features
hist = HistoricalFeaturesDuckDB(lookups=[FeatureLookup(source=fg)])
training_df = hist.to_dataframe(
    entity_df=spine_df,  # Prediction points
    feature_start_time=datetime(2024, 1, 1),
)

# Serve online for inference
online = OnlineFeaturesDuckDB(name="customer_features_online", lookup_key=entity)
features = online.get_features(keys=[{"customer_id": "123"}])
```

### Data Leakage Prevention

Point-in-time joins ensure your model never trains on future data:

```
Timeline:
────────────────────────────────────────────────────>
Jan 1    Jan 10     Jan 15     Jan 20     Jan 30

Events:    purchase   PREDICTION   purchase    purchase
           $50        (here)       $200        $100

Without PIT: avg_spend = ($50 + $200 + $100) / 3 = $116.67  ❌ Data leakage!
With PIT:    avg_spend = $50 / 1 = $50.00                   ✅ Correct!
```

### Training-to-Serving Parity

Single feature definition works for both training and serving:

| Stage | Traditional Approach | Seeknal Approach |
|-------|---------------------|------------------|
| **Training** | SQL script to compute features | `HistoricalFeatures.to_dataframe()` |
| **Serving** | Different code for online features | `OnlineFeatures.get_features()` |
| **Consistency** | Manual verification required | Guaranteed by design |
| **Versioning** | Ad-hoc feature versions | Automatic schema versioning |

---

## Real-World Use Cases

After completing this path, you'll be ready to:

### Churn Prediction
```
Customer Events → Feature Groups → PIT Join → Training Data → Model
                        ↓
                 Online Serving → Inference Features → Real-time Prediction
```

### Recommendation Systems
```
User Interactions → Second-Order Aggregations → User Features → Model Ranking
                                        ↓
                            Item Features + Context → Personalized Recs
```

### Fraud Detection
```
Transaction Stream → Window Aggregations → Risk Features → Model Score
                                    ↓
                         Online Serving → Real-time Fraud Detection
```

---

## See Also

Want to explore related topics?

- **Data Engineer Path** — ELT pipelines and data infrastructure
- **Analytics Engineer Path** — Semantic layers and business metrics
- **[Point-in-Time Joins](../../concepts/point-in-time-joins.md)** — Deep dive on PIT concepts
- **[Glossary](../../concepts/glossary.md)** — Feature store terminology

---

## After This Path

Once you complete the ML Engineer path, you can:

1. **Explore Other Paths**
   - [Data Engineer Path](../data-engineer-path/) — Data pipelines and infrastructure
   - [Analytics Engineer Path](../analytics-engineer-path/) — Semantic layers and metrics

2. **Dive Deeper**
   - [Training to Serving Guide](../../guides/training-to-serving.md) — Complete ML workflow
   - [Python Pipelines](../../guides/python-pipelines.md) — Python API for ML workflows
   - [Second-Order Aggregations](../../concepts/second-order-aggregations.md) — Advanced patterns
   - [CLI Reference](../../reference/cli.md) — All commands and flags

3. **Build Your Feature Store**
   - Define feature groups for your ML use cases
   - Implement point-in-time joins for training data
   - Deploy online serving for inference

---

## Quick Reference

| Concept | Chapter | Key Command/Class |
|---------|---------|-------------------|
| Feature Groups | 1 | `FeatureGroupDuckDB` |
| Entities | 1 | `Entity(join_keys=[...])` |
| Point-in-Time Joins | 1 | `HistoricalFeaturesDuckDB` |
| Feature Versioning | 1 | `seeknal version list <fg_name>` |
| Second-Order Aggregations | 2 | `@second_order_aggregation` decorator |
| Window Aggregations | 2 | Time-based window functions |
| Offline Materialization | 3 | `fg.write(feature_start_time=...)` |
| Online Serving | 3 | `OnlineFeaturesDuckDB.get_features()` |

---

## Before You Start

**Recommended reading:**
- [Point-in-Time Joins](../../concepts/point-in-time-joins.md) — Understand temporal correctness
- [Glossary](../../concepts/glossary.md) — Feature store and ML terminology

**Helpful skills:**
- Python programming (functions, classes, decorators)
- Understanding of ML workflows (features, training, inference)
- Basic data manipulation (Pandas, SQL)
- Familiarity with feature engineering concepts

---

## Migration from Other Tools

Coming from other feature stores? See our migration guides:

- **[Feast to Seeknal](../../reference/migration.md#migrating-from-feast-to-seeknal)** — Feature view and entity mapping
- **[Featuretools to Seeknal](../../reference/migration.md#migrating-from-featuretools-to-seeknal)** — Entity sets and DFS patterns
- **[Spark to DuckDB](../../reference/migration.md#migrating-from-spark-to-duckdb)** — Engine migration for faster development

---

**Ready to build production feature stores?**

[Start Chapter 1: Build Feature Stores →](1-feature-store.md)
