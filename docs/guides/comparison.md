# Seeknal vs dbt vs SQLMesh vs Feast

This guide compares Seeknal with other popular data and ML tooling to help you choose the right tool for your use case.

## Quick Comparison Table

| Feature | Seeknal | dbt | SQLMesh | Feast |
|---------|---------|-----|---------|-------|
| **Primary Focus** | Data pipelines + Feature store | SQL transformations | SQL pipelines + deployment | Feature store only |
| **Pipeline Definition** | YAML + Python | SQL + Jinja | SQL + Python | Python only |
| **Feature Store** | Built-in (Spark + DuckDB) | No | No | Core focus |
| **Point-in-Time Joins** | Built-in | No | No | Built-in |
| **Virtual Environments** | Yes (branch isolation) | No (uses targets) | Yes | No |
| **Change Categorization** | Yes (breaking/non-breaking) | No | Yes (forward/breaking) | No |
| **Parallel Execution** | Built-in (DAG-based) | Requires dbt-core flags | Built-in | No |
| **Semantic Layer** | Built-in (metrics + models) | dbt Semantic Layer (paid) | No | No |
| **DuckDB Support** | Native first-class | dbt-duckdb adapter | Built-in | No |
| **Spark Support** | Native first-class | dbt-spark adapter | Built-in | Spark batch source |
| **CLI Tool** | `seeknal` | `dbt` | `sqlmesh` | `feast` |
| **Deployment Model** | Self-hosted or cloud | Self-hosted or cloud | Self-hosted or cloud | Self-hosted |
| **License** | Open source | Open source (core) | Apache 2.0 | Apache 2.0 |

## Seeknal vs dbt

### Overview

**dbt** (data build tool) focuses on SQL-based transformations for analytics engineering. It's excellent for building data warehouses but lacks feature store capabilities.

**Seeknal** combines SQL transformations with feature store functionality, designed for both analytics and ML workflows.

### Strengths of Seeknal

| Capability | Seeknal | dbt |
|------------|---------|-----|
| **Feature Store** | Built-in with PIT joins, online serving | No (requires separate tool) |
| **Multi-Engine** | Mix DuckDB and Spark in one pipeline | Single adapter per project |
| **Python Tasks** | Native Python transformations | Python models (limited) |
| **Change Detection** | Automatic breaking/non-breaking analysis | Manual test definitions |
| **Virtual Environments** | Branch-based with state isolation | Targets (shared state) |
| **Semantic Layer** | Free and open source | Enterprise feature (paid) |

### Strengths of dbt

| Capability | dbt | Seeknal |
|------------|-----|---------|
| **Ecosystem** | Massive adapter ecosystem (50+) | DuckDB + Spark focus |
| **Community** | Large community, extensive docs | Growing community |
| **Testing Framework** | Mature testing system | Basic validation support |
| **Documentation** | Auto-generated lineage + docs site | Manual documentation |
| **Incremental Models** | Advanced incremental strategies | Basic incremental support |

### When to Choose Seeknal

Choose Seeknal when you need:

1. **Feature Store + Pipelines** - Unified tooling for both
2. **ML Workflows** - Point-in-time joins, online serving, training-serving parity
3. **Multi-Engine Flexibility** - DuckDB for dev, Spark for prod
4. **Python-First** - Mix Python and SQL freely
5. **Semantic Layer** - Free metrics layer without enterprise license

### When to Choose dbt

Choose dbt when you need:

1. **Pure Analytics** - No ML feature store requirements
2. **Broad Adapter Support** - Snowflake, BigQuery, Redshift, etc.
3. **Testing-First** - Extensive data quality testing
4. **Enterprise Support** - dbt Cloud with managed services
5. **Mature Ecosystem** - Leverage existing packages and patterns

### Migration Path

**From dbt to Seeknal:**

```yaml
# dbt model (models/transform/clean_orders.sql)
SELECT
  order_id,
  customer_id,
  order_date,
  total_amount
FROM {{ source('raw', 'orders') }}
WHERE status = 'completed'
```

**Becomes Seeknal YAML:**

```yaml
# seeknal/transforms/clean_orders.yml
kind: transform
name: clean_orders
sql: |
  SELECT
    order_id,
    customer_id,
    order_date,
    total_amount
  FROM ref('source.orders')
  WHERE status = 'completed'
```

**Key differences:**
- `{{ source() }}` → `ref('source.name')`
- Jinja macros → Python functions or SQL CTEs
- Tests → Audits or validation rules

## Seeknal vs SQLMesh

### Overview

**SQLMesh** is a data transformation framework focused on deployment workflows with virtual environments and change categorization.

**Seeknal** provides similar deployment capabilities plus feature store functionality.

### Similarities

Both tools provide:

- **Virtual Environments** - Isolated branch development
- **Change Categorization** - Automatic detection of breaking vs non-breaking changes
- **Plan/Apply Workflow** - Review changes before execution
- **SQL + Python** - Mix SQL transformations with Python logic
- **Multiple Engines** - Support for multiple execution engines

### Key Differences

| Feature | Seeknal | SQLMesh |
|---------|---------|---------|
| **Feature Store** | Built-in | No (separate tool needed) |
| **Point-in-Time Joins** | Native support | No |
| **Online Serving** | Built-in | No |
| **Incremental Models** | Basic support | Advanced strategies |
| **Virtual Env Scope** | Project-level | Model-level granularity |
| **UI** | CLI-focused | Web UI available |
| **Semantic Layer** | Built-in | No |

### Plan/Apply Workflow Comparison

**SQLMesh:**
```bash
sqlmesh plan
# Review changes, categorized as forward-only or breaking
sqlmesh apply
```

**Seeknal:**
```bash
seeknal plan
# Review changes, categorized as breaking or non-breaking
seeknal apply
```

Both tools provide similar workflows, but Seeknal integrates this with feature store operations.

### Environment Handling

**SQLMesh Environments:**
- Model-level isolation
- Clones only changed models
- Sophisticated virtual table management

**Seeknal Environments:**
- Project-level isolation
- State tracked per branch
- Simpler isolation model

### When to Choose Seeknal

Choose Seeknal when you need:

1. **Feature Store** - Built-in feature management
2. **ML Workflows** - Training and serving infrastructure
3. **Simpler Mental Model** - Project-level environments
4. **Semantic Layer** - Metrics and business logic layer

### When to Choose SQLMesh

Choose SQLMesh when you need:

1. **Pure Transformations** - No ML feature requirements
2. **Granular Environments** - Model-level isolation
3. **Web UI** - Visual interface for planning
4. **Advanced Incremental** - Complex incremental patterns
5. **Multi-Warehouse** - Sophisticated cross-warehouse logic

## Seeknal vs Feast

### Overview

**Feast** is a dedicated feature store for ML, focused on feature serving and point-in-time correctness.

**Seeknal** provides feature store capabilities plus data pipeline orchestration in one tool.

### Strengths of Seeknal

| Capability | Seeknal | Feast |
|------------|---------|-------|
| **Pipeline Definition** | Built-in YAML + Python | External (dbt/Spark) required |
| **Transformation Engine** | DuckDB + Spark | No (use external tools) |
| **Semantic Layer** | Metrics + models | No |
| **Deployment Workflow** | Plan/apply with change detection | Apply-only |
| **Virtual Environments** | Branch isolation | No |

### Strengths of Feast

| Capability | Feast | Seeknal |
|------------|-------|---------|
| **Registry** | Centralized feature registry | Project-based |
| **Online Store Options** | Redis, DynamoDB, Firestore, etc. | File-based (extensible) |
| **Stream Processing** | Push sources for streaming | Batch-focused |
| **Managed Service** | Tecton (commercial) | Self-hosted only |
| **Community** | Large ML community | Growing community |

### Feature Definition Comparison

**Feast Feature View:**

```python
from feast import FeatureView, Field
from feast.types import Float32, Int64
from datetime import timedelta

user_features = FeatureView(
    name="user_features",
    entities=["user"],
    ttl=timedelta(days=1),
    schema=[
        Field(name="total_orders", dtype=Int64),
        Field(name="total_revenue", dtype=Float32),
    ],
    source=batch_source,
)
```

**Seeknal Feature Group:**

```python
from seeknal.featurestore.feature_group import FeatureGroup
from seeknal.entity import Entity

fg = FeatureGroup(
    name="user_features",
    entity=Entity(name="user", join_keys=["user_id"]),
    materialization=Materialization(event_time_col="event_time")
)
fg.set_dataframe(df).set_features()
fg.write()
```

### When to Choose Seeknal

Choose Seeknal when you need:

1. **End-to-End Pipeline** - Transformations + feature store
2. **SQL-First Transformations** - Define features with SQL
3. **Deployment Workflow** - Plan/apply with change tracking
4. **Multi-Engine** - DuckDB for dev, Spark for prod
5. **Semantic Layer** - Business metrics on top of features

### When to Choose Feast

Choose Feast when you need:

1. **Feature Store Only** - Already have pipeline tooling
2. **Streaming Features** - Real-time feature computation
3. **Diverse Online Stores** - Redis, DynamoDB, etc.
4. **Managed Service** - Tecton for enterprise deployment
5. **Large Registry** - Centralized feature catalog across teams

### Migration Path

**From Feast to Seeknal:**

1. **Migrate transformations to Seeknal pipelines**
2. **Convert FeatureViews to FeatureGroups**
3. **Adjust entity definitions**
4. **Update serving code to use Seeknal APIs**

Most code remains similar due to shared concepts (entities, features, PIT joins).

## Comprehensive Comparison Matrix

### Pipeline Definition

| Tool | Definition Format | Transformation Language | Python Support |
|------|-------------------|------------------------|----------------|
| **Seeknal** | YAML + Python | SQL + Python | Native |
| **dbt** | SQL + Jinja | SQL | Models only |
| **SQLMesh** | SQL + Python | SQL + Python | Native |
| **Feast** | Python | N/A (external) | Native |

### Feature Store Capabilities

| Capability | Seeknal | dbt | SQLMesh | Feast |
|------------|---------|-----|---------|-------|
| **Point-in-Time Joins** | Yes | No | No | Yes |
| **Online Serving** | Yes | No | No | Yes |
| **Feature Registry** | Project-based | No | No | Centralized |
| **Training Dataset Creation** | Yes | No | No | Yes |
| **Training-Serving Parity** | Yes | No | No | Yes |
| **Second-Order Aggregations** | Yes (unique) | No | No | No |

### Deployment & Environments

| Capability | Seeknal | dbt | SQLMesh | Feast |
|------------|---------|-----|---------|-------|
| **Virtual Environments** | Yes | Targets only | Yes | No |
| **Change Detection** | Yes | No | Yes | No |
| **Plan/Apply Workflow** | Yes | No | Yes | No |
| **Rollback** | Yes | Manual | Yes | Registry versioning |
| **CI/CD Integration** | CLI-based | CLI-based | CLI-based | CLI-based |

### Execution Engines

| Engine | Seeknal | dbt | SQLMesh | Feast |
|--------|---------|-----|---------|-------|
| **DuckDB** | Native | Adapter | Native | No |
| **Spark** | Native | Adapter | Native | Data source |
| **Snowflake** | Planned | Adapter | Adapter | Data source |
| **BigQuery** | Planned | Adapter | Adapter | Data source |
| **Postgres** | Planned | Adapter | Adapter | Data source |
| **Trino** | Planned | Adapter | Adapter | Data source |

## Decision Framework

### Choose Seeknal if you need:

- **3+ of these requirements:**
  - Feature store with PIT joins
  - Both analytics and ML workflows
  - Multi-engine flexibility (DuckDB + Spark)
  - Python + SQL transformations
  - Semantic layer for metrics
  - Plan/apply deployment workflow

### Choose dbt if you need:

- **3+ of these requirements:**
  - Pure analytics transformations
  - Mature testing framework
  - Large adapter ecosystem
  - Enterprise support (dbt Cloud)
  - Extensive documentation generation
  - Large community and packages

### Choose SQLMesh if you need:

- **3+ of these requirements:**
  - Advanced deployment workflows
  - Model-level virtual environments
  - Web UI for planning
  - Complex incremental strategies
  - Pure transformation focus (no ML)
  - Fine-grained change management

### Choose Feast if you need:

- **3+ of these requirements:**
  - Dedicated feature store only
  - Already using dbt/Spark for pipelines
  - Streaming feature computation
  - Diverse online store backends
  - Centralized feature registry
  - Tecton managed service option

## Combining Tools

### Seeknal + dbt

Use dbt for complex analytics, Seeknal for ML features:

```yaml
# dbt generates analytics tables
# seeknal/sources/dbt_output.yml
kind: source
name: dbt_analytics
connection: warehouse
dataset: dbt_prod
table: dim_customers

# Seeknal uses dbt output for features
kind: transform
name: customer_features
sql: |
  SELECT
    customer_id,
    total_orders,
    avg_order_value
  FROM ref('source.dbt_analytics')
```

### Seeknal + Feast

Use Seeknal for transformations, Feast for serving:

1. Seeknal materializes features to Parquet
2. Feast reads Parquet as batch source
3. Feast handles online serving

### dbt + SQLMesh + Feast

Classic separation of concerns:

1. **dbt** - Data warehouse transformations
2. **SQLMesh** - Deployment and environment management
3. **Feast** - Feature store for ML

Seeknal aims to unify #1-3 in a single tool.

## Summary

| If you need... | Choose... |
|----------------|-----------|
| **Feature store + pipelines** | Seeknal |
| **Analytics only** | dbt |
| **Advanced deployments** | SQLMesh |
| **Dedicated feature store** | Feast |
| **All-in-one for ML** | Seeknal |
| **Broad ecosystem** | dbt |
| **Streaming features** | Feast |
| **Web UI for planning** | SQLMesh |

Each tool excels in its domain. Seeknal is unique in combining data pipelines, feature store, and semantic layer in a single, cohesive platform designed for ML workflows.

## See Also

- **Seeknal Guides**: [Semantic Layer](./semantic-layer.md), [Training to Serving](./training-to-serving.md), [Testing & Audits](./testing-and-audits.md)
- **Seeknal Concepts**: [Point-in-Time Joins](../concepts/point-in-time-joins.md), [Virtual Environments](../concepts/virtual-environments.md), [Change Categorization](../concepts/change-categorization.md)
- **Getting Started**: [Installation Guide](../getting-started-comprehensive.md), [YAML Tutorial](../tutorials/yaml-pipeline-tutorial.md)
- **External Tools**: [dbt Documentation](https://docs.getdbt.com/), [SQLMesh Documentation](https://sqlmesh.readthedocs.io/), [Feast Documentation](https://docs.feast.dev/)
