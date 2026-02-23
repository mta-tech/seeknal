# Python Pipelines Guide

Seeknal's Python pipeline API provides decorators for defining pipeline nodes as Python functions. Use Python pipelines when you need custom logic, ML model training, or integration with Python libraries.

## Overview

Python decorators let you define data pipeline nodes directly in Python code with full access to the Python ecosystem.

```python
from seeknal.pipeline import source, transform, feature_group, PipelineContext
from seeknal.pipeline.decorators import materialize, second_order_aggregation
```

**When to Use Python Pipelines:**
- Complex business logic requiring custom algorithms
- Machine learning model training and inference
- External API integrations (REST, GraphQL, etc.)
- Advanced data transformations beyond SQL
- Custom validation and data quality checks
- Integration with Python libraries (scikit-learn, requests, etc.)

**When to Use YAML Pipelines:**
- Simple data sources (CSV, Parquet, databases)
- Basic SQL transformations
- Static configurations
- DBT-style declarative workflows

See [Mixed YAML + Python Tutorial](../tutorials/mixed-yaml-python-pipelines.md) for combining both approaches.

---

## Decorator Reference

### @source

Define data ingestion from files or databases.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | `str` | Yes | — | Unique source name |
| `source` | `str` | No | `"csv"` | Source type: `csv`, `parquet`, `json`, `postgres`, etc. |
| `table` | `str` | No | `""` | Table name or file path |
| `columns` | `dict` | No | `None` | Column schema definitions (`{"col": "type"}`) |
| `query` | `str` | No | `None` | SQL query for database sources (pushdown query) |
| `connection` | `str` | No | `None` | Connection profile name (for database sources) |
| `tags` | `list[str]` | No | `None` | Tags for organization |
| `materialization` | `dict \| MaterializationConfig` | No | `None` | Iceberg materialization config |
| `**params` | `Any` | No | — | Additional source-specific parameters |

**Function Signature:**
```python
def your_source_function(ctx: PipelineContext | None = None) -> pd.DataFrame:
    """Load data from external source."""
    pass
```

**Examples:**

**Simple CSV source:**
```python
@source(name="raw_users", source="csv", table="data/users.csv")
def raw_users():
    pass
```

**Database source:**
```python
@source(
    name="events",
    source="postgres",
    table="public.events",
    columns={"event_id": "int", "user_id": "int", "event_time": "timestamp"},
    tags=["production", "analytics"],
)
def events():
    pass
```

**Custom source with Python logic:**
```python
import pandas as pd

@source(name="api_users", source="csv", table="data/users.csv")
def api_users(ctx=None):
    """Load users from external API."""
    import requests

    # Fetch from API
    response = requests.get("https://api.example.com/users")
    data = response.json()

    # Convert to DataFrame
    return pd.DataFrame(data)
```

**Source with Iceberg materialization:**
```python
from seeknal.pipeline.materialization_config import MaterializationConfig

@source(
    name="raw_events",
    source="postgres",
    table="public.events",
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.raw.events",
        mode="append",
    )
)
def raw_events():
    pass
```

---

### @transform

Define data transformation logic using SQL or Python.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | `str` | No | function name | Transform name |
| `sql` | `str` | No | `None` | Optional SQL transform (alternative to function body) |
| `inputs` | `list[str]` | No | `None` | List of upstream node IDs (`["source.users"]`) |
| `tags` | `list[str]` | No | `None` | Tags for organization |
| `materialization` | `dict \| MaterializationConfig` | No | `None` | Iceberg materialization config |
| `**params` | `Any` | No | — | Additional transform-specific parameters |

**Function Signature:**
```python
def your_transform_function(ctx: PipelineContext) -> pd.DataFrame:
    """Transform data from upstream nodes."""
    pass
```

**Examples:**

**Basic SQL transform:**
```python
@transform(name="clean_users")
def clean_users(ctx):
    df = ctx.ref("source.raw_users")
    return ctx.duckdb.sql("""
        SELECT * FROM df
        WHERE active = true
          AND email IS NOT NULL
    """).df()
```

**Transform with explicit inputs:**
```python
@transform(
    name="enriched_sales",
    inputs=["source.sales", "source.products"],
    tags=["enrichment"],
)
def enriched_sales(ctx):
    sales = ctx.ref("source.sales")
    products = ctx.ref("source.products")

    return ctx.duckdb.sql("""
        SELECT
            s.*,
            p.category,
            p.unit_cost,
            s.unit_price - p.unit_cost AS margin
        FROM sales s
        LEFT JOIN products p ON s.product_id = p.product_id
    """).df()
```

**Complex Python logic:**
```python
import pandas as pd
import numpy as np

@transform(name="rfm_scores")
def rfm_scores(ctx):
    """Calculate RFM scores for customer segmentation."""
    df = ctx.ref("transform.clean_transactions")

    # Convert to pandas
    if not isinstance(df, pd.DataFrame):
        df = df.df()

    # Calculate RFM metrics
    current_date = pd.Timestamp.now()
    rfm = df.groupby("customer_id").agg({
        "order_date": lambda x: (current_date - x.max()).days,  # Recency
        "order_id": "nunique",  # Frequency
        "total_amount": "sum",  # Monetary
    }).rename(columns={
        "order_date": "recency",
        "order_id": "frequency",
        "total_amount": "monetary",
    })

    # Calculate scores (1-5 scale)
    rfm["r_score"] = pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])
    rfm["f_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
    rfm["m_score"] = pd.qcut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5])

    return rfm.reset_index()
```

**Machine learning model:**
```python
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler

@transform(
    name="sales_forecast",
    inputs=["transform.regional_sales"],
    tags=["ml", "forecast"],
)
def sales_forecast(ctx):
    """Forecast sales using RandomForest."""
    df = ctx.ref("transform.regional_sales")

    if not isinstance(df, pd.DataFrame):
        df = df.df()

    # Feature engineering
    features = ["total_quantity", "transaction_count", "avg_price"]
    X = df[features].values
    y = df["total_margin"].values

    # Train model
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X, y)

    # Predict with 10% growth assumption
    X_forecast = X * 1.10
    df["forecast_margin"] = model.predict(X_forecast)
    df["projected_growth"] = df["forecast_margin"] - df["total_margin"]

    return df
```

**Transform with Iceberg materialization:**
```python
from seeknal.pipeline.materialization_config import MaterializationConfig

@transform(
    name="daily_metrics",
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.analytics.daily_metrics",
        mode="overwrite",
    )
)
def daily_metrics(ctx):
    df = ctx.ref("transform.clean_events")
    return ctx.duckdb.sql("""
        SELECT
            DATE(event_time) as date,
            event_type,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_id) as unique_users
        FROM df
        GROUP BY DATE(event_time), event_type
    """).df()
```

---

### @feature_group

Define feature groups for ML models with offline/online materialization.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | `str` | No | function name | Feature group name |
| `entity` | `str` | No | `None` | Entity name for joins (`"user"`, `"product"`) |
| `features` | `dict` | No | `None` | Feature schema definitions |
| `inputs` | `list[str]` | No | `None` | List of upstream node IDs |
| `materialization` | `Materialization \| MaterializationConfig \| dict` | No | `None` | Offline/online store config or Iceberg config |
| `tags` | `list[str]` | No | `None` | Tags for organization |
| `**params` | `Any` | No | — | Additional parameters |

**Function Signature:**
```python
def your_feature_group_function(ctx: PipelineContext) -> pd.DataFrame:
    """Compute features for the entity."""
    pass
```

**Materialization Types:**

1. **Offline/Online Store** (using `Materialization`):
   ```python
   from seeknal.pipeline.materialization import Materialization, OfflineConfig

   materialization=Materialization(
       offline=OfflineConfig(format="parquet", partition_by=["year", "month"])
   )
   ```

2. **Iceberg Table** (using `MaterializationConfig`):
   ```python
   from seeknal.pipeline.materialization_config import MaterializationConfig

   materialization=MaterializationConfig(
       enabled=True,
       table="warehouse.online.user_features",
       mode="append",
   )
   ```

**Examples:**

**Simple feature group:**
```python
@feature_group(
    name="user_features",
    entity="user",
)
def user_features(ctx):
    df = ctx.ref("transform.clean_users")
    return ctx.duckdb.sql("""
        SELECT
            user_id,
            COUNT(DISTINCT order_id) as total_orders,
            SUM(order_total) as lifetime_value,
            AVG(order_total) as avg_order_value,
            MAX(order_date) as last_order_date
        FROM df
        GROUP BY user_id
    """).df()
```

**Feature group with offline store:**
```python
from seeknal.pipeline.materialization import Materialization, OfflineConfig

@feature_group(
    name="customer_rfm_features",
    entity="customer",
    materialization=Materialization(
        offline=OfflineConfig(
            format="parquet",
            partition_by=["year", "month"]
        )
    ),
    tags=["rfm", "segmentation"],
)
def customer_rfm_features(ctx):
    """Calculate RFM features per customer."""
    df = ctx.ref("transform.clean_transactions")

    return ctx.duckdb.sql("""
        WITH customer_metrics AS (
            SELECT
                customer_id,
                DATEDIFF('day', MAX(order_date), CURRENT_DATE) as recency_days,
                COUNT(DISTINCT order_id) as frequency,
                SUM(total_amount) as monetary_value
            FROM df
            GROUP BY customer_id
        )
        SELECT
            *,
            -- RFM Scores (1-5 scale)
            NTILE(5) OVER (ORDER BY recency_days DESC) as recency_score,
            NTILE(5) OVER (ORDER BY frequency ASC) as frequency_score,
            NTILE(5) OVER (ORDER BY monetary_value ASC) as monetary_score
        FROM customer_metrics
    """).df()
```

**Feature group with Iceberg materialization:**
```python
from seeknal.pipeline.materialization_config import MaterializationConfig

@feature_group(
    name="product_affinity",
    entity="product",
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.features.product_affinity",
        mode="append",
    ),
)
def product_affinity(ctx):
    """Calculate product co-purchase affinity."""
    df = ctx.ref("transform.clean_orders")

    # Complex Python logic for affinity calculation
    import pandas as pd
    from itertools import combinations

    # Group by order
    orders = df.groupby("order_id")["product_id"].apply(list)

    # Calculate co-occurrence
    pairs = []
    for products in orders:
        for p1, p2 in combinations(products, 2):
            pairs.append({"product_a": p1, "product_b": p2})

    affinity = pd.DataFrame(pairs).groupby(["product_a", "product_b"]).size().reset_index(name="co_purchase_count")

    return affinity
```

---

### @materialize (Stackable)

Attach materialization targets to any node. Stack multiple `@materialize` decorators to write to multiple targets simultaneously.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `type` | `str` | No | `"iceberg"` | Target type: `iceberg` or `postgresql` |
| `connection` | `str` | No | `None` | Named connection from profiles.yml (required for PostgreSQL) |
| `table` | `str` | No | `""` | Target table name (typically required in practice) |
| `mode` | `str` | No | `"full"` | Write mode: `full`, `append`, `overwrite`, `incremental_by_time`, `upsert_by_key` |
| `time_column` | `str` | No | `None` | Time column for incremental mode |
| `lookback` | `str` | No | `None` | Lookback window (e.g., `"7d"`) |
| `unique_keys` | `list[str]` | No | `None` | Columns for upsert matching |
| `**kwargs` | `Any` | No | — | Additional target-specific parameters |

**Examples:**

**Single target (Iceberg):**
```python
@materialize(type="iceberg", table="atlas.warehouse.orders", mode="append")
@transform(name="enriched_orders", inputs=["source.orders"])
def enriched_orders(ctx):
    orders = ctx.ref("source.orders")
    return ctx.duckdb.sql("SELECT * FROM orders").df()
```

**Multi-target (PostgreSQL + Iceberg):**
```python
@materialize(type="postgresql", connection="local_pg",
             table="analytics.orders", mode="upsert_by_key",
             unique_keys=["order_id"])
@materialize(type="iceberg", table="atlas.warehouse.orders", mode="append")
@transform(name="enriched_orders", inputs=["source.orders"])
def enriched_orders(ctx):
    orders = ctx.ref("source.orders")
    return ctx.duckdb.sql("SELECT * FROM orders").df()
```

**PostgreSQL incremental:**
```python
@materialize(type="postgresql", connection="analytics_db",
             table="events.daily_metrics", mode="incremental_by_time",
             time_column="event_date", lookback="7d")
@transform(name="daily_metrics", inputs=["source.events"])
def daily_metrics(ctx):
    events = ctx.ref("source.events")
    return ctx.duckdb.sql(
        "SELECT event_date, COUNT(*) as count FROM events GROUP BY event_date"
    ).df()
```

> **Note:** Decorators are applied bottom-up. Place `@materialize` above the node decorator (`@transform`, `@source`, `@feature_group`).

---

### @second_order_aggregation

Define aggregations on already-aggregated data for multi-level feature engineering.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | `str` | Yes | — | Second-order aggregation name |
| `source` | `str` | Yes | — | Upstream aggregation reference (`"aggregation.user_daily"`) |
| `id_col` | `str` | Yes | — | Entity ID column for grouping (`"region_id"`) |
| `feature_date_col` | `str` | Yes | — | Date column for time-based operations |
| `application_date_col` | `str` | No | `None` | Reference date column for window calculations |
| `description` | `str` | No | `None` | Human-readable description |
| `owner` | `str` | No | `None` | Team/person responsible |
| `tags` | `list[str]` | No | `None` | Tags for organization |
| `materialization` | `dict \| Any` | No | `None` | Iceberg materialization config |
| `**params` | `Any` | No | — | Additional parameters |

**Function Signature:**
```python
def your_aggregation_function(ctx: PipelineContext, df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate pre-aggregated data."""
    pass
```

**Use Cases:**
- User-level features → Store-level features → Region-level features
- Daily metrics → Weekly metrics → Monthly metrics
- Transaction features → Merchant features → Category features

**Examples:**

**Basic second-order aggregation:**
```python
@second_order_aggregation(
    name="region_metrics",
    source="aggregation.store_daily_metrics",
    id_col="region_id",
    feature_date_col="date",
)
def region_metrics(ctx, df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate store metrics to region level."""
    return df.groupby(["region_id", "date"]).agg({
        "total_sales": "sum",
        "avg_transaction_value": "mean",
        "unique_customers": "sum",
    }).reset_index()
```

**Advanced aggregation with statistics:**
```python
import pandas as pd
import numpy as np

@second_order_aggregation(
    name="category_patterns",
    source="aggregation.product_metrics",
    id_col="category_id",
    feature_date_col="metric_date",
    description="Category-level product performance patterns",
    tags=["analytics", "product"],
)
def category_patterns(ctx, df: pd.DataFrame) -> pd.DataFrame:
    """Calculate category-level patterns from product metrics."""

    result = df.groupby("category_id").agg({
        "total_revenue": ["sum", "mean", "std"],
        "units_sold": ["sum", "mean"],
        "unique_customers": "sum",
        "avg_rating": "mean",
    })

    # Flatten column names
    result.columns = ["_".join(col).strip() for col in result.columns.values]
    result = result.reset_index()

    # Calculate derived metrics
    result["revenue_volatility"] = result["total_revenue_std"] / result["total_revenue_mean"]
    result["avg_units_per_product"] = result["units_sold_sum"] / len(df)

    return result
```

**With Iceberg materialization:**
```python
from seeknal.pipeline.materialization_config import MaterializationConfig

@second_order_aggregation(
    name="regional_user_metrics",
    source="aggregation.user_daily_features",
    id_col="region_id",
    feature_date_col="date",
    application_date_col="report_date",
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.analytics.regional_metrics",
        mode="append",
    ),
)
def regional_user_metrics(ctx, df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate user features to regional level."""
    return ctx.duckdb.sql("""
        SELECT
            region_id,
            date,
            COUNT(DISTINCT user_id) as active_users,
            SUM(total_spend_30d) as region_total_spend,
            AVG(total_spend_30d) as avg_spend_per_user,
            STDDEV(total_spend_30d) as spend_std_dev,
            SUM(transaction_count) as region_transaction_count
        FROM df
        GROUP BY region_id, date
    """).df()
```

---

## Pipeline Context API

The `PipelineContext` object (`ctx`) is passed to all decorated functions (except sources) and provides access to:

### ctx.ref(node_reference)

Reference upstream node outputs.

**Signature:**
```python
def ref(node_id: str) -> pd.DataFrame
```

**Parameters:**
- `node_id` (str): Node identifier in format `"kind.name"` (e.g., `"source.users"`, `"transform.clean_data"`)

**Returns:**
- `pd.DataFrame`: DataFrame from the referenced node

**Examples:**
```python
# Reference a source
users = ctx.ref("source.raw_users")

# Reference a transform
clean_data = ctx.ref("transform.clean_transactions")

# Reference a feature group
features = ctx.ref("feature_group.customer_rfm")

# Reference YAML nodes
yaml_data = ctx.ref("source.sales_data")  # From sales_data.yml
```

**How it works:**
1. First checks in-memory cache
2. If not found, loads from `target/intermediate/<node_id>.parquet`
3. Caches for future references

---

### ctx.duckdb

Access the DuckDB connection for SQL queries.

**Signature:**
```python
@property
def duckdb -> duckdb.DuckDBPyConnection
```

**Returns:**
- DuckDB connection object

**Examples:**

**Simple SQL query:**
```python
@transform(name="filter_active")
def filter_active(ctx):
    df = ctx.ref("source.users")
    return ctx.duckdb.sql("SELECT * FROM df WHERE active = true").df()
```

**Register multiple dataframes:**
```python
@transform(name="join_tables")
def join_tables(ctx):
    orders = ctx.ref("source.orders")
    products = ctx.ref("source.products")

    # Register dataframes for SQL access
    ctx.duckdb.register("orders", orders)
    ctx.duckdb.register("products", products)

    return ctx.duckdb.sql("""
        SELECT
            o.order_id,
            o.customer_id,
            p.product_name,
            p.category,
            o.quantity * p.unit_price as total_amount
        FROM orders o
        LEFT JOIN products p ON o.product_id = p.product_id
    """).df()
```

**Use DuckDB functions:**
```python
@transform(name="advanced_analytics")
def advanced_analytics(ctx):
    df = ctx.ref("source.transactions")

    return ctx.duckdb.sql("""
        SELECT
            customer_id,
            -- Window functions
            SUM(amount) OVER (
                PARTITION BY customer_id
                ORDER BY order_date
                ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
            ) as rolling_30d_sum,
            -- Date functions
            DATE_TRUNC('week', order_date) as week,
            -- Statistical functions
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as median_amount,
            -- String functions
            REGEXP_EXTRACT(email, '.*@(.*)$', 1) as email_domain
        FROM df
    """).df()
```

**Execute statements:**
```python
@transform(name="with_temp_tables")
def with_temp_tables(ctx):
    df = ctx.ref("source.sales")

    # Create temporary table
    ctx.duckdb.execute("""
        CREATE TEMP TABLE daily_totals AS
        SELECT DATE(order_date) as date, SUM(amount) as total
        FROM df
        GROUP BY DATE(order_date)
    """)

    # Query the temp table
    return ctx.duckdb.sql("SELECT * FROM daily_totals").df()
```

---

### ctx.config

Access profile configuration from `profiles.yml`.

**Signature:**
```python
config: dict
```

**Contains:**
- Database credentials
- Environment-specific settings
- Custom configuration

**Example:**
```python
@source(name="db_users")
def db_users(ctx):
    # Access database config
    db_config = ctx.config.get("database", {})
    connection_string = db_config.get("connection_string")

    # Use credentials
    import pandas as pd
    return pd.read_sql("SELECT * FROM users", connection_string)
```

---

### ctx.project_path

Path to the project root directory.

**Signature:**
```python
project_path: Path
```

**Example:**
```python
@source(name="local_file")
def local_file(ctx):
    import pandas as pd
    file_path = ctx.project_path / "data" / "users.csv"
    return pd.read_csv(file_path)
```

---

### ctx.target_dir

Path to the target directory for outputs.

**Signature:**
```python
target_dir: Path
```

**Example:**
```python
@transform(name="export_metrics")
def export_metrics(ctx):
    df = ctx.ref("transform.daily_metrics")

    # Write to target directory
    output_path = ctx.target_dir / "exports" / "metrics.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    return df
```

---

## Running Python Pipelines

### Auto-discovery

Seeknal automatically discovers Python pipeline files in the `seeknal/` directory:

```bash
seeknal run
```

**Discovery rules:**
- Searches `seeknal/` and subdirectories
- Looks for Python files with `@source`, `@transform`, `@feature_group`, `@second_order_aggregation` decorators
- Builds a DAG from all discovered nodes
- Executes in topological order

---

### CLI Options

```bash
# Show execution plan without running
seeknal run --show-plan

# Run specific nodes
seeknal run --nodes transform.clean_users,feature_group.user_features

# Run with parallel execution
seeknal run --parallel --max-workers 4

# Dry run (validate without executing)
seeknal run --dry-run

# Filter by tags
seeknal run --tags production,analytics

# Run specific node types
seeknal run --type transform
```

---

### Execution Flow

```
┌─────────────────────────────────────────────────────────────┐
│  Python Pipeline Execution Flow                              │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  1. Discovery                                                 │
│     └─ Scan seeknal/ directory for .py files                 │
│     └─ Import files and extract @decorated functions         │
│     └─ Build registry of nodes                               │
│                                                               │
│  2. DAG Construction                                          │
│     └─ Analyze inputs/dependencies                           │
│     └─ Create dependency graph                               │
│     └─ Topological sort                                       │
│                                                               │
│  3. Execution (for each node)                                 │
│     ├─ Create PipelineContext                                │
│     ├─ Execute function: result = func(ctx)                  │
│     ├─ Store output: target/intermediate/<node>.parquet      │
│     ├─ Register in DuckDB: CREATE VIEW <node> AS ...         │
│     └─ Apply materialization (if configured)                 │
│                                                               │
│  4. Cleanup                                                   │
│     └─ Close DuckDB connections                              │
│     └─ Clear context                                          │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## Project Structure

Organize Python pipelines using a directory structure that matches node types:

```
my-project/
├── seeknal_project.yml          # Project configuration
├── profiles.yml                  # Database credentials (gitignored)
├── .gitignore
├── seeknal/
│   ├── sources/
│   │   └── users.py             # @source definitions
│   ├── transforms/
│   │   ├── clean_users.py       # @transform definitions
│   │   └── enrich_sales.py
│   ├── feature_groups/
│   │   └── user_features.py     # @feature_group definitions
│   ├── aggregations/
│   │   └── daily_metrics.py     # @second_order_aggregation definitions
│   └── pipelines/               # Mixed node types
│       └── customer_ltv.py
├── data/                         # Input data files
│   ├── users.csv
│   └── transactions.csv
└── target/                       # Outputs (gitignored)
    ├── intermediate/             # Node outputs
    │   ├── source_raw_users.parquet
    │   └── transform_clean_users.parquet
    └── exports/                  # Final exports
```

**Recommended conventions:**
- **`sources/`**: Data ingestion nodes
- **`transforms/`**: Data transformation nodes
- **`feature_groups/`**: ML feature definitions
- **`aggregations/`**: Second-order aggregations
- **`pipelines/`**: Mixed or complex multi-step pipelines

---

## PEP 723 Dependency Management

Each Python pipeline file declares its dependencies inline using [PEP 723](https://peps.python.org/pep-0723/) script metadata:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "duckdb>=0.9.0",
#     "scikit-learn>=1.3.0",
# ]
# ///

from seeknal.pipeline import transform

@transform(name="my_transform")
def my_transform(ctx):
    # Your code here
    pass
```

**Benefits:**
- ✅ **No global `requirements.txt`** - Each file is self-contained
- ✅ **Isolated dependencies** - No version conflicts between nodes
- ✅ **Automatic environment management** - `uv` creates virtual environments automatically
- ✅ **Reproducible builds** - Version pinning ensures consistency

**How it works:**
1. Seeknal uses `uv run` to execute each Python file
2. `uv` reads the PEP 723 header
3. Creates an isolated virtual environment
4. Installs specified dependencies
5. Executes the script
6. Caches environment for future runs

**Common dependencies:**
```python
# Base (required for most pipelines)
"pandas>=2.0"
"pyarrow>=14.0"
"duckdb>=0.9.0"

# Machine learning
"scikit-learn>=1.3.0"
"numpy>=1.24"

# External APIs
"requests>=2.31"

# Data validation
"pydantic>=2.0"
```

---

## Mixed YAML + Python Pipelines

Python nodes can reference YAML nodes and vice versa.

**Python referencing YAML:**
```python
# Python file: seeknal/pipelines/enrich_sales.py
from seeknal.pipeline import transform

@transform(name="enriched_sales")
def enriched_sales(ctx):
    # Reference YAML source
    sales = ctx.ref("source.raw_sales")  # From raw_sales.yml
    return ctx.duckdb.sql("SELECT * FROM sales WHERE amount > 0").df()
```

**YAML referencing Python:**
```yaml
# YAML file: seeknal/transforms/regional_totals.yml
name: regional_totals
kind: transform
inputs:
  - ref: transform.enriched_sales  # From enriched_sales.py
transform: |
  SELECT region, SUM(amount) as total_sales
  FROM __THIS__
  GROUP BY region
```

See [Mixed YAML + Python Tutorial](../tutorials/mixed-yaml-python-pipelines.md) for complete examples.

---

## Common Patterns

### Pattern 1: RFM Analysis

Calculate Recency, Frequency, Monetary features for customer segmentation.

```python
from seeknal.pipeline import transform

@transform(name="customer_rfm")
def customer_rfm(ctx):
    df = ctx.ref("transform.clean_transactions")

    return ctx.duckdb.sql("""
        WITH customer_metrics AS (
            SELECT
                customer_id,
                DATEDIFF('day', MAX(order_date), CURRENT_DATE) as recency_days,
                COUNT(DISTINCT order_id) as frequency,
                SUM(total_amount) as monetary_value
            FROM df
            GROUP BY customer_id
        )
        SELECT
            *,
            NTILE(5) OVER (ORDER BY recency_days DESC) as recency_score,
            NTILE(5) OVER (ORDER BY frequency ASC) as frequency_score,
            NTILE(5) OVER (ORDER BY monetary_value ASC) as monetary_score
        FROM customer_metrics
    """).df()
```

---

### Pattern 2: ML Model Training

Train a scikit-learn model as part of the pipeline.

```python
from seeknal.pipeline import transform
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import pandas as pd

@transform(name="churn_model")
def churn_model(ctx):
    """Train churn prediction model."""
    df = ctx.ref("feature_group.customer_features")

    if not isinstance(df, pd.DataFrame):
        df = df.df()

    # Prepare features
    features = ["recency_days", "frequency", "monetary_value", "avg_order_value"]
    X = df[features]
    y = df["churned"]

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train model
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Evaluate
    train_score = model.score(X_train, y_train)
    test_score = model.score(X_test, y_test)

    # Return predictions
    df["churn_probability"] = model.predict_proba(X)[:, 1]
    df["predicted_churn"] = model.predict(X)

    return df
```

---

### Pattern 3: External API Integration

Fetch data from external APIs.

```python
from seeknal.pipeline import source
import pandas as pd
import requests

@source(name="weather_data")
def weather_data(ctx=None):
    """Fetch weather data from external API."""
    api_key = ctx.config.get("weather_api_key") if ctx else None

    response = requests.get(
        "https://api.weather.com/v3/wx/forecast/daily/5day",
        params={"apiKey": api_key, "format": "json"},
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()
    return pd.DataFrame(data["forecasts"])
```

---

### Pattern 4: Data Quality Validation

Implement custom validation logic.

```python
from seeknal.pipeline import transform
import pandas as pd

@transform(name="validated_orders")
def validated_orders(ctx):
    """Validate orders and flag issues."""
    df = ctx.ref("source.raw_orders")

    if not isinstance(df, pd.DataFrame):
        df = df.df()

    # Validation rules
    df["is_valid"] = True
    df["validation_errors"] = ""

    # Rule 1: Amount must be positive
    invalid_amount = df["amount"] <= 0
    df.loc[invalid_amount, "is_valid"] = False
    df.loc[invalid_amount, "validation_errors"] += "Invalid amount; "

    # Rule 2: Customer ID must exist
    missing_customer = df["customer_id"].isna()
    df.loc[missing_customer, "is_valid"] = False
    df.loc[missing_customer, "validation_errors"] += "Missing customer; "

    # Rule 3: Date must not be in future
    future_date = df["order_date"] > pd.Timestamp.now()
    df.loc[future_date, "is_valid"] = False
    df.loc[future_date, "validation_errors"] += "Future date; "

    # Log validation summary
    total_records = len(df)
    invalid_records = (~df["is_valid"]).sum()
    print(f"Validation: {invalid_records}/{total_records} invalid records")

    return df
```

---

### Pattern 5: Export to Multiple Formats

Export pipeline results to multiple destinations.

```python
from seeknal.pipeline import transform
import pandas as pd

@transform(name="export_results")
def export_results(ctx):
    """Export customer segments to multiple formats."""
    df = ctx.ref("transform.customer_segments")

    if not isinstance(df, pd.DataFrame):
        df = df.df()

    # Export to CSV
    csv_path = ctx.target_dir / "exports" / "segments.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)

    # Export to Parquet
    parquet_path = ctx.target_dir / "exports" / "segments.parquet"
    df.to_parquet(parquet_path, index=False)

    # Export to JSON
    json_path = ctx.target_dir / "exports" / "segments.json"
    df.to_json(json_path, orient="records", indent=2)

    # Export summary statistics
    summary = df.groupby("segment").agg({
        "customer_id": "count",
        "lifetime_value": ["mean", "sum"]
    })
    summary_path = ctx.target_dir / "exports" / "segment_summary.csv"
    summary.to_csv(summary_path)

    return df
```

---

## Troubleshooting

### Issue: Import Errors

**Error:**
```
ModuleNotFoundError: No module named 'pandas'
```

**Solution:** Add to PEP 723 dependencies:
```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
# ]
# ///
```

---

### Issue: Node Not Found

**Error:**
```
ValueError: Node 'source.users' not found
```

**Solutions:**
1. Check node ID format: `"kind.name"` (e.g., `"source.users"`)
2. Ensure upstream node executed first
3. Verify node is registered: `seeknal run --show-plan`

---

### Issue: DuckDB Registration

**Error:**
```
Catalog Error: Table with name "df" does not exist
```

**Solution:** Register DataFrames explicitly:
```python
ctx.duckdb.register("df", df)
result = ctx.duckdb.sql("SELECT * FROM df").df()
```

Or use the DataFrame directly:
```python
# DuckDB can query pandas DataFrames directly
result = ctx.duckdb.sql("SELECT * FROM df").df()
```

---

### Issue: Type Mismatches

**Error:**
```
TypeError: Cannot convert dict to DataFrame
```

**Solution:** Ensure functions return pandas DataFrames:
```python
@transform(name="my_transform")
def my_transform(ctx):
    # Always return DataFrame
    result = {"col1": [1, 2], "col2": [3, 4]}
    return pd.DataFrame(result)
```

---

### Issue: Missing Context

**Error:**
```
AttributeError: 'NoneType' object has no attribute 'ref'
```

**Solution:** Ensure transform/feature_group functions accept `ctx` parameter:
```python
# Correct
@transform(name="my_transform")
def my_transform(ctx):  # ✅ ctx parameter
    df = ctx.ref("source.data")
    return df

# Incorrect
@transform(name="my_transform")
def my_transform():  # ❌ Missing ctx
    # Cannot call ctx.ref() here
    pass
```

---

## Best Practices

### 1. Use Type Hints

```python
from seeknal.pipeline import transform, PipelineContext
import pandas as pd

@transform(name="typed_transform")
def typed_transform(ctx: PipelineContext) -> pd.DataFrame:
    """Transform with explicit types."""
    df: pd.DataFrame = ctx.ref("source.users")
    return df
```

---

### 2. Add Docstrings

```python
@transform(name="clean_users")
def clean_users(ctx):
    """Clean user data by removing invalid records.

    Removes:
    - Null emails
    - Inactive accounts
    - Test users

    Returns:
        DataFrame with cleaned user records
    """
    df = ctx.ref("source.raw_users")
    return ctx.duckdb.sql("""
        SELECT * FROM df
        WHERE email IS NOT NULL
          AND active = true
          AND email NOT LIKE '%@test.com'
    """).df()
```

---

### 3. Use Explicit Inputs

```python
# Good: Explicit dependencies
@transform(
    name="join_data",
    inputs=["source.users", "source.orders"],
)
def join_data(ctx):
    users = ctx.ref("source.users")
    orders = ctx.ref("source.orders")
    # ...
```

---

### 4. Handle Errors Gracefully

```python
@transform(name="safe_transform")
def safe_transform(ctx):
    try:
        df = ctx.ref("source.data")
    except ValueError as e:
        print(f"Warning: Could not load data: {e}")
        return pd.DataFrame()  # Return empty DataFrame

    # Process data
    return df
```

---

### 5. Log Progress

```python
@transform(name="large_transform")
def large_transform(ctx):
    df = ctx.ref("source.large_dataset")

    print(f"Processing {len(df)} records...")

    result = ctx.duckdb.sql("SELECT * FROM df WHERE active").df()

    print(f"Filtered to {len(result)} active records")

    return result
```

---

## See Also

- [Python Pipelines Tutorial](../tutorials/python-pipelines-tutorial.md) - Step-by-step tutorial
- [Mixed YAML + Python Tutorial](../tutorials/mixed-yaml-python-pipelines.md) - Combining YAML and Python
- [YAML Pipeline Tutorial](../tutorials/yaml-pipeline-tutorial.md) - YAML pipeline basics
- [CLI Reference](../reference/cli.md) - Command-line interface documentation
- [Iceberg Materialization](../iceberg-materialization.md) - Data lakehouse integration
