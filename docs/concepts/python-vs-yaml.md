# Python API vs YAML Workflows

Seeknal offers two complementary approaches to define data pipelines. Choose based on your use case, team skills, and operational needs.

## Quick Comparison

| Aspect | Python API (Flow/Task) | YAML Workflows (seeknal run) |
|--------|----------------------|------------------------------|
| **Definition** | Python code | YAML files |
| **Execution** | `flow.run()` in Python | `seeknal run` in CLI |
| **Best for** | Complex logic, ML | SQL transforms, ETL |
| **Learning curve** | Python programming | YAML syntax |
| **Environment support** | Manual | Built-in (`--env`) |
| **Change tracking** | No | Yes (`seeknal diff`) |
| **Parallel execution** | Manual | Built-in (`--parallel`) |
| **Audit/validation** | Manual | Built-in (`seeknal audit`) |
| **State management** | Manual | Automatic (incremental runs) |
| **IDE support** | Full Python tooling | Limited YAML validation |
| **Type safety** | Python type hints | Schema validation |
| **Debugging** | Python debugger | CLI logs + dry-run |
| **Version control** | Git for Python files | Git for YAML files |
| **CI/CD integration** | Custom scripts | Built-in commands |

## When to Use Python API

### Use Cases

**Complex Business Logic**
```python
# Custom feature engineering with business rules
from seeknal.flow import Flow
from seeknal.tasks.duckdb import DuckDBTransform

def calculate_customer_lifetime_value(df):
    # Complex logic not expressible in SQL
    df['ltv'] = df.apply(lambda row:
        proprietary_ltv_model(
            row['purchases'],
            row['engagement_score'],
            row['tenure_days']
        ), axis=1)
    return df

flow = Flow(
    name="customer_ltv",
    tasks=[
        DuckDBTransform(sql="SELECT * FROM source.customers"),
        PythonTransform(func=calculate_customer_lifetime_value)
    ]
)
```

**Machine Learning Pipelines**
```python
# scikit-learn, PyTorch, TensorFlow integration
from sklearn.ensemble import RandomForestClassifier
from seeknal.tasks.ml import TrainModel

flow = Flow(
    name="churn_prediction",
    tasks=[
        LoadFeatures(feature_group="user_features"),
        TrainModel(
            model=RandomForestClassifier(n_estimators=100),
            target_col="churned",
            feature_cols=["tenure", "activity", "spend"]
        ),
        SaveModel(path="models/churn_v1.pkl")
    ]
)
```

**External API Integration**
```python
# Fetch from REST APIs, enrich with external data
import requests

def enrich_with_clearbit(df):
    for idx, row in df.iterrows():
        response = requests.get(
            f"https://company.clearbit.com/v2/companies/find?domain={row['domain']}"
        )
        df.at[idx, 'industry'] = response.json().get('industry')
    return df

flow = Flow(
    name="company_enrichment",
    tasks=[
        LoadCSV(path="data/companies.csv"),
        PythonTransform(func=enrich_with_clearbit),
        WriteDuckDB(table="enriched_companies")
    ]
)
```

**Notebook-Based Development**
```python
# Interactive data exploration in Jupyter
import pandas as pd
from seeknal.flow import Flow

# Load data
df = pd.read_parquet("data/transactions.parquet")

# Explore
df.describe()
df.groupby('category').agg({'amount': 'sum'})

# Define pipeline after exploration
flow = Flow(name="analysis", tasks=[...])
flow.run()
```

### Advantages

- Full Python language features (classes, functions, libraries)
- Rich ecosystem (pandas, numpy, scikit-learn, PyTorch)
- IDE support (autocomplete, type checking, refactoring)
- Debugger integration (pdb, ipdb, IDE debuggers)
- Flexible control flow (loops, conditionals, error handling)
- Easy to test with pytest
- Reusable Python packages

### Limitations

- No automatic change detection (must re-run everything)
- No built-in environments (manual state management)
- Harder to audit (code inspection vs declarative YAML)
- Less visible to non-Python users (analysts, stakeholders)
- Requires Python expertise for team collaboration

## When to Use YAML Workflows

### Use Cases

**SQL-Based Transformations**
```yaml
# Clean and aggregate sales data
kind: transform
name: daily_sales_summary
description: Daily sales aggregated by region
transform: |
  SELECT
    DATE(order_date) as date,
    region,
    COUNT(*) as order_count,
    SUM(total_amount) as total_sales,
    AVG(total_amount) as avg_order_value
  FROM source.orders
  WHERE status = 'completed'
  GROUP BY DATE(order_date), region
inputs:
  - ref: source.orders
tags:
  - sales
  - analytics
```

**Standard ETL Pipelines**
```yaml
# Extract-Transform-Load pattern
# 1. Source: Load from database
kind: source
name: raw_orders
source: postgres
table: public.orders
params:
  connection: prod_db

---
# 2. Transform: Clean data
kind: transform
name: clean_orders
transform: |
  SELECT * FROM source.raw_orders
  WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
inputs:
  - ref: source.raw_orders

---
# 3. Exposure: Export to warehouse
kind: exposure
name: warehouse_orders
output:
  type: bigquery
  table: analytics.clean_orders
inputs:
  - ref: transform.clean_orders
```

**Team Collaboration**
```yaml
# Declarative, reviewable, self-documenting
kind: feature_group
name: customer_features
description: RFM features for customer segmentation
owner: analytics-team
entity:
  name: customer
  join_keys:
    - customer_id
features:
  recency_days:
    description: Days since last purchase
    data_type: integer
  frequency:
    description: Number of purchases
    data_type: integer
  monetary_value:
    description: Total spend
    data_type: float
materialization:
  offline:
    enabled: true
    schedule: daily
  online:
    enabled: true
    ttl_seconds: 86400
tags:
  - customer
  - rfm
  - feature-store
```

**Production Deployment**
```bash
# Safe environment-based deployment
seeknal plan staging
seeknal run --env staging --parallel
pytest tests/
seeknal promote staging
```

### Advantages

- Declarative (what, not how)
- Reviewable (Git diffs show intent clearly)
- Self-documenting (description, owner, tags)
- Change detection (incremental runs)
- Environment support (dev/staging/prod)
- Parallel execution (automatic batching)
- No Python expertise required
- Auditability (YAML is easier to scan than code)
- Version control friendly

### Limitations

- Limited to SQL transformations
- No custom logic (loops, API calls, etc.)
- Less flexible than code
- YAML syntax can be verbose
- Limited IDE support (no autocomplete for feature names)
- Harder to unit test (need seeknal CLI)

## When to Use Both (Mixed Pipelines)

### Use Cases

**Python for Custom Transforms, YAML for Everything Else**

```yaml
# sources/sales.yml - YAML source
kind: source
name: raw_sales
source: csv
table: data/sales.csv
```

```python
# pipelines/enrich_sales.py - Python transform
from seeknal.flow import Flow
from seeknal.tasks.duckdb import DuckDBSource
from seeknal.context import ctx

def enrich_with_weather(df):
    """Enrich sales with weather data from API."""
    import requests
    for idx, row in df.iterrows():
        weather = requests.get(
            f"https://api.weather.com/v1/history?date={row['date']}&location={row['region']}"
        ).json()
        df.at[idx, 'temperature'] = weather['temp']
        df.at[idx, 'precipitation'] = weather['precip']
    return df

# Reference YAML source via ctx.ref()
flow = Flow(
    name="enriched_sales",
    input=ctx.ref("source.raw_sales"),
    tasks=[PythonTransform(func=enrich_with_weather)],
    output="transform.enriched_sales"
)
```

```yaml
# transforms/regional_totals.yml - YAML transform of Python output
kind: transform
name: regional_totals
description: Aggregate enriched sales by region
transform: |
  SELECT
    region,
    AVG(temperature) as avg_temp,
    SUM(total_sales) as total_sales,
    CORR(temperature, total_sales) as temp_sales_correlation
  FROM transform.enriched_sales
  GROUP BY region
inputs:
  - ref: transform.enriched_sales  # References Python pipeline output
```

### Pattern: YAML Pipeline with Python Hooks

Use YAML for structure, Python for custom logic:

```yaml
# Base pipeline definition
kind: feature_group
name: user_features
entity:
  name: user
  join_keys: [user_id]
features:
  lifetime_value:
    source: python://pipelines/custom_ltv.py:calculate_ltv
  churn_risk:
    source: python://pipelines/custom_churn.py:predict_churn
  engagement_score:
    source: sql
    definition: |
      SELECT user_id, COUNT(*) as engagement_score
      FROM events
      GROUP BY user_id
```

```python
# pipelines/custom_ltv.py
def calculate_ltv(user_id: str) -> float:
    """Custom LTV calculation using proprietary model."""
    return proprietary_ltv_model(user_id)
```

### Advantages of Mixed Approach

- Best of both worlds: SQL for simple, Python for complex
- Gradual migration (start with YAML, add Python where needed)
- Team flexibility (analysts write YAML, engineers write Python)
- Leverage existing YAML infrastructure (environments, change detection)
- Python only where necessary (reduce maintenance burden)

## Detailed Comparison

### Syntax Examples

**Python API:**
```python
from seeknal.flow import Flow
from seeknal.tasks.duckdb import DuckDBSource, DuckDBTransform

flow = Flow(
    name="sales_pipeline",
    input=DuckDBSource(
        source="csv",
        path="data/sales.csv",
        schema={"date": "DATE", "amount": "FLOAT"}
    ),
    tasks=[
        DuckDBTransform(
            sql="""
                SELECT
                    DATE(date) as date,
                    SUM(amount) as total
                FROM __input__
                GROUP BY DATE(date)
            """
        )
    ],
    output="transform.daily_sales"
)

flow.run()
```

**YAML Workflow:**
```yaml
kind: source
name: sales
source: csv
table: data/sales.csv
schema:
  - name: date
    data_type: date
  - name: amount
    data_type: float

---
kind: transform
name: daily_sales
transform: |
  SELECT
    DATE(date) as date,
    SUM(amount) as total
  FROM source.sales
  GROUP BY DATE(date)
inputs:
  - ref: source.sales
```

```bash
seeknal run
```

### Environment Management

**Python API (Manual):**
```python
import os

env = os.getenv("SEEKNAL_ENV", "prod")

if env == "dev":
    output_path = "target/dev/daily_sales.parquet"
elif env == "staging":
    output_path = "target/staging/daily_sales.parquet"
else:
    output_path = "target/prod/daily_sales.parquet"

flow.run(output_path=output_path)
```

**YAML Workflow (Built-in):**
```bash
# Automatic environment management
seeknal plan dev
seeknal run --env dev
seeknal promote dev
```

### Change Detection

**Python API (Manual):**
```python
import hashlib
import json

def flow_hash(flow):
    content = json.dumps(flow.to_dict(), sort_keys=True)
    return hashlib.sha256(content.encode()).hexdigest()

current_hash = flow_hash(flow)
stored_hash = load_hash("state.json")

if current_hash != stored_hash:
    print("Flow changed, re-running...")
    flow.run()
else:
    print("No changes, skipping execution")
```

**YAML Workflow (Built-in):**
```bash
# Automatic incremental execution
seeknal run  # Only runs changed nodes
```

### Parallel Execution

**Python API (Manual):**
```python
from concurrent.futures import ThreadPoolExecutor

def run_task(task):
    return task.execute()

# Manual parallelization
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(run_task, task) for task in tasks]
    results = [f.result() for f in futures]
```

**YAML Workflow (Built-in):**
```bash
# Automatic parallel batching
seeknal run --parallel --max-workers 4
```

## Migration Guide

### From Python to YAML

**Step 1**: Identify SQL-only tasks
```python
# Before (Python)
flow = Flow(
    name="sales",
    tasks=[
        DuckDBTransform(sql="SELECT * FROM source.orders WHERE status = 'completed'")
    ]
)
```

**Step 2**: Convert to YAML
```yaml
# After (YAML)
kind: transform
name: sales
transform: |
  SELECT * FROM source.orders WHERE status = 'completed'
inputs:
  - ref: source.orders
```

**Step 3**: Keep Python for custom logic
```python
# Keep this in Python (API calls, ML)
def enrich_with_api(df):
    return fetch_and_merge(df)
```

### From YAML to Python

**Step 1**: Copy SQL to Python
```yaml
# Before (YAML)
kind: transform
name: sales
transform: |
  SELECT * FROM source.orders
```

```python
# After (Python)
from seeknal.tasks.duckdb import DuckDBTransform

transform = DuckDBTransform(
    name="sales",
    sql="SELECT * FROM source.orders"
)
```

**Step 2**: Add Python logic
```python
# Now add custom logic
import pandas as pd

def custom_processing(df: pd.DataFrame) -> pd.DataFrame:
    df['custom_field'] = df.apply(custom_logic, axis=1)
    return df

flow = Flow(
    name="sales",
    tasks=[
        DuckDBTransform(sql="SELECT * FROM source.orders"),
        PythonTransform(func=custom_processing)
    ]
)
```

## Best Practices

### Default to YAML

Start with YAML workflows for:
- Standard ETL pipelines
- SQL transformations
- Team collaboration
- Production deployment

Only use Python when you need:
- Custom business logic
- Machine learning
- External API integration
- Complex control flow

### Keep Python Modular

If using Python:
- Extract reusable logic to separate modules
- Use type hints for clarity
- Write unit tests with pytest
- Document with docstrings

```python
# Good: Modular, testable, documented
from typing import Dict
import pandas as pd

def calculate_rfm(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate Recency, Frequency, Monetary features.

    Args:
        df: DataFrame with customer_id, order_date, total_amount

    Returns:
        DataFrame with RFM features added
    """
    # Implementation
    return df

# tests/test_rfm.py
def test_calculate_rfm():
    df = pd.DataFrame(...)
    result = calculate_rfm(df)
    assert 'recency_days' in result.columns
```

### Use YAML for Interfaces

Define stable interfaces in YAML, implement logic in Python:

```yaml
# Stable interface (YAML)
kind: feature_group
name: customer_features
features:
  lifetime_value:
    description: Predicted customer lifetime value
    data_type: float
    source: python://models/ltv.py
```

```python
# Implementation can change (Python)
def calculate_ltv(customer_id: str) -> float:
    # Model can be updated without changing YAML
    return model_v2.predict(customer_id)
```

### Document Intent

**YAML**: Use description, owner, tags
```yaml
kind: transform
name: clean_orders
description: Remove invalid orders and standardize formats
owner: data-team
tags:
  - data-quality
  - daily
```

**Python**: Use docstrings, type hints
```python
def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Remove invalid orders and standardize formats.

    Removes:
      - Orders with null customer_id
      - Orders with negative amounts
      - Duplicate orders

    Standardizes:
      - Date format to ISO 8601
      - Country codes to ISO 3166

    Args:
        df: Raw orders DataFrame

    Returns:
        Cleaned orders DataFrame
    """
    return df[df['amount'] > 0].drop_duplicates()
```

## Performance Considerations

### Python API

**Pros:**
- Direct DataFrame manipulation (no SQL parsing)
- Can use numba, Cython for speed
- Memory efficient for large data (chunking)

**Cons:**
- Python GIL limits parallelism
- DataFrame operations can be slower than SQL
- Manual optimization required

### YAML Workflows

**Pros:**
- DuckDB SQL engine is highly optimized
- Automatic parallel execution
- Query optimization built-in

**Cons:**
- SQL parsing overhead
- Less control over execution plan
- Limited to DuckDB capabilities

**Recommendation**: Use YAML for large-scale SQL, Python for small-scale custom logic.

## Debugging and Troubleshooting

### Python API

```python
# Use Python debugger
import pdb

def transform(df):
    pdb.set_trace()  # Breakpoint
    return df.groupby('region').sum()

# Run with debugger
python -m pdb pipeline.py
```

### YAML Workflows

```bash
# Dry-run to preview
seeknal dry-run

# Validate without execution
seeknal plan

# Run with verbose logging
seeknal run --log-level DEBUG

# Run single node
seeknal run --select transform.clean_orders
```

## See Also

- **Tutorials**: [YAML Pipeline Tutorial](../tutorials/yaml-pipeline-tutorial.md), [Python Pipelines Tutorial](../tutorials/python-pipelines-tutorial.md), [Mixed YAML + Python](../tutorials/mixed-yaml-python-pipelines.md)
- **Concepts**: [Virtual Environments](virtual-environments.md) (YAML only), [Change Categorization](change-categorization.md) (YAML only)
- **Reference**: [CLI Commands](../reference/cli.md), [YAML Schema](../reference/yaml-schema.md), [Configuration](../reference/configuration.md)
