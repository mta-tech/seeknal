# Quick Start - Python API

> **Estimated Time:** 10 minutes | **Difficulty:** Beginner | **Format:** Python

Build your first Seeknal pipeline using Python. This approach is ideal for:
- **ML Engineers** building feature stores with complex logic
- **Data Scientists** who prefer programmatic control
- **Developers** integrating Seeknal into Python applications

---

## Why Python?

Python pipelines offer several advantages:

- **Programmatic Control**: Use loops, conditionals, and functions
- **Complex Logic**: Build sophisticated transformations with Python
- **Testing**: Write unit tests for your pipelines
- **Integration**: Embed in Jupyter notebooks or Python applications

---

## Prerequisites

Before starting, ensure you have:

| Requirement | Version | Check |
|-------------|---------|-------|
| Python | 3.11+ | `python --version` |
| pip | Latest | `pip --version` |
| pandas | Latest | `pip show pandas` |

!!! warning "Python Version Check"
    ```bash
    # Check your Python version
    python --version
    ```

    Seeknal requires Python 3.11 or higher.

---

## Part 1: Install & Setup (2 minutes)

### Step 1: Install Seeknal

```bash
pip install seeknal

# Verify installation
python -c "import seeknal; print('Seeknal installed successfully!')"
```

**Expected output:** `Seeknal installed successfully!`

!!! info "More Options"
    For detailed installation instructions, see the **[Installation Guide](../install/)**.

### Step 2: Create Your Project

```bash
seeknal init --name quickstart-python --description "Python pipeline quickstart"
cd quickstart-python
```

---

## Part 2: Understand the Python Pipeline Workflow (2 minutes)

Seeknal's Python workflow is straightforward:

```mermaid
graph LR
    A[Load Data] --> B[Create Task]
    B --> C[Add SQL]
    C --> D[Transform]
    D --> E[Save Results]
```

| Step | Method | What It Does |
|------|--------|--------------|
| **1. Load** | `pd.read_csv()` | Load data into pandas |
| **2. Create** | `DuckDBTask()` | Create transformation task |
| **3. Add** | `add_input()` + `add_sql()` | Specify data and SQL |
| **4. Transform** | `transform()` | Execute transformation |
| **5. Save** | `to_parquet()` | Save results |

!!! tip "Why Python?"
    Use Python when you need:
    - Complex conditional logic
    - Dynamic SQL generation
    - Integration with ML frameworks
    - Programmatic pipeline construction

---

## Part 3: Create Your Python Pipeline (4 minutes)

### Step 1: Create Sample Data

Create `data/sales.csv`:

```csv
date,product_category,quantity,revenue
2024-01-01,Electronics,5,500.00
2024-01-01,Clothing,10,200.00
2024-01-01,Electronics,3,300.00
2024-01-02,Clothing,8,160.00
2024-01-02,Electronics,2,200.00
2024-01-02,Home & Garden,4,120.00
2024-01-03,Electronics,6,600.00
2024-01-03,Clothing,12,240.00
2024-01-03,Home & Garden,3,90.00
```

### Step 2: Create Your Pipeline Script

Create `pipeline.py`:

```python
#!/usr/bin/env python3
"""
Quick Start - Python Pipeline
Calculates daily revenue by product category
"""

import pandas as pd
import pyarrow as pa
from pathlib import Path
from seeknal.tasks.duckdb import DuckDBTask

# Load the data
print("Loading data...")
df = pd.read_csv("data/sales.csv")
print(f"Loaded {len(df)} rows")

# Create DuckDB task
print("Creating transformation task...")
task = DuckDBTask()

# Add input data (convert pandas to Arrow)
arrow_table = pa.Table.from_pandas(df)
task.add_input(dataframe=arrow_table)

# Define transformation SQL
sql = """
SELECT
    date,
    product_category,
    SUM(quantity) as total_quantity,
    SUM(revenue) as daily_revenue
FROM __THIS__
GROUP BY date, product_category
ORDER BY date, daily_revenue DESC
"""

task.add_sql(sql)

# Execute transformation
print("Executing transformation...")
result_arrow = task.transform()

# Convert back to pandas
result_df = result_arrow.to_pandas()

# Save results
output_dir = Path("output")
output_dir.mkdir(exist_ok=True)
output_path = output_dir / "daily_revenue.parquet"

result_df.to_parquet(output_path, index=False)
print(f"Results saved to: {output_path}")

# Display results
print("\nResults:")
print(result_df.to_string(index=False))
```

!!! info "__THIS__ Placeholder"
    The `__THIS__` placeholder automatically references your input data. No complex table names needed.

---

## Part 4: Run and Verify (2 minutes)

### Step 1: Execute Your Pipeline

```bash
python pipeline.py
```

**Expected output:**
```
Loading data...
Loaded 9 rows
Creating transformation task...
Executing transformation...
Results saved to: output/daily_revenue.parquet

Results:
        date product_category  total_quantity  daily_revenue
2024-01-01      Electronics               8         800.00
2024-01-01        Clothing              10         200.00
2024-01-02      Electronics               2         200.00
2024-01-02        Clothing               8         160.00
2024-01-02   Home & Garden               4         120.00
2024-01-03      Electronics               6         600.00
2024-01-03        Clothing              12         240.00
2024-01-03   Home & Garden               3          90.00
```

### Step 2: Verify Output

```bash
# Check the output file
ls -lh output/daily_revenue.parquet

# View with Python
python -c "import pandas as pd; print(pd.read_parquet('output/daily_revenue.parquet'))"
```

!!! success "Congratulations! :tada:"
    You've built a complete data pipeline using Python — programmatic, testable, and flexible.

---

## What Makes Python Great?

### For ML Engineers - Feature Engineering
```python
# Dynamic feature generation
def create_feature_groups(df, categories):
    task = DuckDBTask()
    arrow_table = pa.Table.from_pandas(df)
    task.add_input(dataframe=arrow_table)

    for category in categories:
        sql = f"""
        SELECT
            user_id,
            COUNT(*) as {category}_count,
            SUM(revenue) as {category}_revenue
        FROM __THIS__
        WHERE product_category = '{category}'
        GROUP BY user_id
        """
        task.add_sql(sql)

    return task.transform()
```

### For Data Scientists - Conditional Logic
```python
# Adaptive transformations
def smart_aggregation(df, metric_type):
    if metric_type == "revenue":
        sql = "SELECT user_id, SUM(revenue) FROM __THIS__ GROUP BY user_id"
    elif metric_type == "engagement":
        sql = "SELECT user_id, AVG(session_duration) FROM __THIS__ GROUP BY user_id"
    else:
        raise ValueError(f"Unknown metric type: {metric_type}")

    task = DuckDBTask()
    task.add_input(dataframe=pa.Table.from_pandas(df))
    task.add_sql(sql)
    return task.transform()
```

### For Developers - Integration
```python
# Embed in FastAPI application
from fastapi import FastAPI
from seeknal.tasks.duckdb import DuckDBTask

app = FastAPI()

@app.post("/transform")
async def transform_data(data: dict):
    df = pd.DataFrame(data)
    task = DuckDBTask()
    task.add_input(dataframe=pa.Table.from_pandas(df))
    task.add_sql("SELECT * FROM __THIS__ WHERE value > 100")
    result = task.transform()
    return result.to_pandas().to_dict(orient="records")
```

---

## When to Use Python vs YAML

| Use Python When... | Use YAML When... |
|-------------------|-----------------|
| Need complex logic | Want simple, declarative pipelines |
| Building ML features | Creating standard ETL/ELT jobs |
| Dynamic SQL generation | Prefer version-controlled configs |
| Integrating with Python apps | Working with non-technical stakeholders |
| Writing unit tests | Want git-friendly diffs |

!!! tip "Pro Tip"
    You can mix both! Use YAML for standard pipelines and Python for complex feature engineering. Both approaches work with the same Seeknal engine.

---

## What's Next?

Choose your learning path:

| Data Engineer | Analytics Engineer | ML Engineer |
|---------------|-------------------|-------------|
| [Python ELT Pipelines](../getting-started/data-engineer-path/1-elt-pipeline.md) | [Semantic Models in Python](../getting-started/analytics-engineer-path/1-semantic-models.md) | [Feature Store API](../getting-started/ml-engineer-path/1-feature-store.md) |

---

## Troubleshooting

!!! stuck "Common Issues"
    **Problem:** `ModuleNotFoundError: No module named 'seeknal'`
    - Activate your virtual environment: `source .venv/bin/activate`
    - Verify installation: `pip show seeknal`

    **Problem:** `ArrowError: Column type mismatch`
    - Check that pandas DataFrame dtypes match your expectations
    - Use `df.dtypes` to inspect column types

    **Problem:** `SQL syntax error`
    - Verify SQL syntax is valid
    - Check column names match input DataFrame

[Full Troubleshooting Guide →](../troubleshooting/)
