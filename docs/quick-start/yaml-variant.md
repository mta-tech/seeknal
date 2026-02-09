# Quick Start - YAML Pipeline

> **Estimated Time:** 10 minutes | **Difficulty:** Beginner | **Format:** YAML

Build your first Seeknal pipeline using YAML. This approach is ideal for:
- **Data Engineers** who want version-controlled, declarative pipelines
- **Analytics Engineers** who prefer SQL-based transformations
- **ML Engineers** who need reproducible, reviewable feature definitions

---

## Why YAML?

YAML pipelines offer several advantages:

- **Version Control**: Every change is tracked in git
- **Code Review**: Pipeline changes can be reviewed like code
- **Declarative**: Describe what you want, not how to get there
- **Reproducible**: Same pipeline, same results, every time

---

## Prerequisites

Before starting, ensure you have:

| Requirement | Version | Check |
|-------------|---------|-------|
| Python | 3.11+ | `python --version` |
| pip | Latest | `pip --version` |

!!! warning "Python Version Check"
    ```bash
    # Check your Python version
    python --version
    ```

    Seeknal requires Python 3.11 or higher.

---

## Part 1: Install & Setup (2 minutes)

### Step 1: Install Seeknal

Seeknal is distributed as a wheel file via GitHub releases.

!!! info "Installation Guide"
    For detailed installation instructions, see the **[Installation Guide](../install/)**.

    **Quick install summary:**
    1. Download the latest wheel from [GitHub Releases](https://github.com/mta-tech/seeknal/releases)
    2. Install with: `uv pip install seeknal-<version>-py3-none-any.whl`

```bash
# Create a virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .\.venv\Scripts\activate

# Install uv (recommended package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh  # macOS/Linux
# powershell -c "irm https://astral.sh/uv/install.ps1 | iex"  # Windows

# Download Seeknal wheel from GitHub releases
# Visit: https://github.com/mta-tech/seeknal/releases

# Install Seeknal (replace <version> with actual version)
uv pip install seeknal-<version>-py3-none-any.whl

# Or use pip instead:
# pip install seeknal-<version>-py3-none-any.whl

# Verify installation
seeknal --version
```

**Expected output:** `seeknal x.x.x`

### Step 2: Initialize Your Project

```bash
# Create a new project
seeknal init quickstart-yaml
cd quickstart-yaml
```

**Expected output:**
```
Creating project 'quickstart-yaml'...
  ✓ Created seeknal.yml
  ✓ Created pipelines/ directory
  ✓ Created data/ directory
  ✓ Created output/ directory
Project initialized successfully!
```

---

## Part 2: Understand the YAML Pipeline Workflow (2 minutes)

Seeknal's YAML workflow follows a simple 4-step process:

```mermaid
graph LR
    A[init] --> B[draft]
    B --> C[apply]
    C --> D[run]
```

| Step | Command | What It Does |
|------|---------|--------------|
| **1. init** | `seeknal init <name>` | Creates project structure |
| **2. draft** | `seeknal draft <kind>` | Generates YAML template |
| **3. apply** | `seeknal apply <file>` | Adds to your project |
| **4. run** | `seeknal run` | Executes pipeline |

!!! tip "Why This Workflow?"
    YAML files become part of your codebase — track them in git, review them in PRs, and deploy them with confidence.

---

## Part 3: Create Your YAML Pipeline (4 minutes)

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

### Step 2: Draft a Source

```bash
seeknal draft source --name sales_data --path data/sales.csv
```

**Expected output:** `Created draft: pipelines/sources/sales_data.yaml`

Edit `pipelines/sources/sales_data.yaml`:

```yaml
kind: source
name: sales_data

format:
  type: csv
  path: data/sales.csv

schema:
  - column: date
    type: date
  - column: product_category
    type: string
  - column: quantity
    type: integer
  - column: revenue
    type: float
```

### Step 3: Apply the Source

```bash
seeknal apply pipelines/sources/sales_data.yaml
```

**Expected output:** `Source 'sales_data' applied successfully.`

### Step 4: Draft a Transform

```bash
seeknal draft transform --name daily_revenue --input sales_data
```

**Expected output:** `Created draft: pipelines/transforms/daily_revenue.yaml`

Edit `pipelines/transforms/daily_revenue.yaml`:

```yaml
kind: transform
name: daily_revenue

input: sales_data

sql: |
  SELECT
    date,
    product_category,
    SUM(quantity) as total_quantity,
    SUM(revenue) as daily_revenue
  FROM __THIS__
  GROUP BY date, product_category
  ORDER BY date, daily_revenue DESC
```

!!! info "__THIS__ Placeholder"
    The `__THIS__` placeholder automatically references your input. No complex table names or joins needed.

### Step 5: Apply the Transform

```bash
seeknal apply pipelines/transforms/daily_revenue.yaml
```

### Step 6: Draft an Output

```bash
seeknal draft output --name results --input daily_revenue
```

**Expected output:** `Created draft: pipelines/outputs/results.yaml`

Edit `pipelines/outputs/results.yaml`:

```yaml
kind: output
name: results

input: daily_revenue

target:
  type: file
  format: parquet
  path: output/daily_revenue.parquet
```

### Step 7: Apply the Output

```bash
seeknal apply pipelines/outputs/results.yaml
```

!!! success "Checkpoint"
    You now have three YAML files defining your entire pipeline:
    - `pipelines/sources/sales_data.yaml`
    - `pipelines/transforms/daily_revenue.yaml`
    - `pipelines/outputs/results.yaml`

    These files can be version-controlled, code-reviewed, and deployed like any other code.

---

## Part 4: Run and Verify (2 minutes)

### Step 1: Execute Your Pipeline

```bash
seeknal run
```

**Expected output:**
```
Running pipeline...
  → Loading source: sales_data
  → Transforming: daily_revenue
  → Writing output: results
✓ Pipeline completed successfully!
```

### Step 2: View Your Results

```bash
# Inspect the Parquet file
python -c "import pandas as pd; print(pd.read_parquet('output/daily_revenue.parquet'))"
```

**Expected output:**
```
         date product_category  total_quantity  daily_revenue
0  2024-01-01      Electronics               8         800.00
1  2024-01-01        Clothing              10         200.00
2  2024-01-02      Electronics               2         200.00
3  2024-01-02        Clothing               8         160.00
4  2024-01-02   Home & Garden               4         120.00
5  2024-01-03      Electronics               6         600.00
6  2024-01-03        Clothing              12         240.00
7  2024-01-03   Home & Garden               3          90.00
```

!!! success "Congratulations! :tada:"
    You've built a complete data pipeline using YAML — version-controlled, reviewable, and reproducible.

---

## What Makes YAML Great?

### For Data Engineers
```yaml
# Infrastructure-as-code mindset
kind: transform
name: customer_orders
sql: |
  SELECT * FROM staging.orders
  WHERE processed_at IS NULL
```

### For Analytics Engineers
```yaml
# Business metrics as code
kind: transform
name: revenue_kpi
sql: |
  SELECT
    DATE(order_date) as date,
    SUM(revenue) as total_revenue
  FROM orders
  GROUP BY 1
```

### For ML Engineers
```yaml
# Feature definitions as code
kind: transform
name: user_features
sql: |
  SELECT
    user_id,
    AVG(session_duration) as avg_session,
    COUNT(DISTINCT product_id) as product_diversity
  FROM user_events
  GROUP BY user_id
```

---

## What's Next?

Choose your learning path:

| Data Engineer | Analytics Engineer | ML Engineer |
|---------------|-------------------|-------------|
| [Build ELT Pipelines](../getting-started/data-engineer-path/1-elt-pipeline.md) | [Define Semantic Models](../getting-started/analytics-engineer-path/1-semantic-models.md) | [Create Feature Groups](../getting-started/ml-engineer-path/1-feature-store.md) |

---

## Troubleshooting

!!! stuck "Common Issues"
    **Problem:** `YAML validation failed`
    - Check indentation (use spaces, not tabs)
    - Verify no trailing commas

    **Problem:** `Source not found`
    - Run `seeknal status` to verify applied resources
    - Re-apply any missing sources

    **Problem:** `SQL syntax error`
    - Check column names match source schema
    - Verify SQL syntax in your transform

[Full Troubleshooting Guide →](../troubleshooting/)
