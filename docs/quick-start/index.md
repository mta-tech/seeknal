# Quick Start

> **Estimated Time:** 10 minutes | **Difficulty:** Beginner

Get Seeknal installed and running your first data pipeline in under 10 minutes.

!!! info "Choose Your Format"
    Seeknal supports both **YAML** and **Python** workflows. This guide uses YAML (recommended for beginners).

    - **[YAML Quick Start](yaml-variant.md)** — Declarative, version-controlled pipelines
    - **[Python Quick Start](python-variant.md)** — Programmatic control and complex logic

    Both workflows are equally powerful — choose based on your preference.

---

## What You'll Build

A simple data pipeline that:
- Loads sales data from a CSV file
- Calculates daily revenue by product category
- Outputs results to Parquet format

This example works for **all personas** — whether you're a Data Engineer, Analytics Engineer, or ML Engineer, this workflow is the foundation for everything you'll do with Seeknal.

---

## Prerequisites

Before starting, ensure you have:

| Requirement | Version | Check |
|-------------|---------|-------|
| Python | 3.11+ | `python --version` |
| pip | Latest | `pip --version` |

That's it! No databases, no infrastructure, no complex setup.

!!! warning "Python Version Check"
    ```bash
    # Check your Python version
    python --version
    ```

    - If you see **Python 3.11+** — You're ready to go!
    - If you see **Python 3.10 or earlier** — Install a newer version from [python.org](https://www.python.org/downloads/)

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

=== "macOS / Linux"

    ```bash
    # Create a virtual environment (recommended)
    python -m venv .venv
    source .venv/bin/activate

    # Install uv (recommended package manager)
    curl -LsSf https://astral.sh/uv/install.sh | sh

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

=== "Windows (PowerShell)"

    ```powershell
    # Create a virtual environment (recommended)
    python -m venv .venv
    .\.venv\Scripts\activate

    # Install uv (recommended package manager)
    powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

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
seeknal init --name quickstart-demo
cd quickstart-demo
```

**Expected output:**
```
Creating project 'quickstart-demo'...
  ✓ Created seeknal.yml
  ✓ Created pipelines/ directory
  ✓ Created data/ directory
  ✓ Created output/ directory
Project initialized successfully!
```

**What happened?** Seeknal created a project directory with configuration files. You'll see:
- `seeknal.yml` - Project configuration
- `pipelines/` - Where your pipeline definitions go
- `data/` - For sample and input data
- `output/` - Where results are written

!!! success "Checkpoint"
    You should see a `seeknal.yml` file and `pipelines/` directory. If not, check that `seeknal init` completed successfully.

!!! stuck "Stuck? Installation Issues"
    **Problem:** `seeknal: command not found`

    **Solution:** Make sure your virtual environment is activated:
    ```bash
    # macOS/Linux
    source .venv/bin/activate

    # Windows
    .\.venv\Scripts\activate
    ```

    **Problem:** `Permission denied` error

    **Solution:** Use a virtual environment instead of installing globally (see Step 1).

---

## Part 2: Understand the Pipeline Builder Workflow (2 minutes)

Seeknal uses a simple 4-step workflow for all data pipelines:

```mermaid
graph LR
    A[init] --> B[draft]
    B --> C[apply]
    C --> D[run]
```

| Step | Command | What It Does |
|------|---------|--------------|
| **1. init** | `seeknal init <name>` | Creates a new project with configuration |
| **2. draft** | `seeknal draft <kind>` | Generates a template YAML file |
| **3. apply** | `seeknal apply <file>` | Adds the draft to your project |
| **4. run** | `seeknal run` | Executes your pipeline |

!!! tip "Why This Workflow?"
    - **Safety**: Review changes before executing
    - **Versioning**: Track every modification
    - **Collaboration**: Code review for data pipelines
    - **Reproducibility**: Same code, same results

---

## Part 3: Create Your First Pipeline (4 minutes)

### Step 1: Create Sample Data

Create a file at `data/sales.csv` with this content:

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

**Expected output:**
```
Created draft: pipelines/sources/sales_data.yaml
```

This creates `pipelines/sources/sales_data.yaml`. Let's examine it:

```yaml
# pipelines/sources/sales_data.yaml
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

!!! info "What's a Source?"
    A source defines where your data comes from — files, databases, APIs, etc. The `draft` command detected the CSV structure automatically.

### Step 3: Apply the Source

```bash
seeknal apply pipelines/sources/sales_data.yaml
```

**Expected output:**
```
Applying source 'sales_data'...
  ✓ Validated YAML schema
  ✓ Registered source 'sales_data'
Source applied successfully!
```

!!! stuck "Stuck? Apply Errors"
    **Problem:** `YAML validation failed`

    **Solution:** Check that your YAML indentation is correct (use spaces, not tabs)

    **Problem:** `File not found: data/sales.csv`

    **Solution:** Make sure you created the CSV file in the correct location:
    ```bash
    # Verify file exists
    ls data/sales.csv

    # If missing, create it from the example above
    ```

### Step 4: Draft a Transform

Now let's transform the data — calculate daily revenue by product category:

```bash
seeknal draft transform --name daily_revenue --input sales_data
```

**Expected output:**
```
Created draft: pipelines/transforms/daily_revenue.yaml
```

Edit `pipelines/transforms/daily_revenue.yaml`:

```yaml
# pipelines/transforms/daily_revenue.yaml
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
    The `__THIS__` placeholder automatically references your input. No complex joins or table names needed.

### Step 5: Apply the Transform

```bash
seeknal apply pipelines/transforms/daily_revenue.yaml
```

**Expected output:**
```
Applying transform 'daily_revenue'...
  ✓ Validated YAML schema
  ✓ Validated SQL syntax
  ✓ Registered transform 'daily_revenue'
Transform applied successfully!
```

### Step 6: Draft an Output

Finally, let's save the results:

```bash
seeknal draft output --name results --input daily_revenue
```

Edit `pipelines/outputs/results.yaml`:

```yaml
# pipelines/outputs/results.yaml
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

**Expected output:**
```
Applying output 'results'...
  ✓ Validated YAML schema
  ✓ Registered output 'results'
Output applied successfully!
```

!!! success "Checkpoint"
    You should have three applied files:
    - `sales_data` (source)
    - `daily_revenue` (transform)
    - `results` (output)

    **Verify:** Run `seeknal status` to see all applied resources.

---

## Part 4: Run and See Results (2 minutes)

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

Output written to: output/daily_revenue.parquet
```

!!! stuck "Stuck? Pipeline Errors"
    **Problem:** `Source 'sales_data' not found`

    **Solution:** Make sure you applied the source first:
    ```bash
    seeknal status  # Check what's applied
    seeknal apply pipelines/sources/sales_data.yaml  # Re-apply if missing
    ```

    **Problem:** `SQL syntax error`

    **Solution:** Check your SQL in the transform YAML file. Common issues:
    - Missing comma between columns
    - Unmatched parentheses
    - Wrong column name from source schema

    **Problem:** `Permission denied: output/`

    **Solution:** Make sure the output directory exists and is writable:
    ```bash
    mkdir -p output
    chmod +w output  # Unix only
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
    You just built and ran your first Seeknal pipeline in under 10 minutes!

---

## What's Next?

Choose your path to continue learning:

| Data Engineer | Analytics Engineer | ML Engineer |
|---------------|-------------------|-------------|
| [Build ELT Pipelines](../getting-started/data-engineer-path/1-elt-pipeline.md) | [Define Semantic Models](../getting-started/analytics-engineer-path/1-semantic-models.md) | [Create Feature Groups](../getting-started/ml-engineer-path/1-feature-store.md) |
| Process data at scale | Business metrics & KPIs | ML features with point-in-time joins |

!!! question "Not sure which path?"
    Start with the **Data Engineer** path — it covers the fundamentals that apply to all personas.

---

## Troubleshooting

### Installation Issues

**Problem:** `uv pip install` fails with permissions error

```bash
# Solution: Use a virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .\.venv\Scripts\activate
uv pip install seeknal-<version>-py3-none-any.whl
```

**Problem:** Can't find Seeknal wheel file

```bash
# Solution: Download from GitHub Releases
# Visit: https://github.com/mta-tech/seeknal/releases
# Download the .whl file for your platform
```

### Pipeline Errors

**Problem:** `seeknal run` fails with "source not found"

```bash
# Solution: Verify your files are applied
seeknal status  # Shows all applied sources, transforms, outputs
```

**Problem:** "Column not found" error

```bash
# Solution: Check CSV column names match schema
head -5 data/sales.csv  # Verify column names
```

### Need More Help?

- [Full Troubleshooting Guide](../troubleshooting/)
- [GitHub Issues](https://github.com/mta-tech/seeknal/issues)
- [Community Discord](https://discord.gg/seeknal)

---

## Summary

In this Quick Start, you learned:

- [x] How to install Seeknal
- [x] The pipeline builder workflow (init → draft → apply → run)
- [x] How to create sources, transforms, and outputs
- [x] How to run a pipeline and view results

**Time taken:** ~10 minutes | **Next:** Choose your learning path
