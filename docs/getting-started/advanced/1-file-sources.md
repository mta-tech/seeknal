# Chapter 1: File Sources

> **Duration:** 20 minutes | **Difficulty:** Beginner | **Format:** YAML & CLI

Learn to load data from CSV, JSONL, and Parquet files into your Seeknal pipeline.

---

## What You'll Build

Three source nodes, each loading a different file format:

```
products.csv           →  source.products          (CSV)
sales_events.jsonl     →  source.sales_events      (JSONL)
sales_snapshot.parquet →  source.sales_snapshot     (Parquet)
```

**After this chapter, you'll have:**
- A project with three file-based sources
- Understanding of CSV, JSONL, and Parquet source configuration
- Data ready to transform in Chapter 2

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Quick Start](../../quick-start/) — Basic `draft → dry-run → apply` workflow
- [ ] [Advanced Guide Overview](index.md) — Introduction to this guide

---

## Part 1: Project Setup & CSV Source (8 minutes)

### Initialize the Project

```bash
seeknal init --name retail-advanced --description "Advanced guide tutorial project"
```

### Create Sample Data

Create a CSV file with product catalog data:

```bash
mkdir data
cat > data/products.csv << 'EOF'
product_id,name,category,price,launch_date
PRD-001,Wireless Headphones,Electronics,79.99,2025-09-01
PRD-002,Running Shoes,Apparel,120.00,2025-08-15
PRD-003,Coffee Maker,Kitchen,49.99,2025-10-01
PRD-004,Yoga Mat,Sports,35.00,2025-07-20
PRD-005,Desk Lamp,Office,28.50,2025-11-01
PRD-006,Noise Cancelling Headphones,Electronics,199.99,2025-10-15
EOF
```

### Draft the CSV Source

Generate a source template:

```bash
seeknal draft source products
```

This creates `draft_source_products.yml`. Edit it to match your CSV:

```yaml
kind: source
name: products
description: "Product catalog from CSV"
source: csv
table: "data/products.csv"
columns:
  product_id: "Unique product identifier"
  name: "Product name"
  category: "Product category"
  price: "Unit price in USD"
  launch_date: "Product launch date"
```

### Validate and Apply

```bash
# Preview what will happen
seeknal dry-run draft_source_products.yml

# Apply to project
seeknal apply draft_source_products.yml
```

The source is now at `seeknal/sources/products.yml`.

**Checkpoint:** The `dry-run` preview showed 6 rows — your CSV source is configured correctly.

!!! note "REPL Access Requires Running First"
    After `apply`, the source definition is registered but data hasn't been executed yet. The REPL only shows tables from previously executed pipeline runs. We'll run all sources together at the end of this chapter.

---

## Part 2: JSONL Source (6 minutes)

### Create JSONL Data

Create a JSONL (JSON Lines) file with sales event data:

```bash
cat > data/sales_events.jsonl << 'EOF'
{"event_id":"EVT-001","product_id":"PRD-001","quantity":2,"sale_date":"2026-01-10","region":"north"}
{"event_id":"EVT-002","product_id":"PRD-003","quantity":1,"sale_date":"2026-01-10","region":"south"}
{"event_id":"EVT-003","product_id":"PRD-001","quantity":null,"sale_date":"2026-01-11","region":"north"}
{"event_id":"EVT-004","product_id":"PRD-999","quantity":3,"sale_date":"2026-01-11","region":"east"}
{"event_id":"EVT-005","product_id":"PRD-002","quantity":1,"sale_date":"2026-01-12","region":"south"}
{"event_id":"EVT-006","product_id":"PRD-004","quantity":5,"sale_date":"2026-01-12","region":"west"}
{"event_id":"EVT-001","product_id":"PRD-001","quantity":2,"sale_date":"2026-01-13","region":"north"}
EOF
```

!!! info "Data Quality Issues (On Purpose)"
    This sample data includes real-world problems we'll fix with rules in Chapter 3:

    - **Row 3**: `quantity` is `null`
    - **Row 4**: `product_id` is `PRD-999` (doesn't exist in products)
    - **Row 7**: Duplicate `event_id` `EVT-001`

### Draft and Apply

```bash
seeknal draft source sales_events
```

Edit `draft_source_sales_events.yml`:

```yaml
kind: source
name: sales_events
description: "Sales transaction events from JSONL"
source: jsonl
table: "data/sales_events.jsonl"
columns:
  event_id: "Unique event identifier"
  product_id: "Product reference"
  quantity: "Units sold"
  sale_date: "Date of sale"
  region: "Sales region"
```

```bash
seeknal dry-run draft_source_sales_events.yml
seeknal apply draft_source_sales_events.yml
```

**Checkpoint:** The `dry-run` preview showed 7 rows. Notice the null quantity in row 3 and the orphan `PRD-999` — we'll address these with rules in Chapter 3.

---

## Part 3: Parquet Source (6 minutes)

### Create Parquet Data

Parquet is a columnar format commonly used for pre-processed data. Create a sales snapshot:

```bash
python3 -c "
import pandas as pd
snapshot = pd.DataFrame({
    'product_id': ['PRD-001', 'PRD-002', 'PRD-003', 'PRD-004'],
    'total_units_sold': [150, 80, 200, 95],
    'period': ['2025-Q4', '2025-Q4', '2025-Q4', '2025-Q4']
})
snapshot.to_parquet('data/sales_snapshot.parquet', index=False)
print('Created data/sales_snapshot.parquet')
"
```

### Draft and Apply

```bash
seeknal draft source sales_snapshot
```

Edit `draft_source_sales_snapshot.yml`:

```yaml
kind: source
name: sales_snapshot
description: "Quarterly sales snapshot from Parquet"
source: parquet
table: "data/sales_snapshot.parquet"
```

!!! tip "Parquet Schema"
    Unlike CSV and JSONL, Parquet files have their schema embedded. You don't need to declare `columns:` — DuckDB reads the column names and types directly from the file.

```bash
seeknal dry-run draft_source_sales_snapshot.yml
seeknal apply draft_source_sales_snapshot.yml
```

**Checkpoint:** The `dry-run` preview showed 4 rows with product_id, total_units_sold, and period columns.

---

## Run and Explore in REPL

Now that all three sources are applied, run the pipeline to execute them:

```bash
seeknal run
```

This executes each source node and writes intermediate parquet files to `target/`. Once the run completes, the REPL can access them:

```bash
seeknal repl
```

```sql
-- Check all three sources
SELECT * FROM source_products;
SELECT * FROM source_sales_events;
SELECT * FROM source_sales_snapshot;

-- Try a quick join across sources
SELECT
    p.name,
    p.category,
    COUNT(s.event_id) AS event_count
FROM source_products p
JOIN source_sales_events s ON p.product_id = s.product_id
GROUP BY p.name, p.category;
```

!!! tip "Why Run First?"
    The REPL auto-registers intermediate parquet outputs from previous pipeline runs. After `seeknal apply`, the source definition exists but no data has been executed yet. Running the pipeline generates the parquets that the REPL discovers at startup.

**Checkpoint:** You should see `products` (6 rows), `sales_events` (7 rows), and `sales_snapshot` (4 rows) all queryable in the REPL.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. File not found**

    - Symptom: `dry-run` fails with file path error
    - Fix: Paths in `table:` are relative to the project root. Make sure `data/products.csv` exists before running dry-run.

    **2. JSONL vs JSON confusion**

    - Symptom: Parse error when loading JSON data
    - Fix: Use `source: jsonl` for newline-delimited JSON (one object per line). Use `source: json` only for a single JSON array file.

    **3. YAML indentation errors**

    - Symptom: `dry-run` fails with YAML parse error
    - Fix: Use 2-space indentation consistently. Don't mix tabs and spaces.

    **4. Column name mismatch**

    - Symptom: Query returns unexpected column names
    - Fix: Column names in `columns:` must match the actual data. For CSV, check the header row. For JSONL, check the JSON keys.

---

## Summary

In this chapter, you learned:

- [x] **CSV Source** — Load tabular data with `source: csv`
- [x] **JSONL Source** — Load newline-delimited JSON with `source: jsonl`
- [x] **Parquet Source** — Load columnar data with `source: parquet` (schema auto-detected)
- [x] **Draft Workflow** — `seeknal draft source <name>` → edit → `dry-run` → `apply`
- [x] **Run then REPL** — Execute with `seeknal run`, then explore interactively with `seeknal repl`

**Source Type Comparison:**

| Format | `source:` value | Schema | Best For |
|--------|----------------|--------|----------|
| CSV | `csv` | Declare in `columns:` | Tabular data, spreadsheets |
| JSONL | `jsonl` | Declare in `columns:` | Event streams, API data |
| Parquet | `parquet` | Auto-detected | Pre-processed data, analytics |

**Key Commands:**
```bash
seeknal draft source <name>          # Generate source template
seeknal dry-run <draft_file>.yml     # Preview validation
seeknal apply <draft_file>.yml       # Apply to project
seeknal run                          # Execute pipeline (generates outputs)
seeknal repl                         # Explore executed data interactively
```

---

## What's Next?

In **[Chapter 2: Transformations](2-transformations.md)**, you'll clean, join, and aggregate your source data using SQL transforms with `ref()` syntax.
