# Chapter 1: Build ELT Pipeline

> **Duration:** 25 minutes | **Difficulty:** Intermediate | **Format:** YAML & Python

Learn to build production-grade ELT pipelines with Seeknal, transforming raw data sources into clean warehouse tables.

---

## What You'll Build

A complete e-commerce order processing pipeline:

```
REST API → Orders (Raw) → Transform → Orders (Clean) → Warehouse
            ↓                    ↓
          JSON                  Parquet
```

**After this chapter, you'll have:**
- HTTP source that fetches data from REST APIs
- DuckDB transformation with data quality checks
- Warehouse output with proper schema enforcement
- Error handling and retry logic

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Quick Start](../../quick-start/) — Basic pipeline builder workflow
- [ ] [DE Path Overview](index.md) — Introduction to this path
- [ ] Comfortable with SQL JOINs and data types

---

## Part 1: Ingest Data from REST API (8 minutes)

### Understanding HTTP Sources

Seeknal's HTTP sources fetch data from REST APIs automatically:

| Feature | Benefit |
|---------|---------|
| **Scheduled Fetching** | Poll on intervals (every 5 min, hourly, etc.) |
| **Authentication** | Bearer tokens, API keys, OAuth support |
| **Error Handling** | Retry with exponential backoff |
| **Pagination** | Handle large datasets automatically |

=== "YAML Approach"

    Create a new project:

    ```bash
    seeknal init ecommerce-pipeline
    cd ecommerce-pipeline
    ```

    Draft an HTTP source:

    ```bash
    seeknal draft source --name orders_api --type http
    ```

    Edit `pipelines/sources/orders_api.yaml`:

    ```yaml
    kind: source
    name: orders_api

    http:
      # API endpoint
      url: https://api.example.com/orders

      # Authentication (bearer token)
      headers:
        Authorization: "Bearer ${API_TOKEN}"

      # Query parameters
      params:
        status: "completed"
        limit: "1000"

      # Polling configuration
      poll:
        interval: "5m"  # Check every 5 minutes
        strategy: "incremental"  # Only fetch new data

    # Response format
    format:
      type: json
      array: true  # Response is a JSON array

    # Schema definition
    schema:
      - column: order_id
        type: string
        primary_key: true
      - column: customer_id
        type: string
      - column: order_date
        type: timestamp
      - column: status
        type: string
      - column: revenue
        type: float
      - column: items
        type: integer
    ```

!!! tip "Environment Variables"
    Use `${VARIABLE}` syntax for secrets:
    ```bash
    export API_TOKEN="your-token-here"
    seeknal apply pipelines/sources/orders_api.yaml
    ```

=== "Python Approach"

    Create `pipeline.py`:

    ```python
    #!/usr/bin/env python3
    """
    ELT Pipeline - Chapter 1
    Fetches orders from API, transforms, and outputs to warehouse
    """

    import os
    import pandas as pd
    import requests
    import pyarrow as pa
    from pathlib import Path
    from seeknal.tasks.duckdb import DuckDBTask

    # Fetch data from REST API
    def fetch_orders():
        """Fetch orders from the API."""
        url = "https://api.example.com/orders"
        headers = {
            "Authorization": f"Bearer {os.getenv('API_TOKEN')}"
        }
        params = {
            "status": "completed",
            "limit": "1000"
        }

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        return pd.DataFrame(data)

    # Load and prepare data
    print("Fetching orders from API...")
    orders_df = fetch_orders()
    print(f"Fetched {len(orders_df)} orders")
    ```

### Apply the Source

```bash
# YAML approach
seeknal apply pipelines/sources/orders_api.yaml

# Verify the source
seeknal status
```

**Expected output:**
```
Applying source 'orders_api'...
  ✓ Validated HTTP configuration
  ✓ Tested API endpoint (200 OK)
  ✓ Registered source 'orders_api'
Source applied successfully!
```

!!! stuck "Stuck? API Authentication"
    **Problem:** `401 Unauthorized` error

    **Solution:** Check your API token:
    ```bash
    # Verify token is set
    echo $API_TOKEN

    # Test manually
    curl -H "Authorization: Bearer $API_TOKEN" https://api.example.com/orders
    ```

    **Problem:** `Connection timeout`

    **Solution:** Check network connectivity:
    ```bash
    ping api.example.com
    ```

---

## Part 2: Clean and Transform with DuckDB (10 minutes)

### Understanding DuckDB Transformations

DuckDB is an in-process SQL OLAP database — perfect for data transformations:

- **Fast**: Columnar execution engine
- **SQL Compliant**: Standard SQL with advanced analytics
- **Zero Setup**: No database server required
- **Scalable**: Handles millions of rows on a laptop

=== "YAML Approach"

    Draft a transform:

    ```bash
    seeknal draft transform --name orders_cleaned --input orders_api
    ```

    Edit `pipelines/transforms/orders_cleaned.yaml`:

    ```yaml
    kind: transform
    name: orders_cleaned

    input: orders_api

    engine: duckdb

    sql: |
      SELECT
        -- Primary key
        order_id,

        -- Foreign key with validation
        CASE
          WHEN customer_id IS NOT NULL
          AND LENGTH(customer_id) > 0
          THEN customer_id
          ELSE 'UNKNOWN'
        END as customer_id,

        -- Date processing
        DATE(order_date) as order_date,
        CAST(order_date AS TIME) as order_time,

        -- Status normalization
        UPPER(TRIM(status)) as status,

        -- Revenue validation
        CASE
          WHEN revenue >= 0 THEN revenue
          ELSE NULL  -- Flag negative values
        END as revenue,

        -- Items validation
        CASE
          WHEN items >= 0 THEN items
          ELSE 0
        END as items,

        -- Data quality checks
        CASE
          WHEN revenue < 0 THEN 1
          WHEN customer_id IS NULL THEN 1
          WHEN items < 0 THEN 1
          ELSE 0
        END as quality_flag,

        -- Audit columns
        CURRENT_TIMESTAMP as processed_at

      FROM __THIS__

      -- Remove duplicates (keep latest)
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY processed_at DESC
      ) = 1
    ```

=== "Python Approach"

    Add transformation to `pipeline.py`:

    ```python
    # Create transformation task
    print("Creating DuckDB transformation...")

    task = DuckDBTask()

    # Add input data (convert pandas to Arrow)
    arrow_table = pa.Table.from_pandas(orders_df)
    task.add_input(dataframe=arrow_table)

    # Define transformation SQL
    sql = """
    SELECT
      order_id,
      COALESCE(customer_id, 'UNKNOWN') as customer_id,
      DATE(order_date) as order_date,
      CAST(order_date AS TIME) as order_time,
      UPPER(TRIM(status)) as status,
      CASE
        WHEN revenue >= 0 THEN revenue
        ELSE NULL
      END as revenue,
      CASE
        WHEN items >= 0 THEN items
        ELSE 0
      END as items,
      CASE
        WHEN revenue < 0 THEN 1
        WHEN customer_id IS NULL THEN 1
        ELSE 0
      END as quality_flag,
      CURRENT_TIMESTAMP as processed_at
    FROM __THIS__
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY processed_at DESC
    ) = 1
    """

    task.add_sql(sql)

    # Execute transformation
    print("Executing transformation...")
    result_arrow = task.transform()
    cleaned_df = result_arrow.to_pandas()

    print(f"Cleaned {len(cleaned_df)} orders")
    print(f"Quality issues: {cleaned_df['quality_flag'].sum()}")
    ```

### Apply the Transform

```bash
# YAML approach
seeknal apply pipelines/transforms/orders_cleaned.yaml

# Test the transformation
seeknal run --dry-run
```

**Expected output:**
```
Applying transform 'orders_cleaned'...
  ✓ Validated SQL syntax
  ✓ Checked input schema
  ✓ Registered transform 'orders_cleaned'
Transform applied successfully!
```

!!! info "What's QUALIFY?"
    `QUALIFY` is a DuckDB extension that filters window function results. It's like `HAVING` for window functions — cleaner than subqueries!

---

## Part 3: Output to Data Warehouse (7 minutes)

### Understanding Warehouse Outputs

Seeknal can output to multiple warehouse types:

| Type | Use Case | Connection |
|------|----------|------------|
| **File-based** | Local/dev | Local filesystem |
| **PostgreSQL** | Production | JDBC/ODBC |
| **Snowflake** | Cloud warehouse | Connection string |
| **BigQuery** | Analytics | Google Cloud |

=== "YAML Approach"

    Draft an output:

    ```bash
    seeknal draft output --name warehouse_orders --input orders_cleaned
    ```

    Edit `pipelines/outputs/warehouse_orders.yaml`:

    ```yaml
    kind: output
    name: warehouse_orders

    input: orders_cleaned

    target:
      type: warehouse

      # Database connection
      connection:
        type: postgresql
        host: ${WAREHOUSE_HOST}
        port: 5432
        database: analytics
        user: ${WAREHOUSE_USER}
        password: ${WAREHOUSE_PASSWORD}

      # Table configuration
      table: orders

      # Materialization strategy
      materialization:
        strategy: incremental  # Append new records
        key: order_id         # Upsert on this key

      # Partitioning (for large tables)
      partition:
        type: range
        column: order_date
        granularity: day  # One partition per day

      # Performance optimizations
      indexes:
        - column: customer_id
          type: btree
        - column: order_date
          type: btree
        - column: status
          type: btree
    ```

    For local development, use file-based output:

    ```yaml
    kind: output
    name: warehouse_orders

    input: orders_cleaned

    target:
      type: file
      format: parquet
      path: output/orders.parquet

      # Partition output
      partition:
        column: order_date
        granularity: day
    ```

=== "Python Approach"

    Add output to `pipeline.py`:

    ```python
    # Save to warehouse
    print("Saving to warehouse...")

    # For local development (Parquet)
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / "orders.parquet"

    cleaned_df.to_parquet(output_path, index=False)
    print(f"Saved to: {output_path}")

    # For production database
    # import sqlalchemy
    # engine = sqlalchemy.create_engine(
    #     f"postgresql://{user}:{password}@{host}:5432/analytics"
    # )
    # cleaned_df.to_sql(
    #     "orders",
    #     engine,
    #     if_exists="append",
    #     index=False
    # )
    ```

### Apply the Output

```bash
# YAML approach
seeknal apply pipelines/outputs/warehouse_orders.yaml

# Set environment variables
export WAREHOUSE_HOST="localhost"
export WAREHOUSE_USER="analytics"
export WAREHOUSE_PASSWORD="secret"

# Verify output configuration
seeknal status
```

---

## Part 4: Run and Verify (5 minutes)

### Execute the Full Pipeline

```bash
# YAML approach
seeknal run

# Python approach
python pipeline.py
```

**Expected output:**
```
Running pipeline...
  → Fetching from HTTP: orders_api
    Fetched 1,000 orders from API
  → Transforming: orders_cleaned
    Cleaned 998 orders (2 quality issues)
  → Writing output: warehouse_orders
    Wrote 998 orders to warehouse
✓ Pipeline completed successfully!
```

### Verify Output

```bash
# Check the output
python -c "import pandas as pd; df = pd.read_parquet('output/orders.parquet'); print(df.describe())"
```

**Checkpoint:** You should see:
- Order counts by status
- Revenue statistics
- Quality flag counts

!!! success "Congratulations! :tada:"
    You've built a complete ELT pipeline with API ingestion, DuckDB transformation, and warehouse output.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. API Rate Limiting**
    - Symptom: `429 Too Many Requests`
    - Fix: Increase `poll.interval` or implement backoff

    **2. Large JSON Responses**
    - Symptom: Memory errors or timeouts
    - Fix: Use pagination in HTTP source configuration

    **3. Schema Drift**
    - Symptom: `Column not found` errors
    - Fix: Use `COALESCE` for new optional columns

    **4. Duplicate Records**
    - Symptom: More output rows than input
    - Fix: Add `QUALIFY ROW_NUMBER()` to deduplicate

---

## Summary

In this chapter, you learned:

- [x] **HTTP Sources** — Fetch data from REST APIs with authentication
- [x] **DuckDB Transforms** — Clean and validate data with SQL
- [x] **Warehouse Outputs** — Materialize to databases or data lakes
- [x] **Error Handling** — Retry logic and data quality checks
- [x] **Production Patterns** — Incremental loading and partitioning

**Key Commands:**
```bash
seeknal draft source --name <name> --type http
seeknal draft transform --name <name> --engine duckdb
seeknal draft output --name <name> --target warehouse
seeknal run  # Execute the full pipeline
```

---

## What's Next?

[Chapter 2: Add Incremental Models →](2-incremental-models.md)

Make your pipeline more efficient with incremental processing, change data capture, and scheduled runs.

---

## See Also

- **[Virtual Environments](../../concepts/virtual-environments.md)** — Isolate development and production
- **[Change Categorization](../../concepts/change-categorization.md)** — Understanding breaking vs non-breaking changes
- **[Python vs YAML Workflows](../../concepts/python-vs-yaml.md)** — Choose the right paradigm
