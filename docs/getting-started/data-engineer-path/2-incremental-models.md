# Chapter 2: Add Incremental Models

> **Duration:** 30 minutes | **Difficulty:** Intermediate | **Format:** YAML & Python

Learn to optimize Seeknal pipelines with incremental processing, reducing computation time and cost by only processing new or changed data.

---

## What You'll Build

An incremental order processing pipeline:

```
Source (REST API) → Incremental Load → Transform → Warehouse
                          ↓
                    Change Detection
                    (last_modified timestamp)
```

**After this chapter, you'll have:**
- Incremental source that only fetches new/changed data
- CDC (Change Data Capture) patterns for detecting updates
- Scheduled pipeline runs for automated processing
- Monitoring and validation for incremental updates

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: Build ELT Pipeline](1-elt-pipeline.md) — Basic pipeline knowledge
- [ ] Comfortable with SQL window functions
- [ ] Understanding of timestamp-based queries

---

## Part 1: Configure Incremental Source (10 minutes)

### Understanding Incremental Loading

Incremental loading only processes new or changed data since the last run:

| Strategy | Use Case | Benefit |
|----------|----------|---------|
| **Timestamp-based** | Data has `updated_at` column | Simple to implement |
| **CDC-based** | Database has change logs | Captures deletes |
| **Watermark-based** | Streaming data sources | Exactly-once processing |

=== "YAML Approach"

    Modify your HTTP source to support incremental loading:

    Edit `pipelines/sources/orders_api.yaml`:

    ```yaml
    kind: source
    name: orders_api

    http:
      url: https://api.example.com/orders

      headers:
        Authorization: "Bearer ${API_TOKEN}"

      # Incremental loading configuration
      incremental:
        enabled: true
        strategy: timestamp  # Use timestamp-based CDC
        column: updated_at   # Column to track changes

        # Initial load parameters
        initial_load:
          # Fetch all historical data on first run
          mode: full

        # Incremental parameters
        params:
          # Only fetch records modified since last run
          since: "{{ last_successful_run }}"
          limit: 10000

      # Polling configuration
      poll:
        interval: "15m"  # Check every 15 minutes
        strategy: "incremental"

    format:
      type: json
      array: true

    schema:
      - column: order_id
        type: string
        primary_key: true
      - column: customer_id
        type: string
      - column: order_date
        type: timestamp
      - column: updated_at
        type: timestamp
      - column: status
        type: string
      - column: revenue
        type: float
      - column: items
        type: integer
    ```

=== "Python Approach"

    Modify `pipeline.py` to support incremental loading:

    ```python
    #!/usr/bin/env python3
    """
    Incremental ELT Pipeline - Chapter 2
    Only processes new or changed orders
    """

    import os
    import json
    import pandas as pd
    import requests
    from pathlib import Path
    from datetime import datetime, timedelta
    from seeknal.tasks.duckdb import DuckDBTask

    # Track last successful run
    STATE_FILE = Path("pipeline_state.json")

    def load_state():
        """Load the last successful run timestamp."""
        if STATE_FILE.exists():
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        return {"last_successful_run": None}

    def save_state(timestamp):
        """Save the last successful run timestamp."""
        with open(STATE_FILE, 'w') as f:
            json.dump({"last_successful_run": timestamp}, f)

    def fetch_orders_incremental():
        """Fetch only new or changed orders."""
        state = load_state()
        last_run = state.get("last_successful_run")

        url = "https://api.example.com/orders"
        headers = {
            "Authorization": f"Bearer {os.getenv('API_TOKEN')}"
        }

        # Build query parameters
        params = {"limit": "10000"}

        # If we have a previous run, only get new data
        if last_run:
            params["updated_since"] = last_run
            print(f"Fetching orders updated since: {last_run}")
        else:
            print("Initial load: fetching all orders")

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        df = pd.DataFrame(data)

        print(f"Fetched {len(df)} orders")
        return df

    # Load and prepare data
    print("Fetching incremental orders from API...")
    orders_df = fetch_orders_incremental()

    if len(orders_df) == 0:
        print("No new data to process. Exiting.")
        exit(0)
    ```

### Apply the Incremental Source

```bash
# YAML approach
seeknal apply pipelines/sources/orders_api.yaml

# Verify incremental configuration
seeknal describe source orders_api
```

**Expected output:**
```
Source: orders_api
Type: HTTP
Incremental: Enabled
  Strategy: timestamp
  Column: updated_at
  Last Run: 2026-02-09 10:30:00
```

!!! tip "Choosing Your Incremental Strategy"
    **Timestamp-based** (shown above): Best for REST APIs with `updated_at` columns

    **High-watermark**: Best for databases with auto-incrementing IDs

    **CDC logs**: Best for capturing deletes and updates

---

## Part 2: Implement Change Data Capture (12 minutes)

### Understanding CDC Patterns

Change Data Capture (CDC) detects and processes data changes:

| Pattern | Detects | Use Case |
|---------|---------|----------|
| **Append-only** | New rows | Event streams, logs |
| **Updates tracking** | New + updated | Transactional data |
| **Full CDC** | New + updated + deleted | Critical systems |

=== "YAML Approach"

    Create a CDC-aware transform:

    ```bash
    seeknal draft transform --name orders_cdc --input orders_api
    ```

    Edit `pipelines/transforms/orders_cdc.yaml`:

    ```yaml
    kind: transform
    name: orders_cdc

    input: orders_api

    engine: duckdb

    sql: |
      WITH ranked_orders AS (
        SELECT
          order_id,
          customer_id,
          order_date,
          updated_at,
          status,
          revenue,
          items,

          -- CDC operation type
          CASE
            WHEN __operation__ = 'DELETE' THEN 'DELETE'
            WHEN updated_at > __last_run_time__ THEN 'UPDATE'
            ELSE 'INSERT'
          END as cdc_operation,

          -- Row versioning for deduplication
          ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY updated_at DESC
          ) as row_version,

          CURRENT_TIMESTAMP as processed_at

        FROM __THIS__
      )

      SELECT
        order_id,
        customer_id,
        order_date,
        updated_at,
        status,
        revenue,
        items,
        cdc_operation,
        processed_at

      FROM ranked_orders

      -- Only keep latest version of each order
      WHERE row_version = 1

      -- Track quality metrics
      QUALIFY CASE
        WHEN cdc_operation = 'DELETE' THEN 1
        WHEN revenue < 0 THEN 1
        ELSE 0
      END = 0
      OR cdc_operation IN ('UPDATE', 'DELETE')
    ```

=== "Python Approach"

    Add CDC logic to `pipeline.py`:

    ```python
    # Create CDC-aware transformation
    print("Creating CDC transformation...")

    task = DuckDBTask()

    # Add input data
    arrow_table = pa.Table.from_pandas(orders_df)
    task.add_input(dataframe=arrow_table)

    # Define CDC transformation SQL
    sql = """
    WITH ranked_orders AS (
      SELECT
        order_id,
        customer_id,
        order_date,
        updated_at,
        status,
        revenue,
        items,

        -- Detect operation type based on timestamps
        CASE
          WHEN status = 'CANCELLED' THEN 'DELETE'
          ELSE 'UPSERT'
        END as cdc_operation,

        -- Row versioning for deduplication
        ROW_NUMBER() OVER (
          PARTITION BY order_id
          ORDER BY updated_at DESC
        ) as row_version,

        CURRENT_TIMESTAMP as processed_at
      FROM __THIS__
    )
    SELECT
      order_id,
      customer_id,
      order_date,
      updated_at,
      status,
      revenue,
      items,
      cdc_operation,
      processed_at
    FROM ranked_orders
    WHERE row_version = 1  -- Keep latest version only
    """

    task.add_sql(sql)

    # Execute transformation
    print("Executing CDC transformation...")
    result_arrow = task.transform()
    cdc_df = result_arrow.to_pandas()

    # Report CDC statistics
    operation_counts = cdc_df['cdc_operation'].value_counts()
    print(f"CDC Operations:")
    for op, count in operation_counts.items():
        print(f"  {op}: {count}")
    ```

### Apply the CDC Transform

```bash
# YAML approach
seeknal apply pipelines/transforms/orders_cdc.yaml

# Test CDC logic
seeknal run --dry-run --since "2026-02-09 00:00:00"
```

**Expected output:**
```
Applying transform 'orders_cdc'...
  ✓ Validated SQL syntax
  ✓ Verified CDC column references
  ✓ Registered transform 'orders_cdc'
Transform applied successfully!
```

!!! info "CDC Operation Types"
    - **INSERT**: New records (first time seen)
    - **UPDATE**: Existing records with changed values
    - **DELETE**: Records marked for removal
    - **UPSERT**: Combination of insert/update logic

---

## Part 3: Schedule Automated Pipeline Runs (8 minutes)

### Understanding Pipeline Scheduling

Seeknal supports multiple scheduling strategies:

| Strategy | Syntax | Use Case |
|----------|--------|----------|
| **Interval-based** | `--every 15m` | Simple periodic runs |
| **Cron-based** | `--schedule "*/5 * * * *"` | Complex schedules |
| **Event-driven** | `--trigger file arrival` | Data-driven workflows |

=== "YAML Approach"

    Create a scheduled pipeline configuration:

    Create `pipelines/scheduled_orders.yaml`:

    ```yaml
    kind: pipeline
    name: orders_incremental_pipeline

    description: >
      Incremental order processing pipeline.
      Fetches new orders every 15 minutes and updates warehouse.

    # Pipeline components
    sources:
      - orders_api

    transforms:
      - orders_cdc

    outputs:
      - warehouse_orders

    # Schedule configuration
    schedule:
      # Run every 15 minutes
      cron: "*/15 * * * *"

      # Timezone
      timezone: UTC

      # Incremental loading
      incremental:
        enabled: true
        backfill: false  # Don't backfill historical data

    # Monitoring and alerts
    monitoring:
      # Alert on failures
      on_failure:
        notify:
          - type: slack
            webhook: ${SLACK_WEBHOOK}

      # Alert on data quality issues
      on_quality_issue:
        threshold: 0.05  # Alert if >5% records have quality issues
        notify:
          - type: email
            address: data-team@example.com

    # Retention policies
    retention:
      # Keep raw source data for 30 days
      source:
        days: 30

      # Keep transformed data for 1 year
      transformed:
        days: 365
    ```

=== "Python Approach"

    Create a scheduled pipeline script:

    Create `scripts/run_scheduled.py`:

    ```python
    #!/usr/bin/env python3
    """
    Scheduled Incremental Pipeline
    Runs every 15 minutes via cron or scheduler
    """

    import os
    import sys
    from pathlib import Path
    from datetime import datetime

    # Add project root to path
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from pipeline import (
        fetch_orders_incremental,
        transform_orders_cdc,
        save_to_warehouse
    )
    from pipeline_state import load_state, save_state

    def run_pipeline():
        """Execute the incremental pipeline."""
        run_start = datetime.now()
        print(f"Pipeline run started: {run_start}")

        try:
            # 1. Load previous state
            state = load_state()
            last_run = state.get("last_successful_run")

            # 2. Fetch incremental data
            print("Fetching incremental data...")
            orders_df = fetch_orders_incremental()

            if len(orders_df) == 0:
                print("No new data. Pipeline complete.")
                return

            # 3. Apply CDC transformation
            print("Applying CDC transformation...")
            cdc_df = transform_orders_cdc(orders_df)

            # 4. Save to warehouse
            print("Saving to warehouse...")
            save_to_warehouse(cdc_df, mode="upsert")

            # 5. Update state
            save_state(run_start.isoformat())

            # 6. Report metrics
            print(f"Pipeline completed successfully!")
            print(f"Processed {len(cdc_df)} orders")
            print(f"Duration: {datetime.now() - run_start}")

        except Exception as e:
            print(f"Pipeline failed: {e}")
            # Send alert (Slack, email, etc.)
            raise

    if __name__ == "__main__":
        run_pipeline()
    ```

    Set up cron schedule:

    ```bash
    # Edit crontab
    crontab -e

    # Add schedule (run every 15 minutes)
    */15 * * * * cd /path/to/project && python scripts/run_scheduled.py >> logs/pipeline.log 2>&1
    ```

### Apply the Scheduled Pipeline

```bash
# YAML approach
seeknal apply pipelines/scheduled_orders.yaml

# Verify schedule
seeknal describe pipeline orders_incremental_pipeline

# Manually trigger a run
seeknal run --pipeline orders_incremental_pipeline
```

**Expected output:**
```
Pipeline: orders_incremental_pipeline
Schedule: */15 * * * * (Every 15 minutes)
Next Run: 2026-02-09 10:45:00
Last Run: 2026-02-09 10:30:00
Status: Active
```

!!! success "Automated Pipeline Running!"
    Your pipeline is now scheduled to run every 15 minutes, automatically fetching and processing new orders.

---

## Part 4: Monitor and Validate Incremental Updates (5 minutes)

### Understanding Pipeline Monitoring

Monitor key metrics to ensure healthy incremental processing:

| Metric | Healthy Value | Warning Threshold |
|--------|---------------|-------------------|
| **Rows processed** | Consistent with expected rate | <10% of average |
| **Processing time** | <5 minutes | >10 minutes |
| **Error rate** | 0% | >1% |
| **Data quality issues** | <2% | >5% |

=== "YAML Approach"

    Create a monitoring dashboard query:

    Create `pipelines/queries/pipeline_metrics.yaml`:

    ```yaml
    kind: query
    name: pipeline_health_metrics

    description: Monitor incremental pipeline health

    sql: |
      -- Recent pipeline runs
      WITH pipeline_runs AS (
        SELECT
          DATE_TRUNC('hour', processed_at) as hour,
          COUNT(*) as record_count,
          COUNT(DISTINCT order_id) as unique_orders,
          SUM(CASE WHEN quality_flag = 1 THEN 1 ELSE 0 END) as quality_issues,
          AVG(processed_at - updated_at) as processing_lag_seconds

        FROM warehouse_orders

        WHERE processed_at >= NOW() - INTERVAL '24 hours'

        GROUP BY hour
        ORDER BY hour DESC
        LIMIT 24
      )

      SELECT
        hour,
        record_count,
        unique_orders,
        quality_issues,
        ROUND(quality_issues * 100.0 / NULLIF(record_count, 0), 2) as quality_issue_pct,
        ROUND(processing_lag_seconds, 2) as avg_lag_seconds

      FROM pipeline_runs
    ```

    Run health check:

    ```bash
    seeknal query pipelines/queries/pipeline_metrics.yaml
    ```

=== "Python Approach"

    Create a monitoring script:

    Create `scripts/monitor_pipeline.py`:

    ```python
    #!/usr/bin/env python3
    """
    Pipeline Health Monitoring
    """

    import pandas as pd
    from datetime import datetime, timedelta
    from pathlib import Path

    def load_recent_data():
        """Load recent pipeline output."""
        output_path = Path("output/orders.parquet")
        df = pd.read_parquet(output_path)

        # Filter last 24 hours
        cutoff = datetime.now() - timedelta(hours=24)
        df['processed_at'] = pd.to_datetime(df['processed_at'])
        return df[df['processed_at'] >= cutoff]

    def calculate_metrics(df):
        """Calculate pipeline health metrics."""
        metrics = {
            "total_records": len(df),
            "unique_orders": df['order_id'].nunique(),
            "quality_issues": df.get('quality_flag', pd.Series([0])).sum(),
            "quality_issue_pct": 0,
            "processing_lag_hours": (
                df['processed_at'] - pd.to_datetime(df['updated_at'])
            ).mean().total_seconds() / 3600,
            "last_processed": df['processed_at'].max()
        }

        if metrics["total_records"] > 0:
            metrics["quality_issue_pct"] = (
                metrics["quality_issues"] * 100.0 / metrics["total_records"]
            )

        return metrics

    def check_health(metrics):
        """Check if pipeline is healthy."""
        warnings = []
        errors = []

        # Check data freshness
        time_since_last = (
            datetime.now() - metrics["last_processed"]
        ).total_seconds() / 60  # minutes

        if time_since_last > 30:
            errors.append(f"No data processed in {time_since_last:.0f} minutes")

        # Check quality issues
        if metrics["quality_issue_pct"] > 5:
            errors.append(
                f"High quality issue rate: {metrics['quality_issue_pct']:.1f}%"
            )
        elif metrics["quality_issue_pct"] > 2:
            warnings.append(
                f"Elevated quality issues: {metrics['quality_issue_pct']:.1f}%"
            )

        # Check processing lag
        if metrics["processing_lag_hours"] > 1:
            warnings.append(
                f"High processing lag: {metrics['processing_lag_hours']:.1f} hours"
            )

        return warnings, errors

    def main():
        """Run health check."""
        print("Pipeline Health Check")
        print("=" * 50)

        df = load_recent_data()
        metrics = calculate_metrics(df)
        warnings, errors = check_health(metrics)

        print(f"Records (24h): {metrics['total_records']:,}")
        print(f"Unique Orders: {metrics['unique_orders']:,}")
        print(f"Quality Issues: {metrics['quality_issues']} ({metrics['quality_issue_pct']:.2f}%)")
        print(f"Processing Lag: {metrics['processing_lag_hours']:.2f} hours")
        print(f"Last Processed: {metrics['last_processed']}")
        print()

        if errors:
            print("❌ ERRORS:")
            for error in errors:
                print(f"  - {error}")
        if warnings:
            print("⚠️  WARNINGS:")
            for warning in warnings:
                print(f"  - {warning}")
        if not errors and not warnings:
            print("✅ Pipeline is healthy!")

        return len(errors) == 0

    if __name__ == "__main__":
        import sys
        sys.exit(0 if main() else 1)
    ```

    Run health check:

    ```bash
    python scripts/monitor_pipeline.py
    ```

### Run Pipeline Health Check

```bash
# Check recent pipeline runs
seeknal query pipeline_metrics

# View pipeline status
seeknal status pipeline orders_incremental_pipeline

# View recent logs
seeknal logs --pipeline orders_incremental_pipeline --tail 50
```

**Expected output:**
```
Pipeline Health Check
==================================================
Records (24h): 15,432
Unique Orders: 15,432
Quality Issues: 23 (0.15%)
Processing Lag: 0.12 hours
Last Processed: 2026-02-09 10:30:15

✅ Pipeline is healthy!
```

!!! success "Incremental Pipeline Complete!"
    You've built a production-grade incremental pipeline that:
    - Only processes new/changed data
    - Detects updates and deletes with CDC
    - Runs automatically every 15 minutes
    - Monitors health and alerts on issues

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. Missing or Null Timestamps**
    - Symptom: Records not processed incrementally
    - Fix: Add default timestamp or handle NULLs in WHERE clause

    **2. Late-Arriving Data**
    - Symptom: Records appear out of order
    - Fix: Use watermark strategy with buffer period

    **3. Duplicate Processing**
    - Symptom: Same records processed multiple times
    - Fix: Implement idempotent writes with upsert logic

    **4. Schedule Skew**
    - Symptom: Pipeline runs overlap
    - Fix: Ensure run duration < schedule interval

---

## Summary

In this chapter, you learned:

- [x] **Incremental Sources** — Configure HTTP sources to only fetch new data
- [x] **CDC Patterns** — Detect inserts, updates, and deletes
- [x] **Pipeline Scheduling** — Automate runs with cron or interval-based schedules
- [x] **Monitoring** — Track health metrics and alert on issues
- [x] **Validation** — Verify incremental updates are working correctly

**Key Commands:**
```bash
seeknal apply pipelines/scheduled_orders.yaml
seeknal run --pipeline orders_incremental_pipeline
seeknal query pipeline_health_metrics
seeknal logs --pipeline orders_incremental_pipeline
```

---

## What's Next?

[Chapter 3: Deploy to Production Environments →](3-production-environments.md)

Learn to use virtual environments for safe testing and production deployment, with change categorization and promotion workflows.

---

## See Also

- **[Virtual Environments](../../concepts/virtual-environments.md)** — Isolate development and production
- **[Change Categorization](../../concepts/change-categorization.md)** — Understanding breaking vs non-breaking changes
- **[Pipeline Builder](../../concepts/pipeline-builder.md)** — Core workflow for all pipelines
