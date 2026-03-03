# Chapter 10: Custom Sources

> **Duration:** 20 minutes | **Difficulty:** Intermediate | **Format:** Python & CLI

Learn to bring data from REST APIs, cloud storage, web scraping, and any Python-accessible data source into your Seeknal pipeline using custom Python transforms that act as root data nodes.

---

## What You'll Build

Three custom data sources that pull from external systems:

```
@transform: api_weather_data          →  REST API (JSON → DataFrame)
@transform: s3_inventory_data         →  Cloud storage (boto3 → DataFrame)
@transform: generated_synthetic_data  →  Synthetic data (faker → DataFrame)
         |
         └──→ transform.enriched_report (joins all three)
```

**After this chapter, you'll have:**
- Understanding of when to use `@transform` vs `@source` for data ingestion
- A REST API data source with error handling and retry logic
- A cloud storage data source using boto3
- A synthetic data generator for testing
- Best practices for custom sources in production pipelines

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 8: Python Pipelines](8-python-pipelines.md) — `@source` and `@transform` basics
- [ ] Python 3.11+ and `uv` installed
- [ ] Familiarity with `ctx.ref()` for cross-node references

---

## When @source vs @transform for Data Ingestion

### The @source Decorator

`@source` is **declarative** — it tells Seeknal where to find data (CSV, Parquet, PostgreSQL, etc.), and the `SourceExecutor` handles the actual loading:

```python
# Declarative: SourceExecutor loads the CSV
@source(name="products", source="csv", table="data/products.csv")
def products(ctx=None):
    pass  # Function body is not used
```

Supported `source=` types: `csv`, `parquet`, `json`, `jsonl`, `sqlite`, `postgresql`, `starrocks`, `iceberg`, `hive`, `bigquery`, `snowflake`, `redshift`.

### The @transform Approach for Custom Data

For anything **not** in the supported list — REST APIs, cloud storage, web scraping, data generation — use `@transform` with no upstream dependencies. The function body runs your custom Python code and returns a DataFrame:

```python
# Custom: Your code loads the data
@transform(name="api_users", description="Users from external API")
def api_users(ctx):
    import requests
    resp = requests.get("https://api.example.com/users", timeout=30)
    resp.raise_for_status()
    return pd.DataFrame(resp.json())
```

!!! info "Why not @source?"
    The `@source` decorator routes to `SourceExecutor`, which only supports built-in loaders (CSV, Parquet, databases, etc.). It does **not** execute the function body. Using `@transform` gives you full control over data loading logic, error handling, and retry behavior.

| Pattern | Use Case | Executor |
|---------|----------|----------|
| `@source(source="csv")` | Built-in file/database types | SourceExecutor (declarative) |
| `@transform` (no upstream) | APIs, cloud storage, custom logic | PythonExecutor (runs your code) |

---

## Part 1: REST API Source (8 minutes)

### Create a Weather Data Source

Fetch weather data from a public API and return it as a DataFrame:

```bash
seeknal draft transform api_weather_data --python --deps pandas,requests
```

Edit `draft_transform_api_weather_data.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "requests",
# ]
# ///

"""Custom Source: Fetch weather data from Open-Meteo API.

Uses @transform (not @source) because we need custom HTTP logic.
No upstream dependencies — this is a root node in the DAG.
"""

import pandas as pd
from seeknal.pipeline import transform


@transform(
    name="api_weather_data",
    description="Daily weather data from Open-Meteo API (free, no API key)",
)
def api_weather_data(ctx):
    """Fetch 7-day weather forecast for Jakarta."""
    import requests
    from datetime import datetime, timedelta

    # Open-Meteo is free and requires no API key
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": -6.2,
        "longitude": 106.8,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "Asia/Jakarta",
        "past_days": 7,
        "forecast_days": 0,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Parse API response into DataFrame
    daily = data["daily"]
    df = pd.DataFrame({
        "date": pd.to_datetime(daily["time"]),
        "temp_max": daily["temperature_2m_max"],
        "temp_min": daily["temperature_2m_min"],
        "precipitation_mm": daily["precipitation_sum"],
    })

    print(f"  Fetched {len(df)} days of weather data")
    return df
```

```bash
seeknal dry-run draft_transform_api_weather_data.py
seeknal apply draft_transform_api_weather_data.py
```

### Add Retry Logic for Production

For production APIs, add retry with exponential backoff:

```python
@transform(
    name="api_weather_data",
    description="Weather data with retry logic",
)
def api_weather_data(ctx):
    """Fetch weather data with retry on transient failures."""
    import requests
    import time

    url = "https://api.open-meteo.com/v1/forecast"
    params = {"latitude": -6.2, "longitude": 106.8, "daily": "temperature_2m_max"}

    # Retry up to 3 times with exponential backoff
    for attempt in range(3):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            break
        except requests.RequestException as e:
            if attempt == 2:
                raise RuntimeError(f"API failed after 3 attempts: {e}") from e
            wait = 2 ** attempt  # 1s, 2s, 4s
            print(f"  Attempt {attempt + 1} failed, retrying in {wait}s...")
            time.sleep(wait)

    data = response.json()
    return pd.DataFrame({
        "date": pd.to_datetime(data["daily"]["time"]),
        "temp_max": data["daily"]["temperature_2m_max"],
    })
```

!!! tip "Error Handling in Custom Sources"
    If your `@transform` raises an exception, the pipeline run marks that node as FAILED and stops (unless `--continue-on-error` is set). Always:

    - Set `timeout=` on HTTP requests (default is infinite)
    - Use `raise_for_status()` to catch HTTP errors
    - Wrap in retry for transient failures (network timeouts, 429 rate limits)

---

## Part 2: Cloud Storage Source (6 minutes)

### Read from S3-Compatible Storage

Load data from S3, MinIO, GCS, or any S3-compatible storage:

```bash
seeknal draft transform s3_inventory_data --python --deps pandas,boto3
```

Edit `draft_transform_s3_inventory_data.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "boto3",
# ]
# ///

"""Custom Source: Load inventory data from S3-compatible storage.

Reads Parquet files from S3/MinIO using boto3.
Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL
"""

import pandas as pd
from seeknal.pipeline import transform


@transform(
    name="s3_inventory_data",
    description="Product inventory from S3 storage",
)
def s3_inventory_data(ctx):
    """Load inventory data from S3 bucket."""
    import io
    import os
    import boto3

    # Configure S3 client (works with MinIO, AWS S3, etc.)
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )

    # Read Parquet file from S3
    bucket = "data-lake"
    key = "inventory/latest.parquet"

    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    print(f"  Loaded {len(df)} rows from s3://{bucket}/{key}")
    return df
```

!!! note "For Local Testing Without S3"
    If you don't have S3/MinIO available, create a mock version using local files:

    ```python
    @transform(name="s3_inventory_data", description="Mock S3 inventory data")
    def s3_inventory_data(ctx):
        """Load inventory from local file (mock for S3)."""
        import os

        # Check if S3 is configured, fall back to local file
        if os.environ.get("AWS_ENDPOINT_URL"):
            return _load_from_s3()  # Real S3 logic
        else:
            # Local fallback for development
            return pd.DataFrame({
                "product_id": ["SKU-001", "SKU-002", "SKU-003"],
                "product_name": ["Widget A", "Widget B", "Gadget C"],
                "stock_count": [150, 89, 0],
                "warehouse": ["Jakarta", "Surabaya", "Jakarta"],
                "last_updated": pd.to_datetime(["2026-02-25"] * 3),
            })
    ```

---

## Part 3: Synthetic Data Generator (3 minutes)

### Generate Test Data

Create reproducible synthetic data for testing and development:

```bash
seeknal draft transform generated_synthetic_data --python --deps pandas,faker
```

Edit `draft_transform_generated_synthetic_data.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "faker",
# ]
# ///

"""Custom Source: Generate synthetic customer data for testing.

Produces deterministic data using a fixed seed for reproducibility.
"""

import pandas as pd
from seeknal.pipeline import transform


@transform(
    name="generated_synthetic_data",
    description="Synthetic customer data for testing (reproducible with seed=42)",
)
def generated_synthetic_data(ctx):
    """Generate fake customer data for development and testing."""
    from faker import Faker

    fake = Faker()
    Faker.seed(42)  # Reproducible across runs

    n = 100
    records = []
    for _ in range(n):
        records.append({
            "customer_id": fake.uuid4()[:8],
            "name": fake.name(),
            "email": fake.email(),
            "city": fake.city(),
            "signup_date": fake.date_between(start_date="-2y", end_date="today"),
            "lifetime_value": round(fake.pyfloat(min_value=0, max_value=5000), 2),
        })

    df = pd.DataFrame(records)
    print(f"  Generated {len(df)} synthetic customers")
    return df
```

```bash
seeknal apply draft_transform_generated_synthetic_data.py
```

!!! tip "Reproducibility"
    Always use `Faker.seed(42)` (or any fixed seed) so that `seeknal run` produces identical data every time. This makes your pipeline deterministic and test-friendly.

---

## Part 4: Combine Custom Sources (3 minutes)

### Join Custom Sources with Downstream Transforms

Custom sources integrate seamlessly with the rest of your pipeline via `ctx.ref()`:

```bash
seeknal draft transform enriched_report --python --deps pandas,duckdb
```

Edit `draft_transform_enriched_report.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""Transform: Combine data from custom sources into a report."""

from seeknal.pipeline import transform


@transform(
    name="enriched_report",
    description="Report combining API, cloud, and synthetic data",
)
def enriched_report(ctx):
    """Join custom sources into a unified report."""
    # Reference custom sources like any other node
    weather = ctx.ref("transform.api_weather_data")
    customers = ctx.ref("transform.generated_synthetic_data")

    # Use DuckDB SQL to query them
    return ctx.duckdb.sql("""
        SELECT
            c.customer_id,
            c.name,
            c.city,
            c.lifetime_value,
            w.temp_max AS latest_temp_max,
            w.precipitation_mm AS latest_precip
        FROM customers c
        CROSS JOIN (
            SELECT temp_max, precipitation_mm
            FROM weather
            ORDER BY date DESC
            LIMIT 1
        ) w
        WHERE c.lifetime_value > 1000
        ORDER BY c.lifetime_value DESC
        LIMIT 10
    """).df()
```

```bash
seeknal apply draft_transform_enriched_report.py
seeknal run
```

**Checkpoint:** The `enriched_report` node should depend on `api_weather_data` and `generated_synthetic_data` in the DAG. Verify with `seeknal plan`.

---

## Best Practices

### 1. Naming Convention

Use a clear prefix or description to distinguish custom data sources from computational transforms:

```python
# Good: Clear that this is a data source
@transform(name="api_weather_data", description="Weather data from Open-Meteo API")

# Good: Description explains the origin
@transform(name="s3_inventory_data", description="Product inventory from S3 storage")

# Avoid: Ambiguous — is this a source or a computation?
@transform(name="weather")
```

### 2. Timeout and Error Handling

Always protect against network failures:

```python
# Good: Explicit timeout and error handling
response = requests.get(url, timeout=30)
response.raise_for_status()

# Bad: No timeout (hangs forever on network issues)
response = requests.get(url)
```

### 3. Idempotency

Custom sources should return the same data for the same pipeline run. Use caching or fixed seeds:

```python
# Good: Deterministic
Faker.seed(42)

# Good: Cache API response to local file
cache_path = Path("target/.cache/weather.parquet")
if cache_path.exists():
    return pd.read_parquet(cache_path)
# ... fetch and save to cache_path
```

### 4. Credentials via Environment Variables

Never hard-code credentials:

```python
# Good: Environment variables
api_key = os.environ.get("WEATHER_API_KEY")
if not api_key:
    raise RuntimeError("WEATHER_API_KEY environment variable is required")

# Bad: Hard-coded secrets
api_key = "sk-abc123..."
```

### 5. PEP 723 Dependencies

Declare all external packages in the script header. Each custom source runs in its own isolated environment:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "requests",     # HTTP client
#     "boto3",        # AWS S3
#     "faker",        # Synthetic data
# ]
# ///
```

This means different custom sources can use different library versions without conflicts.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. Using @source for custom logic**

    - Symptom: Function body is ignored, data comes from `table=` path instead
    - Fix: Use `@transform` for any custom data loading logic. `@source` is declarative only.

    **2. No timeout on HTTP requests**

    - Symptom: Pipeline hangs indefinitely when API is down
    - Fix: Always set `timeout=30` (or appropriate value) on all network calls

    **3. Non-deterministic data**

    - Symptom: Pipeline produces different results on each run
    - Fix: Use fixed seeds for synthetic data, or cache API responses locally

    **4. Missing dependencies in PEP 723 header**

    - Symptom: `ModuleNotFoundError: No module named 'requests'`
    - Fix: Add the package to `# dependencies = [...]` in the script header

    **5. Credentials not set**

    - Symptom: `403 Forbidden` or `NoCredentialError`
    - Fix: Set required environment variables (`AWS_ACCESS_KEY_ID`, API keys, etc.) before running

---

## Summary

In this chapter, you learned:

- [x] **@source vs @transform** — `@source` is declarative (built-in types only); `@transform` handles custom logic
- [x] **REST API sources** — HTTP requests with timeout, retry, and error handling
- [x] **Cloud storage sources** — S3/MinIO data loading with boto3
- [x] **Synthetic data** — Reproducible test data with Faker and fixed seeds
- [x] **Integration** — Custom sources are referenced with `ctx.ref()` like any other node

**Key Patterns:**

| Pattern | When to Use |
|---------|-------------|
| `@source(source="csv")` | File on disk (CSV, Parquet, JSON) |
| `@source(source="postgresql")` | Database table or query |
| `@transform` (no upstream) | REST API, cloud storage, custom logic |
| `@transform` (with upstream) | Computational transforms |

---

## What's Next?

Return to the [Advanced Guide Overview](index.md) to explore other chapters, or dive into the reference documentation:

- **[Python Pipelines Guide](../../guides/python-pipelines.md)** — Complete decorator reference
- **[CLI Reference](../../reference/cli.md)** — All commands and flags

---

*Last updated: February 2026 | Seeknal Documentation*
