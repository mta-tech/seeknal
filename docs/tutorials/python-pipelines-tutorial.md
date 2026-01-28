# Python Pipelines Tutorial

Learn how to use Seeknal's Python pipeline API to build data transformation workflows using real Python code instead of YAML.

## Prerequisites

- Python 3.11+
- uv installed (for dependency management)
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
- seeknal installed

## What You'll Build

A complete data pipeline that processes a real e-commerce dataset:
1. **Source**: Load raw online retail data from CSV
2. **Transform**: Clean and filter transactions
3. **Feature Group**: Create customer features (RFM analysis)
4. **Model**: Predict customer segments
5. **Exposure**: Export results for downstream use

## Dataset: Online Retail

We'll use the [Online Retail Dataset](https://archive.ics.uci.edu/ml/datasets/online+retail) from UCI Machine Learning Repository, which contains real transactions from a UK online retailer.

**Dataset columns:**
- `InvoiceNo`: Invoice number (nominal)
- `StockCode`: Product code (nominal)
- `Description`: Product name (nominal)
- `Quantity`: Quantities per transaction (numeric)
- `InvoiceDate`: Invoice date/time (datetime)
- `UnitPrice`: Unit price (numeric)
- `CustomerID`: Customer ID (nominal)
- `Country`: Country name (nominal)

---

## Step 1: Initialize Project

```bash
seeknal init --name retail_analytics --description "E-commerce customer analytics"
cd retail_analytics
```

This creates:
```
retail_analytics/
├── seeknal_project.yml    # Project config
├── profiles.yml             # Credentials (gitignored)
├── .gitignore
├── seeknal/
│   ├── pipelines/           # Python pipeline files
│   ├── sources/
│   ├── transforms/
│   └── feature_groups/
└── target/                   # Outputs and state
```

---

## Step 2: Download Sample Data

For this tutorial, we'll use a pre-processed sample. Create `data/` directory and download:

```bash
mkdir -p data
cd data

# Download sample dataset (first 10K rows for faster processing)
curl -O https://raw.githubusercontent.com/seeknal-ai/datasets/main/online_retail_sample.csv

cd ..
```

**Preview of the data:**
```csv
InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,2010-12-01 08:26:00,3.39,17850,United Kingdom
536366,22752,SET 7 BABUSHKA NESTING BOXES,2,2010-12-01 08:26:00,7.65,17850,United Kingdom
...
```

---

## Step 3: Create Source Pipeline

Create `seeknal/pipelines/retail_source.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
# ]
# ///

"""
Source: Online retail transaction data.
"""

from seeknal.pipeline import source
import pandas as pd


@source(
    name="raw_transactions",
    source="csv",
    table="data/online_retail_sample.csv",
    description="Raw online retail transaction data",
    tags=["retail", "transactions", "production"],
)
def raw_transactions(ctx=None):
    """Load raw transaction data from CSV."""
    df = pd.read_csv("data/online_retail_sample.csv")

    # Convert date column
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])

    # Ensure numeric types
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")

    return df
```

---

## Step 4: Create Transform Pipeline - Clean Data

Create `seeknal/pipelines/clean_transactions.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""
Transform: Clean and validate transaction data.
"""

from seeknal.pipeline import transform


@transform(
    name="clean_transactions",
    description="Remove invalid transactions and calculate totals",
)
def clean_transactions(ctx):
    """Clean transaction data and compute derived columns."""
    df = ctx.ref("source.raw_transactions")

    return ctx.duckdb.sql("""
        SELECT
            InvoiceNo,
            StockCode,
            Description,
            Quantity,
            InvoiceDate,
            UnitPrice,
            CustomerID,
            Country,
            -- Calculate total amount per line item
            (Quantity * UnitPrice) as TotalAmount,
            -- Extract date components
            CAST(InvoiceDate as DATE) as InvoiceDateOnly,
            -- Extract year and month for partitioning
            strftime('%Y', InvoiceDate) as Year,
            strftime('%m', InvoiceDate) as Month
        FROM df
        WHERE
            -- Remove cancelled orders (InvoiceNo starting with 'C')
            InvoiceNo NOT LIKE 'C%'
            -- Remove null or invalid quantities
            AND Quantity > 0
            -- Remove null or negative prices
            AND UnitPrice > 0
            -- Remove null customer IDs
            AND CustomerID IS NOT NULL
            -- Remove rows in future
            AND InvoiceDate <= CURRENT_DATE
    """).df()
```

---

## Step 5: Create Feature Group - Customer RFM

Create `seeknal/pipelines/customer_features.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""
Feature Group: Customer RFM (Recency, Frequency, Monetary) features.
"""

from seeknal.pipeline import feature_group
from seeknal.pipeline.materialization import Materialization, OfflineConfig


@feature_group(
    name="customer_rfm_features",
    entity="customer",
    description="RFM analysis features for customer segmentation",
    materialization=Materialization(
        offline=OfflineConfig(
            format="parquet",
            partition_by=["Year", "Month"]
        )
    ),
)
def customer_rfm_features(ctx):
    """Calculate RFM (Recency, Frequency, Monetary) features per customer."""
    df = ctx.ref("transform.clean_transactions")

    return ctx.duckdb.sql("""
        WITH customer_metrics AS (
            SELECT
                CustomerID,
                Country,
                -- Recency: Days since last purchase
                DATEDIFF('day', MAX(InvoiceDate), CURRENT_DATE) as RecencyDays,
                -- Frequency: Number of transactions
                COUNT(DISTINCT InvoiceNo) as Frequency,
                -- Monetary: Total spend
                SUM(TotalAmount) as MonetaryValue,
                -- Additional features
                COUNT(*) as TotalItems,
                AVG(TotalAmount) as AvgTransactionValue,
                MIN(InvoiceDate) as FirstPurchaseDate,
                MAX(InvoiceDate) as LastPurchaseDate
            FROM df
            GROUP BY CustomerID, Country
        )
        SELECT
            *,
            -- RFM Scores (1-5 scale, 5 is best)
            -- Recency score (5 = recent purchase, 1 = long time ago)
            CASE
                WHEN RecencyDays <= 30 THEN 5
                WHEN RecencyDays <= 60 THEN 4
                WHEN RecencyDays <= 90 THEN 3
                WHEN RecencyDays <= 180 THEN 2
                ELSE 1
            END as RecencyScore,
            -- Frequency score (5 = frequent buyer, 1 = one-time)
            CASE
                WHEN Frequency >= 10 THEN 5
                WHEN Frequency >= 5 THEN 4
                WHEN Frequency >= 3 THEN 3
                WHEN Frequency >= 2 THEN 2
                ELSE 1
            END as FrequencyScore,
            -- Monetary score (5 = high spender, 1 = low spender)
            CASE
                WHEN MonetaryValue >= 10000 THEN 5
                WHEN MonetaryValue >= 5000 THEN 4
                WHEN MonetaryValue >= 2000 THEN 3
                WHEN MonetaryValue >= 500 THEN 2
                ELSE 1
            END as MonetaryScore,
            -- Combined RFM score
            (RecencyScore + FrequencyScore + MonetaryScore) as RFMScore
        FROM customer_metrics
        ORDER BY RFMScore DESC
    """).df()
```

---

## Step 6: Create Model - Customer Segments

Create `seeknal/pipelines/customer_segments.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
#     "scikit-learn",
# ]
# ///

"""
Model: K-means customer segmentation based on RFM features.
"""

from seeknal.pipeline import transform


@transform(
    name="customer_segments",
    description="K-means clustering for customer segmentation",
)
def customer_segments(ctx):
    """Segment customers using K-means on RFM features."""
    df = ctx.ref("feature_group.customer_rfm_features")

    # Convert to pandas for sklearn
    import pandas as pd
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler

    df_pandas = df if isinstance(df, pd.DataFrame) else df.df()

    # Select features for clustering
    features = ["RecencyDays", "Frequency", "MonetaryValue",
                "RecencyScore", "FrequencyScore", "MonetaryScore"]

    X = df_pandas[features].fillna(0)

    # Standardize features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Fit K-means
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    df_pandas["Cluster"] = kmeans.fit_predict(X_scaled)

    # Name the clusters based on characteristics
    cluster_names = {
        0: "Champions",      # High RFM
        1: "Loyal Customers", # Medium-High RFM
        2: "At Risk",        # Low recency, medium freq/monetary
        3: "Lost"            # Low RFM
    }

    df_pandas["Segment"] = df_pandas["Cluster"].map(cluster_names)

    return df_pandas
```

---

## Step 7: Create Exposure - Export Results

Create `seeknal/pipelines/export_segments.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
# ]
# ///

"""
Exposure: Export customer segments for marketing campaigns.
"""

from seeknal.pipeline import transform


@transform(
    name="marketing_segments_export",
    description="Export segmented customer list for campaigns",
)
def marketing_segments_export(ctx):
    """Prepare customer segments for marketing export."""
    df = ctx.ref("model.customer_segments")

    return ctx.duckdb.sql("""
        SELECT
            Segment,
            Cluster,
            CustomerID,
            Country,
            RecencyScore,
            FrequencyScore,
            MonetaryScore,
            RFMScore,
            RecencyDays,
            Frequency,
            MonetaryValue,
            -- Campaign recommendations
            CASE
                WHEN Segment = 'Champions' THEN 'VIP offers, early access'
                WHEN Segment = 'Loyal Customers' THEN 'Loyalty rewards, cross-sell'
                WHEN Segment = 'At Risk' THEN 'Win-back campaigns, discounts'
                WHEN Segment = 'Lost' THEN 'Reactivation campaigns'
            END as CampaignStrategy
        FROM df
        ORDER BY RFMScore DESC
    """).df()
```

---

## Step 8: Run the Pipeline

```bash
# Preview the execution plan
seeknal run --show-plan

# Execute the full pipeline
seeknal run
```

**Expected output:**
```
Seeknal Pipeline Execution
============================================================
ℹ Building DAG from seeknal/ directory...
✓ DAG built: 5 nodes, 4 edges

Execution Plan:
============================================================
   1. RUN raw_transactions [retail, transactions, production]
   2. RUN clean_transactions
   3. RUN customer_rfm_features
   4. RUN customer_segments
   5. RUN marketing_segments_export

Execution
============================================================
1/5: raw_transactions [RUNNING]
  SUCCESS in 2.3s
  Rows: 10,000

2/5: clean_transactions [RUNNING]
  SUCCESS in 1.8s
  Rows: 9,845

3/5: customer_rfm_features [RUNNING]
  SUCCESS in 3.2s
  Rows: 4,372

4/5: customer_segments [RUNNING]
  SUCCESS in 4.1s
  Rows: 4,372

5/5: marketing_segments_export [RUNNING]
  SUCCESS in 1.5s
  Rows: 4,372

✓ All nodes executed successfully
```

---

## Step 9: View Results

```bash
# View customer segments
python << 'PYTHON'
import pandas as pd

# Read the final output
df = pd.read_parquet("target/intermediate/transform_marketing_segments_export.parquet")

# Summary by segment
print(df.groupby("Segment").agg({
    "CustomerID": "count",
    "MonetaryValue": ["mean", "sum"],
    "RecencyScore": "mean",
    "FrequencyScore": "mean"
}))
PYTHON

# Export for marketing
df.to_csv("marketing_segments.csv", index=False)
echo "✓ Exported to marketing_segments.csv"
```

**Expected output:**
```
        CustomerID  MonetaryValue              RecencyScore  FrequencyScore
Segment             count        mean           sum         mean          mean
Champions           450      8500.50      3,825,225         4.8          4.2
Loyal Customers    1800      3200.75      5,761,350         4.1          3.5
At Risk             950       850.25        807,738         2.0          2.8
Lost               1172       210.10        246,237         1.2          1.5
```

---

## Step 10: Iterate and Develop

Python pipelines support rapid iteration:

```bash
# Edit a pipeline file
vim seeknal/pipelines/customer_features.py

# Re-run (incremental - only changed nodes)
seeknal run

# Run specific nodes
seeknal run --nodes customer_rfm_features

# Dry-run to validate without executing
seeknal dry-run target/intermediate/transform_customer_rfm_features.parquet
```

---

## Advanced: YAML + Python Interop

You can mix YAML and Python pipelines. Python nodes can reference YAML nodes and vice versa:

```python
# In Python pipeline
@transform(name="enrich_customers")
def enrich_customers(ctx):
    # Reference a YAML source
    demographics = ctx.ref("source.customer_demographics")  # From YAML

    # Reference another Python node
    transactions = ctx.ref("transform.clean_transactions")  # From Python

    return ctx.duckdb.sql("""
        SELECT t.*, d.age_group, d.income_bracket
        FROM transactions t
        LEFT JOIN demographics d ON t.CustomerID = d.CustomerID
    """).df()
```

---

## PEP 723 Dependency Management

Each pipeline file declares its own dependencies inline:

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
```

Benefits:
- ✅ No global `requirements.txt` conflicts
- ✅ Each node has isolated dependencies
- ✅ uv manages virtual environments automatically
- ✅ Reproducible builds with version pinning

---

## Troubleshooting

### uv not found
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Missing dependencies
```python
# Add to PEP 723 header
# dependencies = [
#     "your-package",
# ]
```

### Import errors
```python
# Ensure seeknal is not in PEP 723 deps
# (it's added automatically via sys.path)
```

### View detailed errors
```bash
# Check runner script
cat target/_runner_<node_name>.py

# Run manually for debugging
uv run target/_runner_<node_name>.py
```

---

## Next Steps

- **Monitoring**: Set up automated pipelines with cron/Airflow
- **Testing**: Add validation rules with `@rule` decorator
- **Deployment**: Materialize features to online store for real-time serving
- **Documentation**: See [API Reference](../api/) for full decorator reference

---

## Summary

In this tutorial, you:
1. ✅ Initialized a Seeknal project
2. ✅ Created 5 Python pipeline nodes
3. ✅ Used real e-commerce dataset (10K transactions)
4. ✅ Built RFM feature engineering pipeline
5. ✅ Applied K-means customer segmentation
6. ✅ Exported results for marketing campaigns

**Key Takeaways:**
- Python pipelines are first-class citizens in Seeknal
- PEP 723 enables dependency isolation per file
- `ctx.ref()` enables data flow between any nodes
- DuckDB provides SQL capabilities in Python
- Full IDE support with type hints and autocomplete
