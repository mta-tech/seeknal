# Seeknal Quickstart Examples

This directory contains quickstart examples to help you get started with Seeknal for feature engineering. Complete these examples in **under 15 minutes** to learn the fundamentals of data transformation with Seeknal.

## What You'll Learn

- Loading and exploring data with Seeknal
- Creating data processing pipelines with DuckDBTask
- Engineering user-level features with SQL transformations
- Saving features to local storage for ML modeling

## Prerequisites

Before running these examples, ensure you have:

1. **Python 3.11+** installed
2. **Seeknal** installed:
   ```bash
   pip install seeknal
   ```
3. **Required dependencies**:
   ```bash
   pip install pandas pyarrow
   ```

## Quick Start

### Run the DuckDB Example (Recommended First Step)

The DuckDB example is the fastest way to get started. It runs entirely locally without any external dependencies.

```bash
cd examples/quickstart
python quickstart_duckdb.py
```

**Expected Runtime:** ~1 minute

### What to Expect

When you run the DuckDB example, you'll see:

1. **Data Loading:** The script loads 500 rows of e-commerce user behavior data
2. **Feature Engineering:** SQL transformations create user-level features
3. **Results:** Feature statistics and top users by revenue
4. **Output Files:** Parquet and CSV files saved to `./output/`

Sample output:
```
============================================================
 Seeknal Quickstart - DuckDB Edition
============================================================

Welcome! This quickstart will guide you through feature engineering
using Seeknal with DuckDB for local development.

--- Step 1: Loading Sample Data ---

Loaded 500 rows of e-commerce user behavior data

Columns: ['user_id', 'event_time', 'product_category', 'action_type',
          'device_type', 'purchase_amount', 'session_duration',
          'items_viewed', 'cart_value']
```

## Sample Dataset

The `sample_data.csv` contains simulated e-commerce user behavior data with realistic patterns:

| Column | Type | Description |
|--------|------|-------------|
| `user_id` | string | Unique user identifier (e.g., `user_001`) |
| `event_time` | datetime | Timestamp of the event |
| `product_category` | string | Category of product (Electronics, Clothing, etc.) |
| `action_type` | string | Type of action (view, add_to_cart, purchase, etc.) |
| `device_type` | string | Device used (mobile, desktop, tablet) |
| `purchase_amount` | float | Purchase amount in dollars (0 if no purchase) |
| `session_duration` | int | Session duration in seconds |
| `items_viewed` | int | Number of items viewed in session |
| `cart_value` | float | Current cart value |

**Dataset Stats:**
- 500 events across 100 unique users
- Date range: January 2024
- 10 product categories
- 5 action types

## Features Created

The DuckDB example creates the following user-level features:

### Transaction Features
- `total_events` - Total number of events per user
- `total_purchases` - Number of purchase events
- `total_views` - Number of view events
- `total_cart_adds` - Number of add-to-cart events

### Revenue Features
- `total_revenue` - Total purchase amount
- `avg_purchase_amount` - Average purchase value
- `max_purchase_amount` - Maximum single purchase

### Engagement Features
- `avg_session_duration` - Average session length
- `total_items_viewed` - Total items viewed across all sessions
- `avg_items_per_session` - Average items viewed per session

### Device & Category Features
- `mobile_sessions`, `desktop_sessions`, `tablet_sessions` - Sessions by device
- `favorite_category` - Most frequent product category
- `categories_explored` - Number of unique categories browsed

## Output Files

After running the example, you'll find:

```
examples/quickstart/
  output/
    user_features.parquet   # Efficient columnar format for ML
    user_features.csv       # Human-readable format
```

These files contain the engineered features for all 100 users, ready for use in machine learning models.

## Troubleshooting

### ModuleNotFoundError: No module named 'seeknal'

Ensure Seeknal is installed:
```bash
pip install seeknal
```

### ModuleNotFoundError: No module named 'pandas' or 'pyarrow'

Install the required dependencies:
```bash
pip install pandas pyarrow
```

### Sample data not found

Make sure you're running the script from the `examples/quickstart` directory or that `sample_data.csv` exists in the same directory as the script.

## Next Steps

After completing this quickstart:

1. **Explore the Features:** Open `output/user_features.csv` to inspect the generated features

2. **Modify the SQL:** Edit the feature engineering SQL in `quickstart_duckdb.py` to create your own features

3. **Use Your Own Data:** Replace `sample_data.csv` with your own CSV file and adapt the transformations

4. **Scale with Spark:** For production workloads with larger datasets, see `quickstart_spark.py` which demonstrates Seeknal with Apache Spark (coming soon)

5. **Learn More:**
   - [Seeknal Documentation](https://github.com/mta-tech/seeknal)
   - [Full Getting Started Guide](../../docs/getting-started.md)

## Example Architecture

```
                    +-----------------+
                    |  sample_data.csv |
                    |   (Raw Data)    |
                    +--------+--------+
                             |
                             v
                    +--------+--------+
                    |   DuckDBTask    |
                    | (SQL Transform) |
                    +--------+--------+
                             |
                             v
                    +--------+--------+
                    | User Features   |
                    | (Aggregated)    |
                    +--------+--------+
                             |
              +--------------+--------------+
              |                             |
              v                             v
     +--------+--------+           +--------+--------+
     | user_features   |           | user_features   |
     |   .parquet      |           |     .csv        |
     +-----------------+           +-----------------+
```

## Files in This Directory

| File | Description |
|------|-------------|
| `README.md` | This documentation file |
| `sample_data.csv` | Sample e-commerce user behavior data |
| `quickstart_duckdb.py` | DuckDB quickstart example script |
| `output/` | Directory created when you run the examples (contains generated features) |
