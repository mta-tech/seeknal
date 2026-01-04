#!/usr/bin/env python3
"""
Seeknal Quickstart Guide - DuckDB Edition
==========================================

This quickstart demonstrates how to use Seeknal with DuckDB for local
development and feature engineering. DuckDB is perfect for:
- Rapid prototyping and exploration
- Development and testing with small to medium datasets
- Running analytics locally without Spark infrastructure

Prerequisites:
- Python 3.11+
- Seeknal installed: pip install seeknal
- pandas installed: pip install pandas

Estimated Time: 10 minutes

What You'll Learn:
1. Loading data with DuckDBTask
2. Feature engineering with SQL transformations
3. Aggregating user behavior features
4. Saving features locally for analysis
"""

import os
import sys
from pathlib import Path

# =============================================================================
# STEP 1: Setup and Imports
# =============================================================================
# Import required libraries
import pandas as pd
import pyarrow as pa
from datetime import datetime

# Import Seeknal DuckDB components
from seeknal.tasks.duckdb import DuckDBTask


def print_section(title: str) -> None:
    """Helper function to print section headers."""
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}\n")


def print_step(step_num: int, description: str) -> None:
    """Helper function to print step headers."""
    print(f"\n--- Step {step_num}: {description} ---\n")


def main():
    """
    Main function demonstrating the complete DuckDB workflow.

    This example uses e-commerce user behavior data to create
    features for a recommendation or churn prediction model.
    """

    print_section("Seeknal Quickstart - DuckDB Edition")
    print("Welcome! This quickstart will guide you through feature engineering")
    print("using Seeknal with DuckDB for local development.")

    # =============================================================================
    # STEP 2: Load Sample Data
    # =============================================================================
    print_step(1, "Loading Sample Data")

    # Get the path to sample data (relative to this script)
    script_dir = Path(__file__).parent
    sample_data_path = script_dir / "sample_data.csv"

    # Verify the sample data exists
    if not sample_data_path.exists():
        print(f"ERROR: Sample data not found at {sample_data_path}")
        print("Please ensure sample_data.csv is in the same directory as this script.")
        sys.exit(1)

    # Load data using pandas first to inspect it
    raw_df = pd.read_csv(sample_data_path)

    print(f"Loaded {len(raw_df):,} rows of e-commerce user behavior data")
    print(f"\nColumns: {list(raw_df.columns)}")
    print(f"\nSample data (first 5 rows):")
    print(raw_df.head().to_string(index=False))

    # =============================================================================
    # STEP 3: Create DuckDB Task for Data Processing
    # =============================================================================
    print_step(2, "Creating DuckDB Task")

    # Convert pandas DataFrame to PyArrow Table for DuckDB
    arrow_table = pa.Table.from_pandas(raw_df)

    # Create a DuckDBTask and add the input data
    duckdb_task = DuckDBTask()
    duckdb_task.add_input(dataframe=arrow_table)

    print("DuckDBTask created successfully!")
    print(f"Task type: {duckdb_task.kind}")
    print(f"Uses Spark: {duckdb_task.is_spark_job}")  # Should be False

    # =============================================================================
    # STEP 4: Feature Engineering with SQL
    # =============================================================================
    print_step(3, "Engineering User Behavior Features")

    # Define SQL for user-level feature aggregation
    # This creates features useful for ML models like:
    # - Recommendation systems
    # - Churn prediction
    # - Customer segmentation

    user_features_sql = """
    SELECT
        user_id,

        -- Transaction Features
        COUNT(*) as total_events,
        COUNT(CASE WHEN action_type = 'purchase' THEN 1 END) as total_purchases,
        COUNT(CASE WHEN action_type = 'view' THEN 1 END) as total_views,
        COUNT(CASE WHEN action_type = 'add_to_cart' THEN 1 END) as total_cart_adds,

        -- Revenue Features
        SUM(purchase_amount) as total_revenue,
        AVG(purchase_amount) as avg_purchase_amount,
        MAX(purchase_amount) as max_purchase_amount,

        -- Engagement Features
        AVG(session_duration) as avg_session_duration,
        SUM(items_viewed) as total_items_viewed,
        AVG(items_viewed) as avg_items_per_session,

        -- Cart Features
        AVG(cart_value) as avg_cart_value,
        MAX(cart_value) as max_cart_value,

        -- Device Preferences
        COUNT(CASE WHEN device_type = 'mobile' THEN 1 END) as mobile_sessions,
        COUNT(CASE WHEN device_type = 'desktop' THEN 1 END) as desktop_sessions,
        COUNT(CASE WHEN device_type = 'tablet' THEN 1 END) as tablet_sessions,

        -- Favorite Category (mode)
        MODE(product_category) as favorite_category,
        COUNT(DISTINCT product_category) as categories_explored,

        -- Time Features
        MIN(event_time) as first_event_time,
        MAX(event_time) as last_event_time

    FROM __THIS__
    GROUP BY user_id
    ORDER BY total_revenue DESC
    """

    # Add the SQL transformation to the task
    duckdb_task.add_sql(user_features_sql)

    print("Feature engineering SQL added to the pipeline.")
    print("\nFeatures being created:")
    print("  - Transaction counts (purchases, views, cart adds)")
    print("  - Revenue metrics (total, average, max)")
    print("  - Engagement metrics (session duration, items viewed)")
    print("  - Device preferences")
    print("  - Category preferences")
    print("  - Time-based features")

    # =============================================================================
    # STEP 5: Execute the Transformation
    # =============================================================================
    print_step(4, "Executing Feature Transformation")

    # Transform the data - this runs the SQL query
    result_arrow = duckdb_task.transform()

    # Convert to pandas for easy inspection
    result_df = result_arrow.to_pandas()

    print(f"Feature transformation complete!")
    print(f"Created features for {len(result_df):,} unique users")
    print(f"\nFeature columns ({len(result_df.columns)}):")
    for col in result_df.columns:
        print(f"  - {col}")

    # =============================================================================
    # STEP 6: Inspect the Results
    # =============================================================================
    print_step(5, "Inspecting Feature Results")

    print("Top 5 users by revenue:")
    print(result_df[['user_id', 'total_purchases', 'total_revenue',
                     'favorite_category']].head().to_string(index=False))

    print("\nFeature Statistics:")
    stats_cols = ['total_events', 'total_purchases', 'total_revenue',
                  'avg_session_duration', 'categories_explored']
    print(result_df[stats_cols].describe().round(2).to_string())

    # =============================================================================
    # STEP 7: Save Features Locally
    # =============================================================================
    print_step(6, "Saving Features to Local Storage")

    # Create output directory
    output_dir = script_dir / "output"
    output_dir.mkdir(exist_ok=True)

    # Save as Parquet (efficient columnar format)
    parquet_path = output_dir / "user_features.parquet"
    result_df.to_parquet(parquet_path, index=False)
    print(f"Saved features to: {parquet_path}")

    # Save as CSV for easy viewing
    csv_path = output_dir / "user_features.csv"
    result_df.to_csv(csv_path, index=False)
    print(f"Saved features to: {csv_path}")

    # =============================================================================
    # STEP 8: Additional Analysis with DuckDB
    # =============================================================================
    print_step(7, "Running Additional Analysis")

    # Create a new task to analyze the features
    analysis_task = DuckDBTask()
    analysis_task.add_input(dataframe=pa.Table.from_pandas(result_df))

    # Calculate conversion rate distribution
    conversion_sql = """
    SELECT
        CASE
            WHEN total_purchases = 0 THEN 'No Purchases'
            WHEN total_purchases = 1 THEN '1 Purchase'
            WHEN total_purchases <= 3 THEN '2-3 Purchases'
            ELSE '4+ Purchases'
        END as purchase_segment,
        COUNT(*) as user_count,
        ROUND(AVG(total_revenue), 2) as avg_revenue,
        ROUND(AVG(avg_session_duration), 2) as avg_session_mins
    FROM __THIS__
    GROUP BY 1
    ORDER BY user_count DESC
    """

    analysis_task.add_sql(conversion_sql)
    analysis_result = analysis_task.transform()
    analysis_df = analysis_result.to_pandas()

    print("User Segmentation by Purchase Behavior:")
    print(analysis_df.to_string(index=False))

    # =============================================================================
    # SUMMARY
    # =============================================================================
    print_section("Quickstart Complete!")

    print("You have successfully:")
    print("  1. Loaded e-commerce behavior data")
    print("  2. Created a DuckDBTask for data processing")
    print("  3. Engineered user-level features with SQL")
    print("  4. Executed the transformation")
    print("  5. Saved features locally")
    print("  6. Performed additional analysis")

    print("\nOutput files created:")
    print(f"  - {parquet_path}")
    print(f"  - {csv_path}")

    print("\n" + "="*60)
    print(" Next Steps")
    print("="*60)
    print("""
For production workloads with larger datasets, use Seeknal with Spark:

    from seeknal.project import Project
    from seeknal.workspace import Workspace
    from seeknal.entity import Entity
    from seeknal.featurestore.feature_group import FeatureGroup

    # Set up project and workspace
    project = Project(name="my_project").get_or_create()
    workspace = Workspace(name="my_workspace").get_or_create()

    # Define entity (the primary key for your features)
    entity = Entity(
        name="user",
        join_keys=["user_id"],
        description="User entity for feature store"
    ).get_or_create()

    # Create and materialize feature groups with Spark
    # See quickstart_spark.py for the complete Spark example

For more information:
    - Documentation: https://github.com/mta-tech/seeknal
    - Examples: See the examples/ directory
""")


if __name__ == "__main__":
    main()
