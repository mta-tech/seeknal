#!/usr/bin/env python3
"""
Seeknal Quickstart Guide - Spark Edition
=========================================

This quickstart demonstrates how to use Seeknal with Apache Spark for
production-scale feature engineering. Spark is ideal for:
- Processing large-scale datasets (GB to TB)
- Distributed computing across clusters
- Production feature pipelines
- Integration with data lakes and warehouses

Prerequisites:
- Python 3.11+
- Seeknal installed: pip install seeknal
- Apache Spark installed (spark-submit available)
- PySpark installed: pip install pyspark
- Java 8 or 11 runtime

Estimated Time: 15-20 minutes

What You'll Learn:
1. Setting up a SparkSession for Seeknal
2. Loading data with SparkEngineTask
3. Feature engineering with SQL transformations
4. Aggregating user behavior features at scale
5. Saving features to distributed storage
6. When to use Spark vs DuckDB

WHEN TO USE SPARK VS DUCKDB:
============================
| Scenario                          | Recommended Engine |
|-----------------------------------|-------------------|
| Prototyping & exploration         | DuckDB            |
| Development & testing (<1GB)      | DuckDB            |
| Production workloads (>1GB)       | Spark             |
| Distributed/cluster processing    | Spark             |
| Single machine, small data        | DuckDB            |
| Integration with Hive/data lakes  | Spark             |
"""

import os
import sys
from pathlib import Path
from typing import Optional

# Import PySpark - Required for Spark-based processing
try:
    from pyspark.sql import SparkSession, DataFrame
except ImportError:
    print("ERROR: PySpark is not installed.")
    print("Please install it with: pip install pyspark")
    print("\nNote: You also need Java 8 or 11 installed.")
    sys.exit(1)

# Import Seeknal Spark components
from seeknal.tasks.sparkengine import SparkEngineTask
from seeknal.tasks.sparkengine import transformers as T
from seeknal.tasks.sparkengine import aggregators as G


def print_section(title: str) -> None:
    """Helper function to print section headers."""
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}\n")


def print_step(step_num: int, description: str) -> None:
    """Helper function to print step headers."""
    print(f"\n--- Step {step_num}: {description} ---\n")


def create_spark_session(app_name: str = "Seeknal-Quickstart") -> SparkSession:
    """
    Create a SparkSession for local development.

    In production, you would typically:
    - Configure cluster settings
    - Set up Hive metastore connection
    - Configure S3/HDFS access
    - Tune executor memory and cores

    Args:
        app_name: Name of the Spark application

    Returns:
        SparkSession: Configured Spark session
    """
    spark = (SparkSession.builder
             .appName(app_name)
             .master("local[*]")  # Use all available cores locally
             .config("spark.sql.shuffle.partitions", "4")  # Reduce for local dev
             .config("spark.driver.memory", "2g")
             .config("spark.sql.legacy.createHiveTableByDefault", "false")
             .getOrCreate())

    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    return spark


def main():
    """
    Main function demonstrating the complete Spark workflow.

    This example uses e-commerce user behavior data to create
    features for a recommendation or churn prediction model,
    processing data at scale with Apache Spark.
    """

    print_section("Seeknal Quickstart - Spark Edition")
    print("Welcome! This quickstart will guide you through feature engineering")
    print("using Seeknal with Apache Spark for production-scale processing.")

    # =============================================================================
    # STEP 1: Create SparkSession
    # =============================================================================
    print_step(1, "Creating SparkSession")

    spark = create_spark_session()

    print(f"SparkSession created successfully!")
    print(f"Spark version: {spark.version}")
    print(f"Master: {spark.sparkContext.master}")
    print(f"App name: {spark.sparkContext.appName}")

    # =============================================================================
    # STEP 2: Load Sample Data
    # =============================================================================
    print_step(2, "Loading Sample Data into Spark")

    # Get the path to sample data (relative to this script)
    script_dir = Path(__file__).parent
    sample_data_path = script_dir / "sample_data.csv"

    # Verify the sample data exists
    if not sample_data_path.exists():
        print(f"ERROR: Sample data not found at {sample_data_path}")
        print("Please ensure sample_data.csv is in the same directory as this script.")
        spark.stop()
        sys.exit(1)

    # Load data using Spark's CSV reader
    # In production, you would typically read from Hive tables, S3, or HDFS
    raw_df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(str(sample_data_path)))

    print(f"Loaded {raw_df.count():,} rows of e-commerce user behavior data")
    print(f"\nSchema:")
    raw_df.printSchema()
    print(f"\nSample data (first 5 rows):")
    raw_df.show(5, truncate=False)

    # =============================================================================
    # STEP 3: Create SparkEngineTask for Data Processing
    # =============================================================================
    print_step(3, "Creating SparkEngineTask")

    # Create a SparkEngineTask and add the input data
    # SparkEngineTask is the Spark-based counterpart to DuckDBTask
    spark_task = SparkEngineTask(
        name="user_feature_engineering",
        description="Create user-level features from e-commerce behavior"
    )
    spark_task.add_input(dataframe=raw_df)

    print("SparkEngineTask created successfully!")
    print(f"Task name: {spark_task.name}")
    print(f"Task type: {spark_task.kind}")
    print(f"Uses Spark: {spark_task.is_spark_job}")  # Should be True

    # =============================================================================
    # STEP 4: Feature Engineering with SQL
    # =============================================================================
    print_step(4, "Engineering User Behavior Features")

    # Define SQL for user-level feature aggregation
    # This is similar to the DuckDB example but runs on Spark
    # Note: Spark SQL syntax is used (slightly different from DuckDB in some cases)

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

        -- Category Preferences (using Spark SQL functions)
        FIRST(product_category) as sample_category,
        COUNT(DISTINCT product_category) as categories_explored,

        -- Time Features
        MIN(event_time) as first_event_time,
        MAX(event_time) as last_event_time

    FROM __THIS__
    GROUP BY user_id
    ORDER BY total_revenue DESC
    """

    # Add the SQL transformation to the task
    spark_task.add_sql(user_features_sql)

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
    print_step(5, "Executing Feature Transformation")

    # Transform the data - this runs the SQL query on Spark
    result_df = spark_task.transform(spark)

    # Ensure we got a DataFrame back
    if not isinstance(result_df, DataFrame):
        print("ERROR: Expected a DataFrame result")
        spark.stop()
        sys.exit(1)

    # Cache the result for faster subsequent operations
    result_df.cache()

    print(f"Feature transformation complete!")
    print(f"Created features for {result_df.count():,} unique users")
    print(f"\nFeature columns ({len(result_df.columns)}):")
    for col in result_df.columns:
        print(f"  - {col}")

    # =============================================================================
    # STEP 6: Inspect the Results
    # =============================================================================
    print_step(6, "Inspecting Feature Results")

    print("Top 5 users by revenue:")
    result_df.select("user_id", "total_purchases", "total_revenue", "sample_category").show(5)

    print("\nFeature Statistics:")
    result_df.select(
        "total_events",
        "total_purchases",
        "total_revenue",
        "avg_session_duration",
        "categories_explored"
    ).describe().show()

    # =============================================================================
    # STEP 7: Save Features to Local Storage
    # =============================================================================
    print_step(7, "Saving Features to Local Storage")

    # Create output directory
    output_dir = script_dir / "output"
    output_dir.mkdir(exist_ok=True)

    # Save as Parquet (efficient columnar format, recommended for Spark)
    parquet_path = output_dir / "spark_user_features.parquet"
    result_df.coalesce(1).write.mode("overwrite").parquet(str(parquet_path))
    print(f"Saved features to: {parquet_path}")

    # Save as CSV for easy viewing (with single partition for readability)
    csv_path = output_dir / "spark_user_features_csv"
    result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(csv_path))
    print(f"Saved features to: {csv_path}")

    # =============================================================================
    # STEP 8: Using SparkEngineTask with Aggregators (Alternative Approach)
    # =============================================================================
    print_step(8, "Alternative: Using Built-in Aggregators")

    # SparkEngineTask also supports built-in aggregators for common patterns
    # This is useful when you want more structured feature definitions

    aggregator_task = SparkEngineTask(
        name="user_aggregates",
        description="Using SparkEngineTask aggregators"
    )
    aggregator_task.add_input(dataframe=raw_df)

    # Define aggregation using the Aggregator class
    user_aggregator = G.Aggregator(
        group_by_cols=["user_id"],
        aggregators=[
            G.FunctionAggregator(
                inputCol="purchase_amount",
                outputCol="sum_purchase_amount",
                accumulatorFunction="sum"
            ),
            G.FunctionAggregator(
                inputCol="session_duration",
                outputCol="avg_session_duration",
                accumulatorFunction="avg"
            ),
            G.ExpressionAggregator(
                outputCol="event_count",
                expression="count(*)"
            ),
        ]
    )

    aggregator_task.add_stage(aggregator=user_aggregator)
    aggregated_df = aggregator_task.transform(spark)

    print("Aggregator-based features created:")
    aggregated_df.show(5)

    # =============================================================================
    # STEP 9: User Segmentation Analysis
    # =============================================================================
    print_step(9, "Running User Segmentation Analysis")

    # Create a new task to analyze the features
    analysis_task = SparkEngineTask(name="conversion_analysis")
    analysis_task.add_input(dataframe=result_df)

    # Calculate conversion rate distribution using SQL
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
    GROUP BY
        CASE
            WHEN total_purchases = 0 THEN 'No Purchases'
            WHEN total_purchases = 1 THEN '1 Purchase'
            WHEN total_purchases <= 3 THEN '2-3 Purchases'
            ELSE '4+ Purchases'
        END
    ORDER BY user_count DESC
    """

    analysis_task.add_sql(conversion_sql)
    analysis_df = analysis_task.transform(spark)

    print("User Segmentation by Purchase Behavior:")
    analysis_df.show()

    # =============================================================================
    # STEP 10: Clean Up
    # =============================================================================
    print_step(10, "Cleaning Up")

    # Unpersist cached DataFrames
    result_df.unpersist()

    # Stop the SparkSession
    spark.stop()

    print("SparkSession stopped successfully.")

    # =============================================================================
    # SUMMARY
    # =============================================================================
    print_section("Quickstart Complete!")

    print("You have successfully:")
    print("  1. Created a SparkSession for local processing")
    print("  2. Loaded e-commerce behavior data into Spark")
    print("  3. Created a SparkEngineTask for feature engineering")
    print("  4. Engineered user-level features with SQL")
    print("  5. Used built-in aggregators for structured features")
    print("  6. Saved features to Parquet and CSV formats")
    print("  7. Performed user segmentation analysis")

    print("\nOutput files created:")
    print(f"  - {output_dir}/spark_user_features.parquet")
    print(f"  - {output_dir}/spark_user_features_csv")

    print("\n" + "="*60)
    print(" Next Steps - Production Feature Store")
    print("="*60)
    print("""
For production deployments with feature versioning and serving:

    from seeknal.project import Project
    from seeknal.workspace import Workspace
    from seeknal.entity import Entity
    from seeknal.flow import Flow, FlowInput, FlowOutput
    from seeknal.featurestore.feature_group import (
        FeatureGroup,
        Materialization,
        OfflineMaterialization,
        OfflineStore,
        OfflineStoreEnum,
        FeatureStoreFileOutput,
    )
    from datetime import datetime

    # 1. Set up project
    project = Project(name="ecommerce_features").get_or_create()

    # 2. Define the user entity (primary key for features)
    user_entity = Entity(
        name="user",
        join_keys=["user_id"],
        description="E-commerce user entity"
    ).get_or_create()

    # 3. Create a Flow with your SparkEngineTask
    flow = Flow(
        name="user_behavior_features",
        tasks=[your_spark_task],
        output=FlowOutput()
    ).get_or_create()

    # 4. Create a FeatureGroup with materialization settings
    feature_group = FeatureGroup(
        name="user_behavior_features",
        entity=user_entity,
        materialization=Materialization(
            event_time_col="event_time",
            offline_materialization=OfflineMaterialization(
                store=OfflineStore(
                    kind=OfflineStoreEnum.FILE,
                    name="production_store",
                    value=FeatureStoreFileOutput(
                        path="s3a://your-bucket/feature-store"
                    )
                ),
                mode="overwrite"
            ),
            offline=True
        )
    )
    feature_group.set_flow(flow)
    feature_group.set_features()  # Register all columns as features
    feature_group.get_or_create()

    # 5. Materialize features to the offline store
    feature_group.write(feature_start_time=datetime(2024, 1, 1))

PRODUCTION CONSIDERATIONS:
==========================
1. Cluster Configuration:
   - Use cluster mode: .master("yarn") or .master("k8s://...")
   - Configure executor memory and cores based on data size
   - Set appropriate shuffle partitions

2. Storage:
   - Use S3, HDFS, or Delta Lake for production storage
   - Partition features by date for efficient retrieval
   - Consider using Delta Lake for ACID transactions

3. Scheduling:
   - Use Airflow or similar for scheduled feature updates
   - Implement incremental processing for large datasets
   - Monitor feature freshness and data quality

4. Serving:
   - Configure online store for low-latency serving
   - Set up feature lookup APIs for model inference
   - Implement feature monitoring and drift detection

For more information:
    - Documentation: https://github.com/mta-tech/seeknal
    - Examples: See the examples/ directory
    - DuckDB Quickstart: quickstart_duckdb.py (for development)
""")


if __name__ == "__main__":
    main()
