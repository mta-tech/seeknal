"""Conftest for ask QA tests — builds a realistic seeknal project with data."""

import json
import os
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta


@pytest.fixture(autouse=True)
def clean_spark_state_between_tests():
    """No-op override: QA tests don't use Spark."""
    yield


@pytest.fixture(scope="session")
def qa_project(tmp_path_factory):
    """Create a realistic seeknal e-commerce project with data artifacts.

    Simulates a data pipeline that has already been run:
    - Source: raw orders + raw customers CSVs ingested as parquets
    - Transform: cleaned/enriched intermediate tables
    - Feature Store: customer entity with feature groups
    - DAG manifest describing the pipeline
    """
    project = tmp_path_factory.mktemp("ecommerce_project")
    target = project / "target"
    target.mkdir()

    # ── Seed data generation ──────────────────────────────────────
    np.random.seed(42)
    n_customers = 50
    n_orders = 200

    # Customers
    cities = ["Jakarta", "Surabaya", "Bandung", "Medan", "Semarang"]
    segments = ["Premium", "Standard", "Basic"]
    customers = pd.DataFrame({
        "customer_id": [f"C{str(i).zfill(4)}" for i in range(1, n_customers + 1)],
        "name": [f"Customer {i}" for i in range(1, n_customers + 1)],
        "city": np.random.choice(cities, n_customers),
        "segment": np.random.choice(segments, n_customers, p=[0.2, 0.5, 0.3]),
        "join_date": pd.date_range("2023-01-01", periods=n_customers, freq="5D"),
        "age": np.random.randint(18, 65, n_customers),
    })

    # Orders
    order_dates = pd.date_range("2024-01-01", "2024-12-31", periods=n_orders)
    categories = ["Electronics", "Fashion", "Food", "Home", "Sports"]
    statuses = ["completed", "completed", "completed", "completed", "cancelled", "returned"]
    orders = pd.DataFrame({
        "order_id": [f"ORD{str(i).zfill(5)}" for i in range(1, n_orders + 1)],
        "customer_id": np.random.choice(customers["customer_id"].values, n_orders),
        "order_date": order_dates,
        "amount": np.round(np.random.lognormal(mean=4.0, sigma=1.0, size=n_orders), 2),
        "category": np.random.choice(categories, n_orders),
        "status": np.random.choice(statuses, n_orders),
        "quantity": np.random.randint(1, 10, n_orders),
    })

    # Products (used in intermediate)
    products = pd.DataFrame({
        "product_id": [f"P{str(i).zfill(3)}" for i in range(1, 31)],
        "product_name": [f"Product {i}" for i in range(1, 31)],
        "category": np.random.choice(categories, 30),
        "unit_price": np.round(np.random.uniform(10, 500, 30), 2),
        "stock_quantity": np.random.randint(0, 1000, 30),
    })

    # ── Write intermediate parquets (simulating pipeline outputs) ──
    intermediate = target / "intermediate"
    intermediate.mkdir()

    customers.to_parquet(intermediate / "source_raw_customers.parquet", index=False)
    orders.to_parquet(intermediate / "source_raw_orders.parquet", index=False)
    products.to_parquet(intermediate / "source_raw_products.parquet", index=False)

    # Cleaned orders (transform output)
    completed_orders = orders[orders["status"] == "completed"].copy()
    completed_orders["revenue"] = completed_orders["amount"] * completed_orders["quantity"]
    completed_orders.to_parquet(
        intermediate / "transform_orders_cleaned.parquet", index=False
    )

    # Monthly revenue summary (transform output)
    completed_orders["month"] = completed_orders["order_date"].dt.to_period("M").astype(str)
    monthly = completed_orders.groupby("month").agg(
        total_revenue=("revenue", "sum"),
        total_orders=("order_id", "count"),
        avg_order_value=("revenue", "mean"),
    ).reset_index()
    monthly.to_parquet(
        intermediate / "transform_monthly_revenue.parquet", index=False
    )

    # Customer purchase stats (feature engineering output)
    cust_stats = completed_orders.groupby("customer_id").agg(
        total_orders=("order_id", "count"),
        total_spent=("revenue", "sum"),
        avg_order_value=("revenue", "mean"),
        first_purchase=("order_date", "min"),
        last_purchase=("order_date", "max"),
        favorite_category=("category", lambda x: x.mode().iloc[0] if len(x) > 0 else "Unknown"),
    ).reset_index()
    cust_stats["days_since_last_purchase"] = (
        pd.Timestamp("2025-01-01") - cust_stats["last_purchase"]
    ).dt.days
    cust_stats.to_parquet(
        intermediate / "transform_customer_purchase_stats.parquet", index=False
    )

    # Category performance (transform output)
    cat_perf = completed_orders.groupby("category").agg(
        total_revenue=("revenue", "sum"),
        total_orders=("order_id", "count"),
        avg_order_value=("revenue", "mean"),
        unique_customers=("customer_id", "nunique"),
    ).reset_index()
    cat_perf.to_parquet(
        intermediate / "transform_category_performance.parquet", index=False
    )

    # ── Feature Store: Entity catalog + consolidated parquet ──────
    fs = target / "feature_store" / "customer"
    fs.mkdir(parents=True)

    # Merge customers + stats for entity table
    entity_df = customers.merge(cust_stats, on="customer_id", how="left")
    entity_df.to_parquet(fs / "features.parquet", index=False)

    catalog = {
        "entity_name": "customer",
        "join_keys": ["customer_id"],
        "feature_groups": {
            "demographics": {
                "features": {
                    "name": "VARCHAR",
                    "city": "VARCHAR",
                    "segment": "VARCHAR",
                    "age": "INTEGER",
                    "join_date": "TIMESTAMP",
                }
            },
            "purchase_behavior": {
                "features": {
                    "total_orders": "BIGINT",
                    "total_spent": "DOUBLE",
                    "avg_order_value": "DOUBLE",
                    "days_since_last_purchase": "INTEGER",
                    "favorite_category": "VARCHAR",
                }
            },
        },
    }
    (fs / "_entity_catalog.json").write_text(json.dumps(catalog, indent=2))

    # ── DAG manifest ──────────────────────────────────────────────
    manifest = {
        "nodes": [
            {"name": "raw_customers", "type": "source"},
            {"name": "raw_orders", "type": "source"},
            {"name": "raw_products", "type": "source"},
            {"name": "orders_cleaned", "type": "transform"},
            {"name": "monthly_revenue", "type": "transform"},
            {"name": "customer_purchase_stats", "type": "transform"},
            {"name": "category_performance", "type": "transform"},
            {"name": "customer_demographics", "type": "feature_group"},
            {"name": "customer_purchase_behavior", "type": "feature_group"},
        ],
        "edges": [
            {"from": "raw_orders", "to": "orders_cleaned"},
            {"from": "orders_cleaned", "to": "monthly_revenue"},
            {"from": "orders_cleaned", "to": "customer_purchase_stats"},
            {"from": "orders_cleaned", "to": "category_performance"},
            {"from": "raw_customers", "to": "customer_demographics"},
            {"from": "customer_purchase_stats", "to": "customer_purchase_behavior"},
        ],
    }
    (target / "manifest.json").write_text(json.dumps(manifest, indent=2))

    # ── Run state (for REPL banner) ───────────────────────────────
    run_state = {
        "nodes": {n["name"]: {"status": "success"} for n in manifest["nodes"]},
        "last_run": "2025-01-15T10:30:00",
    }
    (target / "run_state.json").write_text(json.dumps(run_state, indent=2))

    return project
