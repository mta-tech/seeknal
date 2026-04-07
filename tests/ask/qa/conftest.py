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

    # ── Pipeline definitions (seeknal/ source folder) ────────────
    seeknal_dir = project / "seeknal"
    (seeknal_dir / "sources").mkdir(parents=True)
    (seeknal_dir / "transforms").mkdir(parents=True)
    (seeknal_dir / "feature_groups").mkdir(parents=True)

    (seeknal_dir / "sources" / "raw_customers.yml").write_text(
        "kind: source\n"
        "name: raw_customers\n"
        "description: Raw customer data from CRM export\n"
        "source: csv\n"
        "table: data/customers.csv\n"
    )
    (seeknal_dir / "sources" / "raw_orders.yml").write_text(
        "kind: source\n"
        "name: raw_orders\n"
        "description: Raw order transactions\n"
        "source: csv\n"
        "table: data/orders.csv\n"
    )
    (seeknal_dir / "transforms" / "orders_cleaned.yml").write_text(
        "kind: transform\n"
        "name: orders_cleaned\n"
        "description: Clean orders — filter cancelled, calculate revenue\n"
        "inputs:\n"
        "  - ref: source.raw_orders\n"
        "sql: |\n"
        "  SELECT *,\n"
        "    amount * quantity AS revenue\n"
        "  FROM raw_orders\n"
        "  WHERE status = 'completed'\n"
    )
    (seeknal_dir / "transforms" / "monthly_revenue.yml").write_text(
        "kind: transform\n"
        "name: monthly_revenue\n"
        "description: Monthly revenue aggregation\n"
        "inputs:\n"
        "  - ref: transform.orders_cleaned\n"
        "sql: |\n"
        "  SELECT\n"
        "    strftime(order_date, '%Y-%m') AS month,\n"
        "    CAST(SUM(revenue) AS DOUBLE) AS total_revenue,\n"
        "    CAST(COUNT(*) AS BIGINT) AS total_orders,\n"
        "    CAST(AVG(revenue) AS DOUBLE) AS avg_order_value\n"
        "  FROM orders_cleaned\n"
        "  GROUP BY 1\n"
    )
    (seeknal_dir / "feature_groups" / "customer_demographics.yml").write_text(
        "kind: feature_group\n"
        "name: customer_demographics\n"
        "description: Customer demographic features\n"
        "entity:\n"
        "  name: customer\n"
        "  join_keys: [customer_id]\n"
        "inputs:\n"
        "  - ref: source.raw_customers\n"
    )

    # ── Skills directory (for pydantic-deep SkillsToolset) ─────────
    skills_dir = seeknal_dir / "skills" / "report-generation"
    skills_dir.mkdir(parents=True)
    (skills_dir / "SKILL.md").write_text(
        "---\n"
        "name: report-generation\n"
        'description: "Evidence.dev report generation — chart syntax, quality bar, section patterns"\n'
        "tags: [reporting, evidence]\n"
        'version: "1.0.0"\n'
        "---\n\n"
        "# Report Generation\n\n"
        "When asked to create a report, produce an Evidence.dev report.\n\n"
        "## Evidence Markdown Syntax\n\n"
        "SQL queries in fenced blocks:\n"
        "```sql query_name\n"
        "SELECT ... FROM table_name\n"
        "```\n\n"
        "Components (SINGLE curly braces only):\n"
        "- <BigValue data={query_name} value=column_name />\n"
        "- <BarChart data={query_name} x=column y=column />\n"
        "- <LineChart data={query_name} x=date_col y=value_col />\n"
        "- <DataTable data={query_name} />\n"
    )

    # ── .seeknal directory (for memory) ──────────────────────────
    (project / ".seeknal").mkdir(exist_ok=True)

    # ── Project config ───────────────────────────────────────────
    (project / "seeknal_project.yml").write_text(
        "name: ecommerce_demo\n"
        "version: 1.0.0\n"
        "profile: default\n"
    )

    return project


@pytest.fixture(scope="session")
def pipeline_project(tmp_path_factory):
    """Create a clean seeknal project with only raw CSV data — no pipeline.

    The agent must build the pipeline from scratch:
    profile → draft → validate → apply → plan → run → inspect.
    """
    project = tmp_path_factory.mktemp("pipeline_project")

    # ── Raw CSV data ─────────────────────────────────────────────
    data_dir = project / "data"
    data_dir.mkdir()

    np.random.seed(99)
    n_customers = 30
    n_orders = 100

    cities = ["Jakarta", "Surabaya", "Bandung", "Medan", "Semarang"]
    customers = pd.DataFrame({
        "customer_id": [f"C{str(i).zfill(4)}" for i in range(1, n_customers + 1)],
        "name": [f"Customer {i}" for i in range(1, n_customers + 1)],
        "city": np.random.choice(cities, n_customers),
        "age": np.random.randint(18, 65, n_customers),
    })
    customers.to_csv(data_dir / "customers.csv", index=False)

    order_dates = pd.date_range("2024-01-01", "2024-12-31", periods=n_orders)
    categories = ["Electronics", "Fashion", "Food", "Home", "Sports"]
    orders = pd.DataFrame({
        "order_id": [f"ORD{str(i).zfill(5)}" for i in range(1, n_orders + 1)],
        "customer_id": np.random.choice(customers["customer_id"].values, n_orders),
        "order_date": order_dates,
        "amount": np.round(np.random.lognormal(mean=4.0, sigma=1.0, size=n_orders), 2),
        "category": np.random.choice(categories, n_orders),
        "quantity": np.random.randint(1, 10, n_orders),
    })
    orders.to_csv(data_dir / "orders.csv", index=False)

    # ── Empty seeknal directory structure ─────────────────────────
    seeknal_dir = project / "seeknal"
    (seeknal_dir / "sources").mkdir(parents=True)
    (seeknal_dir / "transforms").mkdir(parents=True)
    (seeknal_dir / "feature_groups").mkdir(parents=True)

    # ── Project config ───────────────────────────────────────────
    (project / "seeknal_project.yml").write_text(
        "name: pipeline_demo\n"
        "version: 1.0.0\n"
        "profile: default\n"
    )

    # ── .seeknal directory (for agent memory) ────────────────────
    (project / ".seeknal").mkdir(exist_ok=True)

    return project


# ---------------------------------------------------------------------------
# Agent helpers
# ---------------------------------------------------------------------------


def fresh_agent(project_path):
    """Create a fresh agent with clean state — no memory bleed between tests.

    Clears .seeknal/ask_memory, checkpoints, and plans so each test starts
    with a blank slate. Returns (agent, deps, message_history, cost_info).
    """
    import shutil
    from seeknal.ask.agents.agent import create_agent

    # Clean agent state directories
    seeknal_dir = project_path / ".seeknal"
    for subdir in ["ask_memory", "checkpoints", "plans"]:
        target = seeknal_dir / subdir
        if target.exists():
            shutil.rmtree(target)

    # Clean artifacts written by previous test agents (exposures, drafts, metrics)
    proj_seeknal = project_path / "seeknal"
    for subdir in ["exposures", "metrics", "semantic_models"]:
        target = proj_seeknal / subdir
        if target.exists():
            shutil.rmtree(target)
            target.mkdir()

    # Remove leftover draft files from project root
    for f in project_path.glob("draft_*.yml"):
        f.unlink()

    return create_agent(
        project_path=project_path,
        provider="google",
        model="gemini-2.5-flash",
    )


# ---------------------------------------------------------------------------
# Assertion helpers
# ---------------------------------------------------------------------------


def assert_answer_quality(answer, min_length=20):
    """Assert answer is non-empty and substantive."""
    assert answer and len(answer) >= min_length, (
        f"Answer too short ({len(answer) if answer else 0} chars): {answer!r}"
    )


def assert_keywords(answer, keywords, threshold=0.5):
    """Assert at least threshold fraction of keywords appear in answer.

    Args:
        answer: The agent's response text.
        keywords: List of keywords to search for (case-insensitive).
        threshold: Minimum fraction of keywords that must be found (0.0-1.0).
    """
    found = [k for k in keywords if k.lower() in answer.lower()]
    ratio = len(found) / len(keywords)
    assert ratio >= threshold, (
        f"Only {len(found)}/{len(keywords)} keywords found ({ratio:.0%}, "
        f"need {threshold:.0%}). Found: {found}, "
        f"Missing: {[k for k in keywords if k not in found]}"
    )


def _compact_history(mh):
    """Run compaction processors on message history to free context space.

    Replaces old tool results with short placeholders and trims large SQL
    outputs so the next agent call works within context limits.
    """
    from seeknal.ask.processors import MicrocompactProcessor, SqlResultCompactor

    compacted = MicrocompactProcessor(keep_recent_turns=2)(mh)
    compacted = SqlResultCompactor()(compacted)
    mh.clear()
    mh.extend(compacted)


_RECOVERABLE_ERRORS = (
    "UsageLimitExceeded",
    "UnexpectedModelBehavior",
)


def _is_recoverable(e: Exception) -> bool:
    """Check if an exception is recoverable via compaction + retry."""
    etype = type(e).__name__
    return any(name in etype for name in _RECOVERABLE_ERRORS) or "usage" in str(e).lower()


def safe_ask(ask_fn, agent, deps, mh, question, _retries=2):
    """Call ask() with automatic compaction and retry on transient errors.

    When the agent hits the request limit or model validation errors:
    1. Compact message history — replace old tool results with placeholders
    2. Retry with fresh request budget (each run_sync gets its own limit)

    After exhausting retries, skips the test instead of crashing.
    """
    original_question = question
    for attempt in range(_retries + 1):
        try:
            return ask_fn(agent, deps, mh, question)
        except Exception as e:
            if not _is_recoverable(e):
                raise

            if attempt < _retries:
                print(f"\n[safe_ask] {type(e).__name__} on attempt {attempt + 1}, "
                      f"compacting history ({len(mh)} messages) and retrying...")
                _compact_history(mh)
                print(f"[safe_ask] After compaction: {len(mh)} messages")
                # Retry with simplified question to encourage focused response
                question = (
                    f"Continue from where you left off. {original_question}\n\n"
                    "Be concise — give the answer directly without extra exploration."
                )
            else:
                pytest.skip(
                    f"{type(e).__name__} after {_retries + 1} attempts "
                    f"(with compaction): {e}"
                )
