# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
#     "scikit-learn",
#     "mlflow",
# ]
# ///

"""Transform: Batch-score customers using trained propensity model."""

from pathlib import Path

from seeknal.pipeline import transform


@transform(
    name="score_customers",
    tags=["churn_predict"],
    description="Load trained model from MLflow, score all customers",
)
def score_customers(ctx):
    """Load model from MLflow and produce propensity scores."""
    import mlflow
    import mlflow.sklearn

    # ── 1. Load the trained model ───────────────────────────────────────
    run_id_path = Path.cwd() / "target" / "mlflow_run_id.txt"
    if not run_id_path.exists():
        raise FileNotFoundError(
            "No trained model found. Run the training pipeline first:\n"
            "  seeknal run --nodes transform.train_propensity"
        )
    run_id = run_id_path.read_text().strip()

    tracking_uri = f"file:{Path.cwd() / 'mlruns'}"
    mlflow.set_tracking_uri(tracking_uri)
    model = mlflow.sklearn.load_model(f"runs:/{run_id}/propensity_model")
    print(f"  Loaded model from MLflow run: {run_id}")

    # ── 2. Assemble feature vector for all customers ────────────────────
    soa = ctx.ref("second_order_aggregation.customer_training_features")
    prefs = ctx.ref("feature_group.product_preferences")

    features_df = ctx.duckdb.sql("""
        SELECT
            s.customer_id,
            s.daily_amount_SUM  AS total_spend,
            s.daily_amount_MEAN AS avg_daily_spend,
            s.daily_amount_MAX  AS max_daily_spend,
            s.daily_count_SUM   AS total_orders,
            p.category_count,
            p.electronics_ratio
        FROM soa s
        LEFT JOIN prefs p ON s.customer_id = p.customer_id
    """).df()

    feature_cols = [
        "total_spend",
        "avg_daily_spend",
        "max_daily_spend",
        "total_orders",
        "category_count",
        "electronics_ratio",
    ]

    # ── 3. Score ────────────────────────────────────────────────────────
    X = features_df[feature_cols].fillna(0)
    features_df["propensity_score"] = model.predict_proba(X)[:, 1]

    # ── 4. Rank and segment ─────────────────────────────────────────────
    features_df["propensity_rank"] = (
        features_df["propensity_score"]
        .rank(ascending=False, method="min")
        .astype(int)
    )
    features_df["segment"] = features_df["propensity_score"].apply(
        lambda s: "high" if s > 0.7 else ("medium" if s > 0.3 else "low")
    )

    print(f"  Scored {len(features_df)} customers")
    print(f"  Segments: {features_df['segment'].value_counts().to_dict()}")

    return features_df[
        ["customer_id", "propensity_score", "propensity_rank", "segment"]
        + feature_cols
    ]
