# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "scikit-learn",
#     "mlflow",
# ]
# ///

"""Transform: Train a propensity model and log to MLflow."""

from pathlib import Path

import pandas as pd
from seeknal.pipeline import transform


@transform(
    name="train_propensity",
    tags=["churn_model"],
    description="Train churn propensity model, log experiment to MLflow",
)
def train_propensity(ctx):
    """Train a Random Forest classifier and log to MLflow."""
    import mlflow
    import mlflow.sklearn
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import cross_val_score, train_test_split
    from sklearn.metrics import roc_auc_score

    # ── 1. Load training dataset from Chapter 4 ────────────────────────
    df = ctx.ref("transform.training_dataset")

    feature_cols = [
        "total_spend",
        "avg_daily_spend",
        "max_daily_spend",
        "total_orders",
        "category_count",
        "electronics_ratio",
    ]
    target_col = "churned"

    X = df[feature_cols].fillna(0)
    y = df[target_col]

    # ── 2. Train / evaluate ─────────────────────────────────────────────
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42
    )

    model = RandomForestClassifier(
        n_estimators=100, max_depth=3, random_state=42
    )
    model.fit(X_train, y_train)

    # Metrics
    accuracy = model.score(X_test, y_test)
    cv_scores = cross_val_score(
        model, X, y, cv=min(3, len(y)), scoring="accuracy"
    )
    y_proba = model.predict_proba(X_test)[:, 1]
    try:
        auc = roc_auc_score(y_test, y_proba)
    except ValueError:
        auc = 0.0  # not enough classes in small test set

    print(f"  Accuracy:     {accuracy:.3f}")
    print(f"  CV Accuracy:  {cv_scores.mean():.3f} (+/- {cv_scores.std():.3f})")
    print(f"  AUC-ROC:      {auc:.3f}")

    # ── 3. Log to MLflow ────────────────────────────────────────────────
    tracking_uri = f"file:{Path.cwd() / 'mlruns'}"
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("churn_propensity")

    with mlflow.start_run(run_name="propensity_v1") as run:
        mlflow.log_params({
            "model": "RandomForestClassifier",
            "n_estimators": 100,
            "max_depth": 3,
            "features": ", ".join(feature_cols),
            "train_size": len(X_train),
            "test_size": len(X_test),
        })
        mlflow.log_metrics({
            "accuracy": accuracy,
            "cv_accuracy_mean": cv_scores.mean(),
            "auc_roc": auc,
        })
        mlflow.sklearn.log_model(model, "propensity_model")

        run_id = run.info.run_id
        print(f"  MLflow run:   {run_id}")

    # ── 4. Persist run_id for downstream prediction pipeline ────────────
    run_id_path = Path.cwd() / "target" / "mlflow_run_id.txt"
    run_id_path.parent.mkdir(parents=True, exist_ok=True)
    run_id_path.write_text(run_id)
    print(f"  Run ID saved: target/mlflow_run_id.txt")

    # ── 5. Return metrics as DataFrame ──────────────────────────────────
    importance = dict(zip(feature_cols, model.feature_importances_))
    return pd.DataFrame([{
        "run_id": run_id,
        "accuracy": round(accuracy, 4),
        "cv_accuracy": round(cv_scores.mean(), 4),
        "auc_roc": round(auc, 4),
        **{f"imp_{k}": round(v, 4) for k, v in importance.items()},
    }])
