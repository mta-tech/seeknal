# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
#     "scikit-learn",
#     "numpy",
# ]
# ///

"""
Transform: ML-based sales forecast using scikit-learn.

This demonstrates Python nodes with ML models.
Uses RandomForestRegressor to predict future margins based on
historical performance metrics.
"""

from seeknal.pipeline import transform
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor


@transform(
    name="sales_forecast",
    description="Forecast next period sales using ML regression",
)
def sales_forecast(ctx):
    """Generate ML-based sales forecast."""
    # Reference YAML transform output
    aggregated = ctx.ref("transform.regional_totals")

    # Convert to DataFrame
    if not isinstance(aggregated, pd.DataFrame):
        aggregated = aggregated.df()

    # Prepare features for ML
    df = aggregated.copy()

    # Feature engineering
    df['margin_per_unit'] = df.apply(
        lambda row: row['total_margin'] / row['total_quantity'] if row['total_quantity'] > 0 else 0,
        axis=1
    )
    df['avg_transaction_size'] = df.apply(
        lambda row: row['total_quantity'] / row['transaction_count'] if row['transaction_count'] > 0 else 0,
        axis=1
    )

    # Encode categorical variables
    df['region_encoded'] = df['region'].astype('category').cat.codes
    df['category_encoded'] = df['product_category'].astype('category').cat.codes

    # Prepare feature matrix
    feature_cols = ['total_quantity', 'transaction_count', 'margin_per_unit',
                    'avg_transaction_size', 'region_encoded', 'category_encoded']

    X = df[feature_cols].values
    y = df['total_margin'].values

    # Train model
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X, y)

    # Make predictions with growth factor (simulating future period)
    # Increase quantities by 10% to simulate next period demand
    X_forecast = X.copy()
    # Use numpy slicing for column assignment
    X_forecast[:, 0:1] = X[:, 0:1] * 1.10  # Increase total_quantity by 10%
    X_forecast[:, 1:2] = X[:, 1:2] * 1.08  # Increase transaction_count by 8%

    forecast_margin = model.predict(X_forecast)

    # Add predictions to dataframe
    df['forecast_margin'] = forecast_margin
    df['projected_growth'] = df['forecast_margin'] - df['total_margin']
    df['growth_percentage'] = (df['projected_growth'] / df['total_margin'] * 100).round(2)

    # Determine trend based on ML prediction
    df['trend'] = pd.cut(
        df['growth_percentage'],
        bins=[-np.inf, -2, 2, np.inf],
        labels=['DOWN', 'STABLE', 'UP']
    )

    # Calculate confidence score based on model's R² score
    df['forecast_confidence'] = model.score(X, y)

    # Select output columns
    result = df[[
        'region',
        'product_category',
        'total_margin',
        'forecast_margin',
        'projected_growth',
        'growth_percentage',
        'trend',
        'forecast_confidence'
    ]].rename(columns={'total_margin': 'current_margin'})

    result = result.sort_values('forecast_margin', ascending=False)

    return result
