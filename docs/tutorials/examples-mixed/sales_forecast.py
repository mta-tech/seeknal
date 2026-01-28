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
Transform: Sales forecast using moving average.

This Python node references a YAML transform output.
"""

from seeknal.pipeline import transform


@transform(
    name="sales_forecast",
    description="Forecast next month sales by region",
)
def sales_forecast(ctx):
    """Generate sales forecast using moving average."""
    # Reference YAML transform output
    aggregated = ctx.ref("transform.regional_totals")

    return ctx.duckdb.sql("""
        WITH time_series AS (
            SELECT
                region,
                product_category,
                SUM(total_margin) as monthly_margin,
                ROW_NUMBER() OVER (PARTITION BY region ORDER BY region) as rn
            FROM aggregated
        ),
        moving_avg AS (
            SELECT
                region,
                product_category,
                monthly_margin,
                AVG(monthly_margin) OVER (
                    PARTITION BY region
                    ORDER BY region
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as forecast_margin
            FROM time_series
            WHERE rn <= 12  -- Use first 12 periods for training
        )
        SELECT
            region,
            product_category,
            forecast_margin,
            monthly_margin,
            CASE
                WHEN forecast_margin > monthly_margin * 1.1 THEN 'UP'
                WHEN forecast_margin < monthly_margin * 0.9 THEN 'DOWN'
                ELSE 'STABLE'
            END as trend
        FROM moving_avg
        ORDER BY forecast_margin DESC
    """).df()
