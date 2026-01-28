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
