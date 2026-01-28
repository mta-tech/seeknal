# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""
Transform: Clean and validate transaction data.
"""

from seeknal.pipeline import transform


@transform(
    name="clean_transactions",
    description="Remove invalid transactions and calculate totals",
)
def clean_transactions(ctx):
    """Clean transaction data and compute derived columns."""
    df = ctx.ref("source.raw_transactions")

    return ctx.duckdb.sql("""
        SELECT
            InvoiceNo,
            StockCode,
            Description,
            Quantity,
            InvoiceDate,
            UnitPrice,
            CustomerID,
            Country,
            -- Calculate total amount per line item
            (Quantity * UnitPrice) as TotalAmount,
            -- Extract date components
            CAST(InvoiceDate as DATE) as InvoiceDateOnly,
            -- Extract year and month for partitioning
            strftime('%Y', InvoiceDate) as Year,
            strftime('%m', InvoiceDate) as Month
        FROM df
        WHERE
            -- Remove cancelled orders (InvoiceNo starting with 'C')
            InvoiceNo NOT LIKE 'C%'
            -- Remove null or invalid quantities
            AND Quantity > 0
            -- Remove null or negative prices
            AND UnitPrice > 0
            -- Remove null customer IDs
            AND CustomerID IS NOT NULL
            -- Remove rows in future
            AND InvoiceDate <= CURRENT_DATE
    """).df()
