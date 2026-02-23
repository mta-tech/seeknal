# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
# ]
# ///

"""
Source: Online retail transaction data.
"""

from seeknal.pipeline import source
import pandas as pd


@source(
    name="raw_transactions",
    source="csv",
    table="data/online_retail_sample.csv",
    description="Raw online retail transaction data",
    tags=["retail", "transactions", "production"],
)
def raw_transactions(ctx=None):
    """Load raw transaction data from CSV."""
    df = pd.read_csv("data/online_retail_sample.csv")

    # Convert date column
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])

    # Ensure numeric types
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")

    return df
