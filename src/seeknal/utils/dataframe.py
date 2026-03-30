"""DataFrame utility functions for Seeknal."""

import pandas as pd


def coerce_string_dtype(df: pd.DataFrame) -> pd.DataFrame:
    """Convert Pandas 3.0 StringDtype columns to object for DuckDB compatibility.

    DuckDB's register() doesn't recognize Pandas 3.0's native StringDtype.
    Convert to object dtype so DuckDB can infer VARCHAR.
    """
    str_cols = df.select_dtypes(include=["string"]).columns
    if len(str_cols) > 0:
        df = df.copy()
        for col in str_cols:
            df[col] = df[col].astype("object")
    return df
