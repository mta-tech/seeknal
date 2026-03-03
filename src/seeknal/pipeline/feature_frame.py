"""FeatureFrame: DataFrame-like wrapper for feature group outputs with PIT join support.

FeatureFrame is a duck-typed DataFrame wrapper that carries entity metadata
(entity_name, join_keys, event_time_col) alongside the data. It delegates
standard DataFrame operations to the inner DataFrame while adding
feature-store-specific methods like pit_join() and as_of().

Usage in a pipeline::

    @transform(name="training_data")
    def training_data(ctx):
        labels = ctx.ref("source.churn_labels")
        # ctx.ref("feature_group.X") returns a FeatureFrame
        training = ctx.ref("feature_group.customer_features").pit_join(
            spine=labels,
            date_col="label_date",
            keep_cols=["customer_id", "churned"],
        )
        return training
"""

from __future__ import annotations

from typing import Any, List, Optional


class FeatureFrame:
    """DataFrame-like wrapper for feature group outputs with PIT join support.

    Delegates standard DataFrame operations to the inner DataFrame while
    carrying entity metadata for point-in-time joins.

    Attributes:
        entity_name: Name of the entity (e.g. "customer").
        join_keys: List of column names used as entity keys.
        event_time_col: Column name containing the feature timestamp.
    """

    def __init__(
        self,
        df: Any,
        entity_name: str,
        join_keys: List[str],
        event_time_col: str,
    ) -> None:
        self._df = df
        self.entity_name = entity_name
        self.join_keys = list(join_keys)
        self.event_time_col = event_time_col

    # ------------------------------------------------------------------
    # Feature store methods
    # ------------------------------------------------------------------

    def pit_join(
        self,
        spine: Any,
        date_col: str,
        keep_cols: Optional[List[str]] = None,
    ) -> Any:
        """Point-in-time join: for each spine row, get features as of date_col.

        For each row in *spine*, finds the most recent feature row where
        ``event_time_col <= spine.date_col`` and the entity keys match.

        Args:
            spine: DataFrame with entity keys and a date column.
            date_col: Column in *spine* containing the point-in-time date.
            keep_cols: Optional list of spine columns to keep in the result.
                       Entity keys and *date_col* are always kept.

        Returns:
            A plain pandas DataFrame with spine columns + feature columns.
        """
        import duckdb
        import pandas as pd

        conn = duckdb.connect()
        try:
            # Prepare spine
            spine_copy = spine.copy()
            if date_col in spine_copy.columns:
                spine_copy[date_col] = pd.to_datetime(spine_copy[date_col])
            conn.register("spine", spine_copy)

            # Prepare features
            features = self._df.copy()
            if self.event_time_col in features.columns:
                features[self.event_time_col] = pd.to_datetime(
                    features[self.event_time_col]
                )
            conn.register("features", features)

            # Build PIT join SQL
            join_conditions = " AND ".join(
                [f"spine.{k} = features.{k}" for k in self.join_keys]
            )
            # Exclude join keys + columns overlapping with spine
            # (prevents DuckDB auto-renaming e.g. application_date → application_date_1)
            spine_col_set = set(spine_copy.columns)
            feature_col_set = set(features.columns)
            exclude_set = set(self.join_keys) | (spine_col_set & feature_col_set)
            exclude_cols = ", ".join(sorted(exclude_set))
            partition_cols = ", ".join(
                [f"spine.{k}" for k in self.join_keys]
            )

            query = f"""
            SELECT
                spine.*,
                features.* EXCLUDE ({exclude_cols}),
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_cols}, spine.{date_col}
                    ORDER BY features.{self.event_time_col} DESC
                ) AS rn
            FROM spine
            LEFT JOIN features
                ON {join_conditions}
                AND features.{self.event_time_col} <= spine.{date_col}
            """

            result = conn.execute(
                f"SELECT * EXCLUDE (rn) FROM ({query}) WHERE rn = 1 OR rn IS NULL"
            ).df()

            # Filter columns if keep_cols specified
            if keep_cols:
                keep_set = set(keep_cols) | set(self.join_keys) | {date_col}
                # Feature-only columns always pass through
                feature_only = [
                    c for c in result.columns if c not in spine_col_set
                ]
                result = result[
                    [
                        c
                        for c in result.columns
                        if c in keep_set or c in feature_only
                    ]
                ]

            return result
        finally:
            conn.close()

    def as_of(self, timestamp: str) -> Any:
        """Filter features to latest row per entity key at or before *timestamp*.

        Args:
            timestamp: ISO date/datetime string (e.g. "2026-01-15").

        Returns:
            A plain pandas DataFrame with the latest feature row per entity key.
        """
        import duckdb

        conn = duckdb.connect()
        try:
            conn.register("features", self._df)
            partition_cols = ", ".join(self.join_keys)
            query = f"""
            SELECT * FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY {partition_cols}
                        ORDER BY {self.event_time_col} DESC
                    ) AS rn
                FROM features
                WHERE {self.event_time_col} <= CAST('{timestamp}' AS TIMESTAMP)
            ) WHERE rn = 1
            """
            result = conn.execute(
                f"SELECT * EXCLUDE (rn) FROM ({query})"
            ).df()
            return result
        finally:
            conn.close()

    def to_df(self) -> Any:
        """Return the underlying plain DataFrame."""
        return self._df

    # ------------------------------------------------------------------
    # DataFrame delegation
    # ------------------------------------------------------------------

    def __getitem__(self, key: Any) -> Any:
        return self._df[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        self._df[key] = value

    def __len__(self) -> int:
        return len(self._df)

    def __repr__(self) -> str:
        return (
            f"FeatureFrame(entity='{self.entity_name}', "
            f"keys={self.join_keys}, "
            f"event_time='{self.event_time_col}', "
            f"shape={self._df.shape})\n"
            f"{repr(self._df)}"
        )

    def __str__(self) -> str:
        return self.__repr__()

    def __iter__(self) -> Any:
        return iter(self._df)

    def __contains__(self, item: Any) -> bool:
        return item in self._df

    @property
    def shape(self) -> tuple:
        return self._df.shape

    @property
    def columns(self) -> Any:
        return self._df.columns

    @property
    def dtypes(self) -> Any:
        return self._df.dtypes

    @property
    def index(self) -> Any:
        return self._df.index

    @property
    def empty(self) -> bool:
        return self._df.empty

    @property
    def values(self) -> Any:
        return self._df.values

    def head(self, n: int = 5) -> Any:
        return self._df.head(n)

    def tail(self, n: int = 5) -> Any:
        return self._df.tail(n)

    def describe(self, *args: Any, **kwargs: Any) -> Any:
        return self._df.describe(*args, **kwargs)

    def info(self, *args: Any, **kwargs: Any) -> Any:
        return self._df.info(*args, **kwargs)

    def select_dtypes(self, *args: Any, **kwargs: Any) -> Any:
        return self._df.select_dtypes(*args, **kwargs)

    def fillna(self, *args: Any, **kwargs: Any) -> Any:
        return self._df.fillna(*args, **kwargs)

    def dropna(self, *args: Any, **kwargs: Any) -> Any:
        return self._df.dropna(*args, **kwargs)

    def merge(self, *args: Any, **kwargs: Any) -> Any:
        return self._df.merge(*args, **kwargs)

    def copy(self) -> "FeatureFrame":
        return FeatureFrame(
            df=self._df.copy(),
            entity_name=self.entity_name,
            join_keys=list(self.join_keys),
            event_time_col=self.event_time_col,
        )
