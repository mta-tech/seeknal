"""DuckDB-based Feature Store implementation.

This module provides a feature store implementation using DuckDB instead of Spark,
enabling in-process feature engineering and serving without distributed infrastructure.
"""

from .featurestore import (
    OfflineStoreDuckDB,
    OnlineStoreDuckDB,
    FeatureStoreFileOutput,
)
from .feature_group import (
    FeatureGroupDuckDB,
    HistoricalFeaturesDuckDB,
    OnlineFeaturesDuckDB,
    FeatureLookup,
    FillNull,
    GetLatestTimeStrategy,
    Materialization,
)

__all__ = [
    "OfflineStoreDuckDB",
    "OnlineStoreDuckDB",
    "FeatureStoreFileOutput",
    "FeatureGroupDuckDB",
    "HistoricalFeaturesDuckDB",
    "OnlineFeaturesDuckDB",
    "FeatureLookup",
    "FillNull",
    "GetLatestTimeStrategy",
    "Materialization",
]
