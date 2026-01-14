"""Feature store module for managing and serving ML features.

This module provides the core feature store functionality for Seeknal,
enabling storage, management, and serving of machine learning features
for both offline (batch) and online (real-time) use cases.

Key Components:
    - FeatureGroup: Define and manage groups of features with customizable
      materialization options for offline and online storage.
    - FeatureLookup: Specify feature lookups from feature groups with
      optional feature selection and exclusion.
    - HistoricalFeatures: Retrieve historical feature data with point-in-time
      correctness for training ML models.
    - OnlineFeatures: Serve features in real-time for model inference with
      low-latency access patterns.
    - OfflineStore: Configure offline storage backends (Hive tables or Delta files).
    - OnlineStore: Configure online storage backends for real-time serving.

Storage Backends:
    - Hive Tables: Managed table storage with SQL access.
    - Delta Files: File-based storage with ACID transactions and time travel.

Typical Usage:
    ```python
    from seeknal.featurestore import FeatureGroup, FeatureLookup
    from seeknal.featurestore import HistoricalFeatures, OnlineFeatures

    # Define a feature group
    feature_group = FeatureGroup(
        name="user_features",
        entity=user_entity,
        materialization=Materialization(
            offline=True,
            online=True,
        ),
    )

    # Create and materialize features
    feature_group.set_flow(my_flow).set_features().get_or_create()
    feature_group.write()

    # Retrieve historical features for training
    historical = HistoricalFeatures(
        lookups=[FeatureLookup(source=feature_group)]
    )
    training_df = historical.using_spine(spine_df).to_dataframe()

    # Serve features online for inference
    online = OnlineFeatures(
        lookup_key=user_entity,
        lookups=[FeatureLookup(source=feature_group)],
    )
    features = online.get_features(keys=[{"user_id": "123"}])
    ```

See Also:
    seeknal.entity: Entity definitions for feature store join keys.
    seeknal.flow: Data flow definitions for feature transformations.
    seeknal.tasks: Task definitions for data processing pipelines.
"""

from importlib import import_module
from typing import TYPE_CHECKING

_EXPORTS = {
    # Feature Group classes
    "FeatureGroup": "seeknal.featurestore.feature_group",
    "FeatureLookup": "seeknal.featurestore.feature_group",
    "HistoricalFeatures": "seeknal.featurestore.feature_group",
    "OnlineFeatures": "seeknal.featurestore.feature_group",
    "Materialization": "seeknal.featurestore.feature_group",
    "GetLatestTimeStrategy": "seeknal.featurestore.feature_group",
    # Feature Group submodule (for tests/mocking)
    "feature_group": "seeknal.featurestore.feature_group",
    # Feature Store classes
    "FeatureStore": "seeknal.featurestore.featurestore",
    "Feature": "seeknal.featurestore.featurestore",
    "FillNull": "seeknal.featurestore.featurestore",
    "OfflineStore": "seeknal.featurestore.featurestore",
    "OnlineStore": "seeknal.featurestore.featurestore",
    "OfflineMaterialization": "seeknal.featurestore.featurestore",
    "OnlineMaterialization": "seeknal.featurestore.featurestore",
    # Enums
    "OfflineStoreEnum": "seeknal.featurestore.featurestore",
    "OnlineStoreEnum": "seeknal.featurestore.featurestore",
    "FileKindEnum": "seeknal.featurestore.featurestore",
    # Output configurations
    "FeatureStoreFileOutput": "seeknal.featurestore.featurestore",
    "FeatureStoreHiveTableOutput": "seeknal.featurestore.featurestore",
}

__all__ = list(_EXPORTS.keys())


def __getattr__(name: str):
    module_path = _EXPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module 'seeknal.featurestore' has no attribute '{name}'")
    module = import_module(module_path)
    return getattr(module, name)


def __dir__():
    return sorted(__all__)


if TYPE_CHECKING:
    from .feature_group import (
        FeatureGroup,
        FeatureLookup,
        HistoricalFeatures,
        OnlineFeatures,
        Materialization,
        GetLatestTimeStrategy,
    )
    from .featurestore import (
        FeatureStore,
        Feature,
        FillNull,
        OfflineStore,
        OnlineStore,
        OfflineMaterialization,
        OnlineMaterialization,
        OfflineStoreEnum,
        OnlineStoreEnum,
        FileKindEnum,
        FeatureStoreFileOutput,
        FeatureStoreHiveTableOutput,
    )
