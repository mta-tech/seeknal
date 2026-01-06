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

__all__ = [
    # Feature Group classes
    "FeatureGroup",
    "FeatureLookup",
    "HistoricalFeatures",
    "OnlineFeatures",
    "Materialization",
    "GetLatestTimeStrategy",
    # Feature Store classes
    "FeatureStore",
    "Feature",
    "FillNull",
    "OfflineStore",
    "OnlineStore",
    "OfflineMaterialization",
    "OnlineMaterialization",
    # Enums
    "OfflineStoreEnum",
    "OnlineStoreEnum",
    "FileKindEnum",
    # Output configurations
    "FeatureStoreFileOutput",
    "FeatureStoreHiveTableOutput",
]
