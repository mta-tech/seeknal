# Docstring Coverage Audit - FeatureStore Module

**Audit Date:** 2026-01-04
**Auditor:** Auto-Claude
**Scope:** `src/seeknal/featurestore/` module

## Executive Summary

This audit analyzes docstring coverage across the Seeknal FeatureStore module to identify areas requiring enhancement for API documentation generation. The FeatureStore module is a critical component for feature engineering, storage, and serving workflows.

- **Complete**: Module-level + class-level + method-level docstrings following Google style
- **Partial**: Some docstrings present but incomplete coverage
- **Missing**: No or minimal docstrings

## Coverage Summary

| Category | Count | Files/Classes |
|----------|-------|---------------|
| Complete | 5 | FeatureStoreFileOutput, OfflineStore, OnlineStore, FillNull, Feature |
| Partial | 8 | Materialization, FeatureGroup, FeatureLookup, HistoricalFeatures, OfflineMaterialization, OnlineMaterialization, FeatureStore (ABC) |
| Missing | 5 | __init__.py, FeatureStoreHiveTableOutput, OnlineFeatures, GetLatestTimeStrategy, decorators |

---

## File Analysis

### 1. `__init__.py` (Empty)

**Status:** N/A
**Module Docstring:** No (file is empty/single line)
**Priority:** LOW

**Current State:**
- File appears to be empty or minimal, re-exporting from `feature_group.py`

**Required Enhancement:**
```python
"""FeatureStore module for Seeknal.

This module provides tools for defining, storing, and serving ML features
through offline and online feature stores.

Key Components:
    - FeatureGroup: Define and manage feature groups
    - FeatureStore: Abstract base for feature store implementations
    - HistoricalFeatures: Retrieve historical features with point-in-time joins
    - OnlineFeatures: Serve features for real-time inference
    - FeatureLookup: Configure feature retrieval operations

Example:
    ```python
    from seeknal.featurestore import FeatureGroup, Materialization

    fg = FeatureGroup(
        name="user_features",
        materialization=Materialization(offline=True, online=True)
    )
    ```
"""
```

---

### 2. `featurestore.py`

#### Enums

##### `OfflineStoreEnum`

**Status:** Missing
**Docstring:** No

**Required Enhancement:**
```python
class OfflineStoreEnum(str, Enum):
    """Enumeration of offline store types.

    Attributes:
        HIVE_TABLE: Store features in a Hive table.
        FILE: Store features as Delta files.
    """
    HIVE_TABLE = "hive_table"
    FILE = "file"
```

##### `OnlineStoreEnum`

**Status:** Missing
**Docstring:** No

**Required Enhancement:**
```python
class OnlineStoreEnum(str, Enum):
    """Enumeration of online store types.

    Attributes:
        HIVE_TABLE: Store features in a Hive table.
        FILE: Store features as Parquet files.
    """
    HIVE_TABLE = "hive_table"
    FILE = "file"
```

##### `FileKindEnum`

**Status:** Missing
**Docstring:** No

**Required Enhancement:**
```python
class FileKindEnum(str, Enum):
    """Enumeration of file format types.

    Attributes:
        DELTA: Delta Lake format for ACID transactions.
    """
    DELTA = "delta"
```

---

#### Classes with Complete Docstrings

##### `FeatureStoreFileOutput`

**Status:** Complete
**Docstring:** Yes - Google style with Attributes

**Current Docstring:**
```python
"""
Configuration for file-based feature store output.

Attributes:
    path: The filesystem path for storing feature data. A security warning
        will be logged if this path is in an insecure location (e.g., /tmp).
    kind: The file format to use (default: DELTA).
"""
```

**Enhancement Needed:**
- Add Example section
- Add `__post_init__` method docstring (exists but minimal)

---

##### `FeatureStoreHiveTableOutput`

**Status:** Missing
**Docstring:** No

**Required Enhancement:**
```python
@dataclass
class FeatureStoreHiveTableOutput:
    """Configuration for Hive table-based feature store output.

    Attributes:
        database: The Hive database name for storing feature data.

    Example:
        >>> output = FeatureStoreHiveTableOutput(database="my_feature_db")
    """
```

---

##### `OfflineStore`

**Status:** Complete
**Docstring:** Yes - Google style with Attributes

**Current Docstring:**
```python
"""
Configuration for offline feature store storage.

Attributes:
    value: The storage configuration (path for FILE, database for HIVE_TABLE).
        A security warning will be logged if a file path is in an insecure
        location (e.g., /tmp).
    kind: The storage type (FILE or HIVE_TABLE).
    name: Optional name for this offline store configuration.
"""
```

**Methods Missing Docstrings:**
- `get_or_create()` - No docstring
- `list()` - No docstring
- `__call__()` - No docstring (complex method with multiple behaviors)

**Required Enhancement for Methods:**
```python
def get_or_create(self):
    """Retrieve an existing offline store or create a new one.

    Returns:
        OfflineStore: The retrieved or newly created offline store instance.
    """

@staticmethod
def list():
    """List all configured offline stores.

    Displays a formatted table of all offline stores including their
    names, kinds, and configuration values.
    """

def __call__(
    self,
    result: Optional[DataFrame] = None,
    spark: Optional[SparkSession] = None,
    write: bool = True,
    *args: Any,
    **kwds: Any,
) -> Any:
    """Execute offline store read or write operations.

    Args:
        result: DataFrame to write (required for write operations).
        spark: SparkSession instance. Created if not provided.
        write: If True, write data; if False, read data.
        **kwds: Additional keyword arguments:
            - project: Project ID (required)
            - entity: Entity ID (required)
            - name: Feature name (required for write)
            - mode: Write mode ('overwrite', 'append', 'merge')
            - start_date: Start date for filtering
            - end_date: End date for filtering
            - ttl: Time-to-live in days
            - version: Feature version

    Returns:
        DataFrame when reading (write=False), None when writing.

    Raises:
        ValueError: If required parameters are missing or mode is unsupported.
    """
```

---

##### `OnlineStore`

**Status:** Complete
**Docstring:** Yes - Google style with Attributes

**Current Docstring:**
```python
"""Configuration for online feature store.

Attributes:
    value: The storage configuration (file path or hive table).
        A security warning will be logged if a file path is in an insecure
        location (e.g., /tmp).
    kind: Type of online store (FILE or HIVE_TABLE).
    name: Optional name for the store.
"""
```

**Methods Missing Docstrings:**
- `__call__()` - No docstring
- `delete()` - No docstring

---

##### `FillNull`

**Status:** Complete
**Docstring:** Yes - Google style with Attributes

**Current Docstring:**
```python
"""Configuration for filling null values in columns.

Attributes:
    value: Value to use for filling nulls.
    dataType: Data type for the value (e.g., 'double', 'string').
    columns: Optional list of columns to fill. If None, applies to all columns.
"""
```

**Enhancement Needed:** None (could add Example section)

---

##### `OfflineMaterialization`

**Status:** Complete
**Docstring:** Yes - Google style with Attributes

**Current Docstring:**
```python
"""Configuration for offline materialization.

Attributes:
    store: The offline store configuration.
    mode: Write mode ('overwrite', 'append', 'merge').
    ttl: Time-to-live in days for data retention.
"""
```

**Enhancement Needed:** Add Example section

---

##### `OnlineMaterialization`

**Status:** Complete
**Docstring:** Yes - Google style with Attributes

**Current Docstring:**
```python
"""Configuration for online materialization.

Attributes:
    store: The online store configuration.
    ttl: Time-to-live in minutes for online store data.
"""
```

**Enhancement Needed:** Add Example section

---

##### `Feature`

**Status:** Complete
**Docstring:** Yes - Google style with Attributes

**Current Docstring:**
```python
"""Define a Feature.

Attributes:
    name: Feature name.
    feature_id: Feature ID (assigned by seeknal).
    description: Feature description.
    data_type: Data type for the feature.
    online_data_type: Data type when stored in online-store.
    created_at: Creation timestamp.
    updated_at: Last update timestamp.
"""
```

**Methods Missing Docstrings:**
- `to_dict()` - No docstring

---

##### `FeatureStore` (ABC)

**Status:** Partial
**Docstring:** Yes - Google style with Attributes

**Current Docstring:**
```python
"""Abstract base class for feature stores.

Attributes:
    name: Feature store name.
    entity: Associated entity.
    id: Feature store ID.
"""
```

**Methods Missing Docstrings:**
- `get_or_create()` - Abstract method, no docstring

---

### 3. `feature_group.py`

#### Classes

##### `Materialization`

**Status:** Complete
**Docstring:** Yes - Google style with detailed Attributes

**Current Docstring:**
```python
"""
Materialization options

Attributes:
    event_time_col (str, optional): Specify which column that contains event time. Default to None.
    date_pattern (str, optional): Date pattern that use in event_time_col. Default to "yyyy-MM-dd".
    offline (bool, optional): Set the feature group should be stored in offline-store. Default to True.
    online (bool, optional): Set the feature group should be stored in online-store. Default to False.
    serving_ttl_days (int, optional): Look back window for features defined at the online-store.
        This parameters determines how long features will live in the online store. The unit is in days.
        Shorter TTLs improve performance and reduce computation. Default to 1.
        For example, if we set TTLs as 1 then only one day data available in online-store
    force_update_online (bool, optional): force to update the data in online-store. This will not consider
        to check whether the data going materialized newer than the data that already stored in online-store.
        Default to False.
    online_write_mode (OnlineWriteModeEnum, optional): Write mode when materialize to online-store.
        Default to "Append"
    schema_version (List[dict], optional): Determine which schema version for the feature group.
        Default to None.
"""
```

**Enhancement Needed:** Add Example section

---

##### `FeatureGroup`

**Status:** Partial
**Docstring:** Yes - Google style with Args

**Current Docstring:**
```python
"""
Define Feature Group. Feature Group is a set of features created from a flow.

Args:
    name (str): Feature group name
    entities (List[Entity]): List of entities associated
    materialization (Materialization): Materialization setting for this feature group
    flow (Flow, optional): Flow that used for create features for this feature group
    description (str, optional): Description for this feature group
    features (List[Feature], optional): List of feature to be registered as features in this feature group.
        If set to None, then all columns except join_keys and event_time will
        register as features.
"""
```

**Methods with Docstrings:**
- `set_flow()` - Has docstring
- `_parse_avro_schema()` - Has minimal docstring
- `set_features()` - Has detailed docstring with Args, Raises, Returns
- `get_or_create()` - Has docstring with Args, Returns
- `delete()` - Has docstring with Returns
- `write()` - Has detailed docstring with Args, Returns

**Methods Missing Docstrings:**
- `__post_init__()` - No docstring
- `set_dataframe()` - No docstring
- `update_materialization()` - No docstring

**Required Enhancement:**
```python
def set_dataframe(self, dataframe: DataFrame):
    """Set a DataFrame as the source for this feature group.

    Args:
        dataframe: PySpark DataFrame to use as the feature source.

    Returns:
        FeatureGroup: Self for method chaining.
    """

def update_materialization(
    self,
    offline: Optional[bool] = None,
    online: Optional[bool] = None,
    offline_materialization: Optional[OfflineMaterialization] = None,
    online_materialization: Optional[OnlineMaterialization] = None,
):
    """Update the materialization settings for this feature group.

    Args:
        offline: Enable/disable offline store materialization.
        online: Enable/disable online store materialization.
        offline_materialization: New offline materialization configuration.
        online_materialization: New online materialization configuration.

    Returns:
        FeatureGroup: Self for method chaining.
    """
```

---

##### `GetLatestTimeStrategy`

**Status:** Missing
**Docstring:** No

**Required Enhancement:**
```python
class GetLatestTimeStrategy(Enum):
    """Strategy for fetching the latest feature time across feature groups.

    Attributes:
        REQUIRE_ALL: Only use timestamps where all feature groups have data.
        REQUIRE_ANY: Use the latest timestamp from any feature group.
    """
    REQUIRE_ALL = "require_all"
    REQUIRE_ANY = "require_any"
```

---

##### `FeatureLookup`

**Status:** Complete
**Docstring:** Yes - Google style with Attributes

**Current Docstring:**
```python
"""
A class that represents a feature lookup operation in a feature store.

Attributes:
    source (FeatureStore): The feature store to perform the lookup on.
    features (Optional[List[str]]): A list of feature names to include in the lookup.
        If None, all features in the store will be included.
    exclude_features (Optional[List[str]]): A list of feature names to exclude from the lookup.
        If None, no features will be excluded.
"""
```

**Enhancement Needed:** Add Example section

---

##### `HistoricalFeatures`

**Status:** Partial
**Docstring:** Yes - Google style with Attributes

**Current Docstring:**
```python
"""
A class for retrieving historical features from a feature store.

Attributes:
    lookups (List[FeatureLookup]): A list of FeatureLookup objects representing the features to retrieve.
"""
```

**Methods with Docstrings:**
- `__post_init__()` - Has docstring
- `_deserialize()` - Has docstring with Args, Returns
- `using_spine()` - Has detailed docstring with Args
- `to_dataframe()` - Has docstring with Args

**Methods Missing Docstrings:**
- `_get_offline_watermarks()` - No docstring
- `using_latest()` - No docstring
- `_filter_by_start_end_time()` - No docstring
- `serve()` - No docstring

**Required Enhancement:**
```python
def using_latest(
    self, fetch_strategy: GetLatestTimeStrategy = GetLatestTimeStrategy.REQUIRE_ALL
):
    """Filter features to use only the latest available data.

    Args:
        fetch_strategy: Strategy for determining the latest timestamp
            across multiple feature groups.

    Returns:
        HistoricalFeatures: Self for method chaining.
    """

def serve(
    self,
    feature_start_time: Optional[datetime] = None,
    feature_end_time: Optional[datetime] = None,
    target: Optional[OnlineStore] = None,
    name: Optional[str] = None,
    ttl: Optional[timedelta] = None,
):
    """Serve historical features to an online store.

    Args:
        feature_start_time: Start time for feature filtering.
        feature_end_time: End time for feature filtering.
        target: Online store configuration for serving.
        name: Optional name for the online feature set.
        ttl: Time-to-live for the online features.

    Returns:
        OnlineFeatures: An OnlineFeatures instance for feature retrieval.
    """
```

---

##### `OnlineFeatures`

**Status:** Missing
**Docstring:** No

**Required Enhancement:**
```python
@dataclass
class OnlineFeatures:
    """Online feature serving for real-time inference.

    This class manages online feature storage and retrieval for low-latency
    feature lookups during model inference.

    Attributes:
        lookup_key: Entity defining the lookup keys for feature retrieval.
        dataframe: Optional DataFrame to initialize the online store.
        name: Name for this online feature set.
        description: Description of the feature set.
        lookups: List of FeatureLookup configurations.
        ttl: Time-to-live for cached features.
        online_store: Online store configuration.
        tag: Optional tags for organization.
        online_watermarks: List of data freshness timestamps.

    Example:
        >>> online = OnlineFeatures(
        ...     lookup_key=user_entity,
        ...     lookups=[FeatureLookup(source=user_features)],
        ...     online_store=OnlineStore(kind=OnlineStoreEnum.FILE)
        ... )
        >>> features = online.get_features([{"user_id": "123"}])
    """
```

**Methods Missing Docstrings:**
- `__post_init__()` - No docstring (complex initialization)
- `get_features()` - No docstring (primary API method)
- `delete()` - No docstring

**Required Enhancement for Methods:**
```python
def get_features(
    self,
    keys: Union[List[dict], List[Entity]],
    filter: Optional[str] = None,
    drop_event_time: bool = True,
) -> List[dict]:
    """Retrieve features for the given entity keys.

    Args:
        keys: List of key dictionaries or Entity objects to look up.
        filter: Optional SQL filter expression.
        drop_event_time: If True, exclude event_time from results.

    Returns:
        List of dictionaries containing feature values for each key.

    Raises:
        ValueError: If the online reader is not initialized or entity mismatch.

    Example:
        >>> features = online.get_features([{"user_id": "123"}, {"user_id": "456"}])
        >>> print(features)
        [{"feature1": 0.5, "feature2": "value"}, ...]
    """

def delete(self) -> bool:
    """Delete this online feature set and its data.

    Returns:
        True if deletion was successful.
    """
```

---

#### Decorators (Missing Docstrings)

##### `require_saved`

**Status:** Missing
**Location:** `feature_group.py`

**Required Enhancement:**
```python
def require_saved(func):
    """Decorator to ensure feature group is saved before method execution.

    Raises:
        ValueError: If the feature group has not been loaded or saved.
    """
```

##### `require_set_source`

**Status:** Missing
**Location:** `feature_group.py`

**Required Enhancement:**
```python
def require_set_source(func):
    """Decorator to ensure source is set before method execution.

    Raises:
        ValueError: If the source (Flow or DataFrame) is not set.
    """
```

##### `require_set_features`

**Status:** Missing
**Location:** `feature_group.py`

**Required Enhancement:**
```python
def require_set_features(func):
    """Decorator to ensure features are set before method execution.

    Raises:
        ValueError: If features are not set.
    """
```

##### `require_set_entity`

**Status:** Missing
**Location:** `feature_group.py`

**Required Enhancement:**
```python
def require_set_entity(func):
    """Decorator to ensure entity is set before method execution.

    Raises:
        ValueError: If entity is not set.
    """
```

---

## Priority Enhancement List

### Priority 1: Critical (User-Facing APIs)

| File | Class/Function | Action Required |
|------|---------------|-----------------|
| `feature_group.py` | `OnlineFeatures` class | Add class + all method docstrings |
| `feature_group.py` | `OnlineFeatures.get_features` | Add detailed docstring (primary API) |
| `feature_group.py` | `HistoricalFeatures.serve` | Add docstring |
| `feature_group.py` | `HistoricalFeatures.using_latest` | Add docstring |
| `feature_group.py` | `FeatureGroup.set_dataframe` | Add docstring |
| `feature_group.py` | `FeatureGroup.update_materialization` | Add docstring |

### Priority 2: Important (Secondary APIs)

| File | Class/Function | Action Required |
|------|---------------|-----------------|
| `featurestore.py` | `OfflineStore.__call__` | Add detailed docstring |
| `featurestore.py` | `OfflineStore.get_or_create` | Add docstring |
| `featurestore.py` | `OnlineStore.__call__` | Add docstring |
| `featurestore.py` | `OnlineStore.delete` | Add docstring |
| `featurestore.py` | Enum classes | Add docstrings to all enums |
| `featurestore.py` | `FeatureStoreHiveTableOutput` | Add class docstring |

### Priority 3: Nice to Have (Internal/Helpers)

| File | Class/Function | Action Required |
|------|---------------|-----------------|
| `feature_group.py` | `require_*` decorators | Add docstrings |
| `feature_group.py` | `GetLatestTimeStrategy` | Add enum docstring |
| `feature_group.py` | `HistoricalFeatures._get_offline_watermarks` | Add docstring |
| `feature_group.py` | `HistoricalFeatures._filter_by_start_end_time` | Add docstring |
| `__init__.py` | Module | Add module-level docstring |

---

## Metrics

| Metric | Value |
|--------|-------|
| Total Files | 3 |
| Total Classes | 15 |
| Classes with Complete Docstrings | 5 (33%) |
| Classes with Partial Docstrings | 5 (33%) |
| Classes Missing Docstrings | 5 (33%) |
| Total Methods Needing Docstrings | ~25 |
| Decorators Needing Docstrings | 4 |
| Enums Needing Docstrings | 3 |
| Estimated Enhancement Effort | Medium (1-2 days) |

---

## Recommendations

1. **Immediate Action:** Focus on `OnlineFeatures` class as it's a primary user-facing API for real-time inference
2. **Method Documentation:** Add docstrings to `__call__` methods on store classes as they contain complex logic
3. **Examples:** Add usage examples to all user-facing classes
4. **Consistency:** Ensure all docstrings follow Google-style format
5. **Cross-References:** Add "See Also" sections linking related classes (e.g., OfflineStore <-> OnlineStore)

---

## Style Guidelines for Enhancement

All new docstrings should follow Google-style format consistent with the core modules:

```python
def method_name(self, param1: str, param2: int = 0) -> ReturnType:
    """Short one-line description.

    Longer description if needed, explaining the method's purpose
    and any important behavioral notes.

    Args:
        param1: Description of param1.
        param2: Description of param2. Defaults to 0.

    Returns:
        Description of return value.

    Raises:
        ExceptionType: When this exception is raised.

    Example:
        >>> result = method_name("value", param2=5)
        >>> print(result)
        Expected output
    """
```

---

## Next Steps

1. [Phase 4] Add docstrings to `OnlineFeatures` class and all methods
2. [Phase 4] Add docstrings to `OfflineStore` and `OnlineStore` callable methods
3. [Phase 4] Add enum docstrings to `OfflineStoreEnum`, `OnlineStoreEnum`, `FileKindEnum`
4. [Phase 4] Add docstrings to `FeatureStoreHiveTableOutput` and `GetLatestTimeStrategy`
5. [Phase 4] Add docstrings to decorator functions
6. [Phase 4] Add module-level docstring to `__init__.py`
