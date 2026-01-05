# FeatureStore API Reference

This page documents the FeatureStore module, which provides the core feature store functionality for managing and serving ML features for both offline (batch) and online (real-time) use cases.

## Overview

The FeatureStore module provides essential classes for feature engineering and serving:

| Class | Purpose |
|-------|---------|
| [`FeatureGroup`](#seeknal.featurestore.feature_group.FeatureGroup) | Define and manage groups of features with customizable materialization |
| [`FeatureLookup`](#seeknal.featurestore.feature_group.FeatureLookup) | Specify feature lookups from feature groups |
| [`HistoricalFeatures`](#seeknal.featurestore.feature_group.HistoricalFeatures) | Retrieve historical feature data with point-in-time correctness |
| [`OnlineFeatures`](#seeknal.featurestore.feature_group.OnlineFeatures) | Serve features in real-time for model inference |
| [`OfflineStore`](#seeknal.featurestore.featurestore.OfflineStore) | Configure offline storage backends |
| [`OnlineStore`](#seeknal.featurestore.featurestore.OnlineStore) | Configure online storage backends |

## FeatureGroup

The `FeatureGroup` class is the primary abstraction for defining and managing groups of related features. It handles feature materialization to both offline and online stores.

:::seeknal.featurestore.feature_group.FeatureGroup
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## FeatureLookup

The `FeatureLookup` class specifies how to look up features from a feature group, with optional feature selection and exclusion.

:::seeknal.featurestore.feature_group.FeatureLookup
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Materialization

The `Materialization` class configures how features are materialized to offline and online stores.

:::seeknal.featurestore.feature_group.Materialization
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## HistoricalFeatures

The `HistoricalFeatures` class retrieves historical feature data with point-in-time correctness for training ML models.

:::seeknal.featurestore.feature_group.HistoricalFeatures
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## OnlineFeatures

The `OnlineFeatures` class serves features in real-time for model inference with low-latency access patterns.

:::seeknal.featurestore.feature_group.OnlineFeatures
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## GetLatestTimeStrategy

The `GetLatestTimeStrategy` class defines strategies for retrieving the latest feature values based on timestamp.

:::seeknal.featurestore.feature_group.GetLatestTimeStrategy
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## OfflineStore

The `OfflineStore` class configures offline storage backends for batch feature storage. Supports Hive tables and Delta files.

:::seeknal.featurestore.featurestore.OfflineStore
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## OnlineStore

The `OnlineStore` class configures online storage backends for real-time feature serving.

:::seeknal.featurestore.featurestore.OnlineStore
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Feature

The `Feature` class represents an individual feature definition with name, data type, and fill null handling.

:::seeknal.featurestore.featurestore.Feature
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## FillNull

The `FillNull` class defines how null values should be handled for features.

:::seeknal.featurestore.featurestore.FillNull
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Storage Enums

Enumerations for storage backend configuration.

### OfflineStoreEnum

:::seeknal.featurestore.featurestore.OfflineStoreEnum
    options:
      show_root_heading: false
      show_source: true
      members_order: source

### OnlineStoreEnum

:::seeknal.featurestore.featurestore.OnlineStoreEnum
    options:
      show_root_heading: false
      show_source: true
      members_order: source

### FileKindEnum

:::seeknal.featurestore.featurestore.FileKindEnum
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Output Configurations

Classes for configuring feature store output destinations.

### FeatureStoreHiveTableOutput

:::seeknal.featurestore.featurestore.FeatureStoreHiveTableOutput
    options:
      show_root_heading: false
      show_source: true
      members_order: source

### FeatureStoreFileOutput

:::seeknal.featurestore.featurestore.FeatureStoreFileOutput
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Module Reference

Complete module reference for the featurestore package.

:::seeknal.featurestore
    options:
      show_root_heading: false
      show_source: true
      members_order: source
