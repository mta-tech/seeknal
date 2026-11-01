"""Seeknal Serving discovery package: automated placement and schema discovery."""

from __future__ import annotations

from seeknal.serving.discovery.__main__ import main
from seeknal.serving.discovery.build import DiscoveryResult, discover
from seeknal.serving.discovery.config import (
    DiscoveryConfig,
    DiscoveryConfigError,
    DiscoveryError,
    EngineConfig,
    LakebaseConfig,
    LakekeeperConfig,
    SyncedTableConfig,
)
from seeknal.serving.discovery.cube import CubeMeta, CubeSource
from seeknal.serving.discovery.lakebase import LakebaseSyncSource, SyncState
from seeknal.serving.discovery.lakekeeper import IcebergTable, LakekeeperSource
from seeknal.serving.discovery.refresh import PlacementRefresher

__all__ = [
    "CubeMeta",
    "CubeSource",
    "DiscoveryConfig",
    "DiscoveryConfigError",
    "DiscoveryError",
    "DiscoveryResult",
    "EngineConfig",
    "IcebergTable",
    "LakebaseConfig",
    "LakebaseSyncSource",
    "LakekeeperConfig",
    "LakekeeperSource",
    "PlacementRefresher",
    "SyncState",
    "SyncedTableConfig",
    "discover",
    "main",
]
