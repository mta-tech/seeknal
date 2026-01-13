"""Seeknal CLI - Command-line interface for feature store management.

This module provides the Seeknal CLI with optional Atlas Data Platform integration.

Core commands:
- seeknal init: Initialize a new project
- seeknal run: Execute transformation flows
- seeknal materialize: Write features to stores
- seeknal list: List resources
- seeknal show: Show resource details
- seeknal validate: Validate configurations
- seeknal version: Manage feature group versions

Atlas integration (optional):
- seeknal atlas api: Manage Atlas API server
- seeknal atlas governance: Governance operations
- seeknal atlas lineage: Lineage tracking

Enable Atlas with: pip install seeknal[atlas]
"""

from .main import app

# Conditionally export Atlas CLI if available
try:
    from .atlas import atlas_app
    __all__ = ["app", "atlas_app"]
except ImportError:
    __all__ = ["app"]
