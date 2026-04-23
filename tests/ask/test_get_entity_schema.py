"""Tests for the get_entity_schema ask tool."""

from pathlib import Path
from unittest.mock import MagicMock

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.get_entity_schema import get_entity_schema
from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery


def test_get_entity_schema_supports_list_backed_catalog_features(tmp_path: Path):
    discovery = ArtifactDiscovery(tmp_path)
    catalog = {
        "entity_name": "customer",
        "join_keys": ["customer_id"],
        "feature_groups": {
            "customer_features": {
                "name": "customer_features",
                "features": ["total_orders", "total_revenue"],
                "schema": {
                    "customer_id": "VARCHAR",
                    "total_orders": "BIGINT",
                    "total_revenue": "DOUBLE",
                },
            }
        },
    }

    discovery._entities_cache = [catalog]
    set_tool_context(
        ToolContext(
            repl=MagicMock(),
            artifact_discovery=discovery,
            project_path=tmp_path,
        )
    )

    result = get_entity_schema("customer")

    assert "## Entity: `customer`" in result
    assert "`customer_features.total_orders` (BIGINT)" in result
    assert "`customer_features.total_revenue` (DOUBLE)" in result
