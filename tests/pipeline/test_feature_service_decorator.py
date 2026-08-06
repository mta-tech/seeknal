"""Tests for the declarative Python Feature Service builder."""

import pytest
from pydantic import ValidationError

from seeknal.pipeline import FeatureView, feature_group, feature_service
from seeknal.pipeline.decorators import clear_registry, get_registered_nodes


@pytest.fixture(autouse=True)
def clean_registry():
    clear_registry()
    yield
    clear_registry()


def test_feature_service_registers_contract_only_metadata():
    @feature_service(
        name="customer_analytics",
        version="1",
        variant="default",
        owner="ml-platform",
        views=[
            FeatureView(
                ref="feature_group.customer_profile",
                features=["age", "lifetime_value"],
            )
        ],
    )
    def customer_analytics():
        pass

    node = get_registered_nodes()["feature_service.customer_analytics"]
    assert node["owner"] == "ml-platform"
    assert node["inputs"] == [{"ref": "feature_group.customer_profile"}]
    assert node["views"] == [
        {
            "ref": "feature_group.customer_profile",
            "features": ["age", "lifetime_value"],
        }
    ]


def test_feature_service_rejects_non_feature_group_refs():
    with pytest.raises(ValidationError, match="feature_group"):
        FeatureView(ref="transform.customer_profile", features=["age"])


def test_feature_service_rejects_duplicate_views():
    view = FeatureView(ref="feature_group.customer_profile", features=["age"])

    with pytest.raises(ValidationError, match="unique Feature Groups"):
        feature_service(
            name="customer_analytics",
            version="1",
            owner="ml-platform",
            views=[view, view],
        )


def test_feature_group_accepts_explicit_multi_target_materializations():
    targets = [
        {
            "type": "atlas_online",
            "connection": "atlas_feature_store",
            "table": "customer_profile",
        }
    ]

    @feature_group(
        name="customer_profile",
        entity={"name": "customer", "join_keys": ["customer_id"]},
        features={"customer_id": {"dtype": "string"}},
        materializations=targets,
    )
    def customer_profile(ctx):
        return ctx

    node = get_registered_nodes()["feature_group.customer_profile"]
    assert node["materializations"] == targets


def test_feature_group_rejects_singular_and_plural_materializations():
    with pytest.raises(ValueError, match="either materialization or materializations"):
        feature_group(
            name="customer_profile",
            entity="customer",
            materialization={"offline": {"enabled": True}},
            materializations=[{"type": "atlas_online"}],
        )
