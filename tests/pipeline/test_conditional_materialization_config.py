"""Conditional policies survive the public typed Python pipeline API."""

import pytest

from seeknal.pipeline.decorators import (
    _validate_materialization_config,
    clear_registry,
    get_registered_nodes,
    materialize,
    transform,
)
from seeknal.pipeline.materialization_config import MaterializationConfig

POLICY = {
    "when_matched_update_if": "source.version > target.version",
    "compare_columns": ["version"],
    "when_not_matched": "skip",
}


@pytest.fixture(autouse=True)
def clean_registry():
    clear_registry()
    yield
    clear_registry()


def test_typed_policy_roundtrip_does_not_become_unconditional():
    incoming = {"mode": "upsert", "unique_keys": ["id"], **POLICY}
    config = MaterializationConfig.from_dict(incoming)

    assert config.to_dict() == incoming
    assert MaterializationConfig.from_dict(config.to_dict()) == config


def test_unset_typed_policy_inherits_profile_defaults():
    from seeknal.workflow.materialization.config import (
        MaterializationConfig as ProfileConfig,
        MaterializationMode,
    )

    profile = ProfileConfig(default_mode=MaterializationMode.UPSERT, unique_keys=["id"], **POLICY)
    node = MaterializationConfig(compare_columns=["version", "status"])
    _validate_materialization_config(node)
    merged = profile.merge_with_node_config(node.to_dict())

    assert merged.when_matched_update_if == POLICY["when_matched_update_if"]
    assert merged.compare_columns == ["version", "status"]
    assert merged.when_not_matched == "skip"
    assert MaterializationConfig().to_dict() == {}
    assert MaterializationConfig(compare_columns=[], when_not_matched="insert").to_dict() == {
        "compare_columns": [],
        "when_not_matched": "insert",
    }


def test_typed_decorator_keeps_policy_in_registered_metadata():
    @transform(
        name="customers",
        materialization=MaterializationConfig(
            table="lake.analytics.customers", mode="upsert", unique_keys=["id"], **POLICY
        ),
    )
    def customers(ctx):
        return None

    target = get_registered_nodes()["transform.customers"]["materialization"]
    assert {key: target[key] for key in POLICY} == POLICY


@pytest.mark.parametrize("style", ["typed", "stackable"])
def test_conditional_policy_survives_discovery_and_dag(tmp_path, style):
    from seeknal.workflow.dag import DAGBuilder

    pipelines = tmp_path / "seeknal" / "pipelines"
    pipelines.mkdir(parents=True)
    options = {
        "table": "lake.analytics.customers",
        "mode": "upsert",
        "unique_keys": ["id"],
        **POLICY,
    }
    decorator = (
        f"@transform(name='customers', materialization=MaterializationConfig(**{options!r}))"
        if style == "typed"
        else f"@transform(name='customers')\n@materialize(**{options!r})"
    )
    (pipelines / "conditional.py").write_text(
        "from seeknal.pipeline.decorators import transform, materialize\n"
        "from seeknal.pipeline.materialization_config import MaterializationConfig\n"
        f"{decorator}\ndef customers(ctx):\n    return None\n"
    )
    builder = DAGBuilder(project_path=tmp_path)
    builder.build()
    target = builder.get_node("transform.customers").yaml_data["materializations"][0]

    assert {key: target[key] for key in POLICY} == POLICY


@pytest.mark.parametrize("style", ["typed", "stackable"])
@pytest.mark.parametrize(
    "options,message",
    [
        ({"when_matched_update_if": ""}, "non-empty"),
        ({"when_matched_update_if": True}, "non-empty"),
        (
            {"when_matched_update_if": "TRUE", "compare_columns": ["bad-key"]},
            "Invalid compare_columns",
        ),
        ({"when_not_matched": "ignore"}, "insert.*skip"),
        ({"mode": "append", "when_matched_update_if": "TRUE"}, "require mode 'upsert'"),
    ],
)
def test_both_decorator_forms_reject_invalid_conditional_settings(style, options, message):
    settings = {"mode": "upsert", "unique_keys": ["id"], **options}
    with pytest.raises(ValueError, match=message):
        if style == "typed":
            transform(name="invalid", materialization=MaterializationConfig(**settings))
        else:
            materialize(**settings)(lambda ctx: None)


def test_explicit_null_predicate_with_comparisons_is_rejected():
    with pytest.raises(ValueError, match="requires when_matched_update_if"):
        _validate_materialization_config(
            {
                "mode": "upsert",
                "when_matched_update_if": None,
                "compare_columns": ["version"],
            }
        )
