import pytest
from seeknal.workflow.materialization.config import (
    ConfigurationError, MaterializationConfig, MaterializationMode,
    validate_iceberg_write_options,
)


@pytest.mark.parametrize('options,message', [
    ({'when_matched_update_if': ''}, 'non-empty'),
    ({'when_matched_update_if': True}, 'non-empty'),
    ({'compare_columns': ['v']}, 'requires when_matched_update_if'),
    ({'when_matched_update_if': 'TRUE', 'compare_columns': ['v', 'v']}, 'Duplicate'),
    ({'when_not_matched': 'ignore'}, 'insert.*skip'),
])
def test_invalid_conditions(options, message):
    with pytest.raises(ConfigurationError, match=message):
        validate_iceberg_write_options('upsert', ['id'], **options)


def test_conditional_settings_only_upsert():
    with pytest.raises(ConfigurationError, match='upsert'):
        validate_iceberg_write_options('append', when_not_matched='skip')
    validate_iceberg_write_options('upsert', ['id'], when_not_matched='skip')


def test_node_can_clear_inherited_condition():
    profile = MaterializationConfig(
        default_mode=MaterializationMode.UPSERT, unique_keys=['id'],
        when_matched_update_if='source.v > target.v', compare_columns=['v'],
        when_not_matched='skip',
    )
    merged = profile.merge_with_node_config({
        'when_matched_update_if': None, 'compare_columns': [], 'when_not_matched': 'insert',
    })
    assert merged.when_matched_update_if is None
    assert merged.compare_columns == []
    assert merged.when_not_matched == 'insert'
    assert profile.merge_with_node_config({'table': 'silver.rows'}).compare_columns == ['v']


def test_profile_loads_conditional_defaults(tmp_path):
    from seeknal.workflow.materialization.profile_loader import ProfileLoader
    path = tmp_path / 'profiles.yml'
    path.write_text('''materialization:
  default_mode: upsert
  unique_keys: [id]
  when_matched_update_if: source.v > target.v
  compare_columns: [v]
  when_not_matched: skip
''')
    config = ProfileLoader(profile_path=path).load_profile()
    assert config.when_matched_update_if == 'source.v > target.v'
    assert config.compare_columns == ['v']
    assert config.when_not_matched == 'skip'
