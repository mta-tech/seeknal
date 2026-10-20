"""Deterministic synthetic Iceberg workload; no production data or external catalog.

Run with PYTHONPATH=src:. python tools/validate_conditional_upsert.py --output /tmp/report.json
Uses the authenticated loopback REST integration fixture and real local Parquet files.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa

from seeknal.workflow.materialization.config import CatalogConfig
from seeknal.workflow.materialization.operations import WriteError, write_to_iceberg
from tests.integration.iceberg_rest_fixture import run_rest_fixture

SCHEMA = pa.schema([
    ('tenant', pa.string()), ('id', pa.int64()), ('updated_at', pa.timestamp('us')),
    ('window_at', pa.timestamp('us')), ('extract_at', pa.timestamp('us')),
    ('load_at', pa.timestamp('us')), ('status', pa.string()), ('amount', pa.int64()),
])
BASE = datetime(2026, 10, 1)


def dataset(n=2400):
    return [dict(tenant=f'tenant-{i % 7}', id=i // 7,
                 updated_at=None if i % 6 == 5 else BASE,
                 window_at=BASE, extract_at=BASE, load_at=BASE,
                 status=['pending', 'paid', 'cancelled'][i % 3], amount=i * 137)
            for i in range(n)]


def canonical(rows):
    return sorted(rows, key=lambda r: (r['tenant'], r['id']))


def digest(rows):
    return hashlib.sha256(json.dumps(canonical(rows), default=str, sort_keys=True).encode()).hexdigest()


def run_workload(directory: Path, n=2400):
    started = time.monotonic()
    directory.mkdir(parents=True, exist_ok=True)
    events = []
    with run_rest_fixture(directory) as fixture, duckdb.connect() as con:
        config = CatalogConfig(uri=fixture.uri, warehouse=fixture.warehouse, bearer_token=fixture.token)

        def write(name, rows, **options):
            con.register('incoming', pa.Table.from_pylist(rows, schema=SCHEMA))
            result = write_to_iceberg(con, 'local_rest', name, 'incoming', 'upsert',
                                      unique_keys=['tenant', 'id'], catalog_config=config, **options)
            assert result.success
            return result

        def snapshot(name):
            return fixture.backend.load_table(tuple(name.split('.'))).current_snapshot().snapshot_id

        def verify(name, expected):
            table = fixture.backend.load_table(tuple(name.split('.')))
            actual = table.scan().to_arrow().to_pylist()
            assert canonical(actual) == canonical(expected), name
            # Independent query engine and connection, reading Iceberg metadata rather than an Arrow registration.
            with duckdb.connect() as reader:
                reader.execute('LOAD iceberg')
                duck_rows = reader.execute('SELECT * FROM iceberg_scan(?)',
                                           [table.metadata_location.removeprefix('file://')]).fetch_arrow_table().to_pylist()
            assert canonical(duck_rows) == canonical(expected), name
            return digest(actual)

        initial = dataset(n)
        incoming = []
        expected = []
        for i, old in enumerate(initial):
            row = dict(old, load_at=BASE + timedelta(days=10), status='amended', amount=old['amount']+1)
            case = i % 6
            row['updated_at'] = [BASE+timedelta(days=1), BASE, BASE-timedelta(days=1), None,
                                 BASE+timedelta(seconds=1), None][case]
            incoming.append(row)
            expected.append(row if case in (0, 4, 5) else old)
        new = [dict(row, tenant='new-tenant', id=i) for i, row in enumerate(dataset(300))]
        incoming += new
        write('analytics.versioned', initial)
        result = write('analytics.versioned', incoming,
                       when_matched_update_if='target.updated_at IS NULL OR source.updated_at > target.updated_at',
                       compare_columns=['updated_at'])
        assert result.input_row_count == n + 300
        assert result.row_count == n // 2 + 300
        assert result.inserted_row_count == 300 and result.updated_row_count == n // 2
        assert result.skipped_by_condition == n // 2
        assert result.skipped_not_matched == 0
        expected += new
        events.append(dict(case='version_condition', input=n+300, accepted=result.row_count,
                           skipped_by_condition=result.skipped_by_condition, digest=verify('analytics.versioned', expected)))
        before = snapshot('analytics.versioned')
        rejected = [dict(row, updated_at=BASE-timedelta(days=20)) for row in initial if row['updated_at'] is not None]
        result = write('analytics.versioned', rejected,
                       when_matched_update_if='source.updated_at > target.updated_at', compare_columns=['updated_at'])
        assert result.row_count == 0 and result.outcome == 'noop'
        assert result.skipped_by_condition == len(rejected)
        assert snapshot('analytics.versioned') == before
        events.append(dict(case='all_rejected_snapshot_unchanged', input=len(rejected), snapshot=before,
                           digest=verify('analytics.versioned', expected)))

        write('analytics.windows', initial)
        window_batch, window_expected = [], []
        for i, old in enumerate(initial):
            case = i % 4
            row = dict(old, window_at=BASE+timedelta(days=[-1,0,1,0][case]),
                       extract_at=BASE+timedelta(days=[20,1,0,0][case]), load_at=BASE+timedelta(days=30))
            window_batch.append(row)
            window_expected.append(row if case in (1,2) else old)
        condition = ('source.window_at > target.window_at OR (source.window_at = target.window_at '
                     'AND source.extract_at > target.extract_at)')
        result = write('analytics.windows', window_batch+new, when_matched_update_if=condition,
                       compare_columns=['window_at','extract_at'], when_not_matched='skip')
        assert result.row_count == n//2 and result.skipped_by_condition == n//2
        assert result.inserted_row_count == 0 and result.updated_row_count == n//2
        assert result.skipped_not_matched == 300 and result.input_row_count == n+300
        events.append(dict(case='window_extract_order_and_skip_new', input=n+300, accepted=result.row_count,
                           skipped_by_condition=result.skipped_by_condition, skipped_not_matched=result.skipped_not_matched,
                           digest=verify('analytics.windows', window_expected)))

        # Distinct business equality policy ignores a changing load timestamp.
        before = snapshot('analytics.windows')
        unchanged = [dict(row, load_at=BASE+timedelta(days=100)) for row in window_expected]
        result = write('analytics.windows', unchanged,
                       when_matched_update_if='source.status IS DISTINCT FROM target.status OR source.amount IS DISTINCT FROM target.amount',
                       compare_columns=['status','amount'])
        assert result.row_count == 0 and result.skipped_by_condition == n
        assert snapshot('analytics.windows') == before
        events.append(dict(case='business_unchanged_no_rewrite', input=n, digest=verify('analytics.windows', window_expected)))

        before = snapshot('analytics.windows')
        try:
            write('analytics.windows', [initial[0], initial[0]], when_matched_update_if=condition,
                  compare_columns=['window_at','extract_at'])
        except (WriteError, ValueError) as exc:
            assert 'duplicat' in str(exc).lower()
        else:
            raise AssertionError('Duplicate source key accepted')
        assert snapshot('analytics.windows') == before
        events.append(dict(case='duplicate_source_rejected', snapshot=before))
        # An external writer can violate uniqueness; validation must fail before mutation.
        write('analytics.duplicate_target', initial[:12])
        duplicate_table = fixture.backend.load_table(('analytics', 'duplicate_target'))
        duplicate_table.append(pa.Table.from_pylist([initial[0]], schema=SCHEMA))
        before = snapshot('analytics.duplicate_target')
        try:
            write('analytics.duplicate_target', [incoming[0]],
                  when_matched_update_if='source.updated_at > target.updated_at', compare_columns=['updated_at'])
        except (WriteError, ValueError) as exc:
            assert 'duplicat' in str(exc).lower()
        else:
            raise AssertionError('Duplicate target key accepted')
        assert snapshot('analytics.duplicate_target') == before
        events.append(dict(case='duplicate_target_rejected', snapshot=before))

        result = write('analytics.never_created', initial[:12], when_not_matched='skip')
        assert result.row_count == 0 and result.skipped_not_matched == 12
        assert result.outcome == 'noop'
        assert not fixture.backend.table_exists(('analytics', 'never_created'))
        events.append(dict(case='missing_table_skip_no_create', input=12))
        assert fixture.state.unauthorized_requests == 0
        return dict(dataset='deterministic synthetic; no production data', rows_per_base_table=n,
                    catalog='authenticated loopback REST backed by PyIceberg SqlCatalog and local Parquet',
                    readers=['PyIceberg', 'separate DuckDB iceberg_scan connection'],
                    versions=dict(duckdb=duckdb.__version__, pyarrow=pa.__version__,
                                  pyiceberg=__import__('pyiceberg').__version__),
                    elapsed_seconds=round(time.monotonic()-started,3), cases=events,
                    rest_commit_attempts=fixture.state.commit_attempts)



def run_yaml_smoke(directory: Path):
    """Exercise real YAML profile and node options through dispatcher to REST."""
    import yaml
    from seeknal.workflow.materialization.dispatcher import MaterializationDispatcher
    from seeknal.workflow.materialization.profile_loader import ProfileLoader
    directory.mkdir(parents=True, exist_ok=True)
    with run_rest_fixture(directory) as fixture, duckdb.connect() as con:
        profile = {'materialization': {
            'default_mode': 'upsert', 'unique_keys': ['tenant', 'id'],
            'when_matched_update_if': 'source.updated_at > target.updated_at',
            'compare_columns': ['updated_at'],
            'catalog': {'uri': fixture.uri, 'warehouse': fixture.warehouse,
                        'bearer_token': fixture.token}}}
        profile_path = directory / 'profiles.yml'
        profile_path.write_text(yaml.safe_dump(profile))
        dispatcher = MaterializationDispatcher(ProfileLoader(profile_path=profile_path))
        node_path = directory / 'node.yml'
        node_path.write_text('materialization:\n  - type: iceberg\n    table: analytics.yaml_rows\n')

        def dispatch(rows):
            con.register('incoming', pa.Table.from_pylist(rows, schema=SCHEMA))
            node = yaml.safe_load(node_path.read_text())
            result = dispatcher.dispatch(con, 'incoming', node['materialization'], node_id='transform.synthetic_yaml')
            assert result.all_succeeded, result.results
            return result.results[0]['write_result']

        initial = dataset(12)
        assert dispatch(initial).inserted_row_count == 12
        older = dict(initial[0], updated_at=BASE-timedelta(days=1), amount=-100)
        newer = dict(initial[1], updated_at=BASE+timedelta(days=1), amount=9000)
        new = dict(initial[2], tenant='new-yaml')
        result = dispatch([older, newer, new])
        assert (result.input_row_count, result.row_count, result.skipped_by_condition,
                result.skipped_not_matched) == (3, 2, 1, 0)
        expected = [newer if row == initial[1] else row for row in initial] + [new]
        node_path.write_text('materialization:\n  - type: iceberg\n    table: analytics.yaml_rows\n'
                             '    when_matched_update_if: source.amount > target.amount\n'
                             '    compare_columns: [amount]\n    when_not_matched: skip\n')
        changed = dict(initial[0], amount=9999, updated_at=BASE-timedelta(days=10))
        ignored_new = dict(initial[3], tenant='ignored-new-yaml')
        result = dispatch([changed, ignored_new])
        assert (result.input_row_count, result.row_count, result.skipped_by_condition,
                result.skipped_not_matched) == (2, 1, 0, 1)
        expected = [changed if row == initial[0] else row for row in expected]
        table = fixture.backend.load_table(('analytics', 'yaml_rows'))
        assert canonical(table.scan().to_arrow().to_pylist()) == canonical(expected)
        with duckdb.connect() as reader:
            reader.execute('LOAD iceberg')
            actual = reader.execute('SELECT * FROM iceberg_scan(?)',
                                    [table.metadata_location.removeprefix('file://')]).fetch_arrow_table().to_pylist()
        assert canonical(actual) == canonical(expected)
        assert fixture.state.unauthorized_requests == 0
        return dict(case='yaml_profile_and_node_override_dispatcher_rest', final_rows=len(expected),
                    profile_round=dict(input=3, accepted=2, skipped_by_condition=1),
                    node_override_round=dict(input=2, accepted=1, skipped_not_matched=1),
                    digest=digest(actual))



def run_pipeline_smoke(directory: Path):
    """Actual seeknal run CLI: Parquet source -> deduplicating transform -> REST."""
    from contextlib import chdir
    import pyarrow.parquet as pq
    import yaml
    from typer.testing import CliRunner
    from seeknal.cli.main import app

    started = time.monotonic()
    directory.mkdir(parents=True, exist_ok=True)
    with run_rest_fixture(directory) as fixture:
        project = directory / 'project'
        sources = project / 'seeknal' / 'sources'
        transforms = project / 'seeknal' / 'transforms'
        sources.mkdir(parents=True)
        transforms.mkdir(parents=True)
        input_path = project / 'incoming.parquet'
        profile = project / 'profiles.yml'
        profile.write_text(yaml.safe_dump({'materialization': {
            'catalog': {'type': 'rest', 'uri': fixture.uri, 'warehouse': fixture.warehouse,
                        'bearer_token': fixture.token}}}))
        (sources / 'incoming.yml').write_text(yaml.safe_dump({
            'kind': 'source', 'name': 'incoming', 'source': 'parquet', 'table': str(input_path)}))
        (transforms / 'silver.yml').write_text(yaml.safe_dump({
            'kind': 'transform', 'name': 'silver', 'inputs': [{'ref': 'source.incoming'}],
            'transform': ('SELECT * FROM source.incoming QUALIFY ROW_NUMBER() OVER '
                          '(PARTITION BY tenant, id ORDER BY updated_at DESC NULLS LAST, extract_at DESC) = 1'),
            'materializations': [{'type': 'iceberg', 'table': 'local_rest.analytics.pipeline_silver',
                                  'mode': 'upsert', 'unique_keys': ['tenant', 'id'],
                                  'when_matched_update_if': 'source.updated_at > target.updated_at',
                                  'compare_columns': ['updated_at']}]}))
        rounds = []

        def execute(rows):
            pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), input_path)
            with chdir(project):
                result = CliRunner().invoke(app, ['run', '--full', '--profile', str(profile)])
            assert result.exit_code == 0, result.output + str(result.exception)
            state = json.loads((project / 'target' / 'run_state.json').read_text())
            assert state['nodes']['source.incoming']['status'] == 'success', state
            node = state['nodes']['transform.silver']
            assert node['status'] == 'success', node
            materialization = node['metadata']['materialization']['results'][0]['write_result']
            return state['nodes']['source.incoming'], node, materialization

        initial = [dict(row, updated_at=BASE) for row in dataset(60)]
        source, node, result = execute(initial)
        assert result['input_row_count'] == 60 and result['inserted_row_count'] == 60
        rounds.append(dict(source_rows=source['row_count'], transform_rows=node['row_count'],
                           input=result['input_row_count'], accepted=result['row_count'],
                           skipped_by_condition=result['skipped_by_condition'], skipped_not_matched=result['skipped_not_matched']))
        incoming, expected = [], []
        for i, old in enumerate(initial):
            newer = i % 3 == 0
            row = dict(old, updated_at=BASE+timedelta(days=[1,0,-1][i % 3]),
                       load_at=BASE+timedelta(days=10), amount=old['amount']+999, status='amended')
            incoming.append(row)
            expected.append(row if newer else old)
        new = [dict(row, id=i, updated_at=BASE, tenant='pipeline-new') for i, row in enumerate(dataset(12))]
        # Six duplicate-key source rows are removed by transform SQL, before writer uniqueness validation.
        duplicates = [dict(row, updated_at=BASE-timedelta(days=30)) for row in incoming[:6]]
        source, node, result = execute(incoming+new+duplicates)
        assert source['row_count'] == 78 and node['row_count'] == 72
        assert (result['input_row_count'], result['row_count'], result['inserted_row_count'],
                result['updated_row_count'], result['skipped_by_condition'], result['skipped_not_matched']) == (72,32,12,20,40,0)
        rounds.append(dict(source_rows=source['row_count'], transform_rows=node['row_count'],
                           input=result['input_row_count'], accepted=result['row_count'],
                           inserted=result['inserted_row_count'], updated=result['updated_row_count'],
                           skipped_by_condition=result['skipped_by_condition'], skipped_not_matched=result['skipped_not_matched']))
        expected += new
        table = fixture.backend.load_table(('analytics', 'pipeline_silver'))
        assert canonical(table.scan().to_arrow().to_pylist()) == canonical(expected)
        with duckdb.connect() as reader:
            reader.execute('LOAD iceberg')
            actual = reader.execute('SELECT * FROM iceberg_scan(?)',
                                    [table.metadata_location.removeprefix('file://')]).fetch_arrow_table().to_pylist()
        assert canonical(actual) == canonical(expected)
        manifest = json.loads((project / 'target' / 'manifest.json').read_text())
        assert set(manifest['nodes']) == {'source.incoming', 'transform.silver'}
        return dict(case='seeknal_run_cli_parquet_source_transform_rest',
                    boundary='actual Typer CLI, DAGBuilder, manifest, source/transform execution and run_state; local REST fixture',
                    dataset='deterministic synthetic; no production data',
                    rounds=rounds, final_rows=len(expected), digest=digest(actual),
                    elapsed_seconds=round(time.monotonic()-started,3))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rows', type=int, default=2400)
    parser.add_argument('--pipeline-only', action='store_true', help='Run actual seeknal run CLI source/transform REST smoke')
    parser.add_argument('--yaml-only', action='store_true', help='Run the small real YAML/profile/dispatcher REST smoke')
    args = parser.parse_args()
    if args.rows < 12 or args.rows % 12:
        parser.error('--rows must be a positive multiple of 12, at least 12')
    with tempfile.TemporaryDirectory(prefix='seeknal-conditional-e2e-') as path:
        report = (run_pipeline_smoke(Path(path)) if args.pipeline_only else
                  run_yaml_smoke(Path(path)) if args.yaml_only else run_workload(Path(path), args.rows))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
