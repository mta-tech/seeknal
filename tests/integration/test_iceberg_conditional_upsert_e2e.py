"""Real REST/Parquet synthetic end-to-end conditional upsert workload."""
import os

import pytest

from tools.validate_conditional_upsert import run_workload, run_yaml_smoke, run_pipeline_smoke


@pytest.mark.integration
def test_conditional_upsert_complex_synthetic_e2e(tmp_path, monkeypatch):
    for name in tuple(os.environ):
        if name.startswith(('AWS_', 'S3_', 'LAKEKEEPER_', 'ICEBERG_')):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv('NO_PROXY', '127.0.0.1,localhost')
    report = run_workload(tmp_path)
    assert len(report['cases']) == 7
    assert report['rows_per_base_table'] == 2400


@pytest.mark.integration
def test_yaml_to_dispatcher_conditional_upsert_rest(tmp_path, monkeypatch):
    monkeypatch.setenv('NO_PROXY', '127.0.0.1,localhost')
    result = run_yaml_smoke(tmp_path)
    assert result['final_rows'] == 13


@pytest.mark.integration
def test_seeknal_run_cli_source_transform_conditional_upsert_rest(tmp_path, monkeypatch):
    monkeypatch.setenv('NO_PROXY', '127.0.0.1,localhost')
    result = run_pipeline_smoke(tmp_path)
    assert result['final_rows'] == 72


@pytest.mark.integration
def test_backfill_date_empty_transform_preserves_upsert_snapshot(tmp_path, monkeypatch):
    """Exercise parameter templates, empty publication, and conditional writes together."""
    import json

    import yaml
    from typer.testing import CliRunner

    from seeknal.cli.main import app
    from tests.integration.iceberg_rest_fixture import run_rest_fixture

    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    with run_rest_fixture(tmp_path) as fixture:
        project = tmp_path / "project"
        transforms = project / "seeknal" / "transforms"
        transforms.mkdir(parents=True)
        profile = project / "profiles.yml"
        profile.write_text(yaml.safe_dump({"materialization": {"catalog": {
            "type": "rest", "uri": fixture.uri, "warehouse": fixture.warehouse,
            "bearer_token": fixture.token,
        }}}))
        (transforms / "dated.yml").write_text(yaml.safe_dump({
            "kind": "transform", "name": "dated",
            "transform": (
                "SELECT CAST(1 AS BIGINT) AS id, CAST('{{ today(-1) }}' AS DATE) AS version "
                "WHERE '{{ today }}' = '2026-10-12'"
            ),
            "materializations": [{
                "type": "iceberg", "table": "local.analytics.dated", "mode": "upsert",
                "unique_keys": ["id"], "compare_columns": ["version"],
                "when_matched_update_if": "source.version > target.version",
            }],
        }))
        monkeypatch.chdir(project)
        receipts = []
        for day in ("2026-10-12", "2026-10-13"):
            result = CliRunner().invoke(app, ["run", "--full", "--date", day, "--profile", str(profile)])
            assert result.exit_code == 0, result.output + str(result.exception)
            state = json.loads((project / "target" / "run_state.json").read_text())
            node = state["nodes"]["transform.dated"]
            assert node["status"] == "success"
            receipts.append(node["metadata"]["materialization"]["results"][0]["write_result"])
        table = fixture.backend.load_table(("analytics", "dated"))
        assert receipts[0]["row_count"] == 1
        assert receipts[1]["input_row_count"] == 0
        assert receipts[1]["row_count"] == 0
        assert receipts[1]["outcome"] == "noop"
        assert receipts[0]["snapshot_id"] == receipts[1]["snapshot_id"]
        assert len(table.scan().to_arrow()) == 1
        import duckdb

        with duckdb.connect() as reader:
            rows = reader.execute(
                "SELECT * FROM read_parquet(?)",
                [str(project / "target" / "intermediate" / "transform_dated.parquet")],
            ).fetchall()
        assert rows == []
