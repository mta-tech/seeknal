"""The default install (no optional `spark` extra) must work out of the box.

Each CLI test runs seeknal in a fresh interpreter with pyspark/delta/py4j
blocked, so a Spark import anywhere on the default path fails the test even
when the developer's environment has Spark installed.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

_RUN_WITHOUT_SPARK = textwrap.dedent(
    """
    import sys
    for name in ("pyspark", "delta", "py4j"):
        sys.modules[name] = None  # make `import pyspark` raise ImportError
    from seeknal.cli.main import main
    sys.argv = ["seeknal", *sys.argv[1:]]
    main()
    """
)


def _seeknal_without_spark(project: Path, *args: str) -> subprocess.CompletedProcess:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("ATLAS_", "KEYCLOAK_"))
    }
    env.update(
        HOME=str(project / "home"),
        SEEKNAL_BASE_CONFIG_PATH=str(project / "home" / ".seeknal"),
        PYTHONPATH=os.pathsep.join(sys.path),
    )
    return subprocess.run(
        [sys.executable, "-c", _RUN_WITHOUT_SPARK, *args],
        cwd=project,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )


@pytest.fixture
def duckdb_project(tmp_path: Path) -> Path:
    project = tmp_path / "proj"
    (project / "home").mkdir(parents=True)
    (project / "data").mkdir()
    for sub in ("sources", "transforms", "feature_groups", "rules"):
        (project / "seeknal" / sub).mkdir(parents=True)
    (project / "seeknal_project.yml").write_text(
        "name: nospark\nversion: 1.0.0\nprofile: default\nconfig-version: 1\n"
        "state_backend: file\n"
    )
    (project / "data" / "users.csv").write_text(
        "user_id,name,event_time\n1,Ann,2026-09-01\n2,Budi,2026-09-02\n"
    )
    (project / "seeknal" / "sources" / "raw_users.yml").write_text(
        "kind: source\nname: raw_users\nsource: csv\ntable: data/users.csv\n"
    )
    (project / "seeknal" / "transforms" / "clean_users.yml").write_text(
        "kind: transform\nname: clean_users\n"
        "transform: SELECT user_id, name, CAST(event_time AS TIMESTAMP) AS event_time "
        "FROM ref('source.raw_users')\n"
        "inputs:\n  - ref: source.raw_users\n"
    )
    (project / "seeknal" / "feature_groups" / "user_features.yml").write_text(
        "kind: feature_group\nname: user_features\n"
        "entity:\n  name: user\n  join_keys: [user_id]\n"
        "materialization:\n  event_time_col: event_time\n"
        "inputs:\n  - ref: transform.clean_users\n"
    )
    (project / "seeknal" / "rules" / "no_null_names.yml").write_text(
        "kind: rule\nname: no_null_names\n"
        "inputs:\n  - ref: transform.clean_users\n"
        'rule:\n  type: "null"\n  columns: [name]\n'
        "params:\n  severity: error\n"
    )
    return project


def test_run_duckdb_pipeline_with_feature_group_and_rule_without_spark(duckdb_project):
    result = _seeknal_without_spark(duckdb_project, "run")

    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert "Executed:       4" in output
    assert "pyspark" not in output.lower()


def test_spark_only_command_explains_missing_extra(duckdb_project):
    result = _seeknal_without_spark(duckdb_project, "version", "list", "user_features")

    output = " ".join((result.stdout + result.stderr).split())
    assert result.returncode == 1
    assert "needs the optional Spark extra" in output
    assert "ModuleNotFoundError" not in output


def test_spark_engine_feature_group_reports_missing_extra(monkeypatch):
    from types import SimpleNamespace

    from seeknal.workflow.executors.base import ExecutorExecutionError
    from seeknal.workflow.executors.feature_group_executor import FeatureGroupExecutor

    monkeypatch.setitem(sys.modules, "seeknal.featurestore.feature_group", None)
    executor = FeatureGroupExecutor.__new__(FeatureGroupExecutor)
    executor.node = SimpleNamespace(id="feature_group.user_features", name="user_features")

    with pytest.raises(ExecutorExecutionError, match=r"seeknal\[spark\]"):
        executor._execute_spark(source_df=None, config={})
