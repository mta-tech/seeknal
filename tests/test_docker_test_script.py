"""Check the test build context without invoking Docker."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


def test_test_image_excludes_secrets_and_local_state(tmp_path):
    """Keep sensitive files out of both the tar context and Docker COPY."""
    repo = tmp_path / "repo"
    scripts = repo / "scripts"
    scripts.mkdir(parents=True)
    script = scripts / "test-in-docker.sh"
    shutil.copyfile(Path(__file__).resolve().parents[1] / "scripts" / script.name, script)
    excluded = [
        ".env", ".env.production", ".env.backup-test", ".env.example",
        ".claude/state.json", ".omc/state.json", ".omx/state.json",
        ".codex/state.json", ".agents/state.json",
        "node_modules/package/index.js", "src/ask/tui/node_modules/package/index.js",
        ".venv/bin/python", ".venv-test/bin/python", ".venv312/bin/python",
        ".worktrees/other/file", "LANE_REPORT.md", "nested/.env.local",
        "nested/.claude/state.json",
    ]
    for name in [*excluded, "tests/fixture.txt", "docs/cli/index.md"]:
        path = repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("synthetic fixture")

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(
        f"#!{sys.executable}\n"
        "import json, os, pathlib, sys\n"
        "if sys.argv[1] == 'build':\n"
        "    context = pathlib.Path(sys.argv[-1])\n"
        "    pathlib.Path(os.environ['CONTEXT_RECORD']).write_text(json.dumps({\n"
        "        'path': str(context),\n"
        "        'files': [str(p.relative_to(context)) for p in context.rglob('*')],\n"
        "        'ignore': (context / '.dockerignore').read_text().splitlines(),\n"
        "    }))\n"
        "sys.exit(int(os.environ.get('DOCKER_STUB_EXIT', '0')))\n"
    )
    docker.chmod(0o755)
    record = tmp_path / "context.json"
    temp_dir = tmp_path / "contexts"
    temp_dir.mkdir()
    env = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "CONTEXT_RECORD": str(record),
        "TMPDIR": str(temp_dir),
    }
    result = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    context = json.loads(record.read_text())
    assert not set(excluded).intersection(context["files"])
    assert {"tests/fixture.txt", "docs/cli/index.md"}.issubset(context["files"])
    assert {
        ".env", ".env.*", ".claude", ".omc", ".omx", ".codex", ".agents",
        "node_modules", ".venv*", ".worktrees", "LANE_REPORT.md",
        "**/.env", "**/.env.*", "**/node_modules",
    }.issubset(context["ignore"])
    assert not Path(context["path"]).exists()

    # Failed builds must also remove their temporary context.
    result = subprocess.run(
        ["bash", str(script)], env={**env, "DOCKER_STUB_EXIT": "1"},
        capture_output=True, text=True,
    )
    assert result.returncode == 1
    assert not Path(json.loads(record.read_text())["path"]).exists()
