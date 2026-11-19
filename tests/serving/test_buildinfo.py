"""Unit tests for Seeknal Serving buildinfo and --version CLI."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from seeknal.serving.__main__ import main
from seeknal.serving.buildinfo import (
    BUILD_INFO_KEYS,
    EXPECTED_DUCKDB_VERSION,
    format_build_info,
    get_build_info,
    write_build_info,
)

EXPECTED_KEYS = {"version", "git_sha", "build_time", "duckdb_version", "lock_sha256"}


def test_expected_duckdb_version_constant():
    assert EXPECTED_DUCKDB_VERSION == "2.0.0"
    assert set(BUILD_INFO_KEYS) == EXPECTED_KEYS


def test_get_build_info_fallback_without_metadata_never_calls_git(
    tmp_path, monkeypatch
):
    repo_root = Path(__file__).resolve().parents[2]
    uv_lock = repo_root / "uv.lock"
    expected_lock_sha = hashlib.sha256(uv_lock.read_bytes()).hexdigest()

    def _forbid_subprocess(*args, **kwargs):
        raise AssertionError("get_build_info must never shell out to git or subprocess")

    monkeypatch.setattr(subprocess, "run", _forbid_subprocess)
    monkeypatch.setattr(subprocess, "check_output", _forbid_subprocess)
    monkeypatch.setattr(subprocess, "Popen", _forbid_subprocess)
    monkeypatch.delenv("SEEKNAL_BUILDINFO_PATH", raising=False)

    missing_meta = tmp_path / "nonexistent_buildinfo.json"
    info = get_build_info(missing_meta, lock_path=uv_lock)

    assert set(info.keys()) == EXPECTED_KEYS
    assert info["version"] == "2.14.0"
    assert info["git_sha"] == "unknown"
    assert info["build_time"] == "unknown"
    assert info["duckdb_version"] == duckdb.__version__ == "2.0.0"
    assert info["lock_sha256"] == expected_lock_sha


def test_get_build_info_missing_lockfile_and_package_not_found(tmp_path, monkeypatch):
    def _not_installed(name: str) -> str:
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, "version", _not_installed)

    info = get_build_info(
        tmp_path / "missing_buildinfo.json",
        lock_path=tmp_path / "missing_uv.lock",
    )
    assert set(info.keys()) == EXPECTED_KEYS
    assert info == {
        "version": "2.14.0",
        "git_sha": "unknown",
        "build_time": "unknown",
        "duckdb_version": str(duckdb.__version__),
        "lock_sha256": "unknown",
    }


def test_write_and_read_build_info_roundtrip_and_env_override(tmp_path, monkeypatch):
    fake_lock = tmp_path / "uv.lock"
    fake_lock.write_bytes(b"version = 1\n")
    expected_lock_sha = hashlib.sha256(b"version = 1\n").hexdigest()

    meta_file = tmp_path / "buildinfo.json"
    written = write_build_info(
        meta_file,
        git_sha="deadbeefcafe1234",
        build_time="2026-10-25T00:00:00Z",
        lock_path=fake_lock,
        version="2.14.0",
        duckdb_version="2.0.0",
    )
    assert set(written.keys()) == EXPECTED_KEYS
    assert written["lock_sha256"] == expected_lock_sha
    assert meta_file.is_file()

    # Read via SEEKNAL_BUILDINFO_PATH env override with missing runtime uv.lock
    monkeypatch.setenv("SEEKNAL_BUILDINFO_PATH", str(meta_file))
    loaded = get_build_info(lock_path=tmp_path / "absent.lock")
    assert loaded == {
        "version": "2.14.0",
        "git_sha": "deadbeefcafe1234",
        "build_time": "2026-10-25T00:00:00Z",
        "duckdb_version": "2.0.0",
        "lock_sha256": expected_lock_sha,
    }


def test_corrupt_buildinfo_file_falls_back_gracefully(tmp_path):
    bad_meta = tmp_path / "corrupt.json"
    bad_meta.write_text("{not-valid-json", encoding="utf-8")

    info = get_build_info(bad_meta, lock_path=tmp_path / "absent.lock")
    assert set(info.keys()) == EXPECTED_KEYS
    assert info["git_sha"] == "unknown"
    assert info["build_time"] == "unknown"
    assert info["lock_sha256"] == "unknown"


def test_cli_version_human_and_json_without_config(tmp_path, monkeypatch, capsys):
    meta_file = tmp_path / "buildinfo.json"
    meta_file.write_text(
        json.dumps(
            {
                "version": "2.14.0",
                "git_sha": "abc123456789",
                "build_time": "2026-10-25T12:34:56Z",
                "duckdb_version": "2.0.0",
                "lock_sha256": "a" * 64,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SEEKNAL_BUILDINFO_PATH", str(meta_file))

    ret_human = main(["--version"])
    assert ret_human == 0
    out_human = capsys.readouterr().out.strip()
    assert "\n" not in out_human
    assert "2.14.0" in out_human
    assert "abc123456789" in out_human
    assert "2.0.0" in out_human
    assert ("a" * 64) in out_human

    ret_json = main(["--version", "--json"])
    assert ret_json == 0
    out_json = capsys.readouterr().out.strip()
    parsed = json.loads(out_json)
    assert set(parsed.keys()) == EXPECTED_KEYS
    assert parsed == {
        "version": "2.14.0",
        "git_sha": "abc123456789",
        "build_time": "2026-10-25T12:34:56Z",
        "duckdb_version": "2.0.0",
        "lock_sha256": "a" * 64,
    }


def test_cli_missing_config_still_fails_when_version_not_passed(capsys):
    assert main([]) == 2
    err = capsys.readouterr().err
    assert "--config" in err

    assert main(["--check"]) == 2
    err_check = capsys.readouterr().err
    assert "--config" in err_check


def test_cli_module_subprocess_version_json():
    repo_root = Path(__file__).resolve().parents[2]
    env = dict(os.environ, PYTHONPATH=str(repo_root / "src"))
    env.pop("SEEKNAL_BUILDINFO_PATH", None)

    proc = subprocess.run(
        [sys.executable, "-m", "seeknal.serving", "--version", "--json"],
        capture_output=True,
        text=True,
        check=False,
        cwd=str(repo_root),
        env=env,
    )
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout.strip())
    assert set(payload.keys()) == EXPECTED_KEYS
    assert payload["version"] == "2.14.0"
    assert payload["duckdb_version"] == "2.0.0"
    assert len(payload["lock_sha256"]) == 64


def test_format_build_info():
    sample = {
        "version": "2.14.0",
        "git_sha": "111",
        "build_time": "2026-01-01T00:00:00Z",
        "duckdb_version": "2.0.0",
        "lock_sha256": "f" * 64,
    }
    line = format_build_info(sample, as_json=False)
    assert "\n" not in line
    assert json.loads(format_build_info(sample, as_json=True)) == sample
