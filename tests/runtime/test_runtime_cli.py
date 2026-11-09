"""Tests for seeknal.runtime CLI commands (validate and resolve)."""

import copy
import hashlib
from pathlib import Path
import pytest
import yaml

from seeknal.runtime.__main__ import main


def make_manifest_yaml(name: str, channel: str = "stable") -> str:
    digest_hex = hashlib.sha256(name.encode()).hexdigest()
    data = {
        "apiVersion": "atlas.io/v1",
        "kind": "RuntimeRelease",
        "metadata": {
            "name": name,
            "channel": channel,
        },
        "seeknal": {
            "version": "2.14.0",
            "commit": "a" * 40,
            "lock_sha256": "b" * 64,
        },
        "python": "3.11.9",
        "images": {
            "atlas-rt": {
                "digest": f"sha256:{digest_hex}",
            },
            "atlas-rt-serving": {
                "digest": f"sha256:{'1' * 64}",
            },
        },
        "engines": {
            "duckdb": "1.4.4",
        },
        "contracts": {
            "grant": ["v2"],
            "cube_mode": ["jwt"],
        },
        "serving": {
            "grant_max_lifetime_seconds": 60,
        },
        "acu_weights": {
            "job": 1.0,
            "session": 1.5,
            "warehouse": 2.0,
            "sandbox": 1.0,
        },
        "licence_scan": "clean",
    }
    return yaml.dump(data)


# --- validate command tests ---


def test_cli_validate_single_valid(capsys):
    valid_file = "src/seeknal/runtime/examples/atlas-rt-1.0.0.yaml"
    exit_code = main(["validate", valid_file])
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "OK atlas-rt/1.0.0" in out


def test_cli_validate_multiple_valid(tmp_path: Path, capsys):
    f1 = tmp_path / "rt-1.yaml"
    f2 = tmp_path / "rt-2.yaml"
    f1.write_text(make_manifest_yaml("atlas-rt/1.0.1"))
    f2.write_text(make_manifest_yaml("atlas-rt/1.0.2"))

    exit_code = main(["validate", str(f1), str(f2)])
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "OK atlas-rt/1.0.1" in out
    assert "OK atlas-rt/1.0.2" in out


def test_cli_validate_invalid_manifest(tmp_path: Path, capsys):
    bad = tmp_path / "bad.yaml"
    bad.write_text("apiVersion: atlas.io/v999\nkind: Unknown")

    exit_code = main(["validate", str(bad)])
    assert exit_code == 1

    out = capsys.readouterr().out
    assert "ERR" in out
    assert "apiVersion" in out


def test_cli_validate_mixed(tmp_path: Path, capsys):
    good = tmp_path / "good.yaml"
    bad = tmp_path / "bad.yaml"
    good.write_text(make_manifest_yaml("atlas-rt/1.0.0"))
    bad.write_text("invalid: yaml: doc")

    exit_code = main(["validate", str(good), str(bad)])
    assert exit_code == 1

    out = capsys.readouterr().out
    assert "OK atlas-rt/1.0.0" in out
    assert "ERR" in out


def test_cli_validate_missing_file(capsys):
    exit_code = main(["validate", "nonexistent-file.yaml"])
    assert exit_code == 1

    out = capsys.readouterr().out
    assert "ERR" in out
    assert "file not found" in out


# --- resolve command tests ---


def test_cli_resolve_line_pin(tmp_path: Path, capsys):
    # Setup project
    proj_dir = tmp_path / "project"
    proj_dir.mkdir()
    (proj_dir / "seeknal_project.yml").write_text(
        yaml.dump({"runtime": "atlas-rt/1.0"})
    )

    # Setup releases
    rel_dir = tmp_path / "releases"
    rel_dir.mkdir()
    (rel_dir / "rt-1.0.0.yaml").write_text(make_manifest_yaml("atlas-rt/1.0.0"))
    (rel_dir / "rt-1.0.1.yaml").write_text(make_manifest_yaml("atlas-rt/1.0.1"))

    exit_code = main(
        ["resolve", "--project", str(proj_dir), "--releases", str(rel_dir)]
    )
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "atlas-rt/1.0.1" in out
    assert "ghcr.io/mta-tech/atlas-rt@sha256:" in out


def test_cli_resolve_env_override(tmp_path: Path, capsys):
    proj_dir = tmp_path / "project"
    proj_dir.mkdir()
    (proj_dir / "seeknal_project.yml").write_text(
        yaml.dump(
            {
                "runtime": "atlas-rt/1.0",
                "environments": {
                    "prod": {"runtime": "atlas-rt/1.0.5"},
                },
            }
        )
    )

    rel_dir = tmp_path / "releases"
    rel_dir.mkdir()
    (rel_dir / "rt-1.0.1.yaml").write_text(make_manifest_yaml("atlas-rt/1.0.1"))
    (rel_dir / "rt-1.0.5.yaml").write_text(
        make_manifest_yaml("atlas-rt/1.0.5", channel="preview")
    )

    exit_code = main(
        [
            "resolve",
            "--project",
            str(proj_dir),
            "--releases",
            str(rel_dir),
            "--env",
            "prod",
        ]
    )
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "atlas-rt/1.0.5" in out


def test_cli_resolve_custom_registry(tmp_path: Path, capsys):
    proj_dir = tmp_path / "project"
    proj_dir.mkdir()
    (proj_dir / "seeknal_project.yml").write_text(
        yaml.dump({"runtime": "atlas-rt/1.0.0"})
    )

    rel_dir = tmp_path / "releases"
    rel_dir.mkdir()
    (rel_dir / "rt-1.0.0.yaml").write_text(make_manifest_yaml("atlas-rt/1.0.0"))

    exit_code = main(
        [
            "resolve",
            "--project",
            str(proj_dir),
            "--releases",
            str(rel_dir),
            "--registry",
            "registry.internal.corp/seeknal",
        ]
    )
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "registry.internal.corp/seeknal/atlas-rt@sha256:" in out


def test_cli_resolve_no_pin_exits_2(tmp_path: Path, capsys):
    proj_dir = tmp_path / "project"
    proj_dir.mkdir()
    (proj_dir / "seeknal_project.yml").write_text(yaml.dump({"name": "unpinned_proj"}))

    rel_dir = tmp_path / "releases"
    rel_dir.mkdir()

    exit_code = main(
        ["resolve", "--project", str(proj_dir), "--releases", str(rel_dir)]
    )
    assert exit_code == 2

    captured = capsys.readouterr()
    assert "no runtime pin" in (captured.out + captured.err)


def test_cli_resolve_unresolvable_pin_exits_1(tmp_path: Path, capsys):
    proj_dir = tmp_path / "project"
    proj_dir.mkdir()
    (proj_dir / "seeknal_project.yml").write_text(
        yaml.dump({"runtime": "atlas-rt/9.9"})
    )

    rel_dir = tmp_path / "releases"
    rel_dir.mkdir()
    (rel_dir / "rt-1.0.0.yaml").write_text(make_manifest_yaml("atlas-rt/1.0.0"))

    exit_code = main(
        ["resolve", "--project", str(proj_dir), "--releases", str(rel_dir)]
    )
    assert exit_code == 1


def test_cli_resolve_skips_invalid_manifest(tmp_path: Path, capsys):
    proj_dir = tmp_path / "project"
    proj_dir.mkdir()
    (proj_dir / "seeknal_project.yml").write_text(
        yaml.dump({"runtime": "atlas-rt/1.0.0"})
    )

    rel_dir = tmp_path / "releases"
    rel_dir.mkdir()
    (rel_dir / "rt-1.0.0.yaml").write_text(make_manifest_yaml("atlas-rt/1.0.0"))
    (rel_dir / "broken.yaml").write_text("invalid_manifest_content")

    exit_code = main(
        ["resolve", "--project", str(proj_dir), "--releases", str(rel_dir)]
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "atlas-rt/1.0.0" in captured.out
    assert "broken.yaml" in captured.err


def test_cli_resolve_missing_releases_dir(tmp_path: Path, capsys):
    proj_dir = tmp_path / "project"
    proj_dir.mkdir()
    (proj_dir / "seeknal_project.yml").write_text(
        yaml.dump({"runtime": "atlas-rt/1.0.0"})
    )

    exit_code = main(
        [
            "resolve",
            "--project",
            str(proj_dir),
            "--releases",
            str(tmp_path / "nonexistent"),
        ]
    )
    assert exit_code == 1


def test_cli_invalid_arguments():
    # Calling with invalid arguments returns non-zero (2)
    exit_code = main(["unknown_subcommand"])
    assert exit_code == 2
