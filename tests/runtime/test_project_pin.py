"""Tests for runtime pin reading, resolution, and project release ID."""

from pathlib import Path
import tempfile
import pytest
import yaml

from seeknal.runtime.project_pin import (
    PinError,
    project_release_id,
    read_runtime_pin,
    resolve_pin,
)
from seeknal.runtime.release import ImageRef, RuntimeRelease


def make_release(name: str, channel: str = "stable") -> RuntimeRelease:
    parts = name.split("/")[1].split(".")
    version = (int(parts[0]), int(parts[1]), int(parts[2]))
    return RuntimeRelease(
        name=name,
        version=version,
        channel=channel,
        images={
            "atlas-rt": ImageRef(
                digest=f"sha256:{name.replace('/', '-').replace('.', '')[:32].ljust(64, '0')}"
            ),
            "atlas-rt-serving": ImageRef(digest=f"sha256:{'1' * 64}"),
        },
        contracts={},
        acu_weights={"job": 1.0, "session": 1.0, "warehouse": 1.0, "sandbox": 1.0},
        grant_max_lifetime_seconds=60,
        raw={},
    )


# --- read_runtime_pin tests ---


def test_read_runtime_pin_top_level(tmp_path: Path):
    project_file = tmp_path / "seeknal_project.yml"
    project_file.write_text(yaml.dump({"name": "test_proj", "runtime": "atlas-rt/1.0"}))

    pin = read_runtime_pin(tmp_path)
    assert pin == "atlas-rt/1.0"


def test_read_runtime_pin_environment_override(tmp_path: Path):
    project_file = tmp_path / "seeknal_project.yml"
    project_file.write_text(
        yaml.dump(
            {
                "runtime": "atlas-rt/1.0",
                "environments": {
                    "dev": {"principal": "auto"},
                    "prod": {"principal": "auto", "runtime": "atlas-rt/1.0.3"},
                },
            }
        )
    )

    # Top-level when env is None
    assert read_runtime_pin(tmp_path) == "atlas-rt/1.0"

    # Fallback to top-level when env has no runtime override
    assert read_runtime_pin(tmp_path, environment="dev") == "atlas-rt/1.0"

    # Override takes precedence
    assert read_runtime_pin(tmp_path, environment="prod") == "atlas-rt/1.0.3"

    # Nonexistent env falls back to top-level
    assert read_runtime_pin(tmp_path, environment="staging") == "atlas-rt/1.0"


def test_read_runtime_pin_no_pin(tmp_path: Path):
    project_file = tmp_path / "seeknal_project.yml"
    project_file.write_text(yaml.dump({"name": "test_proj"}))

    assert read_runtime_pin(tmp_path) is None


def test_read_runtime_pin_no_project_file(tmp_path: Path):
    assert read_runtime_pin(tmp_path) is None


def test_read_runtime_pin_tolerates_extra_keys(tmp_path: Path):
    project_file = tmp_path / "seeknal_project.yml"
    project_file.write_text(
        yaml.dump(
            {
                "name": "my_big_project",
                "description": "testing runtime pin tolerance",
                "feature_store": {"online": "redis", "offline": "duckdb"},
                "runtime": "atlas-rt/2.1.0",
                "custom_metadata": [1, 2, 3],
            }
        )
    )

    assert read_runtime_pin(tmp_path) == "atlas-rt/2.1.0"


def test_read_runtime_pin_invalid_pin_format(tmp_path: Path):
    project_file = tmp_path / "seeknal_project.yml"
    project_file.write_text(yaml.dump({"runtime": "not-a-valid-pin"}))

    with pytest.raises(PinError, match="Invalid runtime pin"):
        read_runtime_pin(tmp_path)


def test_read_runtime_pin_invalid_env_pin_format(tmp_path: Path):
    project_file = tmp_path / "seeknal_project.yml"
    project_file.write_text(
        yaml.dump(
            {
                "runtime": "atlas-rt/1.0",
                "environments": {
                    "prod": {"runtime": "invalid_pin_123"},
                },
            }
        )
    )

    with pytest.raises(PinError, match="Invalid runtime pin"):
        read_runtime_pin(tmp_path, environment="prod")


# --- resolve_pin tests ---


@pytest.fixture
def sample_releases():
    return [
        make_release("atlas-rt/1.0.0", channel="stable"),
        make_release("atlas-rt/1.0.1", channel="stable"),
        make_release("atlas-rt/1.0.2", channel="stable"),
        make_release("atlas-rt/1.0.3", channel="preview"),
        make_release("atlas-rt/1.1.0", channel="stable"),
        make_release("atlas-rt/1.1.1", channel="lts"),
        make_release("atlas-rt/2.0.0", channel="stable"),
    ]


def test_resolve_line_pin_highest_patch_on_channel(sample_releases):
    # On stable channel, 1.0.2 is the highest patch
    res = resolve_pin("atlas-rt/1.0", sample_releases, channel="stable")
    assert res.name == "atlas-rt/1.0.2"

    # On preview channel, 1.0.3 is available
    res_preview = resolve_pin("atlas-rt/1.0", sample_releases, channel="preview")
    assert res_preview.name == "atlas-rt/1.0.3"

    # On lts channel, 1.1.1 is available
    res_lts = resolve_pin("atlas-rt/1.1", sample_releases, channel="lts")
    assert res_lts.name == "atlas-rt/1.1.1"


def test_resolve_line_pin_no_match(sample_releases):
    with pytest.raises(PinError, match="No release found"):
        resolve_pin("atlas-rt/9.9", sample_releases, channel="stable")

    with pytest.raises(PinError, match="No release found"):
        resolve_pin("atlas-rt/1.0", sample_releases, channel="lts")


def test_resolve_exact_pin_any_channel(sample_releases):
    # Exact pin matches preview release even when resolving on default stable channel
    res = resolve_pin("atlas-rt/1.0.3", sample_releases)
    assert res.name == "atlas-rt/1.0.3"
    assert res.channel == "preview"

    # Exact pin matches LTS release
    res_lts = resolve_pin("atlas-rt/1.1.1", sample_releases)
    assert res_lts.name == "atlas-rt/1.1.1"
    assert res_lts.channel == "lts"


def test_resolve_exact_pin_no_match(sample_releases):
    with pytest.raises(PinError, match="No release found"):
        resolve_pin("atlas-rt/1.0.99", sample_releases)


def test_resolve_invalid_pin_format(sample_releases):
    with pytest.raises(PinError, match="Invalid runtime pin format"):
        resolve_pin("atlas-rt/1", sample_releases)

    with pytest.raises(PinError, match="Invalid runtime pin format"):
        resolve_pin("random-string", sample_releases)


# --- project_release_id tests ---


def test_project_release_id_deterministic():
    release = make_release("atlas-rt/1.0.0", channel="stable")
    manifest_sha = "abcdef0123456789fedcba987654321011112222333344445555666677778888"
    commit_sha = "9fce3a1b4d2e8f1c0a5b6c7d8e9f0a1b2c3d4e5f"

    id1 = project_release_id(
        project_id="my-project",
        commit_sha=commit_sha,
        release=release,
        manifest_sha256=manifest_sha,
    )
    id2 = project_release_id(
        project_id="my-project",
        commit_sha=commit_sha,
        release=release,
        manifest_sha256=manifest_sha,
    )

    assert id1 == id2
    expected = f"my-project@{commit_sha}+atlas-rt/1.0.0@{release.images['atlas-rt'].digest}#{manifest_sha[:16]}"
    assert id1 == expected
    assert len(manifest_sha[:16]) == 16
