"""Tests for Atlas Runtime release pipeline, manifest builder, Dockerfiles, and CI workflow."""

from __future__ import annotations

import hashlib
import re
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.release.build_manifest import compute_file_sha256, main
from seeknal.runtime.release import RuntimeRelease, load_manifest, parse_manifest

RT_DIGEST = "sha256:" + "a" * 64
SERVING_DIGEST = "sha256:" + "b" * 64
AGENT_DIGEST = "sha256:" + "c" * 64
COMMIT_SHA = "0123456789abcdef0123456789abcdef01234567"


def _make_build_args(
    out_path: Path,
    release: str = "atlas-rt/1.0.0",
    licence_scan: str = "clean",
    include_serving: bool = True,
    rt_ref: str | None = None,
) -> list[str]:
    rt_image = rt_ref or f"atlas-rt=ghcr.io/mta-tech/atlas-rt@{RT_DIGEST}"
    args = [
        "--release",
        release,
        "--channel",
        "stable",
        "--commit",
        COMMIT_SHA,
        "--lock",
        str(REPO_ROOT / "uv.lock"),
        "--image",
        rt_image,
        "--extension",
        "iceberg=1095c1fa",
        "--extension",
        "httpfs=13f8a810",
        "--contracts-file",
        str(REPO_ROOT / "docker/release-contracts.yaml"),
        "--acu-weights-file",
        str(REPO_ROOT / "docker/release-acu-weights.yaml"),
        "--licence-scan",
        licence_scan,
        "--out",
        str(out_path),
    ]
    if include_serving:
        args.extend(
            [
                "--image",
                f"atlas-rt-serving=ghcr.io/mta-tech/atlas-rt-serving@{SERVING_DIGEST}",
            ]
        )
    return args


class TestBuildManifest:
    """Tests for scripts/release/build_manifest.py module function main(argv)."""

    def test_build_manifest_success_and_digests(self, tmp_path: Path):
        out_file = tmp_path / "manifest.yaml"
        args = _make_build_args(out_file)
        args.extend(["--image", f"atlas-rt-agent=ghcr.io/mta-tech/atlas-rt-agent@{AGENT_DIGEST}"])

        exit_code = main(args)
        assert exit_code == 0
        assert out_file.is_file()

        release = load_manifest(out_file)
        assert isinstance(release, RuntimeRelease)
        assert release.name == "atlas-rt/1.0.0"
        assert release.channel == "stable"
        assert release.images["atlas-rt"].digest == RT_DIGEST
        assert release.images["atlas-rt-serving"].digest == SERVING_DIGEST
        assert release.images["atlas-rt-agent"].digest == AGENT_DIGEST

        raw = release.raw
        expected_lock_sha = compute_file_sha256(REPO_ROOT / "uv.lock")
        assert raw["seeknal"]["lock_sha256"] == expected_lock_sha
        assert raw["licence_scan"] == "clean"
        assert raw["serving"]["grant_max_lifetime_seconds"] == 60
        assert raw["acu_weights"]["job"] == 1.0

    def test_missing_serving_fails(self, tmp_path: Path):
        out_file = tmp_path / "manifest.yaml"
        args = _make_build_args(out_file, include_serving=False)

        exit_code = main(args)
        assert exit_code != 0

    def test_tag_reference_fails(self, tmp_path: Path):
        out_file = tmp_path / "manifest.yaml"
        args = _make_build_args(
            out_file,
            rt_ref="atlas-rt=ghcr.io/mta-tech/atlas-rt:latest",
        )

        exit_code = main(args)
        assert exit_code != 0

    def test_tag_only_image_name_fails(self, tmp_path: Path):
        out_file = tmp_path / "manifest.yaml"
        args = _make_build_args(
            out_file,
            rt_ref="atlas-rt=v1.0.0",
        )

        exit_code = main(args)
        assert exit_code != 0

    def test_licence_dirty_fails(self, tmp_path: Path):
        out_file = tmp_path / "manifest.yaml"
        args = _make_build_args(out_file, licence_scan="dirty")

        exit_code = main(args)
        assert exit_code != 0

    def test_lock_sha256_correctness(self, tmp_path: Path):
        out_file = tmp_path / "manifest.yaml"
        args = _make_build_args(out_file)

        exit_code = main(args)
        assert exit_code == 0

        manifest_data = yaml.safe_load(out_file.read_text(encoding="utf-8"))
        actual_lock_bytes = (REPO_ROOT / "uv.lock").read_bytes()
        expected_sha = hashlib.sha256(actual_lock_bytes).hexdigest()
        assert manifest_data["seeknal"]["lock_sha256"] == expected_sha

    def test_output_byte_stability_across_runs(self, tmp_path: Path):
        out1 = tmp_path / "m1.yaml"
        out2 = tmp_path / "m2.yaml"

        args1 = _make_build_args(out1)
        args2 = _make_build_args(out2)

        exit1 = main(args1)
        exit2 = main(args2)

        assert exit1 == 0
        assert exit2 == 0

        bytes1 = out1.read_bytes()
        bytes2 = out2.read_bytes()
        assert bytes1 == bytes2
        assert len(bytes1) > 0


def _parse_env_keys(dockerfile_content: str) -> list[str]:
    """Extract environment variable keys defined via ENV instructions."""
    keys: list[str] = []
    # Match ENV instructions spanning multiple lines
    lines = dockerfile_content.splitlines()
    in_env = False
    current_env = ""
    for line in lines:
        stripped = line.strip()
        if not in_env:
            if stripped.startswith("ENV "):
                in_env = True
                current_env = stripped[4:]
        else:
            current_env += " " + stripped

        if in_env:
            if not stripped.endswith("\\"):
                in_env = False
                # Parse key=val or key val tokens
                tokens = current_env.replace("\\", " ").split()
                for token in tokens:
                    if "=" in token:
                        key = token.split("=", 1)[0].strip()
                        if key:
                            keys.append(key)
                current_env = ""
    return keys


class TestDockerfileStaticChecks:
    """Static checks for docker/Dockerfile.rt and docker/Dockerfile.agent."""

    @pytest.fixture(scope="class")
    def rt_content(self) -> str:
        return (REPO_ROOT / "docker/Dockerfile.rt").read_text(encoding="utf-8")

    @pytest.fixture(scope="class")
    def agent_content(self) -> str:
        return (REPO_ROOT / "docker/Dockerfile.agent").read_text(encoding="utf-8")

    def test_base_is_not_latest(self, rt_content: str, agent_content: str):
        for name, content in [("Dockerfile.rt", rt_content), ("Dockerfile.agent", agent_content)]:
            for line in content.splitlines():
                stripped = line.strip()
                if stripped.startswith("ARG BASE_IMAGE="):
                    val = stripped.split("=", 1)[1]
                    assert not val.endswith(":latest"), f"{name} ARG BASE_IMAGE cannot use :latest"
                elif stripped.startswith("FROM ") and not stripped.startswith("FROM ${"):
                    image = stripped.split()[1]
                    assert not image.endswith(":latest"), f"{name} FROM cannot use :latest"

    def test_user_is_numeric_and_non_zero(self, rt_content: str, agent_content: str):
        user_pattern = re.compile(r"^USER\s+(\d+)(?::(\d+))?$")
        for name, content in [("Dockerfile.rt", rt_content), ("Dockerfile.agent", agent_content)]:
            found = False
            for line in content.splitlines():
                m = user_pattern.match(line.strip())
                if m:
                    uid = int(m.group(1))
                    assert uid > 0, f"{name}: USER must be non-zero (non-root), got {uid}"
                    found = True
            assert found, f"{name}: numeric USER instruction not found"

    def test_copy_sources_are_limited(self, rt_content: str, agent_content: str):
        for name, content in [("Dockerfile.rt", rt_content), ("Dockerfile.agent", agent_content)]:
            for line in content.splitlines():
                stripped = line.strip()
                if stripped.startswith("COPY "):
                    assert ".env" not in stripped, f"{name}: COPY instruction contains .env: {line}"
                    parts = stripped.split()[1:]
                    assert parts[0] != ".", f"{name}: whole-repo COPY . is forbidden: {line}"

    def test_extensions_installed_at_build_time_in_rt(self, rt_content: str):
        assert "INSTALL iceberg" in rt_content
        assert "INSTALL httpfs" in rt_content
        assert "DUCKDB_EXTENSION_DIRECTORY" in rt_content

    def test_atlas_ask_mode_governed_in_agent(self, agent_content: str):
        assert "ATLAS_ASK_MODE=governed" in agent_content

    def test_no_sensitive_env_keys(self, rt_content: str, agent_content: str):
        sensitive_re = re.compile(r"(?i)(secret|token|password|passwd|key)")
        for name, content in [("Dockerfile.rt", rt_content), ("Dockerfile.agent", agent_content)]:
            keys = _parse_env_keys(content)
            for k in keys:
                assert not sensitive_re.search(
                    k
                ), f"{name}: ENV key '{k}' matches sensitive pattern"


class TestWorkflowStaticChecks:
    """Static checks for .github/workflows/atlas-rt-release.yml."""

    @pytest.fixture(scope="class")
    def workflow_data(self) -> dict:
        wf_path = REPO_ROOT / ".github/workflows/atlas-rt-release.yml"
        assert wf_path.is_file(), "atlas-rt-release.yml workflow file missing"
        return yaml.safe_load(wf_path.read_text(encoding="utf-8"))

    @pytest.fixture(scope="class")
    def workflow_text(self) -> str:
        wf_path = REPO_ROOT / ".github/workflows/atlas-rt-release.yml"
        return wf_path.read_text(encoding="utf-8")

    def test_workflow_triggers(self, workflow_data: dict):
        on = workflow_data.get("on") or workflow_data.get(True)
        assert on is not None, "Workflow triggers missing"
        assert "workflow_dispatch" in on, "workflow_dispatch trigger missing"
        assert "push" in on, "push trigger missing"
        push_tags = on["push"].get("tags", [])
        assert any("atlas-rt/v*" in tag for tag in push_tags), "push tags must include atlas-rt/v*"

    def test_permissions_include_id_token_write(self, workflow_data: dict):
        top_perms = workflow_data.get("permissions", {})
        job_perms = workflow_data.get("jobs", {}).get("release", {}).get("permissions", {})
        combined = {**top_perms, **job_perms}
        assert combined.get("id-token") == "write", "Workflow must include id-token: write"

    def test_every_build_push_action_has_push_true(self, workflow_data: dict):
        steps = workflow_data.get("jobs", {}).get("release", {}).get("steps", [])
        build_steps = [s for s in steps if "docker/build-push-action" in s.get("uses", "")]
        assert len(build_steps) >= 3, "Expected at least 3 build-push-action steps (rt, serving, agent)"
        for s in build_steps:
            with_block = s.get("with", {})
            push_val = with_block.get("push")
            assert push_val is True or push_val == "true", f"Step {s.get('name')} must have push: true"

    def test_cosign_sign_references_digest_outputs_never_latest(self, workflow_text: str):
        # Locate cosign sign steps (image signing, not sign-blob)
        assert "cosign sign --yes" in workflow_text
        lines = workflow_text.splitlines()
        found_sign = False
        for line in lines:
            if "cosign sign" in line and "sign-blob" not in line:
                assert "@${{" in line, "cosign sign step must reference @${{ digest outputs"
                assert ":latest" not in line, "cosign sign step must never reference :latest"
                found_sign = True
        assert found_sign, "cosign sign command for images not found"

    def test_build_manifest_invoked_with_image_digests(self, workflow_text: str):
        assert "scripts/release/build_manifest.py" in workflow_text
        assert "--image" in workflow_text
        # Ensure image digests are passed via build step outputs
        assert "@${{ steps.build_rt.outputs.digest }}" in workflow_text
        assert "@${{ steps.build_serving.outputs.digest }}" in workflow_text
        assert "@${{ steps.build_agent.outputs.digest }}" in workflow_text
