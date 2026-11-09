"""Tests for the atlas-rt-serving Docker image and dependencies."""

from __future__ import annotations

import importlib.metadata
import json
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

import packaging.requirements


def normalize_pep503(name: str) -> str:
    """Normalize distribution name per PEP 503."""
    return re.sub(r"[-_.]+", "-", name).lower()


def get_transitive_requirements(direct_names: set[str]) -> set[str]:
    """Compute transitive requirements using importlib.metadata.requires, ignoring extras."""
    reachable = set(direct_names)
    queue = list(direct_names)
    while queue:
        current = queue.pop(0)
        try:
            reqs = importlib.metadata.requires(current) or []
        except importlib.metadata.PackageNotFoundError:
            continue
        for req_str in reqs:
            req = packaging.requirements.Requirement(req_str)
            if req.marker and not req.marker.evaluate({"extra": ""}):
                continue
            norm = normalize_pep503(req.name)
            if norm not in reachable:
                reachable.add(norm)
                queue.append(norm)
    return reachable


SUBPROCESS_CLOSURE_SCRIPT = """
import sys

blocked = {
    "pydantic_ai",
    "pydantic_deep",
    "telegram",
    "redis",
    "openai",
    "google.genai",
    "sklearn",
    "scipy",
    "pyspark",
    "prefect",
    "temporalio",
    "ddgs",
    "anthropic",
}

class BlockedFinder:
    def find_spec(self, fullname, path=None, target=None):
        for b in blocked:
            if fullname == b or fullname.startswith(b + "."):
                raise ImportError(f"Blocked import: {fullname}")
        return None

sys.meta_path.insert(0, BlockedFinder())

import seeknal.serving.__main__
import seeknal.serving.server.app
import seeknal.serving.server.config
import seeknal.serving.discovery

import json
import importlib.metadata

pkg_dist = importlib.metadata.packages_distributions()
loaded_top_levels = {
    mod.split(".")[0]
    for mod in sys.modules
    if mod.split(".")[0] not in sys.stdlib_module_names
}

loaded_dists = set()
for top in loaded_top_levels:
    for d in pkg_dist.get(top, []):
        loaded_dists.add(d)

print(json.dumps(sorted(loaded_dists)))
"""


def test_import_closure():
    """Verify Serving import closure with blocked non-serving packages and requirements coverage."""
    repo_root = Path(__file__).resolve().parents[2]
    reqs_path = repo_root / "docker" / "serving-requirements.txt"
    assert reqs_path.is_file(), f"Missing {reqs_path}"

    # Ensure docs/cli exists so hatchling build doesn't fail if triggered
    (repo_root / "docs" / "cli").mkdir(parents=True, exist_ok=True)

    proc = subprocess.run(
        [sys.executable, "-c", SUBPROCESS_CLOSURE_SCRIPT],
        capture_output=True,
        text=True,
        check=False,
        cwd=str(repo_root),
        env=dict(os.environ, PYTHONPATH=str(repo_root / "src")),
    )
    assert proc.returncode == 0, (
        f"Serving import closure subprocess failed (exit code {proc.returncode}):\n{proc.stderr}"
    )

    loaded_dists = json.loads(proc.stdout)
    assert loaded_dists, "Subprocess reported no loaded distributions"

    direct_reqs = set()
    with open(reqs_path, encoding="utf-8") as f:
        for line in f:
            line = line.split("#")[0].strip()
            if line:
                direct_reqs.add(normalize_pep503(line))

    assert direct_reqs, f"No requirements parsed from {reqs_path}"
    closure = get_transitive_requirements(direct_reqs)

    for dist in loaded_dists:
        norm_dist = normalize_pep503(dist)
        if norm_dist == "seeknal":
            continue
        assert norm_dist in closure, (
            f"Distribution '{dist}' (normalized: '{norm_dist}') loaded during import closure "
            f"is neither in serving-requirements.txt nor in its transitive dependency closure."
        )


def test_serving_requirements_file():
    """Verify that serving-requirements.txt is constrained to uv.lock and excludes forbidden packages."""
    repo_root = Path(__file__).resolve().parents[2]
    reqs_path = repo_root / "docker" / "serving-requirements.txt"
    lock_path = repo_root / "uv.lock"

    assert reqs_path.is_file(), f"Missing {reqs_path}"
    assert lock_path.is_file(), f"Missing {lock_path}"

    with open(reqs_path, encoding="utf-8") as f:
        lines = [line.split("#")[0].strip() for line in f]
    listed_names = [line for line in lines if line]

    assert listed_names, "serving-requirements.txt is empty"

    # Ensure no version specifiers are present (only distribution names)
    for name in listed_names:
        assert re.match(
            r"^[A-Za-z0-9_.-]+$", name
        ), f"Version specifiers or invalid characters found in requirement: {name}"

    with open(lock_path, "rb") as f:
        lock_data = tomllib.load(f)

    lock_packages = {
        normalize_pep503(p["name"]) for p in lock_data.get("package", [])
    }

    # Every listed name exists as a package in uv.lock
    for name in listed_names:
        norm_name = normalize_pep503(name)
        assert (
            norm_name in lock_packages
        ), f"Requirement '{name}' not found as package in uv.lock"

    # Forbidden packages that must not be listed
    forbidden = {
        "pydantic-ai-slim",
        "pydantic-deep",
        "python-telegram-bot",
        "redis",
        "openai",
        "google-genai",
        "anthropic",
        "scikit-learn",
        "scipy",
        "pyspark",
        "prefect",
        "temporalio",
        "ddgs",
        "ipykernel",
    }
    forbidden_normalized = {normalize_pep503(f) for f in forbidden}
    for name in listed_names:
        norm_name = normalize_pep503(name)
        assert (
            norm_name not in forbidden_normalized
        ), f"Forbidden package listed in serving-requirements.txt: {name}"


def test_dockerfile_static_checks():
    """Verify static properties of docker/Dockerfile.serving."""
    repo_root = Path(__file__).resolve().parents[2]
    dockerfile_path = repo_root / "docker" / "Dockerfile.serving"
    assert dockerfile_path.is_file(), f"Missing {dockerfile_path}"

    content = dockerfile_path.read_text(encoding="utf-8")

    # 1. Base image is not 'latest'
    arg_base_matches = re.findall(
        r"^ARG\s+BASE_IMAGE\s*=\s*(\S+)", content, re.MULTILINE
    )
    from_matches = re.findall(r"^FROM\s+(\S+)", content, re.MULTILINE)
    bases_to_check = arg_base_matches + [
        b for b in from_matches if not b.startswith("$")
    ]
    assert bases_to_check, "No base image found in Dockerfile"
    for b in bases_to_check:
        assert not b.endswith(":latest") and ":latest" not in b.lower(), (
            f"Base image must not be 'latest': {b}"
        )

    # 2. USER is numeric and non-zero
    user_match = re.search(r"^USER\s+(\S+)", content, re.MULTILINE)
    assert user_match, "USER instruction missing in Dockerfile"
    user_val = user_match.group(1).strip()
    uid_str = user_val.split(":")[0]
    assert (
        uid_str.isdigit() and int(uid_str) > 0
    ), f"USER must be numeric and non-zero, got: {user_val}"

    # 3. ENTRYPOINT is exactly the JSON array ['python', '-m', 'seeknal.serving']
    ep_match = re.search(r"^ENTRYPOINT\s+(.+)$", content, re.MULTILINE)
    assert ep_match, "ENTRYPOINT instruction missing in Dockerfile"
    ep_str = ep_match.group(1).strip()
    try:
        ep_json = json.loads(ep_str)
    except json.JSONDecodeError as exc:
        raise AssertionError(
            f"ENTRYPOINT is not valid JSON array: {ep_str}"
        ) from exc
    assert ep_json == [
        "python",
        "-m",
        "seeknal.serving",
    ], f"ENTRYPOINT must be exactly ['python', '-m', 'seeknal.serving'], got: {ep_json}"

    # 4. COPY sources are a subset of the allowed list
    allowed_sources = {
        "pyproject.toml",
        "uv.lock",
        "README.md",
        "LICENSE",
        "docker/serving-requirements.txt",
        "src/",
        "src",
    }
    copy_matches = re.findall(r"^COPY\s+(.+)$", content, re.MULTILINE)
    assert copy_matches, "No COPY instructions found in Dockerfile"
    for copy_line in copy_matches:
        tokens = [t for t in copy_line.split() if not t.startswith("--")]
        assert len(tokens) >= 2, f"Invalid COPY line: {copy_line}"
        sources = tokens[:-1]
        for src in sources:
            assert (
                src in allowed_sources
            ), f"COPY source '{src}' is not in allowed sources {allowed_sources}"

    # 5. Install uses -c /tmp/constraints.txt
    assert "-c /tmp/constraints.txt" in content, (
        "Install instruction must use '-c /tmp/constraints.txt'"
    )

    # 6. No ENV key matches (?i)(secret|token|password|passwd|key)
    env_key_pattern = re.compile(
        r"(secret|token|password|passwd|key)", re.IGNORECASE
    )
    all_env_keys = []
    lines = content.splitlines()
    in_env = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("ENV "):
            in_env = True
            body = stripped[4:]
        elif in_env:
            body = stripped
        else:
            continue

        in_env = stripped.endswith("\\")
        chunk = body.rstrip("\\").strip()
        for token in chunk.split():
            if "=" in token:
                key = token.split("=")[0].strip()
                all_env_keys.append(key)
            elif " " in chunk:
                key = chunk.split()[0].strip()
                all_env_keys.append(key)

    for key in all_env_keys:
        assert not env_key_pattern.search(key), f"Forbidden ENV key found: {key}"
