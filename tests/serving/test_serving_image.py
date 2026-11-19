"""Tests for the atlas-rt-serving Docker image and dependencies."""

from __future__ import annotations

import ast
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


def test_ast_import_closure():
    """Scan all serving files and starrocks.py for imports with AST and verify requirement coverage."""
    repo_root = Path(__file__).resolve().parents[2]
    serving_dir = repo_root / "src" / "seeknal" / "serving"
    starrocks_file = repo_root / "src" / "seeknal" / "connections" / "starrocks.py"
    reqs_path = repo_root / "docker" / "serving-requirements.txt"

    assert serving_dir.is_dir(), f"Missing {serving_dir}"
    assert starrocks_file.is_file(), f"Missing {starrocks_file}"
    assert reqs_path.is_file(), f"Missing {reqs_path}"

    files_to_scan = sorted(serving_dir.rglob("*.py")) + [starrocks_file]

    imported_top_levels = set()
    for py_file in files_to_scan:
        source_code = py_file.read_text(encoding="utf-8")
        tree = ast.parse(source_code, filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top_name = alias.name.split(".")[0]
                    imported_top_levels.add(top_name)
            elif isinstance(node, ast.ImportFrom):
                if node.module and node.level == 0:
                    top_name = node.module.split(".")[0]
                    imported_top_levels.add(top_name)

    pkg_dist = importlib.metadata.packages_distributions()

    direct_reqs = set()
    with open(reqs_path, encoding="utf-8") as f:
        for line in f:
            line = line.split("#")[0].strip()
            if line:
                direct_reqs.add(normalize_pep503(line))

    closure = get_transitive_requirements(direct_reqs)

    for top in sorted(imported_top_levels):
        if top in sys.stdlib_module_names or top == "seeknal":
            continue
        dists = pkg_dist.get(top, [])
        assert dists, f"Top-level module '{top}' could not be mapped to any installed distribution"
        for dist in dists:
            norm_dist = normalize_pep503(dist)
            assert norm_dist in closure, (
                f"Distribution '{dist}' (providing '{top}') required by serving codebase "
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

    # Verify [dependency-groups].serving in pyproject.toml matches serving-requirements.txt
    pyproject_path = repo_root / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        pyproject_data = tomllib.load(f)
    serving_group = pyproject_data.get("dependency-groups", {}).get("serving", [])
    assert serving_group, "Missing [dependency-groups].serving in pyproject.toml"
    group_names = {
        normalize_pep503(packaging.requirements.Requirement(req).name)
        for req in serving_group
    }
    assert group_names == {normalize_pep503(n) for n in listed_names}, (
        "[dependency-groups].serving in pyproject.toml must match docker/serving-requirements.txt"
    )


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
        "docker/serving-image/",
        "docker/serving-image",
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

    # 5. Lock-only hash-verified install sequence (no floating constraints)
    assert (
        "uv export --locked --only-group serving --no-emit-project --format requirements-txt -o /tmp/requirements.txt"
        in content
    ), "Dockerfile must export locked serving requirements with hashes"
    assert (
        "uv pip install --system --require-hashes -r /tmp/requirements.txt" in content
    ), "Dockerfile must install serving requirements with --require-hashes"
    assert (
        "uv pip install --system --no-deps ." in content
    ), "Dockerfile must install seeknal with --no-deps"
    assert (
        "uv pip install --system --offline --no-index --no-deps --no-build-isolation ."
        in content
    ), "Offline Dockerfile path must install seeknal from source with --no-build-isolation"
    assert (
        "vendor/project" not in content
    ), "Dockerfile must not install unchecked prebuilt seeknal wheel from vendor/project"
    assert (
        not re.search(r"^\s*#\s*syntax\s*=", content, re.MULTILINE | re.IGNORECASE)
    ), "Dockerfile must not contain a # syntax= directive"

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
