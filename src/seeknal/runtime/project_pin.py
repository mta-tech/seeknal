"""Project runtime pin parsing, resolution, and release ID generation."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, Mapping

import yaml

from .release import RuntimeRelease


class PinError(ValueError):
    """Raised when a runtime pin is invalid or cannot be resolved."""

    pass


_LINE_PIN_RE = re.compile(r"^atlas-rt/(\d+)\.(\d+)$")
_EXACT_PIN_RE = re.compile(r"^atlas-rt/(\d+)\.(\d+)\.(\d+)$")


def _is_valid_pin(pin: str) -> bool:
    return bool(_LINE_PIN_RE.match(pin) or _EXACT_PIN_RE.match(pin))


def read_runtime_pin(
    project_dir: str | Path,
    environment: str | None = None,
) -> str | None:
    """Read the runtime pin from seeknal_project.yml.

    The environment-level runtime pin overrides the top-level pin.
    Returns None if no pin is set.
    Raises PinError if a pin is present but invalid.
    """
    p = Path(project_dir)
    if p.is_dir():
        project_file = p / "seeknal_project.yml"
        if not project_file.is_file():
            project_file = p / "seeknal_project.yaml"
    else:
        project_file = p

    if not project_file.is_file():
        return None

    content = project_file.read_text(encoding="utf-8")
    data = yaml.safe_load(content)
    if not isinstance(data, Mapping):
        return None

    pin: str | None = None

    # Top-level runtime
    top_runtime = data.get("runtime")
    if top_runtime is not None:
        pin = str(top_runtime)

    # Environment-level override
    if environment is not None:
        envs = data.get("environments")
        if isinstance(envs, Mapping):
            env_cfg = envs.get(environment)
            if isinstance(env_cfg, Mapping) and "runtime" in env_cfg:
                env_runtime = env_cfg["runtime"]
                if env_runtime is not None:
                    pin = str(env_runtime)

    if pin is None:
        return None

    if not _is_valid_pin(pin):
        raise PinError(
            f"Invalid runtime pin '{pin}'. Pin must be either a line 'atlas-rt/X.Y' or exact 'atlas-rt/X.Y.Z'."
        )

    return pin


def resolve_pin(
    pin: str,
    releases: Iterable[RuntimeRelease],
    *,
    channel: str = "stable",
) -> RuntimeRelease:
    """Resolve a runtime pin against available releases.

    - Exact pin (atlas-rt/X.Y.Z) matches a release name exactly (any channel).
    - Line pin (atlas-rt/X.Y) picks the highest patch of that X.Y on the given channel.
    - Raises PinError if the pin format is invalid or no match is found.
    """
    exact_match = _EXACT_PIN_RE.match(pin)
    line_match = _LINE_PIN_RE.match(pin)

    if not exact_match and not line_match:
        raise PinError(
            f"Invalid runtime pin format '{pin}'. Must match 'atlas-rt/X.Y' or 'atlas-rt/X.Y.Z'."
        )

    if exact_match:
        for r in releases:
            if r.name == pin:
                return r
        raise PinError(f"No release found matching exact pin '{pin}'")

    assert line_match is not None
    major = int(line_match.group(1))
    minor = int(line_match.group(2))

    candidates = [
        r
        for r in releases
        if r.channel == channel and r.version[0] == major and r.version[1] == minor
    ]

    if not candidates:
        raise PinError(
            f"No release found matching line pin '{pin}' on channel '{channel}'"
        )

    candidates.sort(key=lambda r: r.version[2], reverse=True)
    return candidates[0]


def project_release_id(
    project_id: str,
    commit_sha: str,
    release: RuntimeRelease,
    manifest_sha256: str,
) -> str:
    """Compute deterministic project release identifier string.

    Format: <project_id>@<commit_sha>+<release.name>@<atlas-rt image digest>#<manifest_sha256[:16]>
    """
    if "atlas-rt" not in release.images:
        raise KeyError("Release does not contain required 'atlas-rt' image")

    atlas_rt_digest = release.images["atlas-rt"].digest
    short_manifest_sha = manifest_sha256[:16]
    return f"{project_id}@{commit_sha}+{release.name}@{atlas_rt_digest}#{short_manifest_sha}"
