"""Write security utilities for agent filesystem tools.

Restricts which directories/files the Ask agent can write to,
building on the read security in _security.py.
"""

import re
from pathlib import Path

# Pre-compiled patterns for path sanitization
_PATH_TRAVERSAL_RE = re.compile(r"\.\.[/\\]")
_BACKSLASH_RE = re.compile(r"[\\]")
_DOUBLE_DOT_RE = re.compile(r"\.\.")

# Only these directories (relative to project root) are writable
WRITABLE_DIRS = {
    ".",              # Project root (for draft files)
    "seeknal",        # Pipeline definitions
    "data",           # Data files
}

# Files that must never be written by the agent
BLOCKED_WRITE_FILES = {
    ".env", "profiles.yml", "profiles.yaml",
    "seeknal_project.yml", "seeknal_project.yaml",
}

# Valid draft filename pattern: draft_<type>_<name>.(yml|py)
_DRAFT_FILENAME_RE = re.compile(
    r"^draft_[a-zA-Z_]+_[a-zA-Z0-9_]+\.(yml|py)$"
)


def validate_write_path(path: str, project_path: Path) -> Path:
    """Validate a file path for write operations.

    Same defense-in-depth as _security.validate_project_path but with
    additional write-specific restrictions (writable dirs, blocked files).

    Args:
        path: Relative path from user/agent input.
        project_path: Absolute path to the project root.

    Returns:
        Resolved absolute Path.

    Raises:
        ValueError: If the path fails any validation layer.
    """
    # Layer 1: Sanitize path traversal sequences
    sanitized = _PATH_TRAVERSAL_RE.sub("", path)
    sanitized = _BACKSLASH_RE.sub("/", sanitized)
    sanitized = _DOUBLE_DOT_RE.sub("", sanitized)

    # Layer 2: Resolve to absolute
    resolved = (project_path / sanitized).resolve()
    project_root = project_path.resolve()

    # Layer 3: Containment check
    if not resolved.is_relative_to(project_root):
        raise ValueError(f"Write path traversal blocked: {path}")

    # Layer 4: Blocked file check (before dir check — catches root-level files)
    if resolved.name.lower() in BLOCKED_WRITE_FILES:
        raise ValueError(f"Write to protected file blocked: {resolved.name}")

    # Layer 5: Writable directory check
    rel = resolved.relative_to(project_root)
    parts = rel.parts

    # Files at root level (single part) — check if it's a draft or in "."
    if len(parts) <= 1:
        top_dir = "."
    else:
        top_dir = parts[0]

    if top_dir not in WRITABLE_DIRS:
        raise ValueError(
            f"Write to '{top_dir}/' not allowed. "
            f"Writable directories: {', '.join(sorted(WRITABLE_DIRS))}"
        )

    # Extra check: root-level files must be drafts
    if len(parts) == 1 and top_dir == "." and not _DRAFT_FILENAME_RE.match(parts[0]):
        raise ValueError(
            f"Only draft files can be written to project root. "
            f"Got: {parts[0]}"
        )

    return resolved


def validate_draft_path(path: str, project_path: Path) -> Path:
    """Validate a draft file path specifically.

    Draft files must be in the project root and match the naming pattern.

    Args:
        path: Draft filename (e.g., 'draft_source_customers.yml').
        project_path: Absolute path to the project root.

    Returns:
        Resolved absolute Path.

    Raises:
        ValueError: If the path is not a valid draft file.
    """
    filename = Path(path).name
    if not _DRAFT_FILENAME_RE.match(filename):
        raise ValueError(
            f"Invalid draft filename: '{filename}'. "
            f"Expected: draft_<type>_<name>.yml or draft_<type>_<name>.py"
        )

    return validate_write_path(filename, project_path)
