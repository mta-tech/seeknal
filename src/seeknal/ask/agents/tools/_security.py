"""Shared security utilities for agent filesystem tools.

Defense-in-depth path validation following the pattern documented in
docs/solutions/security-vulnerabilities/path-traversal-file-state-backend.md
"""

import re
from pathlib import Path

# Pre-compiled patterns for path sanitization
_PATH_TRAVERSAL_RE = re.compile(r"\.\.[/\\]")
_BACKSLASH_RE = re.compile(r"[\\]")
_DOUBLE_DOT_RE = re.compile(r"\.\.")

# Files that must never be exposed to the agent
BLOCKED_FILES = {".env", "profiles.yml", "profiles.yaml"}

# Directories to skip during filesystem traversal
EXCLUDED_DIRS = {
    ".git", "target", "__pycache__", "node_modules",
    ".venv", "venv", ".tox", ".eggs", ".worktrees",
    ".claude", ".mypy_cache", ".pytest_cache",
}

# File extensions to skip (binary / compiled)
EXCLUDED_EXTENSIONS = {".pyc", ".pyo", ".so", ".dylib", ".dll", ".egg"}

# Maximum file size for reading (500KB)
MAX_FILE_SIZE = 500_000

# Maximum file size for grep scanning (1MB)
MAX_SCAN_FILE_SIZE = 1_000_000


def validate_project_path(path: str, project_path: Path) -> Path:
    """Validate and resolve a file path with defense-in-depth.

    Layers:
    1. Sanitize: strip ../ and ..\\ sequences, normalize separators
    2. Resolve: resolve to absolute path
    3. Containment: must be within project root
    4. Symlink: resolved symlink targets must stay within project
    5. Sensitive file check: block .env, profiles.yml, etc.

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
    sanitized = _BACKSLASH_RE.sub("/", sanitized)  # Normalize separators
    sanitized = _DOUBLE_DOT_RE.sub("", sanitized)  # Remove remaining ..

    # Layer 2: Resolve to absolute
    resolved = (project_path / sanitized).resolve()
    project_root = project_path.resolve()

    # Layer 3: Containment check
    if not resolved.is_relative_to(project_root):
        raise ValueError(f"Path traversal blocked: {path}")

    # Layer 4: Symlink check — resolved path must still be inside project
    if resolved.is_symlink():
        real = resolved.resolve(strict=True)
        if not real.is_relative_to(project_root):
            raise ValueError(f"Symlink escapes project root: {path}")

    # Layer 5: Sensitive file check
    if resolved.name.lower() in BLOCKED_FILES:
        raise ValueError(f"Access to sensitive file blocked: {resolved.name}")

    return resolved
