"""Seeknal project path discovery — shared across ask CLI modules."""

from pathlib import Path
from typing import Optional


def find_project_path(project: Optional[str] = None) -> Path:
    """Find the seeknal project root directory.

    Strategy:
    1. If explicit path given, validate it exists.
    2. Check current directory for seeknal markers.
    3. Walk up parent directories looking for markers.
    4. Fall back to current directory.

    Args:
        project: Optional explicit path to the project root.

    Returns:
        Resolved Path to the project directory.

    Raises:
        FileNotFoundError: If explicit path does not exist.
    """
    if project:
        p = Path(project)
        if p.exists():
            return p
        raise FileNotFoundError(f"Project path not found: {project}")

    cwd = Path.cwd()

    # Seeknal project markers (any of these indicate a project root)
    markers = ["seeknal", "seeknal.yml", "seeknal.yaml", "target"]

    # Check current directory
    for marker in markers:
        if (cwd / marker).exists():
            return cwd

    # Walk up directories
    for parent in cwd.parents:
        for marker in markers:
            if (parent / marker).exists():
                return parent

    # Default to cwd
    return cwd
