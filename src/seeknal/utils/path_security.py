"""
Path security utilities for Seeknal.

This module provides functions to detect insecure storage paths (like /tmp)
and recommend secure alternatives. Using world-readable/writable directories
for sensitive data creates security risks including:
- Other users can read feature data
- Symlink attacks are possible
- Data may not be cleaned up properly
- In containerized environments, /tmp might be shared across containers
"""

import logging
import os
import stat
from pathlib import Path
from typing import Optional, Tuple

# Common insecure path prefixes that should trigger warnings
INSECURE_PATH_PREFIXES = (
    "/tmp",
    "/var/tmp",
    "/dev/shm",
)

# XDG Base Directory Specification for data storage
XDG_DATA_HOME_ENV = "XDG_DATA_HOME"
DEFAULT_XDG_DATA_HOME = "~/.local/share"
SEEKNAL_DIR_NAME = "seeknal"
SEEKNAL_LEGACY_DIR = "~/.seeknal"

# Environment variable to override the base config path
SEEKNAL_BASE_CONFIG_PATH_ENV = "SEEKNAL_BASE_CONFIG_PATH"


def is_insecure_path(path: str) -> bool:
    """
    Check if a path is in an insecure location.

    A path is considered insecure if:
    1. It starts with a known insecure prefix (e.g., /tmp, /var/tmp, /dev/shm)
    2. The directory is world-writable

    Args:
        path: The filesystem path to check.

    Returns:
        True if the path is in an insecure location, False otherwise.

    Examples:
        >>> is_insecure_path("/tmp/my_data")
        True
        >>> is_insecure_path("/home/user/.seeknal/data")
        False
    """
    if not path:
        return False

    # Normalize and expand the path
    normalized_path = os.path.normpath(os.path.expanduser(path))

    # Check against known insecure prefixes
    for prefix in INSECURE_PATH_PREFIXES:
        normalized_prefix = os.path.normpath(prefix)
        if normalized_path == normalized_prefix or normalized_path.startswith(
            normalized_prefix + os.sep
        ):
            return True

    # Check if the parent directory is world-writable
    return _is_world_writable(normalized_path)


def _is_world_writable(path: str) -> bool:
    """
    Check if a path or its nearest existing parent is world-writable.

    Args:
        path: The filesystem path to check.

    Returns:
        True if the path or its nearest parent is world-writable.
    """
    check_path = path
    while check_path and check_path != os.sep:
        if os.path.exists(check_path):
            try:
                mode = os.stat(check_path).st_mode
                # Check if world-writable bit is set
                if mode & stat.S_IWOTH:
                    return True
            except OSError:
                # Cannot stat the path, assume it's not world-writable
                pass
            break
        check_path = os.path.dirname(check_path)
    return False


def get_secure_default_path(subdir: Optional[str] = None) -> str:
    """
    Get a secure default path for Seeknal data storage.

    The path is determined in the following order of precedence:
    1. SEEKNAL_BASE_CONFIG_PATH environment variable (if set)
    2. XDG_DATA_HOME/seeknal (if XDG_DATA_HOME is set)
    3. ~/.seeknal (legacy default, user-specific and secure)

    Args:
        subdir: Optional subdirectory to append to the base path.

    Returns:
        A secure path string for data storage.

    Examples:
        >>> get_secure_default_path()
        '/home/user/.seeknal'
        >>> get_secure_default_path("data")
        '/home/user/.seeknal/data'
    """
    # Check for explicit override via environment variable
    base_path = os.getenv(SEEKNAL_BASE_CONFIG_PATH_ENV)

    if not base_path:
        # Check for XDG Base Directory compliance
        xdg_data_home = os.getenv(XDG_DATA_HOME_ENV)
        if xdg_data_home:
            base_path = os.path.join(xdg_data_home, SEEKNAL_DIR_NAME)
        else:
            # Fall back to legacy ~/.seeknal directory
            base_path = os.path.expanduser(SEEKNAL_LEGACY_DIR)

    # Expand user home directory if needed
    base_path = os.path.expanduser(base_path)

    if subdir:
        return os.path.join(base_path, subdir)

    return base_path


def get_secure_path_recommendation(insecure_path: str) -> str:
    """
    Generate a secure path recommendation based on an insecure path.

    Args:
        insecure_path: The insecure path that needs a replacement.

    Returns:
        A recommended secure path.
    """
    # Extract the subdirectory component from the insecure path
    normalized_path = os.path.normpath(insecure_path)

    # Find which prefix matched
    for prefix in INSECURE_PATH_PREFIXES:
        normalized_prefix = os.path.normpath(prefix)
        if normalized_path == normalized_prefix:
            return get_secure_default_path()
        if normalized_path.startswith(normalized_prefix + os.sep):
            # Get the relative part after the prefix
            relative_part = normalized_path[len(normalized_prefix) :].lstrip(os.sep)
            return get_secure_default_path(relative_part)

    # If no prefix matched, just return the secure default
    return get_secure_default_path()


def warn_if_insecure_path(
    path: str,
    context: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Log a warning if the path is in an insecure location.

    This function checks if the provided path is in a world-writable or
    otherwise insecure location, and logs a warning with a secure alternative
    recommendation.

    Args:
        path: The filesystem path to check.
        context: Optional context string describing what the path is used for
            (e.g., "offline store", "feature store").
        logger: Optional logger instance. If not provided, uses the module logger.

    Returns:
        A tuple of (is_insecure, secure_alternative) where:
        - is_insecure: True if the path is insecure
        - secure_alternative: A recommended secure path if insecure, None otherwise

    Examples:
        >>> is_insecure, alt = warn_if_insecure_path("/tmp/data", "offline store")
        >>> is_insecure
        True
        >>> alt  # Will be something like '/home/user/.seeknal/data'
    """
    if not path:
        return False, None

    if logger is None:
        logger = logging.getLogger("seeknal")

    if is_insecure_path(path):
        secure_alternative = get_secure_path_recommendation(path)
        context_str = f" for {context}" if context else ""

        warning_message = (
            f"Security Warning: Using insecure path '{path}'{context_str}. "
            f"The /tmp directory and other world-writable locations are shared "
            f"among all system users and may expose sensitive data. "
            f"Consider using a secure alternative like '{secure_alternative}' "
            f"or set the SEEKNAL_BASE_CONFIG_PATH environment variable."
        )
        logger.warning(warning_message)

        return True, secure_alternative

    return False, None
