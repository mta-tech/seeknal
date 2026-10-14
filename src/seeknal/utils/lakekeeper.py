"""Lakekeeper catalog URI lookup from environment variables."""

import os

# LAKEKEEPER_URI is canonical (materialization, profiles.yml templates).
# LAKEKEEPER_URL is accepted as an alias: the Iceberg source, REPL, dry-run and
# getting-started docs historically used it.
LAKEKEEPER_URI_ENV_VARS = ("LAKEKEEPER_URI", "LAKEKEEPER_URL")


def lakekeeper_uri_from_env(default: str = "") -> str:
    """Return the Lakekeeper catalog URI from the environment.

    Checks ``LAKEKEEPER_URI`` first, then ``LAKEKEEPER_URL``. Empty or
    whitespace-only values are treated as unset.

    Args:
        default: Value returned when neither variable is set.

    Returns:
        The catalog URI, or ``default``.
    """
    for name in LAKEKEEPER_URI_ENV_VARS:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return default
