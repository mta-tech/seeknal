"""MkDocs hooks for documentation build configuration.

This module contains hooks that are called during the MkDocs build process.

Note on Griffe Warnings:
    When building with `mkdocs build --strict`, you may see warnings from griffe
    about missing type annotations in source code. These are informational warnings
    indicating that function signatures lack Python type hints while docstrings
    document types using Google-style inline documentation.

    These warnings do not affect documentation quality - the build succeeds and
    generates correct API documentation. To eliminate these warnings, the source
    code would need Python type annotations added to all function signatures.

    For development, use `mkdocs build` (without --strict) or `mkdocs serve`.
"""


def on_startup(command: str, dirty: bool) -> None:
    """Hook called when MkDocs starts.

    Args:
        command: The MkDocs command being run (serve, build, etc.)
        dirty: Whether this is a dirty build (incremental)
    """
    pass


def on_config(config):
    """Hook called after MkDocs config is loaded.

    Args:
        config: The MkDocs config object

    Returns:
        The config object (unmodified)
    """
    return config
