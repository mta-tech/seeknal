"""Read reusable prompt-to-SQL examples stored in the project."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

_SUPPORTED_SUFFIXES = {".yml", ".yaml", ".md"}
_MAX_BYTES = 64 * 1024


def read_sql_pair(name_or_path: str) -> str:
    """Read one SQL-pair example by listed path, file stem, or file name."""
    from seeknal.ask.agents.tools._context import get_tool_context

    if not name_or_path or not isinstance(name_or_path, str):
        return "Error: name_or_path is required."
    if Path(name_or_path).is_absolute() or ".." in Path(name_or_path).parts:
        return "Error: name_or_path must be relative and cannot contain '..'."

    ctx = get_tool_context()
    project_root = ctx.project_path.resolve()
    target = _resolve_sql_pair(project_root, name_or_path.strip())
    if target is None:
        return f"Error: SQL pair not found: `{name_or_path}`. Use `list_sql_pairs` first."
    data = target.read_bytes()
    truncated = len(data) > _MAX_BYTES
    if truncated:
        data = data[:_MAX_BYTES]
    text = data.decode("utf-8", errors="replace")
    header = f"# SQL pair: {target.relative_to(project_root).as_posix()}\n\n"
    _record_loaded_sql_pair(target, project_root, text)
    if truncated:
        return header + text + "\n\n... (truncated)"
    return header + text


def _resolve_sql_pair(project_path: Path, name_or_path: str) -> Path | None:
    project_path = project_path.resolve()
    roots = [project_path / "seeknal" / "sql_pairs", project_path / "context" / "sql_pairs"]
    direct = project_path / name_or_path
    candidates: list[Path] = []
    if direct.suffix.lower() in _SUPPORTED_SUFFIXES and direct.exists():
        candidates.append(direct)
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in _SUPPORTED_SUFFIXES:
                continue
            rel = path.relative_to(project_path).as_posix()
            if name_or_path in {rel, path.name, path.stem}:
                candidates.append(path)
    unique = sorted({p.resolve() for p in candidates})
    return unique[0] if len(unique) == 1 else None


def _record_loaded_sql_pair(target: Path, project_root: Path, text: str) -> None:
    """Remember loaded SQL pairs for same-turn drift warnings.

    This is best-effort and intentionally non-fatal: reading a context file
    should never fail because YAML parsing or context recording failed.
    """
    try:
        data = yaml.safe_load(text)
    except Exception:
        return
    if not isinstance(data, dict):
        return
    sql = data.get("sql")
    if not isinstance(sql, str) or not sql.strip():
        return
    try:
        from seeknal.ask.agents.tools._context import (
            get_loaded_sql_pairs,
            get_tool_context,
            mark_sql_pairs_checked,
        )

        ctx = get_tool_context()
        mark_sql_pairs_checked(ctx)
        key = _pair_key(target, project_root, data)
        get_loaded_sql_pairs(ctx)[key] = sql
    except Exception:
        return


def _pair_key(target: Path, project_root: Path, data: dict[str, Any]) -> str:
    name = data.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return target.relative_to(project_root).as_posix()


__all__ = ["read_sql_pair", "_resolve_sql_pair"]
