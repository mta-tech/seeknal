"""List context files tool — discovers domain knowledge files in the project's context/ directory."""

import asyncio

_CONTEXT_EXTENSIONS = {".md", ".yml", ".yaml", ".txt"}
_CONTEXT_DIR = "context"


async def list_context_files() -> str:
    """List available domain context files in the project's context/ directory."""
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    def _run() -> list[tuple[str, str]]:
        context_dir = ctx.project_path / _CONTEXT_DIR
        if not context_dir.exists():
            return []  # empty list signals "does not exist"

        files = []
        for f in sorted(context_dir.rglob("*")):
            if not f.is_file() or f.suffix.lower() not in _CONTEXT_EXTENSIONS:
                continue
            rel = str(f.relative_to(ctx.project_path))
            hint = ""
            try:
                for line in f.read_text(encoding="utf-8").splitlines():
                    stripped = line.strip().lstrip("#").strip()
                    if stripped:
                        hint = stripped[:100]
                        break
            except Exception:
                pass
            files.append((rel, hint))
        return files

    try:
        files = await asyncio.to_thread(_run)
    except Exception as e:
        return f"Error listing context files: {e}"

    if not files:
        return (
            "No context/ directory found in the project root. "
            "Create context/ and add .md, .yml, or .txt files with "
            "business rules, glossaries, and domain knowledge."
        )

    lines = [f"Context files ({len(files)} found):\n"]
    for path, hint in files:
        suffix = f" — {hint}" if hint else ""
        lines.append(f"- `{path}`{suffix}")
    lines.append("\nUse read_project_file(path) to load any of these files.")
    return "\n".join(lines)
