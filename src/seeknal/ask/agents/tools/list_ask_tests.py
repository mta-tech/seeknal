"""List project-local Seeknal Ask SQL tests."""

from __future__ import annotations


def list_ask_tests(query: str | None = None) -> str:
    """List executable Ask SQL QA tests in the current project.

    These tests live in `seeknal/tests/`, `context/tests/`, or top-level
    `tests/`. They are QA oracles, separate from `sql_pairs` examples.

    Args:
        query: Optional case-insensitive filter across name, path, prompt, and tags.
    """
    from seeknal.ask.agents.tools._context import get_tool_context
    from seeknal.ask.testing import discover_ask_sql_tests

    ctx = get_tool_context()
    q = (query or "").strip().lower()
    cases = discover_ask_sql_tests(ctx.project_path)
    if q:
        cases = [
            case
            for case in cases
            if q
            in " ".join(
                [
                    case.name,
                    case.file_path.relative_to(ctx.project_path).as_posix(),
                    case.prompt,
                    " ".join(case.tags),
                ]
            ).lower()
        ]

    if not cases:
        suffix = f" matching `{query}`" if query else ""
        return (
            f"No Ask SQL tests found{suffix}. Add YAML tests under "
            "`seeknal/tests/`, `context/tests/`, or `tests/`."
        )

    lines = [f"## Ask SQL tests ({len(cases)} found)", ""]
    for case in cases[:80]:
        rel = case.file_path.relative_to(ctx.project_path).as_posix()
        tags = f" [{', '.join(case.tags)}]" if case.tags else ""
        skip = f" — skipped: {case.skip}" if case.skip else ""
        lines.append(f"- `{case.name}` — `{rel}`{tags}{skip}")
        if case.prompt:
            lines.append(f"  prompt: {case.prompt[:180]}")
    if len(cases) > 80:
        lines.append(f"- ... {len(cases) - 80} more; use `query` to narrow")
    lines.append("")
    lines.append("Use `read_ask_test(name_or_path=...)` for details.")
    return "\n".join(lines)


__all__ = ["list_ask_tests"]
