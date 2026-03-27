"""Subagent registry for Seeknal Ask.

Defines specialized subagent specs that are passed to deepagents
create_deep_agent(). Each subagent has restricted tools and a focused
system prompt for its domain.

Subagents share the parent's ToolContext (process-global singleton),
so they access the same REPL, DuckDB connection, and project path.
"""

import warnings
from functools import wraps
from pathlib import Path
from typing import Any, Optional

from jinja2 import Environment, FileSystemLoader
from langchain_core.tools import StructuredTool

# Template directory for subagent prompts
_PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

# Tools that should never be given to subagents (parent-only)
_PARENT_ONLY_TOOLS = {"submit_plan", "ask_user"}

# Tree-view glyphs
_TREE_BRANCH = "\u251c\u2500"  # ├─
_TREE_LAST = "\u2514\u2500"    # └─
_TREE_PIPE = "\u2502"          # │


def _load_prompt(template_name: str, **kwargs) -> str:
    """Load and render a Jinja2 prompt template for a subagent."""
    env = Environment(
        loader=FileSystemLoader(str(_PROMPTS_DIR)),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template(template_name)
    return template.render(**kwargs)


def _filter_tools(all_tools: list, allowed_names: set) -> list:
    """Filter tool objects to only those with names in allowed_names."""
    return [t for t in all_tools if t.name in allowed_names]


def _summarize_args(tool_name: str, kwargs: dict) -> str:
    """Create a brief arg summary for tree-view display."""
    if not kwargs:
        return ""
    # Show key identifying args, not everything
    parts = []
    for key in ("node_type", "type", "name", "file_path", "draft_file",
                "table_name", "node_name"):
        if key in kwargs:
            val = str(kwargs[key])
            if len(val) > 40:
                val = val[:37] + "..."
            parts.append(val)
    if parts:
        return ", ".join(parts)
    # Fallback: first arg value
    first = str(next(iter(kwargs.values()), ""))
    return first[:40] if len(first) > 40 else first


def _wrap_tool_for_subagent(tool_obj) -> StructuredTool:
    """Wrap a tool to emit tree-view progress to ctx.console.

    Creates a new StructuredTool that prints progress lines before/after
    execution. Only prints when ctx.console is set (interactive mode).
    This is the same pattern used by run_pipeline for live progress.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    original_func = tool_obj.func

    @wraps(original_func)
    def wrapper(*args, **kwargs):
        ctx = get_tool_context()
        arg_summary = _summarize_args(tool_obj.name, kwargs)

        if ctx.console:
            try:
                from rich.markup import escape
                display = f"{tool_obj.name}({escape(arg_summary)})" if arg_summary else tool_obj.name
                ctx.console.print(f"  [dim]{_TREE_BRANCH} {display}[/dim]")
            except Exception:
                pass

        ctx.subagent_tool_count += 1
        result = original_func(*args, **kwargs)

        if ctx.console:
            try:
                from rich.markup import escape
                result_str = str(result) if result else ""
                brief = result_str[:120].split("\n")[0]
                ok = "error" not in result_str.lower()[:200] and "failed" not in result_str.lower()[:200]
                glyph = "\u2713" if ok else "\u2717"  # ✓ or ✗
                style = "dim" if ok else "red"
                ctx.console.print(f"  [dim]{_TREE_PIPE}[/dim]  [{style}]\u23bf  {glyph} {escape(brief)}[/{style}]")
            except Exception:
                pass

        return result

    return StructuredTool.from_function(
        func=wrapper,
        name=tool_obj.name,
        description=tool_obj.description,
        args_schema=tool_obj.args_schema,
    )


def _wrap_tools_for_subagent(all_tools: list, allowed_names: set) -> list:
    """Filter and wrap tools for a subagent with event emission."""
    filtered = _filter_tools(all_tools, allowed_names)
    return [_wrap_tool_for_subagent(t) for t in filtered]


def get_subagent_specs(
    all_tools: list,
    config: dict[str, Any] | None = None,
    llm: Optional[Any] = None,
) -> list[dict]:
    """Build the list of SubAgent specs for create_deep_agent().

    Args:
        all_tools: All available tool objects (from the active profile).
        config: Agent config from seeknal_agent.yml.
        llm: The parent's LLM instance (used as default for subagents).

    Returns:
        List of SubAgent TypedDict-compatible dicts.
    """
    config = config or {}
    disabled = set(config.get("disabled_subagents", []))
    model_overrides = config.get("subagent_models", {})

    # Available tools minus parent-only
    available_tools = [t for t in all_tools if t.name not in _PARENT_ONLY_TOOLS]

    specs = []

    # --- Builder subagent ---
    if "builder" not in disabled:
        builder_tool_names = {
            "draft_node", "edit_node", "dry_run_draft", "apply_draft",
        }
        builder_tools = _wrap_tools_for_subagent(available_tools, builder_tool_names)

        if builder_tools:
            spec: dict[str, Any] = {
                "name": "builder",
                "description": (
                    "Builds seeknal pipeline nodes from a structured plan. "
                    "Handles the draft \u2192 validate \u2192 fix loop for each node. "
                    "Use for multi-node pipeline building after you have "
                    "designed the pipeline (node names, types, refs, SQL)."
                ),
                "system_prompt": _load_prompt("builder_agent.j2"),
                "tools": builder_tools,
            }
            if "builder" in model_overrides:
                spec["model"] = model_overrides["builder"]
            specs.append(spec)

    # --- Verifier subagent ---
    if "verifier" not in disabled:
        verifier_tool_names = {
            "execute_sql", "inspect_output", "execute_python",
        }
        verifier_tools = _wrap_tools_for_subagent(available_tools, verifier_tool_names)

        if verifier_tools:
            spec = {
                "name": "verifier",
                "description": (
                    "Inspects pipeline output quality after a successful run. "
                    "Checks row counts, NULL rates, value ranges, and join "
                    "fanout. Returns a structured quality report. Read-only \u2014 "
                    "cannot modify files or run pipelines."
                ),
                "system_prompt": _load_prompt("verifier_agent.j2"),
                "tools": verifier_tools,
            }
            if "verifier" in model_overrides:
                spec["model"] = model_overrides["verifier"]
            specs.append(spec)

    # Validate disabled_subagents for unknown names
    known_names = {"builder", "verifier"}
    unknown = disabled - known_names
    if unknown:
        warnings.warn(
            f"Unknown subagent names in disabled_subagents: {unknown}. "
            f"Known subagents: {sorted(known_names)}"
        )

    return specs
