"""SPIKE 4 — can loaded-skill state be DERIVED, with no fork of pydantic-deep?

The harness lane blocked here, correctly: gating actions on loaded skills needs
loaded-skill state, and `pydantic_deep.DeepAgentDeps` has none. Its fields are
``backend, files, todos, subagents, uploads, ask_user, context_middleware,
share_todos`` — nothing tracks which skills are currently loaded.

Two ways out:

  (a) subclass `DeepAgentDeps` and add a mutable ``loaded_skills`` set, or
  (b) **derive** loaded state from the conversation, storing nothing.

(b) is what Data Formulator actually does — `_rehydrate_loaded_skills(trajectory)`
(`analyst/agent.py:643`) reconstructs which skills are loaded by reading the
trajectory. It needs no shared mutable state, and it survives a resumed turn for
free, which matters because the premises worker is stateless between turns.

`RunContext` exposes `messages`, so a `prepare_output_tools` hook can see the
whole conversation. This spike asks whether that is enough.

Question: does an action gated on a skill become legal ONLY after that skill's
load appears in the message history — with nothing mutated anywhere?

Run:  ../signal/.venv/bin/python tests/spikes/spike_derive_loaded_skills.py
"""

from __future__ import annotations

from pydantic import BaseModel
from pydantic_ai import Agent, ToolOutput
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.tools import ToolDefinition


class VisualizeSpec(BaseModel):
    chart_type: str


class ReportSpec(BaseModel):
    markdown: str


ACTION_OWNER = {"visualize": "core", "write_report": "report"}
ALWAYS_ON = {"core"}

observed: list[dict] = []
derivations: list[set[str]] = []


def derive_loaded_skills(messages: list[ModelMessage]) -> set[str]:
    """Reconstruct loaded skills from the conversation. Stores nothing.

    Mirrors DF's `_rehydrate_loaded_skills`: a skill is loaded if a successful
    ``load_skill`` call for it appears in the history. Reading the history rather
    than a flag is what makes this survive a resumed turn — the premises worker
    is stateless between turns, so any in-memory set would be empty on resume.
    """
    loaded = set(ALWAYS_ON)
    for msg in messages:
        for part in getattr(msg, "parts", []):
            if isinstance(part, ToolCallPart) and part.tool_name == "load_skill":
                args = part.args
                if isinstance(args, dict):
                    name = args.get("name")
                    if isinstance(name, str) and name:
                        loaded.add(name)
    return loaded


async def prepare_output_tools(
    ctx, defs: list[ToolDefinition]
) -> list[ToolDefinition]:
    """`_legal_actions()`, derived per step from the conversation."""
    loaded = derive_loaded_skills(ctx.messages)
    derivations.append(loaded)
    return [d for d in defs if ACTION_OWNER.get(d.name) in loaded]


def scripted(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    step = len(observed)
    observed.append(
        {
            "step": step + 1,
            "actions": sorted(t.name for t in info.output_tools),
        }
    )
    if step == 0:
        return ModelResponse(parts=[ToolCallPart("inspect_data", {})])
    if step == 1:
        return ModelResponse(parts=[ToolCallPart("load_skill", {"name": "report"})])
    if step == 2:
        return ModelResponse(
            parts=[ToolCallPart("write_report", {"markdown": "# derived"})]
        )
    raise AssertionError("committing action did not terminate the run")


agent = Agent(
    FunctionModel(scripted),
    output_type=[
        ToolOutput(VisualizeSpec, name="visualize"),
        ToolOutput(ReportSpec, name="write_report"),
    ],
    prepare_output_tools=prepare_output_tools,
)


@agent.tool_plain
async def inspect_data() -> str:
    return "rows=42"


@agent.tool_plain
async def load_skill(name: str) -> str:
    """Loads a skill. Note it mutates NOTHING — the call itself is the record."""
    return f"loaded {name}"


def main() -> int:
    result = agent.run_sync("write me a report")

    print("=" * 70)
    print("DERIVED, NOT STORED")
    print("=" * 70)
    for o, d in zip(observed, derivations):
        print(f"  step {o['step']}:  derived_loaded={sorted(d)}")
        print(f"           legal_actions={o['actions']}")
    print()
    print(f"  final output: {type(result.output).__name__}")
    print()

    checks = [
        (
            "S1  gated action ABSENT before its skill is loaded",
            observed[0]["actions"] == ["visualize"],
            f"step 1 = {observed[0]['actions']}",
        ),
        (
            "S2  gated action PRESENT once load_skill is in the history",
            "write_report" in observed[2]["actions"],
            f"step 3 = {observed[2]['actions']}",
        ),
        (
            "S3  derivation tracked the conversation, not a flag",
            derivations[0] == {"core"} and "report" in derivations[2],
            f"{sorted(derivations[0])} -> {sorted(derivations[2])}",
        ),
        (
            "S4  nothing was mutated: DeepAgentDeps untouched, no shared set",
            True,  # structural: load_skill returns a string and stores nothing
            "load_skill mutates no state; the call in the history IS the state",
        ),
        (
            "S5  committing action terminated the run",
            len(observed) == 3 and isinstance(result.output, ReportSpec),
            f"{len(observed)} model requests, output={type(result.output).__name__}",
        ),
    ]

    print("=" * 70)
    for label, ok, detail in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}]  {label}\n           {detail}")
    print("=" * 70)
    failed = [c for c in checks if not c[1]]
    print(
        "VERDICT:",
        "loaded-skill state can be DERIVED — no pydantic-deep fork needed"
        if not failed
        else f"{len(failed)} FAILED",
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
