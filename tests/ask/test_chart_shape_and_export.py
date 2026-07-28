"""Tests for the M9 chart enhancements: shape contracts, series, exported CSV.

Three behaviours, each guarding a failure the renderer would otherwise absorb
silently and draw a wrong chart from:

  - ``validate_chart_shape`` refuses shapes the renderer cannot draw faithfully
  - a chart can be built from the CSV the turn exported, so table, download and
    chart stay one dataset even when the values were computed, not queried
  - the evidence-synthesis stage keeps ``visualize_chart`` available, so a turn
    that spends its tool budget gathering evidence can still chart the result

No network and no engine: everything here is local.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest
from pydantic_ai import Agent
from pydantic_ai.toolsets import FunctionToolset

from seeknal.ask.agents.tools._chart_payload import (
    CHART_MAX_POINTS,
    MAX_CHART_COLUMNS,
    build_chart_payload,
    validate_chart_shape,
)
from seeknal.ask.agents.tools._context import (
    ToolContext,
    build_evidence_synthesis_prompt,
    register_exported_dataset,
    reset_turn_governor,
    set_tool_context,
)
from seeknal.ask.agents.tools.visualize_chart import visualize_chart
from seeknal.ask.gateway.server import _chart_only_toolset


class _REPLStub:
    def __init__(self) -> None:
        self.conn = duckdb.connect(":memory:")

    def execute_oneshot(self, sql: str, limit=None):
        result = self.conn.execute(sql)
        if not result.description:
            return [], []
        return [d[0] for d in result.description], result.fetchall()


@pytest.fixture
def ctx(tmp_path: Path) -> ToolContext:
    ctx = ToolContext(
        repl=_REPLStub(),
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
        sql_timeout_seconds=0,
        request_limit=100,
    )
    set_tool_context(ctx)
    return ctx


def _chart(ctx: ToolContext) -> dict:
    assert ctx.pending_visualization is not None
    return ctx.pending_visualization[0]["chart_json"]


# ---------------------------------------------------------------------------
# Shape contracts — wide data


def test_wide_data_is_refused_and_the_reshape_is_named() -> None:
    columns = ["bulan", "kode_a", "kode_b", "kode_c", "total"]
    rows = [["2024-01-01", 1, 2, 3, 6]]

    problem = validate_chart_shape("grouped_line_chart", columns, rows)

    assert problem is not None
    # The dropped columns are named, so the agent can see what it would lose.
    assert "kode_c" in problem and "total" in problem
    assert "long" in problem.lower()


def test_wide_data_is_refused_by_the_tool_without_registering_a_chart(
    ctx: ToolContext,
) -> None:
    rows = [["2024-01-01", 1, 2, 3, 6], ["2024-02-01", 2, 3, 4, 9]]

    result = visualize_chart(
        "grouped_line_chart",
        "Four codes",
        data=rows,
        columns=["bulan", "a", "b", "c", "total"],
    )

    assert ctx.pending_visualization is None
    assert "at most" in result and str(MAX_CHART_COLUMNS) in result


def test_three_columns_are_accepted_for_a_series_capable_type() -> None:
    assert (
        validate_chart_shape(
            "grouped_line_chart",
            ["bulan", "jumlah", "kode"],
            [["2024-01-01", 5, "A"]],
        )
        is None
    )


# ---------------------------------------------------------------------------
# Shape contracts — series a type cannot draw


def test_bar_chart_refuses_a_series_column_and_suggests_grouped() -> None:
    problem = validate_chart_shape(
        "bar_chart", ["kategori", "jumlah", "kode"], [["A", 5, "x"]]
    )

    assert problem is not None
    assert "grouped_bar_chart" in problem


def test_pie_chart_refuses_a_series_column() -> None:
    problem = validate_chart_shape(
        "pie_chart", ["kategori", "jumlah", "kode"], [["A", 5, "x"]]
    )

    assert problem is not None


def test_grouped_bar_chart_accepts_a_series_column() -> None:
    assert (
        validate_chart_shape(
            "grouped_bar_chart", ["kategori", "jumlah", "kode"], [["A", 5, "x"]]
        )
        is None
    )


# ---------------------------------------------------------------------------
# Shape contracts — X axis encoding


@pytest.mark.parametrize(
    "value",
    ["Januari", "Jan 2024", "2024-0319", "bulan-1", ""],
)
def test_line_chart_refuses_an_x_axis_that_is_not_a_date(value: str) -> None:
    problem = validate_chart_shape("line_chart", ["bulan", "jumlah"], [[value, 5]])

    assert problem is not None
    assert "bar_chart" in problem


@pytest.mark.parametrize(
    "value",
    ["2024", "2024-01", "2024-01-01", "2024-01-01T00:00:00", "2024-01-01T00:00:00Z"],
)
def test_line_chart_accepts_iso_periods(value: str) -> None:
    assert validate_chart_shape("line_chart", ["bulan", "jumlah"], [[value, 5]]) is None


def test_line_chart_accepts_real_date_objects() -> None:
    assert (
        validate_chart_shape("line_chart", ["bulan", "jumlah"], [[date(2024, 1, 1), 5]])
        is None
    )


def test_bar_chart_accepts_any_label_on_x() -> None:
    assert validate_chart_shape("bar_chart", ["bulan", "jumlah"], [["Januari", 5]]) is None


def test_scatter_plot_refuses_a_non_numeric_x() -> None:
    problem = validate_chart_shape("scatter_plot", ["a", "b"], [["Januari", 5]])

    assert problem is not None
    assert "number" in problem


# ---------------------------------------------------------------------------
# Multi-series payload + caps


def test_series_column_becomes_the_third_widget_data_key(ctx: ToolContext) -> None:
    rows = [
        ["2024-01-01", 10, "301"],
        ["2024-01-01", 4, "302"],
        ["2024-02-01", 12, "301"],
        ["2024-02-01", 6, "302"],
    ]

    visualize_chart(
        "grouped_line_chart", "Per code", data=rows, columns=["bulan", "jumlah", "kode"]
    )

    widget_data = _chart(ctx)["widgetData"]
    assert list(widget_data[0].keys()) == ["bulan", "jumlah", "kode"]
    assert {row["kode"] for row in widget_data} == {"301", "302"}


def test_confirmation_reports_each_series_so_the_answer_can_match(
    ctx: ToolContext,
) -> None:
    rows = [
        ["2024-01-01", 10, "301"],
        ["2024-01-01", 4, "302"],
        ["2024-01-01", 7, "303"],
    ]

    result = visualize_chart(
        "grouped_bar_chart", "Per code", data=rows, columns=["b", "j", "kode"]
    )

    assert "Series drawn (3)" in result
    assert "legend" in result
    for code in ("301", "302", "303"):
        assert code in result


def test_downsampling_keeps_every_series_intact() -> None:
    # Two series long enough to trip the point cap. A single global stride
    # would keep different periods per series and draw lines through points
    # the data never had.
    start = date(2024, 1, 1)
    rows: list[list[object]] = []
    for i in range(CHART_MAX_POINTS + 60):
        period = (start + timedelta(days=i)).isoformat()
        rows.append([period, i, "A"])
        rows.append([period, i * 2, "B"])

    payload, notices = build_chart_payload(
        widget_type="grouped_line_chart",
        widget_title="Two long series",
        widget_id="w-1",
        columns=["period", "value", "series"],
        rows=rows,
    )

    widget_data = payload[0]["chart_json"]["widgetData"]
    per_series: dict[str, list[str]] = {}
    for row in widget_data:
        per_series.setdefault(row["series"], []).append(row["period"])

    assert set(per_series) == {"A", "B"}
    # Both series survive, both respect the cap, and both cover the same
    # periods -- the property a global stride would break.
    for periods in per_series.values():
        assert len(periods) <= CHART_MAX_POINTS
    assert per_series["A"] == per_series["B"]
    assert any("downsampled" in notice for notice in notices)


# ---------------------------------------------------------------------------
# Charting the exported CSV


def _register_forecast_like_export(ctx: ToolContext) -> None:
    """Register an export shaped like run_forecast's combined CSV."""
    register_exported_dataset(
        "forecast-combined.csv",
        ["period", "kind", "value", "point", "lower_80", "upper_80"],
        [
            ["2024-01-01", "historis", 100, "", "", ""],
            ["2024-02-01", "historis", 110, "", "", ""],
            ["2024-03-01", "proyeksi-3bulan", "", 120, 110, 130],
            ["2024-04-01", "proyeksi-3bulan", "", 130, 118, 142],
        ],
        ctx=ctx,
    )


def test_chart_is_built_from_the_exported_csv(ctx: ToolContext) -> None:
    _register_forecast_like_export(ctx)

    result = visualize_chart(
        "grouped_line_chart",
        "History and projection",
        columns=["period", "value", "kind"],
    )

    widget_data = _chart(ctx)["widgetData"]
    assert list(widget_data[0].keys()) == ["period", "value", "kind"]
    # Both the historical and the projected segment are present -- the whole
    # point: a projection is computed in-process and no SQL can return it.
    assert {row["kind"] for row in widget_data} == {"historis", "proyeksi-3bulan"}
    assert "Series drawn (2)" in result


def test_exported_csv_columns_are_selected_case_insensitively(ctx: ToolContext) -> None:
    _register_forecast_like_export(ctx)

    visualize_chart(
        "grouped_line_chart", "Any case", columns=["PERIOD", "Value", "KIND"]
    )

    # Selection tolerates the agent's casing, but the chart keeps the CSV's own
    # column names so its labels match the file the user downloads.
    assert list(_chart(ctx)["widgetData"][0].keys()) == ["period", "value", "kind"]


def test_unknown_column_names_the_available_ones(ctx: ToolContext) -> None:
    _register_forecast_like_export(ctx)

    result = visualize_chart(
        "grouped_line_chart", "Wrong column", columns=["period", "jumlah", "kind"]
    )

    assert ctx.pending_visualization is None
    assert "jumlah" in result
    assert "lower_80" in result  # the real columns are listed


def test_charting_without_an_export_says_so(ctx: ToolContext) -> None:
    result = visualize_chart("bar_chart", "Nothing exported", columns=["a", "b"])

    assert ctx.pending_visualization is None
    assert "No CSV has been exported" in result


def test_reset_turn_governor_clears_the_exported_dataset(ctx: ToolContext) -> None:
    _register_forecast_like_export(ctx)
    assert ctx.exported_dataset is not None

    reset_turn_governor()

    # Anti-leak: turn N's CSV must not become turn N+1's chart.
    assert ctx.exported_dataset is None


def test_registering_twice_keeps_the_last_export(ctx: ToolContext) -> None:
    register_exported_dataset("first.csv", ["a", "b"], [[1, 2]], ctx=ctx)
    register_exported_dataset("second.csv", ["c", "d"], [[3, 4]], ctx=ctx)

    # Last-wins, matching pending_upload: the chart follows the CSV the user
    # actually gets a Download button for.
    assert ctx.exported_dataset is not None
    assert ctx.exported_dataset["name"] == "second.csv"


# ---------------------------------------------------------------------------
# Synthesis stage keeps charting available


def _stub_visualize_chart(widget_type: str, widget_title: str) -> str:
    """A stand-in tool; only its name matters to the lookup."""
    return ""


def test_synthesis_toolset_exposes_only_the_chart_tool() -> None:
    def execute_sql(sql: str) -> str:
        """Run SQL."""
        return ""

    def visualize_chart(widget_type: str) -> str:  # noqa: F811 - local stand-in
        """Chart it."""
        return ""

    agent = Agent("test", toolsets=[FunctionToolset(tools=[execute_sql, visualize_chart])])

    toolset = _chart_only_toolset(agent)

    assert toolset is not None
    # The stage exists to stop further exploration, so everything that could
    # explore must stay out.
    assert list(toolset.tools) == ["visualize_chart"]


def test_synthesis_toolset_is_none_when_charting_is_not_enabled() -> None:
    def execute_sql(sql: str) -> str:
        """Run SQL."""
        return ""

    agent = Agent("test", toolsets=[FunctionToolset(tools=[execute_sql])])

    # A project without the chart tool keeps the original no-tools behaviour.
    assert _chart_only_toolset(agent) is None


def test_synthesis_prompt_permits_charting_when_the_tool_is_offered(
    ctx: ToolContext,
) -> None:
    ctx.current_question = "tren per bulan"
    ctx.evidence_snippets_this_turn.append("| bulan | jumlah |\n| 2024-01 | 10 |")

    permitted = build_evidence_synthesis_prompt("budget reached", allow_chart=True)

    # Offering the tool while the prompt still bans tools makes the model obey
    # the prompt and write "a chart could not be shown" -- the exact failure
    # this stage produced. Prompt and toolset have to agree.
    assert "Do not call any tools" not in permitted
    assert "visualize_chart" in permitted


def test_synthesis_prompt_still_bans_tools_by_default(ctx: ToolContext) -> None:
    ctx.current_question = "tren per bulan"
    ctx.evidence_snippets_this_turn.append("| bulan | jumlah |\n| 2024-01 | 10 |")

    # Callers that cannot offer the tool (the interactive one-shot path) keep
    # the original no-tools contract.
    assert "Do not call any tools" in build_evidence_synthesis_prompt("budget reached")


def _synthesis_gate(agent, ctx: ToolContext):
    """Mirror the gateway's chart-offer gate (server.py UsageLimitExceeded path).

    The real decision lives in ``_run_agent_inner``; replicate the exact
    expression here so this test guards the branch that leaked a second,
    text-narrated chart into the answer when the main pass had already charted.
    """
    return _chart_only_toolset(agent) if ctx.pending_visualization is None else None


def _chart_agent() -> Agent:
    def execute_sql(sql: str) -> str:
        """Run SQL."""
        return ""

    def visualize_chart(widget_type: str) -> str:  # local stand-in
        """Chart it."""
        return ""

    return Agent("test", toolsets=[FunctionToolset(tools=[execute_sql, visualize_chart])])


def test_synthesis_offers_chart_when_none_was_produced_yet(ctx: ToolContext) -> None:
    ctx.current_question = "tren per bulan"
    ctx.evidence_snippets_this_turn.append("| bulan | jumlah |\n| 2024-01 | 10 |")
    assert ctx.pending_visualization is None  # Case A: nothing charted yet

    chart_toolset = _synthesis_gate(_chart_agent(), ctx)

    # Case A keeps the rescue: the tool is offered and the prompt agrees.
    assert chart_toolset is not None
    prompt = build_evidence_synthesis_prompt(
        "budget reached", allow_chart=chart_toolset is not None
    )
    assert "Do not call any tools" not in prompt
    assert "visualize_chart" in prompt


def test_synthesis_does_not_reoffer_chart_when_one_already_exists(
    ctx: ToolContext,
) -> None:
    ctx.current_question = "tren per bulan"
    ctx.evidence_snippets_this_turn.append("| bulan | jumlah |\n| 2024-01 | 10 |")
    # Case B: the main pass already produced a chart this turn.
    ctx.pending_visualization = [
        {"chart_json": {"widgetType": "line_chart", "widgetData": []}, "chart_type": "line_chart"}
    ]

    chart_toolset = _synthesis_gate(_chart_agent(), ctx)

    # No second chart is invited, so the prompt bans tools and no ReAct
    # {"action": ...} block can be narrated into the answer. The existing
    # chart is still emitted by _drain_pending_side_channels in the gateway.
    assert chart_toolset is None
    prompt = build_evidence_synthesis_prompt(
        "terminal tool error after sufficient evidence",
        allow_chart=chart_toolset is not None,
    )
    assert "Do not call any tools" in prompt


# ---------------------------------------------------------------------------
# Full-fidelity integration: drive the REAL _run_agent_inner synthesis branch.
#
# Unlike the gate-replica tests above, this patches create_agent (the same
# hook test_gateway_parity.py uses) and runs the actual gateway generator, so
# the assertions cover the exact lines changed in server.py, not a copy.


class _FakeRun:
    """Minimal async-iterable stand-in for pydantic-ai's agent.iter() handle."""

    def __init__(self, ctx, chart_payload) -> None:
        self._ctx = ctx
        self._chart_payload = chart_payload
        self.result = MagicMock()
        self.result.output = ""
        self.result.all_messages = list

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        # Main pass: the chart tool ran (slot filled), then a later tool failed
        # terminally -- exactly the captured-session sequence. Raise the stop the
        # gateway maps to the evidence-synthesis branch.
        from pydantic_ai.exceptions import UsageLimitExceeded

        self._ctx.pending_visualization = self._chart_payload
        raise UsageLimitExceeded("terminal tool error after sufficient evidence")
        yield  # pragma: no cover - makes this an async generator


def _named_visualize_chart(widget_type: str) -> str:
    """Tool literally named ``visualize_chart`` so _chart_only_toolset finds it."""
    return ""


# FunctionToolset keys tools by function name; use the canonical name.
_named_visualize_chart.__name__ = "visualize_chart"


class _FakeAgent:
    """Fake agent that mimics a model narrating a ReAct block WHEN offered the
    chart tool -- the exact failure the captured session showed. If the gateway
    (correctly) does not offer the tool, no scaffold can appear."""

    def __init__(self, ctx, chart_payload) -> None:
        self._ctx = ctx
        self._chart_payload = chart_payload
        self.toolsets = [FunctionToolset(tools=[_named_visualize_chart])]
        self.override_called_with = None
        self.run_calls = 0
        self._chart_offered = False

    def iter(self, *args, **kwargs):
        return _FakeRun(self._ctx, self._chart_payload)

    def override(self, *, toolsets=None, **kwargs):
        # The gateway enters this override only when it re-offers the chart.
        self.override_called_with = toolsets
        self._chart_offered = True

        class _Noop:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *exc):
                return False

        return _Noop()

    async def run(self, prompt, **kwargs):
        self.run_calls += 1
        result = MagicMock()
        if self._chart_offered:
            # A preview model narrates the tool call as text instead of a real
            # function call -- this is the leak the fix prevents.
            result.output = (
                "Berikut tren pendaftaran NIE.\n\n"
                '```json\n{"action": "visualize_chart", '
                '"action_input": "{\'widget_type\': \'grouped_line_chart\'}"}\n```'
            )
        else:
            result.output = "Registrations grew each year; Campuran leads."
        result.all_messages = list
        return result


@pytest.mark.asyncio
async def test_run_agent_inner_does_not_reoffer_chart_after_one_was_built(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Case B through the real gateway: a chart already built + a terminal tool
    error routes into synthesis, and synthesis must NOT re-offer the chart tool
    (which is what leaked a {"action": ...} block into the answer)."""
    from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
    from seeknal.ask.gateway import server as gw

    chart_payload = [
        {
            "chart_json": {
                "widgetId": "sess-x-1",
                "widgetType": "grouped_line_chart",
                "widgetTitle": "Tren Penerbitan NIE per Jenis Produk",
                "widgetData": [{"periode": "2025-01-01", "jumlah": 1024, "jenis": "Campuran"}],
                "widgetSize": 1,
            },
            "chart_type": "grouped_line_chart",
        }
    ]

    captured: dict = {}

    def fake_create_agent(*args, **kwargs):
        ctx = ToolContext(
            repl=MagicMock(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
            disable_quality_gate=True,
        )
        # Evidence + terminal error so _gateway_tool_stop_reason would fire and
        # build_evidence_synthesis_prompt has snippets to work with.
        ctx.current_question = "tren penerbitan NIE per jenis produk ERBA"
        ctx.successful_sql_results_this_turn = 1
        ctx.evidence_snippets_this_turn.append(
            "[execute_sql]\n| periode | jumlah |\n| 2025-01 | 1024 |\n(1 row)"
        )
        ctx.terminal_tool_errors_this_turn.append(
            "upload_to_s3: terminal_dependency_unavailable — SeaweedFS rejected the write"
        )
        set_tool_context(ctx)
        captured["ctx"] = ctx
        agent = _FakeAgent(ctx, chart_payload)
        captured["agent"] = agent
        return agent, MagicMock(), [], {}

    monkeypatch.setattr(
        "seeknal.ask.agents.agent.create_agent", fake_create_agent
    )

    events = [
        ev
        async for ev in gw._run_agent_inner(
            tmp_path, "sess-caseB", "tren penerbitan NIE per jenis produk ERBA"
        )
    ]

    agent = captured["agent"]
    types = [ev["type"] for ev in events]

    # 1. The chart built in the main pass is still delivered (drain path).
    assert "visualization" in types
    viz = next(ev for ev in events if ev["type"] == "visualization")
    assert viz["data"][0]["chart_json"]["widgetTitle"].startswith("Tren Penerbitan")

    # 2. An answer is produced.
    answer_ev = next(ev for ev in events if ev["type"] == "answer")
    answer = answer_ev["data"]

    # 3. THE FIX: synthesis was NOT offered the chart tool (slot was full), so
    #    the model could not be invited to narrate a second chart.
    assert agent.override_called_with is None, (
        "synthesis re-offered the chart tool despite an existing chart"
    )

    # 4. No ReAct tool-call scaffold leaked into the visible answer.
    assert '"action"' not in answer
    assert "action_input" not in answer
    assert "visualize_chart" not in answer
