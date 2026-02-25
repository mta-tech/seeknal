"""
Data quality visualization module for Seeknal.

Reads profile stats and rule check results from pipeline runs and generates
a data quality dashboard as either interactive HTML (Chart.js) or ASCII output.

Architecture mirrors visualize.py (lineage):
  Data layer: RunState + profile parquets + dq_history.parquet
  Builder layer: DQDataBuilder -> frozen DQData dataclass
  Renderer layer: generate_dq_html() / render_dq_ascii()
"""

import dataclasses
import json
import logging
import webbrowser
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import jinja2  # ty: ignore[unresolved-import]

from seeknal.dag.manifest import Manifest, NodeType  # ty: ignore[unresolved-import]
from seeknal.utils.path_security import is_insecure_path  # ty: ignore[unresolved-import]
from seeknal.workflow.state import RunState, NodeStatus  # ty: ignore[unresolved-import]

logger = logging.getLogger(__name__)


class DQVisualizationError(Exception):
    """Raised when data quality visualization fails."""


# ---------------------------------------------------------------------------
# Frozen dataclasses (JSON-serializable via dataclasses.asdict)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ColumnStats:
    """Per-column statistics from a profile parquet."""
    name: str
    metrics: dict[str, Optional[str]]   # metric_name -> value (VARCHAR)
    detail: dict[str, Optional[str]]    # metric_name -> detail JSON


@dataclass(frozen=True)
class ProfileSummary:
    """Summary of one profile node."""
    node_id: str
    name: str
    row_count: int
    column_count: int
    columns: list[ColumnStats]


@dataclass(frozen=True)
class RuleResult:
    """Result of one rule node."""
    node_id: str
    name: str
    rule_type: str
    status: str            # "pass", "warn", "fail"
    checks: list[dict]
    message: str
    duration_ms: int


@dataclass(frozen=True)
class TrendPoint:
    """Single data point in a trend series."""
    run_id: str
    timestamp: str
    value: float


@dataclass(frozen=True)
class NodeSummary:
    """Execution summary for any pipeline node."""
    node_id: str
    name: str
    node_type: str         # "source", "transform", "feature_group", etc.
    status: str            # "success", "failed", "skipped", "pending", "cached"
    row_count: int
    duration_ms: int
    last_run: str


@dataclass(frozen=True)
class DQMetadata:
    """Dashboard-level metadata."""
    project: str
    run_id: str
    timestamp: str
    total_nodes: int
    nodes_succeeded: int
    nodes_failed: int
    total_profiles: int
    total_rules: int
    rules_passed: int
    rules_warned: int
    rules_failed: int
    health_score: float    # (passed + warned) / total * 100


@dataclass(frozen=True)
class DQData:
    """Complete data quality report -- top-level container."""
    node_summaries: list[NodeSummary]
    profiles: list[ProfileSummary]
    rules: list[RuleResult]
    trends: dict[str, list[TrendPoint]]
    metadata: DQMetadata


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------

class DQDataBuilder:
    """Reads RunState + profile parquets + history -> DQData."""

    def __init__(
        self,
        state: RunState,
        manifest: Manifest,
        target_path: Path,
    ):
        self.state = state
        self.manifest = manifest
        self.target_path = target_path

    def build(self, focus_node: Optional[str] = None) -> DQData:
        """Build complete DQ data, optionally filtered to one node."""
        if focus_node and focus_node not in self.manifest.nodes:
            available = ", ".join(sorted(self.manifest.nodes.keys())[:10])
            raise DQVisualizationError(
                f"Node '{focus_node}' not found. Available: {available}"
            )

        node_summaries = self._build_node_summaries(focus_node)
        profiles = self._build_profiles(focus_node)
        rules = self._build_rules(focus_node)
        trends = self._build_trends()
        metadata = self._build_metadata(node_summaries, profiles, rules)

        return DQData(
            node_summaries=node_summaries,
            profiles=profiles,
            rules=rules,
            trends=trends,
            metadata=metadata,
        )

    # -- node summaries (all node types) ------------------------------------

    def _build_node_summaries(
        self, focus_node: Optional[str] = None
    ) -> list[NodeSummary]:
        """Build execution summaries for ALL nodes in the manifest."""
        summaries: list[NodeSummary] = []

        for node_id, node in sorted(self.manifest.nodes.items()):
            if focus_node and node_id != focus_node:
                continue

            node_state = self.state.nodes.get(node_id)

            if node_state:
                status = node_state.status
                row_count = node_state.row_count
                duration_ms = node_state.duration_ms
                last_run = node_state.last_run
            else:
                status = "pending"
                row_count = 0
                duration_ms = 0
                last_run = ""

            summaries.append(NodeSummary(
                node_id=node_id,
                name=node.name,
                node_type=node.node_type.value,
                status=status,
                row_count=row_count,
                duration_ms=duration_ms,
                last_run=last_run,
            ))

        return summaries

    # -- profiles -----------------------------------------------------------

    def _build_profiles(self, focus_node: Optional[str] = None) -> list[ProfileSummary]:
        """Read profile parquets and build ProfileSummary list."""
        profiles: list[ProfileSummary] = []

        for node_id, node in sorted(self.manifest.nodes.items()):
            if node.node_type != NodeType.PROFILE:
                continue
            if focus_node and node_id != focus_node:
                continue

            profile_name = node.name
            parquet_path = (
                self.target_path / "intermediate" / f"profile_{profile_name}.parquet"
            )
            if not parquet_path.exists():
                continue

            columns_data = self._read_profile_parquet(parquet_path)
            row_count = 0
            # Extract table-level row_count
            for col_stats in columns_data:
                if col_stats.name == "_table_":
                    rc = col_stats.metrics.get("row_count")
                    if rc is not None:
                        try:
                            row_count = int(float(rc))
                        except (ValueError, TypeError):
                            pass

            # Filter out _table_ pseudo-column for the column list
            column_list = [c for c in columns_data if c.name != "_table_"]

            profiles.append(ProfileSummary(
                node_id=node_id,
                name=profile_name,
                row_count=row_count,
                column_count=len(column_list),
                columns=column_list,
            ))

        return profiles

    def _read_profile_parquet(self, parquet_path: Path) -> list[ColumnStats]:
        """Read a profile parquet and group rows by column_name."""
        import duckdb  # ty: ignore[unresolved-import]

        con = duckdb.connect()
        try:
            rows = con.execute(
                f"SELECT column_name, metric, value, detail "
                f"FROM '{parquet_path}' ORDER BY column_name, metric"
            ).fetchall()
        finally:
            con.close()

        # Group by column_name
        grouped: dict[str, tuple[dict[str, Optional[str]], dict[str, Optional[str]]]] = {}
        for col_name, metric, value, detail_val in rows:
            if col_name not in grouped:
                grouped[col_name] = ({}, {})
            grouped[col_name][0][metric] = value
            grouped[col_name][1][metric] = detail_val

        return [
            ColumnStats(name=col_name, metrics=metrics, detail=detail)
            for col_name, (metrics, detail) in sorted(grouped.items())
        ]

    # -- rules --------------------------------------------------------------

    def _build_rules(self, focus_node: Optional[str] = None) -> list[RuleResult]:
        """Extract rule results from RunState metadata."""
        rules: list[RuleResult] = []

        for node_id, node in sorted(self.manifest.nodes.items()):
            if node.node_type != NodeType.RULE:
                continue
            if focus_node and node_id != focus_node:
                continue

            node_state = self.state.nodes.get(node_id)
            if not node_state:
                continue

            meta = node_state.metadata or {}
            rule_type = meta.get("rule_type", "unknown")
            # profile_check stores detailed results in "results", not "checks"
            checks = meta.get("results", [])
            if not isinstance(checks, list):
                checks = []

            # Determine status
            if node_state.status == NodeStatus.FAILED.value:
                status = "fail"
            elif meta.get("warned", 0) > 0:
                status = "warn"
            elif meta.get("passed", True):
                status = "pass"
            else:
                status = "fail"

            # Build message
            message = meta.get("message", "")
            if not message:
                violations = meta.get("violations", 0)
                if status == "pass":
                    message = "All checks passed"
                elif status == "warn":
                    warned = meta.get("warned", 0)
                    message = f"{warned} warning(s)"
                else:
                    message = f"{violations} violation(s)"

            rules.append(RuleResult(
                node_id=node_id,
                name=node.name,
                rule_type=rule_type,
                status=status,
                checks=checks,
                message=message,
                duration_ms=node_state.duration_ms,
            ))

        return rules

    # -- trends -------------------------------------------------------------

    def _build_trends(self) -> dict[str, list[TrendPoint]]:
        """Read dq_history.parquet for trend data."""
        history_path = self.target_path / "dq_history.parquet"
        if not history_path.exists():
            return {}

        import duckdb  # ty: ignore[unresolved-import]

        con = duckdb.connect()
        try:
            rows = con.execute(
                f"SELECT node_id, column_name, metric, run_id, timestamp, value "
                f"FROM '{history_path}' "
                f"ORDER BY timestamp ASC"
            ).fetchall()
        except Exception:
            logger.warning("Failed to read DQ history: %s", history_path)
            return {}
        finally:
            con.close()

        trends: dict[str, list[TrendPoint]] = {}
        for node_id, column_name, metric, run_id, ts, value in rows:
            key = f"{node_id}.{column_name}.{metric}"
            if key not in trends:
                trends[key] = []
            ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
            trends[key].append(TrendPoint(
                run_id=str(run_id),
                timestamp=ts_str,
                value=float(value) if value is not None else 0.0,
            ))

        # Keep last 20 runs per trend
        for key in trends:
            trends[key] = trends[key][-20:]

        return trends

    # -- metadata -----------------------------------------------------------

    def _build_metadata(
        self,
        node_summaries: list[NodeSummary],
        profiles: list[ProfileSummary],
        rules: list[RuleResult],
    ) -> DQMetadata:
        """Build dashboard metadata from collected data."""
        passed = sum(1 for r in rules if r.status == "pass")
        warned = sum(1 for r in rules if r.status == "warn")
        failed = sum(1 for r in rules if r.status == "fail")
        total = len(rules)
        health = ((passed + warned) / total * 100) if total > 0 else 100.0

        # Node-level counts
        total_nodes = len(node_summaries)
        nodes_succeeded = sum(
            1 for n in node_summaries if n.status == NodeStatus.SUCCESS.value
        )
        nodes_failed = sum(
            1 for n in node_summaries if n.status == NodeStatus.FAILED.value
        )

        # Determine run_id from state
        run_id = ""
        timestamp = ""
        for ns in self.state.nodes.values():
            if ns.last_run:
                timestamp = ns.last_run
                break

        if not run_id and timestamp:
            try:
                dt = datetime.fromisoformat(timestamp)
                run_id = dt.strftime("%Y%m%d_%H%M%S")
            except (ValueError, TypeError):
                run_id = "unknown"

        project_name = getattr(self.manifest, "project", "") or ""

        return DQMetadata(
            project=project_name,
            run_id=run_id,
            timestamp=timestamp or datetime.now().isoformat(),
            total_nodes=total_nodes,
            nodes_succeeded=nodes_succeeded,
            nodes_failed=nodes_failed,
            total_profiles=len(profiles),
            total_rules=total,
            rules_passed=passed,
            rules_warned=warned,
            rules_failed=failed,
            health_score=round(health, 1),
        )


# ---------------------------------------------------------------------------
# ASCII renderer
# ---------------------------------------------------------------------------

_HSEP = "\u2500"  # horizontal line

_STATUS_ICONS = {
    "success": "\u2714",  # checkmark
    "failed": "\u2718",   # X
    "skipped": "\u21b7",  # skip arrow
    "cached": "\u25cb",   # circle
    "pending": "\u2026",  # ellipsis
}


def render_dq_ascii(dq_data: DQData) -> str:
    """Render DQData as a detailed ASCII report string."""
    lines: list[str] = []

    lines.append(f"Data Quality Report (run: {dq_data.metadata.run_id})")
    lines.append("\u2550" * 60)

    has_content = (
        dq_data.node_summaries or dq_data.profiles or dq_data.rules
    )
    if not has_content:
        lines.append("")
        lines.append("  No data quality data found.")
        lines.append("  Run 'seeknal run' first.")
        lines.append("\u2550" * 60)
        return "\n".join(lines)

    # -- Pipeline Overview --------------------------------------------------
    if dq_data.node_summaries:
        lines.append("")
        lines.append("\u2588 Pipeline Overview")

        # Calculate column widths
        name_w = max(len(n.name) for n in dq_data.node_summaries)
        name_w = max(name_w, 4)  # min "Name" header
        type_w = max(len(n.node_type) for n in dq_data.node_summaries)
        type_w = max(type_w, 4)  # min "Type" header

        # Header
        lines.append(
            f"  {'Name':<{name_w}} \u2502 {'Type':<{type_w}} \u2502 "
            f"{'Status':>8} \u2502 {'Rows':>8} \u2502 {'Duration':>10}"
        )
        lines.append(
            f"  {_HSEP * name_w}\u253c{_HSEP * (type_w + 2)}"
            f"\u253c{_HSEP * 10}\u253c{_HSEP * 10}\u253c{_HSEP * 12}"
        )

        # Data rows
        for ns in dq_data.node_summaries:
            icon = _STATUS_ICONS.get(ns.status, "?")
            status_label = ns.status.upper()

            if ns.duration_ms >= 1000:
                dur_str = f"{ns.duration_ms / 1000:.1f}s"
            else:
                dur_str = f"{ns.duration_ms}ms"

            lines.append(
                f"  {ns.name:<{name_w}} \u2502 {ns.node_type:<{type_w}} \u2502 "
                f"{icon} {status_label:>6} \u2502 {ns.row_count:>8} \u2502 {dur_str:>10}"
            )

    # -- Profile sections ---------------------------------------------------
    for profile in dq_data.profiles:
        lines.append("")
        lines.append(
            f"\u2588 Profile: {profile.name} "
            f"({profile.row_count} rows, {profile.column_count} cols)"
        )

        if profile.columns:
            # Determine which metrics to show
            display_metrics = ["null_percent", "distinct_count", "min", "avg", "max"]
            display_headers = ["null%", "distinct", "min", "avg", "max"]

            # Calculate column widths
            col_name_width = max(len(c.name) for c in profile.columns)
            col_name_width = max(col_name_width, 6)  # minimum "Column" header

            # Header
            header_parts = [f"  {'Column':<{col_name_width}}"]
            for h in display_headers:
                header_parts.append(f"{h:>8}")
            lines.append(" \u2502 ".join(header_parts))

            # Separator
            sep_parts = [f"  {_HSEP * col_name_width}"]
            for _ in display_headers:
                sep_parts.append(_HSEP * 8)
            lines.append("\u253c".join(sep_parts))

            # Data rows
            for col in profile.columns:
                row_parts = [f"  {col.name:<{col_name_width}}"]
                for metric in display_metrics:
                    val = col.metrics.get(metric)
                    if val is None:
                        row_parts.append(f"{'--':>8}")
                    elif metric == "null_percent":
                        try:
                            row_parts.append(f"{float(val):>7.1f}%")
                        except (ValueError, TypeError):
                            row_parts.append(f"{val:>8}")
                    elif metric in ("avg", "min", "max"):
                        try:
                            fval = float(val)
                            if abs(fval) >= 1000:
                                row_parts.append(f"{fval:>8.0f}")
                            else:
                                row_parts.append(f"{fval:>8.2f}")
                        except (ValueError, TypeError):
                            row_parts.append(f"{str(val)[:8]:>8}")
                    else:
                        try:
                            row_parts.append(f"{int(float(val)):>8}")
                        except (ValueError, TypeError):
                            row_parts.append(f"{str(val)[:8]:>8}")
                lines.append(" \u2502 ".join(row_parts))

    # -- Rules section ------------------------------------------------------
    if dq_data.rules:
        lines.append("")
        lines.append("\u2588 Rules")

        for rule in dq_data.rules:
            if rule.status == "pass":
                icon = "\u2714"
                status_str = "PASS"
            elif rule.status == "warn":
                icon = "\u26a0"
                status_str = "WARN"
            else:
                icon = "\u2718"
                status_str = "FAIL"

            lines.append(f"  {icon} {rule.name:<25} {status_str}  {rule.message}")

    # -- Footer -------------------------------------------------------------
    lines.append("")
    lines.append("\u2550" * 60)

    m = dq_data.metadata

    # Node summary line
    node_parts = []
    if m.nodes_succeeded > 0:
        node_parts.append(f"{m.nodes_succeeded} succeeded")
    if m.nodes_failed > 0:
        node_parts.append(f"{m.nodes_failed} failed")
    pending = m.total_nodes - m.nodes_succeeded - m.nodes_failed
    if pending > 0:
        node_parts.append(f"{pending} pending")
    if m.total_nodes > 0:
        lines.append(f"{m.total_nodes} nodes: {', '.join(node_parts)}")

    # Rule summary line
    rule_parts = []
    if m.rules_passed > 0:
        rule_parts.append(f"{m.rules_passed} passed")
    if m.rules_warned > 0:
        rule_parts.append(f"{m.rules_warned} warning(s)")
    if m.rules_failed > 0:
        rule_parts.append(f"{m.rules_failed} failed")

    if m.total_rules > 0:
        lines.append(f"{m.total_rules} rules: {', '.join(rule_parts)}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# HTML renderer
# ---------------------------------------------------------------------------

TEMPLATE_DIR = Path(__file__).parent / "templates"


def generate_dq_html(
    dq_data: DQData,
    output_path: Path,
    open_browser: bool = True,
) -> Path:
    """Generate HTML dashboard from DQData and optionally open browser."""
    if is_insecure_path(str(output_path)):
        raise DQVisualizationError(
            f"Insecure output path: '{output_path}'. "
            "Use a path within your project directory."
        )

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=True,
    )
    template = env.get_template("dq_report.html.j2")

    # Sanitize JSON for embedding in JS single-quoted string literal
    json_str = json.dumps(dataclasses.asdict(dq_data))
    json_str = json_str.replace("\\", "\\\\")  # Escape backslashes first
    json_str = json_str.replace("'", "\\'")    # Escape single quotes for JS
    json_str = json_str.replace("</", "<\\/")  # Prevent </script> injection

    html_content = template.render(dq_data_json=json_str)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html_content)

    if open_browser:
        import os
        if not os.environ.get("SSH_CONNECTION"):
            webbrowser.open(f"file://{output_path.resolve()}")
        else:
            logger.info("SSH session detected. Open %s in your browser.", output_path)

    return output_path
