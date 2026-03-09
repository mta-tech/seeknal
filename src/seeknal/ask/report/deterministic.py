"""Deterministic report renderer for exposure YAML with pinned queries.

When an exposure YAML contains a ``sections`` key, this module generates
the Evidence.dev markdown page programmatically — SQL blocks and chart
components come from the YAML spec, and the LLM only writes narrative
commentary between charts (when ``narrative: true``).
"""

import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional


class ChartType(str, Enum):
    """Supported Evidence.dev chart component types."""

    BIG_VALUE = "BigValue"
    BAR_CHART = "BarChart"
    LINE_CHART = "LineChart"
    AREA_CHART = "AreaChart"
    DATA_TABLE = "DataTable"
    SCATTER_PLOT = "ScatterPlot"
    HISTOGRAM = "Histogram"
    FUNNEL_CHART = "FunnelChart"


# Props consumed by the renderer, not passed through to Evidence
_RESERVED_PROPS = frozenset({"name", "sql", "chart", "value", "labels"})

# BigValue-specific: these are expanded, not passed through
_BIGVALUE_RESERVED = frozenset({"name", "sql", "chart", "value", "labels"})

# Narrative placeholder pattern
_NARRATIVE_SLOT = "<!-- NARRATIVE_SLOT_{idx} -->"

# Max rows to include in LLM narrative context
_NARRATIVE_ROW_LIMIT = 100


@dataclass
class QueryConfig:
    """Configuration for a single pinned SQL query with chart mapping."""

    name: str
    sql: str
    chart: ChartType
    props: dict = field(default_factory=dict)


@dataclass
class SectionConfig:
    """Configuration for a report section."""

    title: str
    queries: list[QueryConfig] = field(default_factory=list)
    narrative: bool = False
    description: str = ""


# ---------------------------------------------------------------------------
# Parsing & Validation
# ---------------------------------------------------------------------------


def parse_sections(raw_sections: list[dict]) -> list[SectionConfig]:
    """Parse and validate raw YAML sections into typed configs.

    Args:
        raw_sections: List of section dicts from YAML.

    Returns:
        List of validated SectionConfig objects.

    Raises:
        ValueError: If schema validation fails.
    """
    if not isinstance(raw_sections, list) or not raw_sections:
        raise ValueError("sections must be a non-empty list.")

    seen_query_names: set[str] = set()
    sections: list[SectionConfig] = []

    for i, raw in enumerate(raw_sections):
        if not isinstance(raw, dict):
            raise ValueError(f"Section {i} must be a mapping.")

        title = raw.get("title")
        if not title or not isinstance(title, str):
            raise ValueError(f"Section {i} is missing a 'title' string.")

        narrative = raw.get("narrative", False)
        description = raw.get("description", "")

        queries: list[QueryConfig] = []
        for j, raw_q in enumerate(raw.get("queries", [])):
            if not isinstance(raw_q, dict):
                raise ValueError(
                    f"Section '{title}', query {j} must be a mapping."
                )
            q = _parse_query(raw_q, title, j, seen_query_names)
            queries.append(q)

        sections.append(SectionConfig(
            title=title,
            queries=queries,
            narrative=bool(narrative),
            description=str(description),
        ))

    return sections


def _parse_query(
    raw: dict, section_title: str, idx: int, seen_names: set[str]
) -> QueryConfig:
    """Parse a single query config from YAML dict."""
    name = raw.get("name")
    if not name or not isinstance(name, str):
        raise ValueError(
            f"Section '{section_title}', query {idx}: 'name' is required."
        )
    if not re.match(r"^[a-zA-Z_]\w*$", name):
        raise ValueError(
            f"Section '{section_title}', query '{name}': "
            "name must be alphanumeric with underscores."
        )
    if name in seen_names:
        raise ValueError(
            f"Duplicate query name '{name}'. "
            "Query names must be unique across all sections."
        )
    seen_names.add(name)

    chart_str = raw.get("chart")
    if not chart_str:
        raise ValueError(
            f"Section '{section_title}', query '{name}': 'chart' is required."
        )
    try:
        chart = ChartType(chart_str)
    except ValueError:
        valid = ", ".join(c.value for c in ChartType)
        raise ValueError(
            f"Section '{section_title}', query '{name}': "
            f"invalid chart type '{chart_str}'. Must be one of: {valid}"
        )

    sql = raw.get("sql", "").strip().rstrip(";").strip()

    # Validate SQL if present (DataTable queries may omit sql
    # when they share a query name defined elsewhere)
    if sql:
        from seeknal.ask.security import validate_sql_for_agent
        validate_sql_for_agent(sql)

    # Collect passthrough props (everything except reserved keys)
    props = {k: v for k, v in raw.items() if k not in _RESERVED_PROPS}

    # For BigValue, keep value/labels in props for expansion
    if chart == ChartType.BIG_VALUE:
        if "value" in raw:
            props["value"] = raw["value"]
        if "labels" in raw:
            props["labels"] = raw["labels"]

    return QueryConfig(name=name, sql=sql, chart=chart, props=props)


# ---------------------------------------------------------------------------
# Evidence Markdown Generation
# ---------------------------------------------------------------------------


def generate_evidence_page(
    sections: list[SectionConfig],
    title: str = "",
) -> str:
    """Generate Evidence.dev markdown page from section configs.

    SQL blocks and chart components are produced programmatically.
    Narrative slots are inserted as HTML comments for later replacement.

    Args:
        sections: Validated section configs.
        title: Optional report title for the page header.

    Returns:
        Evidence-compatible markdown string with narrative placeholders.
    """
    parts: list[str] = []

    if title:
        parts.append(f"# {title}\n")

    for i, section in enumerate(sections):
        parts.append(f"## {section.title}\n")

        if section.description:
            parts.append(f"{section.description}\n")

        for query in section.queries:
            # SQL fenced block (skip if no SQL — e.g. shared query reference)
            if query.sql:
                parts.append(f"```sql {query.name}")
                parts.append(query.sql)
                parts.append("```\n")

            # Chart component
            parts.append(_chart_to_evidence(query))
            parts.append("")

        # Narrative placeholder
        if section.narrative:
            parts.append(_NARRATIVE_SLOT.format(idx=i))
            parts.append("")

    return "\n".join(parts)


def _chart_to_evidence(query: QueryConfig) -> str:
    """Map a QueryConfig to Evidence.dev component markup."""
    if query.chart == ChartType.BIG_VALUE:
        return _bigvalue_to_evidence(query)

    # Build props string from passthrough props
    props_parts: list[str] = []
    for k, v in query.props.items():
        if isinstance(v, str):
            props_parts.append(f'{k}="{v}"')
        elif isinstance(v, bool):
            if v:
                props_parts.append(k)
        elif isinstance(v, (int, float)):
            props_parts.append(f"{k}={{{v}}}")
        elif isinstance(v, list):
            props_parts.append(f'{k}="{v[0]}"' if v else "")
        else:
            props_parts.append(f'{k}="{v}"')

    props_str = " ".join(p for p in props_parts if p)
    data_ref = f"data={{{query.name}}}"

    if props_str:
        return f"<{query.chart.value} {data_ref} {props_str} />"
    return f"<{query.chart.value} {data_ref} />"


def _bigvalue_to_evidence(query: QueryConfig) -> str:
    """Expand BigValue config into multiple <BigValue> components."""
    values = query.props.get("value", [])
    labels = query.props.get("labels", values)

    if isinstance(values, str):
        values = [values]
    if isinstance(labels, str):
        labels = [labels]

    if not values:
        return f'<BigValue data={{{query.name}}} />'

    lines: list[str] = []
    for val, label in zip(values, labels):
        lines.append(
            f'<BigValue data={{{query.name}}} value="{val}" label="{label}" />'
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Query Execution
# ---------------------------------------------------------------------------


def execute_section_queries(
    sections: list[SectionConfig],
    project_path: Path,
    on_progress: Optional[Callable[[str], None]] = None,
) -> dict[str, dict[str, Any]]:
    """Pre-execute all pinned SQL queries and capture results.

    Results are used for LLM narrative context — Evidence also
    re-executes queries at build time via the .report.duckdb file.

    Args:
        sections: Validated section configs.
        project_path: Path to the seeknal project root.
        on_progress: Optional callback for progress updates.

    Returns:
        Dict mapping query name to {"columns": [...], "rows": [...], "error": str|None}.
    """
    from seeknal.ask.report.data_bridge import create_duckdb_from_parquets

    results: dict[str, dict[str, Any]] = {}

    # Create a temporary in-memory DuckDB with project views
    try:
        import duckdb
    except ImportError:
        for section in sections:
            for q in section.queries:
                if q.sql:
                    results[q.name] = {
                        "columns": [], "rows": [],
                        "error": "duckdb not available",
                    }
        return results

    # Build a temporary .duckdb with views from parquets
    import tempfile
    import os

    fd, tmp_db_path = tempfile.mkstemp(suffix=".duckdb")
    os.close(fd)

    try:
        create_duckdb_from_parquets(project_path, Path(tmp_db_path))
        conn = duckdb.connect(str(tmp_db_path), read_only=True)

        for section in sections:
            for q in section.queries:
                if not q.sql:
                    continue
                if on_progress:
                    on_progress(f"Executing query: {q.name}")
                try:
                    result = conn.execute(q.sql)
                    columns = [desc[0] for desc in result.description]
                    rows = result.fetchmany(_NARRATIVE_ROW_LIMIT)
                    results[q.name] = {
                        "columns": columns,
                        "rows": [list(r) for r in rows],
                        "error": None,
                    }
                except Exception as e:
                    results[q.name] = {
                        "columns": [], "rows": [],
                        "error": str(e),
                    }

        conn.close()
    finally:
        try:
            os.unlink(tmp_db_path)
        except OSError:
            pass
        # DuckDB creates .wal files
        wal_path = tmp_db_path + ".wal"
        try:
            os.unlink(wal_path)
        except OSError:
            pass

    return results


def format_results_as_markdown(
    query_results: dict[str, dict[str, Any]],
    query_names: list[str],
) -> str:
    """Format query results as markdown tables for LLM context.

    Args:
        query_results: Results from execute_section_queries().
        query_names: Names of queries to include.

    Returns:
        Markdown string with tables.
    """
    parts: list[str] = []

    for name in query_names:
        result = query_results.get(name)
        if not result:
            continue

        if result["error"]:
            parts.append(f"**{name}**: Error — {result['error']}\n")
            continue

        columns = result["columns"]
        rows = result["rows"]

        if not columns:
            parts.append(f"**{name}**: No results.\n")
            continue

        parts.append(f"**{name}** ({len(rows)} rows):\n")
        # Header
        parts.append("| " + " | ".join(columns) + " |")
        parts.append("| " + " | ".join("---" for _ in columns) + " |")
        # Rows
        for row in rows:
            cells = [str(v) if v is not None else "NULL" for v in row]
            parts.append("| " + " | ".join(cells) + " |")
        parts.append("")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# LLM Narrative Generation
# ---------------------------------------------------------------------------

_NARRATIVE_PROMPT = """\
You are a data analyst writing commentary for a report section.

Section: {section_title}
{section_description}

Query results:
{formatted_results}

{global_context}

Write 2-3 sentences of analytical insight interpreting the data above.
Reference SPECIFIC numbers from the results (e.g., "Premium segment \
contributes 45% of revenue despite representing only 16% of customers").
Do NOT write SQL queries, chart components, or markdown headers.
Return ONLY plain text with markdown formatting (bold, bullet lists)."""

# Pattern to strip LLM-generated Evidence components from narrative text
_EVIDENCE_COMPONENT_RE = re.compile(
    r"<(?:BigValue|BarChart|LineChart|AreaChart|DataTable|ScatterPlot|"
    r"Histogram|FunnelChart)\b[^>]*/?>",
    re.IGNORECASE,
)
_SQL_BLOCK_RE = re.compile(r"```sql\b.*?```", re.DOTALL)


def generate_narratives(
    sections: list[SectionConfig],
    query_results: dict[str, dict[str, Any]],
    provider: Optional[str] = None,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    global_context: str = "",
    on_progress: Optional[Callable[[str], None]] = None,
) -> dict[int, str]:
    """Generate LLM narratives for sections with narrative=true.

    Args:
        sections: Validated section configs.
        query_results: Pre-executed query results.
        provider: LLM provider override.
        model: Model name override.
        api_key: API key override.
        global_context: Global prompt context (from params.prompt).
        on_progress: Optional callback for progress updates.

    Returns:
        Dict mapping section index to narrative text.
    """
    narrative_sections = [
        (i, s) for i, s in enumerate(sections) if s.narrative
    ]

    if not narrative_sections:
        return {}

    try:
        from seeknal.ask.agents.providers import get_llm
        llm = get_llm(provider=provider, model=model, api_key=api_key)
    except Exception:
        # LLM unavailable — return placeholders
        return {
            i: "*Narrative unavailable.*"
            for i, _ in narrative_sections
        }

    # Collect all query results up to each section for context
    narratives: dict[int, str] = {}
    all_query_names: list[str] = []

    for i, section in enumerate(sections):
        # Track cumulative queries
        for q in section.queries:
            if q.sql:
                all_query_names.append(q.name)

        if not section.narrative:
            continue

        if on_progress:
            on_progress(f"Writing narrative: {section.title}")

        # For this section, include its own queries + context from prior
        section_query_names = [q.name for q in section.queries if q.sql]
        if not section_query_names:
            # Narrative-only section: use all prior results
            context_names = list(all_query_names)
        else:
            context_names = section_query_names

        formatted = format_results_as_markdown(query_results, context_names)

        prompt = _NARRATIVE_PROMPT.format(
            section_title=section.title,
            section_description=section.description or "",
            formatted_results=formatted or "No data available.",
            global_context=global_context,
        )

        try:
            response = llm.invoke(prompt)
            text = response.content if hasattr(response, "content") else str(response)
            text = _sanitize_narrative(text)
            narratives[i] = text
        except Exception:
            narratives[i] = "*Narrative unavailable.*"

    return narratives


def _sanitize_narrative(text: str) -> str:
    """Strip Evidence components and SQL blocks from LLM narrative output."""
    text = _SQL_BLOCK_RE.sub("", text)
    text = _EVIDENCE_COMPONENT_RE.sub("", text)
    # Collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ---------------------------------------------------------------------------
# Page Assembly
# ---------------------------------------------------------------------------


def assemble_page(
    skeleton: str,
    narratives: dict[int, str],
) -> str:
    """Replace narrative placeholders in the Evidence markdown skeleton.

    Args:
        skeleton: Evidence markdown with NARRATIVE_SLOT comments.
        narratives: Dict mapping section index to narrative text.

    Returns:
        Final Evidence markdown page.
    """
    result = skeleton
    for idx, text in narratives.items():
        placeholder = _NARRATIVE_SLOT.format(idx=idx)
        result = result.replace(placeholder, text)

    # Remove any unreplaced placeholders
    result = re.sub(r"<!-- NARRATIVE_SLOT_\d+ -->", "", result)
    return result


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------


def render_deterministic_report(
    exposure: dict[str, Any],
    project_path: Path,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    on_progress: Optional[Callable[[str], None]] = None,
) -> tuple[str, str]:
    """Render a deterministic report from an exposure with sections.

    This is the main entry point called from the CLI when an exposure
    YAML contains a ``sections`` key.

    Args:
        exposure: Validated exposure dict with ``sections`` key.
        project_path: Path to the seeknal project root.
        provider: LLM provider override.
        model: Model name override.
        on_progress: Optional callback for progress updates.

    Returns:
        Tuple of (report_html_path, markdown_content).

    Raises:
        ValueError: If sections are invalid or report build fails.
    """
    from seeknal.ask.report.builder import build_report
    from seeknal.ask.report.scaffolder import scaffold_report

    def _progress(msg: str) -> None:
        if on_progress:
            on_progress(msg)

    # 1. Parse and validate sections
    _progress("Validating sections...")
    sections = parse_sections(exposure["sections"])
    total_queries = sum(len(s.queries) for s in sections)
    narrative_count = sum(1 for s in sections if s.narrative)
    _progress(
        f"Validated {len(sections)} sections, "
        f"{total_queries} queries, {narrative_count} narratives"
    )

    # 2. Pre-execute queries for LLM context
    _progress(f"Executing {total_queries} pinned queries...")
    query_results = execute_section_queries(
        sections, project_path, on_progress=on_progress,
    )
    errors = sum(1 for r in query_results.values() if r.get("error"))
    if errors:
        _progress(f"Queries done — {errors} failed")
    else:
        _progress(f"All {len(query_results)} queries executed")

    # 3. Generate Evidence markdown skeleton
    _progress("Generating Evidence markdown...")
    report_title = exposure.get("description", exposure.get("name", "Report"))
    skeleton = generate_evidence_page(sections, title=report_title)

    # 4. Generate LLM narratives (optional)
    params = exposure.get("params", {})
    global_context = params.get("prompt", "")
    has_narrative = any(s.narrative for s in sections)

    if has_narrative:
        _progress(f"Generating {narrative_count} narratives via LLM...")
        narratives = generate_narratives(
            sections, query_results,
            provider=provider, model=model,
            global_context=global_context,
            on_progress=on_progress,
        )
    else:
        narratives = {}

    # 5. Assemble final page
    _progress("Assembling final page...")
    page_content = assemble_page(skeleton, narratives)

    # 6. Scaffold & build via existing pipeline
    _progress("Scaffolding Evidence project...")
    pages = [{"name": "index", "content": page_content}]
    report_dir = scaffold_report(
        project_path=project_path,
        title=report_title,
        pages=pages,
    )

    _progress("Building HTML report (npm)...")
    html_path = build_report(report_dir)

    return html_path, page_content
