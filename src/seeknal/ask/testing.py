"""Project-local SQL-oracle tests for Seeknal Ask.

The runner is intentionally project-owned and domain-agnostic.  Test YAML
files provide a natural-language prompt plus expected SQL.  Seeknal executes
the SQL through the existing read-only REPL, optionally asks the real agent the
prompt, then checks the answer with generic assertions.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

TEST_ROOTS = (
    Path("seeknal") / "tests",
    Path("context") / "tests",
    Path("tests"),
)
SUPPORTED_SUFFIXES = {".yml", ".yaml"}


@dataclass
class AskSqlTestCase:
    """One project-local Ask SQL test case."""

    name: str
    prompt: str
    sql: str
    file_path: Path
    tags: list[str] = field(default_factory=list)
    assertions: dict[str, Any] = field(default_factory=dict)
    skip: str | None = None

    @classmethod
    def from_yaml(cls, file_path: Path) -> "AskSqlTestCase":
        """Load a test case from YAML.

        Accepted aliases mirror Nao's minimal shape while allowing clearer
        Seeknal names:
        - `prompt` or `question`
        - `sql` or `expected_sql`
        - `assert` or `assertions`
        """
        with file_path.open(encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        if not isinstance(data, dict):
            raise ValueError("test YAML must contain a mapping")

        prompt = data.get("prompt") or data.get("question")
        sql = data.get("sql") or data.get("expected_sql")
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError("test YAML must define `prompt` or `question`")
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError("test YAML must define `sql` or `expected_sql`")

        assertions = data.get("assertions", data.get("assert", {})) or {}
        if not isinstance(assertions, dict):
            raise ValueError("`assert`/`assertions` must be a mapping")

        raw_tags = data.get("tags", [])
        tags = [str(tag) for tag in raw_tags] if isinstance(raw_tags, list) else []

        skip = data.get("skip") or data.get("skip_reason")
        return cls(
            name=str(data.get("name") or data.get("id") or file_path.stem),
            prompt=prompt.strip(),
            sql=sql.strip(),
            file_path=file_path,
            tags=tags,
            assertions=assertions,
            skip=str(skip) if skip else None,
        )


@dataclass
class SqlOracleResult:
    """Structured expected-SQL result."""

    columns: list[str] = field(default_factory=list)
    rows: list[list[Any]] = field(default_factory=list)
    error: str | None = None


@dataclass
class AnswerCheckResult:
    """Result of comparing an agent answer with generic assertions."""

    passed: bool
    message: str
    missing: list[str] = field(default_factory=list)


@dataclass
class AskSqlTestResult:
    """Serializable result for one Ask SQL test case."""

    name: str
    file: str
    prompt: str
    passed: bool
    mode: str
    message: str
    skipped: bool = False
    sql_error: str | None = None
    expected_columns: list[str] = field(default_factory=list)
    expected_rows: list[list[Any]] = field(default_factory=list)
    actual_columns: list[str] = field(default_factory=list)
    actual_rows: list[list[Any]] = field(default_factory=list)
    answer: str | None = None
    missing_assertions: list[str] = field(default_factory=list)


@dataclass
class AskSqlTestSuiteResult:
    """Serializable result for a full Ask SQL test run."""

    project_path: str
    timestamp: str
    mode: str
    results: list[AskSqlTestResult]
    output_file: str | None = None

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def passed(self) -> int:
        return sum(1 for result in self.results if result.passed)

    @property
    def failed(self) -> int:
        return sum(1 for result in self.results if not result.passed)

    @property
    def skipped(self) -> int:
        return sum(1 for result in self.results if result.skipped)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["summary"] = {
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "skipped": self.skipped,
        }
        return data


def discover_ask_sql_tests(
    project_path: Path, select: str | None = None
) -> list[AskSqlTestCase]:
    """Discover project-local Ask SQL tests.

    Search order:
    - `seeknal/tests/*.yml`
    - `context/tests/*.yml`
    - `tests/*.yml`

    Top-level `tests/` is scanned only one level deep to avoid accidentally
    treating unrelated nested test fixtures as Ask SQL tests.
    """
    cases: list[AskSqlTestCase] = []
    for rel_root in TEST_ROOTS:
        root = project_path / rel_root
        if not root.exists():
            continue
        paths = sorted(root.glob("*.yml")) + sorted(root.glob("*.yaml"))
        if rel_root != Path("tests"):
            paths = sorted(root.rglob("*.yml")) + sorted(root.rglob("*.yaml"))
        for path in paths:
            if path.name == "outputs" or "outputs" in path.parts:
                continue
            try:
                case = AskSqlTestCase.from_yaml(path)
            except Exception as exc:  # noqa: BLE001 - surface as failed pseudo-case
                case = AskSqlTestCase(
                    name=path.stem,
                    prompt="",
                    sql="",
                    file_path=path,
                    assertions={"__load_error": f"Invalid Ask SQL test YAML: {exc}"},
                )
            if select and select not in {case.name, path.stem, path.name}:
                continue
            cases.append(case)
    return cases


def run_ask_sql_tests(
    project_path: Path,
    *,
    select: str | None = None,
    provider: str | None = None,
    model: str | None = None,
    sql_only: bool = False,
    output_dir: Path | None = None,
    save: bool = True,
) -> AskSqlTestSuiteResult:
    """Run project-local Ask SQL tests and optionally persist JSON results."""
    project_path = project_path.resolve()
    cases = discover_ask_sql_tests(project_path, select=select)
    mode = "sql-only" if sql_only else "agent"
    # Issue #68.3: read the project's number_format locale once so
    # _value_variants can emit dot-thousands-separator forms (e.g. '30.276')
    # for Indonesian / German / etc. agents.
    locale_tag = _read_project_number_locale(project_path)
    results = [
        _run_one_case(
            case,
            project_path,
            provider=provider,
            model=model,
            sql_only=sql_only,
            locale=locale_tag,
        )
        for case in cases
    ]
    suite = AskSqlTestSuiteResult(
        project_path=str(project_path),
        timestamp=datetime.now(timezone.utc).isoformat(),
        mode=mode,
        results=results,
    )
    if save:
        out = save_ask_sql_test_results(
            suite,
            output_dir or default_output_dir(project_path),
        )
        suite.output_file = str(out)
    return suite


def execute_expected_sql(project_path: Path, sql: str) -> SqlOracleResult:
    """Execute expected SQL — prefer direct psycopg2 for PG-only references.

    Routing:
    - If the SQL references only one connected-PG namespace and no
      unqualified tables, run it directly via psycopg2 (oracle PG path).
    - Otherwise, fall back to the legacy DuckDB REPL path.

    Once psycopg2 routing succeeds, execution failures are FATAL for this
    oracle invocation — they propagate up or return as
    ``SqlOracleResult(error=...)``. They do NOT silently fall back to
    DuckDB, because doing so would give the user a wrong-engine answer
    and defeat the purpose of the oracle path.
    """
    from seeknal.sources.config import SourceConfigError

    ns: str | None = None
    source = None
    try:
        from seeknal.ask._pg_oracle import detect_pg_only_namespace
        from seeknal.sources.config import load_source_registry

        registry = load_source_registry(project_path)
        ns = detect_pg_only_namespace(sql, registry)
        if ns is not None:
            # CRITICAL: registry.sources is keyed by .name (see
            # src/seeknal/sources/config.py:334 -- sources[source.name] =
            # source) not by .namespace (declared at line 294).
            # Iterate values() and match on .namespace.
            source = next(
                (s for s in registry.sources.values() if s.namespace == ns),
                None,
            )
    except (KeyError, SourceConfigError):
        # Routing/detection error -> fall through to DuckDB path.
        ns = None
        source = None

    if ns is not None and source is not None:
        # Routing succeeded. From here on, psycopg2 errors are FATAL for
        # this oracle invocation -- no DuckDB fallback (Principle #2).
        from seeknal.ask._pg_oracle import (
            execute_via_psycopg2,
            resolve_pg_dsn,
            strip_namespace,
        )
        from seeknal.workflow.materialization.profile_loader import ProfileLoader

        # PRIVATE-API COUPLING: ProfileLoader._load_profile_data() is
        # private (leading underscore). A regression test in
        # tests/ask/test_oracle_psycopg2.py (AC-D6) asserts the attribute
        # exists so a future rename fails loudly with a clear pointer.
        profile_path = project_path / "profiles.yml"
        loader = ProfileLoader(
            profile_path=profile_path if profile_path.exists() else None
        )
        profile_data = loader._load_profile_data()

        dsn = resolve_pg_dsn(source, profile_data)
        stripped = strip_namespace(sql, ns)
        return execute_via_psycopg2(dsn, stripped)

    # --- DuckDB REPL path (with rewrite) ---
    # Issue #66: REPL holds a DuckDB connection plus ATTACH'd PG sources.
    # Without an explicit close, each oracle SQL invocation leaks the
    # underlying libpq connections until GC runs, exhausting the PG server's
    # ``max_connections`` on suites with 20+ tests. Close deterministically
    # in finally so connections are released as soon as the oracle returns.
    repl = None
    try:
        from seeknal.ask.agents.tools.execute_sql import _rewrite_for_pg_pushdown
        from seeknal.cli.repl import REPL

        repl = REPL(project_path=project_path, skip_history=True)
        sql_for_duckdb, _notices = _rewrite_for_pg_pushdown(sql)
        columns, rows = repl.execute_oneshot(sql_for_duckdb)
        return SqlOracleResult(
            columns=[str(col) for col in columns],
            rows=[[_jsonable(cell) for cell in row] for row in rows],
        )
    except Exception as exc:  # noqa: BLE001 - test result should capture failure
        return SqlOracleResult(error=str(exc))
    finally:
        if repl is not None:
            try:
                repl.conn.close()
            except Exception:  # noqa: BLE001 - best-effort close on teardown
                pass


def run_agent_answer(
    project_path: Path,
    prompt: str,
    *,
    provider: str | None = None,
    model: str | None = None,
) -> str:
    """Run the real Ask agent once in a headless environment."""
    from seeknal.ask.agents.agent import ask as agent_ask
    from seeknal.ask.agents.agent import create_agent
    from seeknal.ask.agents.tools._context import get_tool_context, set_tool_context

    try:
        previous_context = get_tool_context()
    except RuntimeError:
        previous_context = None

    try:
        agent, deps, message_history, _cost_info = create_agent(
            project_path,
            provider=provider,
            model=model,
            environment="gateway",
        )
        return agent_ask(agent, deps, message_history, prompt)
    finally:
        if previous_context is not None:
            set_tool_context(previous_context)


def check_answer(
    answer: str,
    oracle: SqlOracleResult,
    assertions: dict[str, Any] | None = None,
    *,
    locale: str | None = None,
) -> AnswerCheckResult:
    """Check an answer using generic project-owned assertions.

    Supported assertions:
    - `answer_contains`: string or list of strings required in answer.
    - `answer_not_contains`: string or list of forbidden strings.
    - `expected_values`: bool. Active **only** when `answer_contains` is not
      set. When active, samples values from the oracle SQL result and requires
      each to appear in the answer text. Set to ``false`` to disable. **Ignored
      (treated as ``false``) whenever ``answer_contains`` is present** — these
      two settings are mutually exclusive.
    - `max_expected_values`: int, default 20.
    - `case_sensitive`: bool, default false.
    - `compare: dataframe`: parse a markdown/JSON table from the answer and
      compare it with the expected SQL rows for the expected columns.
    - `numeric_tolerance` / `tolerance`: absolute tolerance for dataframe
      numeric comparisons. Default: 0.01.

    Args:
        locale: Optional locale tag (e.g. ``"id_ID"``, ``"de_DE"``). When set,
            ``_value_variants`` emits locale-aware number variants so dot-as-
            thousands-separator agents (Indonesian, German, etc.) match.
    """
    assertions = assertions or {}
    compare = str(assertions.get("compare", "")).strip().lower()
    if compare in {"dataframe", "table", "rows"}:
        return _check_answer_dataframe(answer, oracle, assertions, locale=locale)

    case_sensitive = bool(assertions.get("case_sensitive", False))
    haystack = answer if case_sensitive else answer.lower()
    missing: list[str] = []

    required = _as_list(assertions.get("answer_contains"))
    forbidden = _as_list(assertions.get("answer_not_contains"))

    for item in required:
        needle = item if case_sensitive else item.lower()
        if needle not in haystack:
            missing.append(f"missing required text: {item}")
    for item in forbidden:
        needle = item if case_sensitive else item.lower()
        if needle in haystack:
            missing.append(f"forbidden text present: {item}")

    expected_values_enabled = bool(assertions.get("expected_values", not required))
    # Issue #68.1: a test with no assertions otherwise passes silently. Warn
    # loudly when nothing is active so misconfigured YAML surfaces in CI logs.
    if not required and not forbidden and not expected_values_enabled:
        import warnings

        warnings.warn(
            "Ask SQL test has no active assertions and will always pass. "
            "Add answer_contains, answer_not_contains, or expected_values: true.",
            stacklevel=2,
        )
    if expected_values_enabled and not required:
        max_values = int(assertions.get("max_expected_values", 20))
        for value in _sample_expected_values(oracle, max_values=max_values):
            variants = _value_variants(value, locale=locale)
            if not variants:
                continue
            if not any(
                (variant if case_sensitive else variant.lower()) in haystack
                for variant in variants
            ):
                missing.append(f"missing expected value: {value}")

    if missing:
        return AnswerCheckResult(False, "answer assertions failed", missing)
    return AnswerCheckResult(True, "answer matched assertions")


def extract_answer_table(
    answer: str,
    expected_columns: list[str],
) -> tuple[list[str], list[list[Any]], str | None]:
    """Extract a structured table from an agent answer.

    This deterministic parser supports the common forms agents already emit in
    TUI/chat: markdown tables and JSON arrays of objects.  It deliberately does
    not know anything about a domain; expected SQL column names are the schema.
    """
    columns, rows = _extract_json_rows(answer, expected_columns)
    if columns:
        return columns, rows, None

    columns, rows = _extract_markdown_rows(answer, expected_columns)
    if columns:
        return columns, rows, None

    return [], [], "no parseable markdown or JSON table found in answer"


def save_ask_sql_test_results(suite: AskSqlTestSuiteResult, output_dir: Path) -> Path:
    """Write suite results to a timestamped JSON file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"ask_sql_results_{timestamp}.json"
    path.write_text(
        json.dumps(suite.to_dict(), indent=2, default=str), encoding="utf-8"
    )
    return path


def default_output_dir(project_path: Path) -> Path:
    """Return the default project-local result directory."""
    if (project_path / "seeknal" / "tests").exists():
        return project_path / "seeknal" / "tests" / "outputs"
    if (project_path / "tests").exists():
        return project_path / "tests" / "outputs"
    return project_path / "seeknal" / "tests" / "outputs"


def resolve_ask_sql_test_path(project_path: Path, name_or_path: str) -> Path | None:
    """Resolve a test by listed path, file name, stem, or test name."""
    if not name_or_path or not isinstance(name_or_path, str):
        return None
    candidate = Path(name_or_path)
    if candidate.is_absolute() or ".." in candidate.parts:
        return None

    direct = project_path / candidate
    matches: list[Path] = []
    if (
        direct.exists()
        and direct.is_file()
        and direct.suffix.lower() in SUPPORTED_SUFFIXES
    ):
        matches.append(direct)

    for case in discover_ask_sql_tests(project_path):
        rel = case.file_path.relative_to(project_path).as_posix()
        if name_or_path in {case.name, case.file_path.stem, case.file_path.name, rel}:
            matches.append(case.file_path)

    unique = sorted({path.resolve() for path in matches})
    return unique[0] if len(unique) == 1 else None


def discover_ask_sql_result_files(project_path: Path) -> list[Path]:
    """List saved Ask SQL test JSON result files, newest first."""
    roots = [
        project_path / "seeknal" / "tests" / "outputs",
        project_path / "context" / "tests" / "outputs",
        project_path / "tests" / "outputs",
    ]
    files: list[Path] = []
    for root in roots:
        if root.exists():
            files.extend(root.glob("ask_sql_results_*.json"))
    return sorted(
        {path.resolve() for path in files},
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )


def resolve_ask_sql_result_path(
    project_path: Path, name_or_path: str = "latest"
) -> Path | None:
    """Resolve a saved result by path, filename/stem, or `latest`."""
    target = (name_or_path or "latest").strip()
    if target == "latest":
        files = discover_ask_sql_result_files(project_path)
        return files[0] if files else None
    candidate = Path(target)
    if candidate.is_absolute() or ".." in candidate.parts:
        return None
    direct = project_path / candidate
    if direct.exists() and direct.is_file() and direct.suffix.lower() == ".json":
        return direct
    matches = []
    for path in discover_ask_sql_result_files(project_path):
        rel = path.relative_to(project_path).as_posix()
        if target in {path.name, path.stem, rel}:
            matches.append(path)
    unique = sorted({path.resolve() for path in matches})
    return unique[0] if len(unique) == 1 else None


def _run_one_case(
    case: AskSqlTestCase,
    project_path: Path,
    *,
    provider: str | None,
    model: str | None,
    sql_only: bool,
    locale: str | None = None,
) -> AskSqlTestResult:
    rel = case.file_path.relative_to(project_path).as_posix()
    load_error = case.assertions.get("__load_error")
    if load_error:
        return AskSqlTestResult(
            name=case.name,
            file=rel,
            prompt=case.prompt,
            passed=False,
            mode="load",
            message=str(load_error),
        )
    if case.skip:
        return AskSqlTestResult(
            name=case.name,
            file=rel,
            prompt=case.prompt,
            passed=True,
            mode="skipped",
            message=case.skip,
            skipped=True,
        )

    oracle = execute_expected_sql(project_path, case.sql)
    if oracle.error:
        return AskSqlTestResult(
            name=case.name,
            file=rel,
            prompt=case.prompt,
            passed=False,
            mode="sql-only" if sql_only else "agent",
            message="expected SQL failed",
            sql_error=oracle.error,
        )

    if sql_only:
        return AskSqlTestResult(
            name=case.name,
            file=rel,
            prompt=case.prompt,
            passed=True,
            mode="sql-only",
            message="expected SQL passed",
            expected_columns=oracle.columns,
            expected_rows=oracle.rows,
        )

    try:
        answer = run_agent_answer(
            project_path, case.prompt, provider=provider, model=model
        )
    except Exception as exc:  # noqa: BLE001 - capture agent failures in QA result
        return AskSqlTestResult(
            name=case.name,
            file=rel,
            prompt=case.prompt,
            passed=False,
            mode="agent",
            message="agent run failed",
            sql_error=None,
            expected_columns=oracle.columns,
            expected_rows=oracle.rows,
            answer=None,
            missing_assertions=[str(exc)],
        )

    check = check_answer(answer, oracle, case.assertions, locale=locale)
    actual_columns: list[str] = []
    actual_rows: list[list[Any]] = []
    if str(case.assertions.get("compare", "")).strip().lower() in {
        "dataframe",
        "table",
        "rows",
    }:
        actual_columns, actual_rows, _parse_error = extract_answer_table(
            answer,
            oracle.columns,
        )
    return AskSqlTestResult(
        name=case.name,
        file=rel,
        prompt=case.prompt,
        passed=check.passed,
        mode="agent",
        message=check.message,
        expected_columns=oracle.columns,
        expected_rows=oracle.rows,
        actual_columns=actual_columns,
        actual_rows=actual_rows,
        answer=answer,
        missing_assertions=check.missing,
    )


def _check_answer_dataframe(
    answer: str,
    oracle: SqlOracleResult,
    assertions: dict[str, Any],
    *,
    locale: str | None = None,
) -> AnswerCheckResult:
    actual_columns, actual_rows, parse_error = extract_answer_table(
        answer, oracle.columns
    )
    if parse_error:
        return AnswerCheckResult(False, "dataframe comparison failed", [parse_error])

    tolerance = float(
        assertions.get("numeric_tolerance", assertions.get("tolerance", 0.01))
    )
    order_sensitive = bool(assertions.get("order_sensitive", False))
    passed, message = _compare_rows(
        actual_rows,
        oracle.rows,
        columns=oracle.columns,
        tolerance=tolerance,
        order_sensitive=order_sensitive,
    )
    if not passed:
        return AnswerCheckResult(False, "dataframe comparison failed", [message])

    # Optional prose checks still apply after dataframe match.
    required = _as_list(assertions.get("answer_contains"))
    forbidden = _as_list(assertions.get("answer_not_contains"))
    if required or forbidden:
        extra = dict(assertions)
        extra.pop("compare", None)
        extra["expected_values"] = False
        prose = check_answer(answer, oracle, extra, locale=locale)
        if not prose.passed:
            return prose
    return AnswerCheckResult(True, "dataframe matched expected SQL")


def _extract_json_rows(
    answer: str,
    expected_columns: list[str],
) -> tuple[list[str], list[list[Any]]]:
    snippets = [answer.strip()]
    snippets.extend(
        re.findall(r"```(?:json)?\s*(.*?)```", answer, flags=re.DOTALL | re.I)
    )
    expected_lower = [col.lower() for col in expected_columns]
    for snippet in snippets:
        try:
            data = json.loads(snippet)
        except Exception:
            continue
        if isinstance(data, dict) and isinstance(data.get("data"), list):
            data = data["data"]
        if not isinstance(data, list) or not all(isinstance(row, dict) for row in data):
            continue
        keys = {str(key).lower(): str(key) for row in data for key in row}
        if not all(col in keys for col in expected_lower):
            continue
        columns = [keys[col] for col in expected_lower]
        rows = [
            [_jsonable(row.get(keys[col])) for col in expected_lower] for row in data
        ]
        return expected_columns, rows
    return [], []


def _extract_markdown_rows(
    answer: str,
    expected_columns: list[str],
) -> tuple[list[str], list[list[Any]]]:
    lines = [line.strip() for line in answer.splitlines()]
    expected_lower = [col.lower() for col in expected_columns]
    for idx, line in enumerate(lines[:-1]):
        if "|" not in line:
            continue
        separator = lines[idx + 1]
        if not re.fullmatch(r"\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?", separator):
            continue
        headers = [_clean_cell(cell) for cell in line.strip("|").split("|")]
        header_map = {header.lower(): pos for pos, header in enumerate(headers)}
        if not all(col in header_map for col in expected_lower):
            continue
        rows: list[list[Any]] = []
        for raw in lines[idx + 2 :]:
            if "|" not in raw or re.fullmatch(r"\|?\s*-+\s*(\|\s*-+\s*)+\|?", raw):
                break
            cells = [_clean_cell(cell) for cell in raw.strip("|").split("|")]
            if len(cells) < len(headers):
                continue
            rows.append(
                [_parse_scalar(cells[header_map[col]]) for col in expected_lower]
            )
        return expected_columns, rows
    return [], []


def _compare_rows(
    actual_rows: list[list[Any]],
    expected_rows: list[list[Any]],
    *,
    columns: list[str],
    tolerance: float,
    order_sensitive: bool,
) -> tuple[bool, str]:
    if len(actual_rows) != len(expected_rows):
        return (
            False,
            f"row count differs: actual {len(actual_rows)} vs expected {len(expected_rows)}",
        )

    actual = [_normalize_row(row, tolerance=tolerance) for row in actual_rows]
    expected = [_normalize_row(row, tolerance=tolerance) for row in expected_rows]
    if not order_sensitive:
        actual = sorted(
            actual, key=lambda row: json.dumps(row, sort_keys=True, default=str)
        )
        expected = sorted(
            expected, key=lambda row: json.dumps(row, sort_keys=True, default=str)
        )

    for row_idx, (actual_row, expected_row) in enumerate(
        zip(actual, expected), start=1
    ):
        for col_idx, (actual_value, expected_value) in enumerate(
            zip(actual_row, expected_row),
        ):
            if not _values_equal(actual_value, expected_value, tolerance=tolerance):
                column = (
                    columns[col_idx] if col_idx < len(columns) else f"col{col_idx + 1}"
                )
                return (
                    False,
                    f"row {row_idx} column {column} differs: actual={actual_value!r}, expected={expected_value!r}",
                )
    return True, "match"


def _normalize_row(row: list[Any], *, tolerance: float) -> list[Any]:
    return [_parse_scalar(value) for value in row]


def _values_equal(actual: Any, expected: Any, *, tolerance: float) -> bool:
    actual_num = _to_number(actual)
    expected_num = _to_number(expected)
    if actual_num is not None and expected_num is not None:
        return abs(actual_num - expected_num) <= tolerance
    return str(actual).strip().lower() == str(expected).strip().lower()


def _to_number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (int, float, bool)):
        return value
    text = _clean_cell(str(value))
    if text.lower() in {"null", "none", "nan", ""}:
        return None
    number = _to_number(text)
    if number is not None:
        if float(number).is_integer():
            return int(number)
        return number
    return text


def _clean_cell(value: str) -> str:
    return re.sub(r"<[^>]+>", "", value).replace("**", "").replace("`", "").strip()


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


def _sample_expected_values(oracle: SqlOracleResult, *, max_values: int) -> list[Any]:
    values: list[Any] = []
    seen: set[str] = set()
    for row in oracle.rows:
        for value in row:
            if value is None:
                continue
            key = str(value)
            if key in seen:
                continue
            seen.add(key)
            values.append(value)
            if len(values) >= max_values:
                return values
    return values


def _read_project_number_locale(project_path: Path) -> str | None:
    """Return the project's ``locale.number_format`` tag, or ``None``.

    Issue #68.3: when ``seeknal_agent.yml`` declares a non-English number
    locale, the agent writes numbers using dot-as-thousands-separator
    (e.g. ``30.276`` for thirty thousand in Indonesian). ``_value_variants``
    consults this tag so ``expected_values: true`` keeps matching.
    """
    try:
        from seeknal.ask.config import load_agent_config

        cfg = load_agent_config(project_path)
    except Exception:  # noqa: BLE001 - best-effort; missing/invalid YAML is fine
        return None
    locale_section = cfg.get("locale") if isinstance(cfg, dict) else None
    if not isinstance(locale_section, dict):
        return None
    tag = locale_section.get("number_format") or locale_section.get("language")
    return str(tag) if tag else None


# Issue #68.3: locales that use '.' as thousands separator and ',' as decimal.
# When the project's number_format matches one of these, the agent will
# typically write `30.276` (thirty thousand) rather than `30,276`; the variants
# table must include that form so expected_values doesn't always fail.
_DOT_THOUSANDS_LOCALES = frozenset(
    {"id", "de", "nl", "pt", "it", "es", "fr", "tr", "el", "ro", "pl", "cs"}
)


def _locale_uses_dot_thousands(locale: str | None) -> bool:
    if not locale:
        return False
    return locale.split("_")[0].lower() in _DOT_THOUSANDS_LOCALES


def _value_variants(value: Any, locale: str | None = None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, bool):
        return [str(value).lower()]
    if isinstance(value, int):
        variants = [str(value), f"{value:,}"]
        if _locale_uses_dot_thousands(locale):
            # Indonesian / German / etc. agents emit `30.276` for 30276.
            variants.append(f"{value:,}".replace(",", "."))
        return variants
    if isinstance(value, float):
        if not math.isfinite(value):
            return []
        variants = {
            str(value),
            f"{value:g}",
            f"{value:,.0f}",
            f"{value:,.2f}",
            f"{value:.2f}",
        }
        out = sorted(
            variant.rstrip("0").rstrip(".") if "." in variant else variant
            for variant in variants
        )
        if _locale_uses_dot_thousands(locale):
            # For floats, dot-thousands locales also swap the decimal: emit
            # the comma-decimal form alongside the en-US dot-decimal forms.
            comma_decimal = f"{value:,.2f}".replace(",", "\x00").replace(".", ",").replace("\x00", ".")
            out = sorted(set(out) | {comma_decimal})
        return out
    text = str(value).strip()
    if not text:
        return []
    # Very short strings such as category code "1" are usually too noisy unless
    # explicitly listed in `answer_contains`.
    if len(text) < 2:
        return []
    return [text, re.sub(r"\s+", " ", text)]


def _jsonable(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        return str(value)
