"""Serving pipeline: verification, routing, enforcement, and parameter preparation."""

from __future__ import annotations

import hashlib
import re
import threading
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp

from seeknal.serving import enforce as _enforce_mod
from seeknal.serving.compilation_cache import (
    DEFAULT_COMPILATION_CACHE_MAX_ENTRIES,
    MAX_CACHEABLE_SQL_BYTES,
    CompilationCache,
    CompilationCacheKey,
)
from seeknal.serving.enforce import (
    EnforcementError,
    Grant,
    _table_full_name,
    apply_null_ordering,
    enforce,
)
from seeknal.serving.grant_token import verify_grant_token
from seeknal.serving.guardrails import check_guardrails
from seeknal.serving.parameter_binding import verify_parameter_digest
from seeknal.serving.placement import Placement
from seeknal.serving.router import Router, RoutingError

PARAM_MARKER_PATTERN = re.compile(r"__SEEKNAL_PARAM_\d+__")


@dataclass(frozen=True)
class PreparedQuery:
    """An engine-ready query prepared by ServingPipeline."""

    engine: str
    engine_kind: str
    sql: str
    params: tuple
    paramstyle: str
    rule: str
    reason: str
    row_limit: int | None
    masked_columns: tuple[str, ...]
    redacted_sql: str
    subject: str
    workspace: str
    audit_id: str
    cube_security_context: Mapping | None
    workload: str = "interactive"
    binding: Mapping[str, Any] = field(default_factory=dict)
    expires_at: float | None = None
    tables: tuple[str, ...] = ()
    access_pattern: str | None = None
    guard: str | None = None
    filter_columns: tuple[str, ...] = ()
    effective_max_rows: int | None = None
    api: str | None = None


def _extract_placeholder_index(node: exp.Expression) -> int:
    """Extract 1-based placeholder index from an exp.Parameter or exp.Placeholder."""
    raw = getattr(node, "name", None) or getattr(node, "this", None)
    if raw is None:
        raise EnforcementError(f"Anonymous placeholder node without index is not supported: {node}")
    raw_str = str(raw).lstrip("$")
    try:
        idx = int(raw_str)
    except ValueError as exc:
        raise EnforcementError(f"Non-numeric placeholder '${raw_str}' is not supported") from exc
    if idx < 1:
        raise EnforcementError(f"Placeholder index must be >= 1, got ${idx}")
    return idx


def _check_round_trip(
    enforced_stmt: exp.Expression,
    rendered_sql: str,
    target_dialect: str,
) -> None:
    """Re-parse rendered_sql with read=<target dialect> and verify tables/columns/literals."""
    try:
        reparsed_stmts = sqlglot.parse(rendered_sql, read=target_dialect)
    except Exception as exc:
        raise EnforcementError(
            "rendered SQL does not round-trip to the checked statement"
        ) from exc

    if len(reparsed_stmts) != 1 or reparsed_stmts[0] is None:
        raise EnforcementError(
            "rendered SQL does not round-trip to the checked statement"
        )

    reparsed = reparsed_stmts[0]

    # Tables: referenced physical names must be a subset
    cte_names_reparsed = {cte.alias_or_name.lower() for cte in reparsed.find_all(exp.CTE)}
    reparsed_tables = {
        _table_full_name(tbl).lower()
        for tbl in reparsed.find_all(exp.Table)
        if tbl.name.lower() not in cte_names_reparsed
    }

    cte_names_enforced = {cte.alias_or_name.lower() for cte in enforced_stmt.find_all(exp.CTE)}
    enforced_tables = {
        _table_full_name(tbl).lower()
        for tbl in enforced_stmt.find_all(exp.Table)
        if tbl.name.lower() not in cte_names_enforced
    }

    if not (reparsed_tables <= enforced_tables):
        raise EnforcementError(
            "rendered SQL does not round-trip to the checked statement"
        )

    # Columns: column names must be a subset
    reparsed_cols = {
        col.name.lower()
        for col in reparsed.find_all(exp.Column)
        if not isinstance(col.this, exp.Star) and not PARAM_MARKER_PATTERN.match(col.name)
    }

    enforced_cols = {
        col.name.lower()
        for col in enforced_stmt.find_all(exp.Column)
        if not isinstance(col.this, exp.Star) and not PARAM_MARKER_PATTERN.match(col.name)
    }
    enforced_cols |= {
        alias.alias.lower()
        for alias in enforced_stmt.find_all(exp.Alias)
        if not PARAM_MARKER_PATTERN.match(alias.alias)
    }

    if not (reparsed_cols <= enforced_cols):
        raise EnforcementError(
            "rendered SQL does not round-trip to the checked statement"
        )

    # Literals: literal values must never become identifiers
    enforced_literals = {
        lit.this
        for lit in enforced_stmt.find_all(exp.Literal)
        if lit.is_string
    }
    reparsed_literals = {
        lit.this
        for lit in reparsed.find_all(exp.Literal)
        if lit.is_string
    }
    lost_literals = enforced_literals - reparsed_literals

    if lost_literals:
        enforced_identifiers = {
            ident.name.lower()
            for ident in enforced_stmt.find_all((exp.Identifier, exp.Column))
            if not PARAM_MARKER_PATTERN.match(ident.name)
        }
        reparsed_identifiers = {
            ident.name.lower()
            for ident in reparsed.find_all((exp.Identifier, exp.Column))
            if not PARAM_MARKER_PATTERN.match(ident.name)
        }
        new_identifiers = reparsed_identifiers - enforced_identifiers
        if new_identifiers:
            raise EnforcementError(
                "rendered SQL does not round-trip to the checked statement"
            )


class ServingPipeline:
    """Pipelines grant verification, routing, policy enforcement, and parameter binding."""

    def __init__(
        self,
        placement: Placement,
        schemas: Mapping[str, Sequence[str]],
        public_keys: Mapping[str, Any],
        *,
        audience: str = "seeknal-serving",
        issuer: str = "atlas-catalog",
        default_catalog: str | None = None,
        cube_mode: str = "refuse",
        require_grant_typ: bool = False,
        max_grant_lifetime_seconds: float = 300,
        compilation_cache_max_entries: int = DEFAULT_COMPILATION_CACHE_MAX_ENTRIES,
        pii_tables: Collection[str] = (),
        on_row_cap: str = "truncate",
        metrics: Any = None,
    ) -> None:
        if cube_mode not in ("refuse", "jwt"):
            raise EnforcementError(f"invalid cube_mode '{cube_mode}', must be 'refuse' or 'jwt'")
        if on_row_cap not in ("truncate", "error"):
            raise EnforcementError(f"invalid on_row_cap '{on_row_cap}', must be 'truncate' or 'error'")
        self._compilation_cache = CompilationCache(
            max_entries=compilation_cache_max_entries
        )
        self.compilation_cache_max_entries = self._compilation_cache.max_entries
        self._lock = threading.Lock()
        self.placement = placement
        self.schemas = schemas
        self.public_keys = public_keys
        self.audience = audience
        self.issuer = issuer
        self.default_catalog = default_catalog
        self.cube_mode = cube_mode
        self.require_grant_typ = require_grant_typ
        self.max_grant_lifetime_seconds = float(max_grant_lifetime_seconds)
        self.pii_tables = tuple(pii_tables)
        self.on_row_cap = on_row_cap
        self.metrics = metrics
        self.router = Router(placement, pii_tables=self.pii_tables)
        self._catalog_generation = self._compilation_cache.generation

    def compilation_cache_info(self) -> dict[str, int]:
        """Return read-only numeric compilation cache counters."""
        return self._compilation_cache.info()

    def update_catalog(
        self,
        placement: Placement,
        schemas: Mapping[str, Sequence[str]],
    ) -> None:
        """Atomically update placement, schemas, router, and invalidate compilation cache."""
        new_router = Router(placement, pii_tables=self.pii_tables)
        with self._lock:
            self.placement = placement
            self.schemas = schemas
            self.router = new_router
            self._catalog_generation = self._compilation_cache.invalidate()

    def verify_grant(
        self,
        token: str,
        *,
        sql: str | bytes | None = None,
        params: Sequence | None = None,
        now: float | None = None,
    ) -> Grant:
        """Verify signed grant token and optional SQL/body and parameter digests."""
        claims = verify_grant_token(
            token,
            self.public_keys,
            audience=self.audience,
            issuer=self.issuer,
            now=now,
            require_grant_typ=self.require_grant_typ,
            max_lifetime_seconds=self.max_grant_lifetime_seconds,
        )
        audit_id = claims.get("jti")
        subject = claims.get("sub")

        try:
            grant = Grant.from_claims(claims)
            if "api" in claims and not hasattr(grant, "api"):
                object.__setattr__(grant, "_claims_api", claims.get("api"))

            if sql is not None:
                if isinstance(sql, str):
                    sql_bytes = sql.encode("utf-8")
                elif isinstance(sql, (bytes, bytearray, memoryview)):
                    sql_bytes = bytes(sql)
                else:
                    raise EnforcementError(
                        "SQL query must be a string", code="invalid_sql"
                    )
                computed_sql_sha256 = hashlib.sha256(sql_bytes).hexdigest()
                if computed_sql_sha256.lower() != grant.sql_sha256.lower():
                    raise EnforcementError(
                        f"SQL SHA256 mismatch: expected {grant.sql_sha256}, got {computed_sql_sha256}"
                    )

            if params is not None:
                if isinstance(
                    params, (str, bytes, bytearray, memoryview, Mapping)
                ) or not isinstance(params, Sequence):
                    raise EnforcementError("Parameters must be a sequence")
                raw_params = tuple(params)
                if grant.params_sha256 is not None:
                    verify_parameter_digest(raw_params, grant.params_sha256)

            return grant
        except EnforcementError as exc:
            exc.audit_id = getattr(exc, "audit_id", None) or audit_id
            exc.subject = getattr(exc, "subject", None) or subject
            raise

    def prepare_statement(
        self,
        statement: exp.Expression,
        grant: Grant,
        *,
        sql: str | bytes | None = None,
        params: Sequence = (),
        hint: str | None = None,
        max_staleness_seconds: float | None = None,
        now: float | None = None,
        max_rows: int | None = None,
        api: str | None = None,
        metrics: Any = None,
        _snapshot: tuple[Placement, Mapping[str, Sequence[str]], Router] | None = None,
    ) -> PreparedQuery:
        """Route, guardrail-check, enforce, rewrite, and render a parsed AST statement."""
        if _snapshot is not None:
            placement, schemas, router = _snapshot
        else:
            with self._lock:
                placement = self.placement
                schemas = self.schemas
                router = self.router

        try:
            if isinstance(
                params, (str, bytes, bytearray, memoryview, Mapping)
            ) or not isinstance(params, Sequence):
                raise EnforcementError("Parameters must be a sequence")
            raw_params = tuple(params)

            if raw_params and grant.sql_dialect in ("starrocks", "mysql"):
                raise EnforcementError(
                    f"Parameters are not supported for '{grant.sql_dialect}' source dialect"
                )

            if sql is None:
                enforce_sql_input: str | bytes = statement.sql(
                    dialect=grant.sql_dialect
                )
            elif isinstance(sql, str):
                enforce_sql_input = sql
            elif isinstance(sql, (bytes, bytearray, memoryview)):
                raw_sql_bytes = bytes(sql)
                try:
                    raw_sql_bytes.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise EnforcementError(
                        "Request payload must be valid UTF-8", code="invalid_sql"
                    ) from exc
                enforce_sql_input = raw_sql_bytes
            else:
                raise EnforcementError("SQL query must be a string", code="invalid_sql")

            # Canonicalize default catalog on two-part table references
            if self.default_catalog is not None:
                cte_names = {
                    cte.alias_or_name.lower() for cte in statement.find_all(exp.CTE)
                }
                for tbl in statement.find_all(exp.Table):
                    if (
                        tbl.name.lower() in cte_names
                        and not tbl.db
                        and not tbl.catalog
                    ) or (
                        tbl.parts
                        and ".".join(p.name for p in tbl.parts).lower() in cte_names
                    ):
                        continue
                    if tbl.db and tbl.name and not tbl.catalog:
                        tbl.set("catalog", exp.to_identifier(self.default_catalog))

            # 1. Routing decision on canonical statement
            decision = router.decide_statement(
                statement,
                hint=hint,
                max_staleness_seconds=max_staleness_seconds,
            )
            target_engine = placement.engines[decision.engine]
            target_dialect = target_engine.dialect
            target_kind = target_engine.kind

            # 2. Access pattern guardrails (after routing, before enforce)
            active_metrics = self.metrics if metrics is None else metrics
            guard_res = check_guardrails(
                statement,
                decision=decision,
                placement=placement,
                grant=grant,
                request_max_rows=max_rows,
                metrics=active_metrics,
            )

            cube_mask_variants = {
                c.name.lower(): c.mask_variants for c in placement.cubes.values()
            }
            cube_measures = {
                c.name.lower(): set(c.measures) for c in placement.cubes.values()
            }

            enforce_target_kind = (
                "lakebase"
                if target_kind == "postgres"
                and "postgres" not in _enforce_mod.VALID_TARGET_KINDS
                else target_kind
            )

            # 3. Policy enforcement (masks, row filters, row cap, column allowlists)
            enforced = enforce(
                enforce_sql_input,
                grant,
                schemas=schemas,
                target_dialect=target_dialect,
                target_kind=enforce_target_kind,
                dialect=grant.sql_dialect,
                statement=statement,
                now=now,
                max_rows=guard_res.effective_max_rows,
                cube_mode=self.cube_mode,
                cube_mask_variants=cube_mask_variants,
                cube_measures=cube_measures,
                params=raw_params,
                on_row_cap=self.on_row_cap,
            )

            # 4. Table rewrite for chosen engine physical names
            router.rewrite(enforced.statement, decision.engine)
            apply_null_ordering(enforced.statement, grant.sql_dialect, target_dialect)

            # 5. Parameters and dialect rendering
            if target_kind == "duckdb":
                paramstyle = "qmark"
            elif target_kind in ("starrocks", "lakebase", "postgres", "cube"):
                paramstyle = "format"
            else:
                paramstyle = "format"

            num_params = len(raw_params)
            placeholder_nodes = list(
                enforced.statement.find_all((exp.Parameter, exp.Placeholder))
            )

            if not placeholder_nodes:
                if num_params > 0:
                    raise EnforcementError(
                        f"Provided {num_params} parameters, but SQL query contains no placeholders"
                    )
                final_params: tuple = ()
                rendered_sql = enforced.statement.sql(dialect=target_dialect)
                _check_round_trip(enforced.statement, rendered_sql, target_dialect)
                if paramstyle == "format":
                    rendered_sql = rendered_sql.replace("%", "%%")
                final_sql = rendered_sql
            else:
                marker_to_idx: dict[str, int] = {}
                for i, node in enumerate(placeholder_nodes):
                    idx = _extract_placeholder_index(node)
                    if idx > num_params:
                        raise EnforcementError(
                            f"Placeholder ${idx} has no matching parameter ({num_params} provided)"
                        )
                    marker = f"__SEEKNAL_PARAM_{i}__"
                    marker_to_idx[marker] = idx
                    node.replace(exp.var(marker))

                used_indices = set(marker_to_idx.values())
                for p_idx in range(1, num_params + 1):
                    if p_idx not in used_indices:
                        raise EnforcementError(
                            f"Parameter ${p_idx} is never used in the query"
                        )

                rendered_sql = enforced.statement.sql(dialect=target_dialect)
                _check_round_trip(enforced.statement, rendered_sql, target_dialect)
                matches = PARAM_MARKER_PATTERN.findall(rendered_sql)
                ordered_indices = [marker_to_idx[m] for m in matches]
                final_params = tuple(raw_params[idx - 1] for idx in ordered_indices)

                if paramstyle == "format":
                    rendered_sql = rendered_sql.replace("%", "%%")
                    final_sql = PARAM_MARKER_PATTERN.sub("%s", rendered_sql)
                else:
                    final_sql = PARAM_MARKER_PATTERN.sub("?", rendered_sql)

            resolved_api = (
                api
                if api is not None
                else (
                    getattr(grant, "api", None)
                    or getattr(grant, "_claims_api", None)
                )
            )
            expires_at = (
                float(grant.expires_at) if grant.expires_at is not None else None
            )

            return PreparedQuery(
                engine=decision.engine,
                engine_kind=target_kind,
                sql=final_sql,
                params=final_params,
                paramstyle=paramstyle,
                rule=decision.rule,
                reason=decision.reason,
                row_limit=enforced.row_limit,
                masked_columns=enforced.masked_columns,
                redacted_sql=enforced.redacted_sql,
                subject=grant.subject,
                workspace=grant.workspace,
                audit_id=grant.audit_id,
                cube_security_context=enforced.cube_security_context,
                workload=grant.workload,
                binding=grant.binding,
                expires_at=expires_at,
                tables=enforced.tables,
                access_pattern=guard_res.access_pattern,
                guard=guard_res.outcome,
                filter_columns=guard_res.filter_columns,
                effective_max_rows=guard_res.effective_max_rows,
                api=resolved_api,
            )
        except (EnforcementError, RoutingError) as exc:
            exc.audit_id = getattr(exc, "audit_id", None) or grant.audit_id
            exc.subject = getattr(exc, "subject", None) or grant.subject
            raise

    def prepare(
        self,
        sql: str,
        token: str,
        *,
        params: Sequence = (),
        hint: str | None = None,
        max_staleness_seconds: float | None = None,
        now: float | None = None,
        max_rows: int | None = None,
        metrics: Any = None,
    ) -> PreparedQuery:
        """Prepare an engine-ready query from a client SQL and signed grant token."""
        with self._lock:
            placement = self.placement
            schemas = self.schemas
            router = self.router
            catalog_generation = self._catalog_generation

        if not isinstance(sql, str):
            # Verify token first so audit_id / subject are attached to the error
            grant = self.verify_grant(token, now=now)
            exc = EnforcementError("SQL query must be a string", code="invalid_sql")
            exc.audit_id = grant.audit_id
            exc.subject = grant.subject
            raise exc

        grant = self.verify_grant(token, sql=sql, params=params, now=now)

        try:
            raw_params = tuple(params)
            if raw_params and grant.sql_dialect in ("starrocks", "mysql"):
                raise EnforcementError(
                    f"Parameters are not supported for '{grant.sql_dialect}' source dialect"
                )

            sql_bytes = sql.encode("utf-8")
            computed_sql_sha256 = hashlib.sha256(sql_bytes).hexdigest()
            cache_eligible = (
                self.compilation_cache_max_entries > 0
                and grant.is_named_operation
                and len(sql_bytes) <= MAX_CACHEABLE_SQL_BYTES
            )
            cache_key: CompilationCacheKey | None = None
            statement: exp.Expression | None = None

            if cache_eligible:
                cache_key = CompilationCacheKey(
                    sql_sha256=computed_sql_sha256,
                    sql_dialect=grant.sql_dialect,
                    default_catalog=self.default_catalog,
                    catalog_generation=catalog_generation,
                )
                statement = self._compilation_cache.get(cache_key)

            if statement is None:
                try:
                    parsed_statements = sqlglot.parse(sql, read=grant.sql_dialect)
                except Exception as exc:
                    raise EnforcementError(
                        f"SQL parse error: {exc}", code="invalid_sql"
                    ) from exc

                if len(parsed_statements) != 1 or parsed_statements[0] is None:
                    raise EnforcementError(
                        f"expected exactly one statement, got {len(parsed_statements)}",
                        code="invalid_sql",
                    )

                statement = parsed_statements[0]
                if cache_eligible and cache_key is not None:
                    self._compilation_cache.put(cache_key, statement)

            return self.prepare_statement(
                statement,
                grant,
                sql=sql,
                params=raw_params,
                hint=hint,
                max_staleness_seconds=max_staleness_seconds,
                now=now,
                max_rows=max_rows,
                metrics=metrics,
                _snapshot=(placement, schemas, router),
            )
        except (EnforcementError, RoutingError) as exc:
            exc.audit_id = getattr(exc, "audit_id", None) or grant.audit_id
            exc.subject = getattr(exc, "subject", None) or grant.subject
            raise
