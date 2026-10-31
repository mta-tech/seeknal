"""Serving pipeline: verification, routing, enforcement, and parameter preparation."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from sqlglot import exp

from seeknal.serving.enforce import EnforcementError, Grant, enforce
from seeknal.serving.grant_token import verify_grant_token
from seeknal.serving.placement import Placement
from seeknal.serving.router import Router

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
    ) -> None:
        self.placement = placement
        self.schemas = schemas
        self.public_keys = public_keys
        self.audience = audience
        self.issuer = issuer
        self.router = Router(placement)

    def prepare(
        self,
        sql: str,
        token: str,
        *,
        params: Sequence = (),
        hint: str | None = None,
        max_staleness_seconds: float | None = None,
        now: float | None = None,
    ) -> PreparedQuery:
        """Prepare an engine-ready query from a client SQL and signed grant token."""
        # 1. Verify token and decode Grant
        claims = verify_grant_token(
            token,
            self.public_keys,
            audience=self.audience,
            issuer=self.issuer,
            now=now,
        )
        grant = Grant.from_claims(claims)

        # 2. Routing decision on original SQL to preserve client statement shape
        decision = self.router.decide(
            sql,
            hint=hint,
            max_staleness_seconds=max_staleness_seconds,
        )
        target_engine = self.placement.engines[decision.engine]
        target_dialect = target_engine.dialect
        target_kind = target_engine.kind

        # 3. Policy enforcement (masks, row filters, row cap, column allowlists)
        enforced = enforce(
            sql,
            grant,
            schemas=self.schemas,
            target_dialect=target_dialect,
            target_kind=target_kind,
            dialect="postgres",
            now=now,
        )

        # 4. Table rewrite for chosen engine physical names
        self.router.rewrite(enforced.statement, decision.engine)

        # 5. Parameters and dialect rendering
        if target_kind == "duckdb":
            paramstyle = "qmark"
        elif target_kind in ("starrocks", "lakebase", "cube"):
            paramstyle = "format"
        else:
            paramstyle = "format"

        raw_params = tuple(params)
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
            matches = PARAM_MARKER_PATTERN.findall(rendered_sql)
            ordered_indices = [marker_to_idx[m] for m in matches]
            final_params = tuple(raw_params[idx - 1] for idx in ordered_indices)

            if paramstyle == "format":
                # Escape every literal % as %% so driver format strings survive
                rendered_sql = rendered_sql.replace("%", "%%")
                final_sql = PARAM_MARKER_PATTERN.sub("%s", rendered_sql)
            else:
                final_sql = PARAM_MARKER_PATTERN.sub("?", rendered_sql)

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
        )
