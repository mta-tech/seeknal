"""Runtime data-access governance gate, active only when Atlas is configured.

This extends the apply-time contract gate in :mod:`seeknal.integrations.atlas_client`
to *runtime* data access. When ``ATLAS_API_URL`` is set, Seeknal delegates every
governed read to the Atlas policy backend (OpenFGA-backed): Atlas decides whether
the caller may read a resource and which columns must be masked. The rules live in
Atlas — not in editable local YAML — so a project author cannot loosen governance
by editing a file.

Two deliberate properties:

* **Config-activated.** With no ``ATLAS_API_URL`` the gate is inactive and reads pass
  through unchanged (full backward compatibility). Governance turns on exactly when
  the operator configures the Atlas setup.
* **Fail-closed.** When the gate *is* active, a denied decision or a transport error
  results in *denied* access (unless ``ATLAS_FAIL_OPEN`` is explicitly set). Governance
  cannot be bypassed by breaking the connection to Atlas.

The gate reuses the connection settings and error types of the apply-time client so a
single Atlas configuration drives both surfaces.
"""

from __future__ import annotations

import getpass
import os
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

import httpx

from seeknal.integrations.atlas_client import (
    AtlasContractConfig,
    AtlasContractError,
    AtlasPolicyDenied,
)

__all__ = [
    "AccessDecision",
    "GovernanceGate",
    "create_governance_gate_from_env",
    "apply_column_masks",
    "mask_rows",
    "govern_read",
    "mask_value",
    "MASK_TOKEN",
]

#: Replacement rendered for a fully masked value.
MASK_TOKEN = "******"

_TRUTHY = {"1", "true", "yes", "on"}


def _fail_open_default() -> bool:
    """Return whether the gate should fail *open* on transport errors.

    Defaults to ``False`` (fail-closed). Set ``ATLAS_FAIL_OPEN=true`` only in
    environments where availability must win over enforcement.
    """

    return os.getenv("ATLAS_FAIL_OPEN", "").strip().lower() in _TRUTHY


@dataclass(frozen=True)
class AccessDecision:
    """Outcome of an Atlas access check for a single resource."""

    allowed: bool
    reason: str = ""
    masked_columns: tuple[str, ...] = ()
    classification: str = "internal"

    @property
    def has_masking(self) -> bool:
        """``True`` when Atlas requires one or more columns to be masked."""

        return bool(self.masked_columns)


def create_governance_gate_from_env() -> "GovernanceGate | None":
    """Build a gate only when Atlas access enforcement is configured.

    Returns ``None`` when ``ATLAS_API_URL`` is unset, signalling that governance is
    inactive and callers should pass data through unchanged.
    """

    base_url = os.getenv("ATLAS_API_URL", "").strip()
    if not base_url:
        return None

    timeout = float(os.getenv("ATLAS_API_TIMEOUT_SECONDS", "10"))
    token = os.getenv("ATLAS_API_TOKEN")
    config = AtlasContractConfig(
        base_url=base_url.rstrip("/"),
        token=token,
        timeout_seconds=timeout,
    )
    return GovernanceGate(config)


def mask_value(value: Any, *, keep: int = 0) -> Any:
    """Mask a single cell value, optionally preserving a leading prefix.

    ``None`` is preserved (a null stays null). With ``keep > 0`` the first ``keep``
    characters of the stringified value are retained and the remainder replaced with
    :data:`MASK_TOKEN` (e.g. ``keep=6`` turns ``"3201011234567"`` into ``"320101******"``).
    """

    if value is None:
        return None
    if keep <= 0:
        return MASK_TOKEN
    text = str(value)
    if len(text) <= keep:
        return text
    return f"{text[:keep]}{MASK_TOKEN}"


def apply_column_masks(
    rows: Iterable[Mapping[str, Any]],
    masked_columns: Sequence[str],
    *,
    keep: int = 0,
) -> list[dict[str, Any]]:
    """Return a masked copy of ``rows`` with ``masked_columns`` redacted.

    Operates on a sequence of row mappings (the lowest-common-denominator shape across
    DuckDB/Spark result rows). Columns not present in a row are ignored; non-masked
    columns are copied verbatim. The input is never mutated.
    """

    masked_set = set(masked_columns)
    if not masked_set:
        return [dict(row) for row in rows]

    result: list[dict[str, Any]] = []
    for row in rows:
        new_row = dict(row)
        for column in masked_set:
            if column in new_row:
                new_row[column] = mask_value(new_row[column], keep=keep)
        result.append(new_row)
    return result


def mask_rows(
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    masked_columns: Sequence[str],
    *,
    keep: int = 0,
) -> list[tuple[Any, ...]]:
    """Mask positional (``columns``, ``rows``) results — the ``execute_oneshot`` shape.

    ``columns`` are the column names aligned to each row tuple. Masked columns not
    present in ``columns`` are ignored. Returns new row tuples; inputs are not mutated.
    """

    target_indices = [index for index, name in enumerate(columns) if name in set(masked_columns)]
    if not target_indices:
        return [tuple(row) for row in rows]

    result: list[tuple[Any, ...]] = []
    for row in rows:
        new_row = list(row)
        for index in target_indices:
            if index < len(new_row):
                new_row[index] = mask_value(new_row[index], keep=keep)
        result.append(tuple(new_row))
    return result


def govern_read(
    gate: "GovernanceGate | None",
    *,
    resource: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    action: str = "read",
    actor: str | None = None,
    keep: int = 0,
) -> list[tuple[Any, ...]]:
    """Apply runtime governance to a positional read result.

    The single chokepoint a read call site needs:

    * ``gate is None`` (Atlas not configured) → rows pass through unchanged.
    * otherwise the read is access-checked via :meth:`GovernanceGate.enforce_access`
      (raising :class:`~seeknal.integrations.atlas_client.AtlasPolicyDenied` on deny),
      then any columns Atlas marks sensitive are masked.

    Returns the (possibly masked) rows as tuples.
    """

    if gate is None:
        return [tuple(row) for row in rows]
    decision = gate.enforce_access(resource=resource, action=action, actor=actor)
    return mask_rows(columns, rows, decision.masked_columns, keep=keep)


class GovernanceGate:
    """Delegates runtime access decisions and masking to the Atlas backend.

    Construct via :func:`create_governance_gate_from_env`. Pass an explicit
    ``client`` (e.g. backed by :class:`httpx.MockTransport`) in tests.
    """

    def __init__(
        self,
        config: AtlasContractConfig,
        *,
        fail_open: bool | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self.config = config
        self._fail_open = _fail_open_default() if fail_open is None else fail_open
        self._client = client

    @property
    def fail_open(self) -> bool:
        """Whether the gate allows access when Atlas is unreachable (default: False)."""

        return self._fail_open

    # -- HTTP plumbing -------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.config.token:
            headers["Authorization"] = f"Bearer {self.config.token}"
        return headers

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.config.base_url}{path}"
        try:
            if self._client is not None:
                response = self._client.post(url, json=payload, headers=self._headers())
            else:
                response = httpx.post(
                    url,
                    json=payload,
                    headers=self._headers(),
                    timeout=self.config.timeout_seconds,
                )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            message = exc.response.text.strip() or str(exc)
            raise AtlasContractError(
                f"Atlas governance request failed for {path}: {message}"
            ) from exc
        except httpx.HTTPError as exc:
            raise AtlasContractError(f"Atlas governance request failed for {path}: {exc}") from exc

    # -- Public API ----------------------------------------------------------

    def check_access(
        self,
        *,
        resource: str,
        action: str = "read",
        actor: str | None = None,
        classification: str | None = None,
    ) -> AccessDecision:
        """Ask Atlas whether ``actor`` may ``action`` ``resource``.

        On a transport error the result honours the fail-open/closed policy: a
        fail-closed gate (the default) returns a *denied* decision rather than
        propagating the error, so callers uniformly act on :class:`AccessDecision`.
        """

        resolved_actor = actor or os.getenv("ATLAS_ACTOR") or getpass.getuser()
        payload: dict[str, Any] = {
            "operation": "access-check",
            "resource": resource,
            "action": action,
            "actor": resolved_actor,
        }
        if classification:
            payload["classification"] = classification

        try:
            decision = self._post("/api/contracts/access-check", payload)
        except AtlasContractError as exc:
            if self._fail_open:
                return AccessDecision(
                    allowed=True,
                    reason=f"fail-open: Atlas unreachable ({exc})",
                )
            return AccessDecision(
                allowed=False,
                reason=f"fail-closed: Atlas unreachable ({exc})",
            )

        masked = decision.get("masked_columns") or []
        masked_columns = tuple(str(column) for column in masked if column)
        return AccessDecision(
            allowed=bool(decision.get("allowed", False)),
            reason=str(decision.get("reason", "")),
            masked_columns=masked_columns,
            classification=str(decision.get("classification", classification or "internal")),
        )

    def enforce_access(
        self,
        *,
        resource: str,
        action: str = "read",
        actor: str | None = None,
        classification: str | None = None,
    ) -> AccessDecision:
        """Like :meth:`check_access` but raise :class:`AtlasPolicyDenied` on deny.

        Returns the :class:`AccessDecision` (carrying any required masking) when
        access is allowed. Emits a best-effort audit event for the denial.
        """

        decision = self.check_access(
            resource=resource,
            action=action,
            actor=actor,
            classification=classification,
        )
        if not decision.allowed:
            self.audit(
                action=action,
                resource=resource,
                actor=actor,
                status="denied",
                details={"reason": decision.reason},
            )
            raise AtlasPolicyDenied(decision.reason or f"Atlas denied {action} on {resource}")
        return decision

    def audit(
        self,
        *,
        action: str,
        resource: str,
        actor: str | None = None,
        status: str = "success",
        details: dict[str, Any] | None = None,
    ) -> None:
        """Record a best-effort audit event. Never raises.

        Audit is advisory from the engine's perspective — the authoritative, immutable
        record lives in Atlas. A failure to log must not break a governed read.
        """

        resolved_actor = actor or os.getenv("ATLAS_ACTOR") or getpass.getuser()
        payload: dict[str, Any] = {
            "operation": "access",
            "action": action,
            "resource": resource,
            "actor": resolved_actor,
            "status": status,
            "project_name": os.getenv("SEEKNAL_PROJECT_NAME", ""),
            "environment": os.getenv("ATLAS_ENVIRONMENT", os.getenv("SEEKNAL_ENV", "dev")),
        }
        if details:
            payload["details"] = details
        try:
            self._post("/api/contracts/runs/report", payload)
        except AtlasContractError:
            pass
