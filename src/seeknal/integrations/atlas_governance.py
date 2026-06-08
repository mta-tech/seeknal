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

import base64
import binascii
import getpass
import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import httpx

from seeknal.integrations.atlas_client import (
    AtlasAuthError,
    AtlasContractConfig,
    AtlasContractError,
    AtlasPolicyDenied,
    SESSION_EXPIRED_HINT,
)
from seeknal.integrations.atlas_config import atlas_config

__all__ = [
    "AccessDecision",
    "GovernanceGate",
    "create_governance_gate_from_env",
    "apply_column_masks",
    "mask_rows",
    "govern_read",
    "govern_query",
    "referenced_tables",
    "mask_value",
    "MASK_TOKEN",
    "credentials_path",
    "user_token_from_credentials",
    "user_sub_from_credentials",
    "user_email_from_credentials",
    "refresh_access_token",
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


#: Default location of the seeknal credentials file written by the login flow.
_DEFAULT_CREDENTIALS_PATH = "~/.config/seeknal/credentials.json"


def credentials_path() -> Path:
    """Return the resolved path to the seeknal credentials file.

    Honours the ``SEEKNAL_CREDENTIALS_PATH`` environment override, falling back to
    ``~/.config/seeknal/credentials.json``. The file holds the JSON written by the
    login flow (``access_token``, ``refresh_token``, ...); this function does not
    require the file to exist.
    """

    override = os.getenv("SEEKNAL_CREDENTIALS_PATH", "").strip()
    raw = override or _DEFAULT_CREDENTIALS_PATH
    return Path(raw).expanduser()


def _read_credentials() -> dict[str, Any] | None:
    """Load the credentials JSON, returning ``None`` if absent or invalid.

    Never raises: a missing file, unreadable file, or malformed JSON all yield
    ``None`` so callers can treat "logged out" and "broken creds" uniformly.
    """

    path = credentials_path()
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, ValueError):
        return None
    try:
        data = json.loads(text)
    except (ValueError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def _user_token_from_credentials() -> str | None:
    """Return the stored ``access_token`` from the credentials file, or ``None``.

    Used by :func:`create_governance_gate_from_env` so per-user identity flows to
    Atlas once the user has logged in. Returns ``None`` (never raises) when the
    credentials file is missing, malformed, or carries no usable token.
    """

    creds = _read_credentials()
    if not creds:
        return None
    token = creds.get("access_token")
    if isinstance(token, str) and token.strip():
        return token
    return None


#: Public alias for reuse by the CLI; mirrors the private gate-side reader.
user_token_from_credentials = _user_token_from_credentials


#: OIDC config for refreshing the stored access token. Mirrors ``cli/auth.py`` so a
#: single ``KEYCLOAK_ISSUER`` / ``SEEKNAL_OIDC_CLIENT_ID`` drives login *and* refresh.
_DEFAULT_OIDC_ISSUER = "http://localhost:8080/realms/atlas"
_DEFAULT_OIDC_CLIENT_ID = "seeknal-cli"
#: Seconds to wait on the token endpoint before giving up on a refresh.
_REFRESH_TIMEOUT_SECONDS = 30.0


def _oidc_issuer() -> str:
    """Return the Keycloak realm issuer used for token refresh (no trailing slash)."""

    return (
        os.getenv("KEYCLOAK_ISSUER", "").strip()
        or atlas_config().keycloak_issuer
        or _DEFAULT_OIDC_ISSUER
    ).rstrip("/")


def _oidc_client_id() -> str:
    """Return the public OIDC client id used for token refresh."""

    return (
        os.getenv("SEEKNAL_OIDC_CLIENT_ID", "").strip()
        or atlas_config().oidc_client_id
        or _DEFAULT_OIDC_CLIENT_ID
    )


def _persist_refreshed_tokens(existing: dict[str, Any], tokens: dict[str, Any]) -> None:
    """Best-effort write of refreshed tokens to the credentials file (mode 0600).

    Preserves the previous ``refresh_token``/``refresh_expires_in`` when the refresh
    response omits them (Keycloak usually rotates the refresh token, but not always).
    Never raises: a refresh whose token we already hold in memory must still succeed
    even if the on-disk write fails.
    """

    payload = {
        "access_token": tokens.get("access_token"),
        "refresh_token": tokens.get("refresh_token") or existing.get("refresh_token"),
        "expires_in": tokens.get("expires_in"),
        "refresh_expires_in": (
            tokens.get("refresh_expires_in") or existing.get("refresh_expires_in")
        ),
    }
    try:
        path = credentials_path()
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        # Write to a temp file that is owner-only (0600) from creation, then atomically
        # rename into place. This avoids the brief world/group-readable window of
        # ``write_text`` + ``chmod`` and prevents a torn file if the process dies mid-write.
        fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".credentials-", suffix=".tmp")
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, indent=2))
            os.replace(tmp, path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
    except OSError:
        pass


def refresh_access_token(*, client: httpx.Client | None = None) -> str | None:
    """Mint a fresh access token from the stored refresh token, or return ``None``.

    Uses the OAuth2 ``refresh_token`` grant against the Keycloak realm (the public
    ``seeknal-cli`` client). On success the rotated tokens are persisted to the
    credentials file (mode 0600) and the new access token is returned. Returns
    ``None`` (never raises) when there is no usable refresh token, the grant is
    rejected (e.g. the refresh token itself has expired), or the identity provider is
    unreachable — callers then surface a "log in again" hint instead of a traceback.

    ``client`` lets tests inject an :class:`httpx.Client` (e.g. ``MockTransport``).
    """

    creds = _read_credentials()
    if not creds:
        return None
    refresh_token = creds.get("refresh_token")
    if not isinstance(refresh_token, str) or not refresh_token.strip():
        return None

    data = {
        "grant_type": "refresh_token",
        "client_id": _oidc_client_id(),
        "refresh_token": refresh_token,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    endpoint = f"{_oidc_issuer()}/protocol/openid-connect/token"
    try:
        if client is not None:
            response = client.post(
                endpoint, data=data, headers=headers, timeout=_REFRESH_TIMEOUT_SECONDS
            )
        else:
            response = httpx.post(
                endpoint, data=data, headers=headers, timeout=_REFRESH_TIMEOUT_SECONDS
            )
        response.raise_for_status()
        tokens = response.json()
    except (httpx.HTTPError, ValueError):
        return None
    if not isinstance(tokens, dict):
        return None
    new_token = tokens.get("access_token")
    if not isinstance(new_token, str) or not new_token.strip():
        return None
    _persist_refreshed_tokens(creds, tokens)
    return new_token


def _decode_jwt_payload(token: str) -> dict[str, Any] | None:
    """Base64url-decode a JWT's payload segment into a dict.

    No signature verification is performed — the Atlas backend validates the token;
    this only needs to read claims locally. Returns ``None`` (never raises) when the
    token is not a well-formed three-segment JWT or the payload is not a JSON object.
    """

    if not isinstance(token, str):
        return None
    parts = token.split(".")
    if len(parts) != 3:
        return None
    segment = parts[1]
    padding = "=" * (-len(segment) % 4)
    try:
        raw = base64.urlsafe_b64decode(segment + padding)
        payload = json.loads(raw)
    except (ValueError, TypeError, binascii.Error):
        return None
    return payload if isinstance(payload, dict) else None


def user_sub_from_credentials() -> str | None:
    """Return the ``sub`` claim of the stored access token, or ``None``.

    Reads the credentials file, base64url-decodes the JWT payload (no signature
    check), and extracts the ``sub`` claim. Returns ``None`` (never raises) when the
    file is absent/invalid, the token is not a JWT, or no ``sub`` claim is present.
    """

    token = _user_token_from_credentials()
    if not token:
        return None
    payload = _decode_jwt_payload(token)
    if not payload:
        return None
    sub = payload.get("sub")
    return sub if isinstance(sub, str) and sub else None


def user_email_from_credentials() -> str | None:
    """Return the caller's email from the stored access token, or ``None``.

    Prefers the ``email`` claim, falling back to ``preferred_username``. Returns
    ``None`` (never raises) when no usable identity claim is available.
    """

    token = _user_token_from_credentials()
    if not token:
        return None
    payload = _decode_jwt_payload(token)
    if not payload:
        return None
    for claim in ("email", "preferred_username"):
        value = payload.get(claim)
        if isinstance(value, str) and value:
            return value
    return None


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

    base_url = os.getenv("ATLAS_API_URL", "").strip() or atlas_config().api_url
    if not base_url:
        return None

    timeout = float(os.getenv("ATLAS_API_TIMEOUT_SECONDS", "10"))
    # An explicit service token wins; otherwise fall back to the logged-in user's
    # token so per-user identity flows to Atlas. Both absent → token stays ``None``.
    token = os.getenv("ATLAS_API_TOKEN") or _user_token_from_credentials()
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


_FROM_JOIN_RE = re.compile(r"\b(?:FROM|JOIN)\s+([\"`\w.]+)", re.IGNORECASE)
_WITH_CTE_RE = re.compile(r"\b(\w+)\s+AS\s*\(", re.IGNORECASE)


def referenced_tables(sql: str) -> list[str]:
    """Best-effort extraction of base table identifiers referenced by a query.

    Returns ``FROM``/``JOIN`` targets (surrounding quotes stripped), excluding
    names defined as CTEs in a ``WITH`` clause. This is a heuristic used to scope
    governance checks — Atlas remains the authority, so over- or under-identifying
    a table only changes which resources are checked, not whether Atlas enforces.
    """

    ctes = {name.lower() for name in _WITH_CTE_RE.findall(sql)}
    tables: list[str] = []
    seen: set[str] = set()
    for raw in _FROM_JOIN_RE.findall(sql):
        name = raw.strip('"`')
        key = name.lower()
        if not name or key in ctes or key in seen:
            continue
        seen.add(key)
        tables.append(name)
    return tables


def govern_query(
    gate: "GovernanceGate | None",
    *,
    sql: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    actor: str | None = None,
    keep: int = 0,
) -> list[tuple[Any, ...]]:
    """Govern an ad-hoc SQL read: access-check referenced tables, then mask columns.

    * ``gate is None`` → rows pass through unchanged.
    * otherwise each base table referenced by ``sql`` is access-checked via
      :meth:`GovernanceGate.enforce_access` (raising
      :class:`~seeknal.integrations.atlas_client.AtlasPolicyDenied` on the first
      deny), and the union of columns Atlas marks sensitive across those tables is
      masked in the result.

    A tableless query (no ``FROM``/``JOIN``) reads no governed resource and passes
    through unchanged.
    """

    if gate is None:
        return [tuple(row) for row in rows]
    masked: set[str] = set()
    for table in referenced_tables(sql):
        decision = gate.enforce_access(resource=table, action="read", actor=actor)
        masked.update(decision.masked_columns)
    return mask_rows(columns, rows, tuple(masked), keep=keep)


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
        token_refresher: Callable[[], str | None] | None = None,
    ) -> None:
        self.config = config
        self._fail_open = _fail_open_default() if fail_open is None else fail_open
        self._client = client
        # Mutable bearer copy so a 401 can be recovered by refreshing the access token
        # (the frozen config stays the source of the *initial* token).
        self._token = config.token
        self._token_refresher = token_refresher or refresh_access_token

    @property
    def fail_open(self) -> bool:
        """Whether the gate allows access when Atlas is unreachable (default: False)."""

        return self._fail_open

    # -- HTTP plumbing -------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.config.base_url}{path}"
        refreshed = False
        while True:
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
                # Transparently refresh the access token once on a 401, then retry.
                if exc.response.status_code == 401:
                    if not refreshed:
                        refreshed = True
                        new_token = self._token_refresher()
                        if new_token:
                            self._token = new_token
                            continue
                    # Unrecoverable auth failure. Raise AtlasAuthError (a subclass of
                    # AtlasContractError) to mirror AtlasCatalogClient._send: check_access
                    # still catches it and fails closed, so the run/ask/repl callers that
                    # only expect AtlasPolicyDenied are unaffected, but the decision now
                    # carries a clear "session expired" reason instead of "unreachable".
                    raise AtlasAuthError(SESSION_EXPIRED_HINT) from exc
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
        except AtlasAuthError as exc:
            # An expired/missing session is an authentication problem, not an
            # availability one: always fail closed (even when fail-open is enabled —
            # an unauthenticated caller must never be waved through) and surface the
            # re-authentication hint so the reason is actionable.
            return AccessDecision(allowed=False, reason=str(exc))
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
