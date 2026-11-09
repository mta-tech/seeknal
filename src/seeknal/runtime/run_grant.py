"""Run grant verification and plan coverage checking for Atlas Runtime."""

from __future__ import annotations

import json
import math
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature

from seeknal.serving.grant_token import (
    GrantTokenError,
    _b64url_decode,
    _load_public_key,
    _parse_numeric_claim,
)

from .run_plan import ConnectionAccess, RunPlan, TableAccess, plan_sha256

HEX64_PATTERN = re.compile(r"^[0-9a-f]{64}$")
VALID_TRIGGER_TYPES = {"schedule", "user", "agent"}


class RunGrantError(Exception):
    """Raised when run grant verification, parsing or plan coverage fails."""


@dataclass(frozen=True)
class RunGrant:
    """Decoded and verified Atlas Runtime run grant."""

    typ: str
    iss: str
    aud: str
    sub: str
    principal: str
    ws: str
    jti: str
    release: str
    plan_sha256: str
    trigger: dict[str, Any]
    iat: int
    nbf: int
    exp: int
    reads: tuple[TableAccess, ...]
    writes: tuple[TableAccess, ...]
    connections: tuple[ConnectionAccess, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "typ": self.typ,
            "iss": self.iss,
            "aud": self.aud,
            "sub": self.sub,
            "principal": self.principal,
            "ws": self.ws,
            "jti": self.jti,
            "release": self.release,
            "plan_sha256": self.plan_sha256,
            "trigger": dict(self.trigger),
            "iat": self.iat,
            "nbf": self.nbf,
            "exp": self.exp,
            "reads": [r.to_dict() for r in self.reads],
            "writes": [w.to_dict() for w in self.writes],
            "connections": [c.to_dict() for c in self.connections],
        }

    @classmethod
    def parse(cls, claims: dict[str, Any]) -> RunGrant:
        """Parse validated claims into a RunGrant instance."""
        reads_list = claims.get("reads") or []
        parsed_reads = tuple(
            TableAccess(table=r["table"], actions=tuple(r["actions"]))
            for r in reads_list
        )

        writes_list = claims.get("writes") or []
        parsed_writes = tuple(
            TableAccess(table=w["table"], actions=tuple(w["actions"]))
            for w in writes_list
        )

        conns_list = claims.get("connections") or []
        parsed_conns = tuple(
            ConnectionAccess(name=c["name"], actions=tuple(c["actions"]))
            for c in conns_list
        )

        return cls(
            typ=str(claims["typ"]),
            iss=str(claims["iss"]),
            aud=str(claims["aud"]),
            sub=str(claims["sub"]),
            principal=str(claims["principal"]),
            ws=str(claims["ws"]),
            jti=str(claims["jti"]),
            release=str(claims["release"]),
            plan_sha256=str(claims["plan_sha256"]),
            trigger=dict(claims["trigger"]),
            iat=int(claims["iat"]),
            nbf=int(claims["nbf"]),
            exp=int(claims["exp"]),
            reads=parsed_reads,
            writes=parsed_writes,
            connections=parsed_conns,
        )


def _validate_table_name(table: str, claim_name: str) -> None:
    if not isinstance(table, str):
        raise RunGrantError(f"Table in '{claim_name}' must be a string")
    parts = table.split(".")
    if len(parts) != 3 or not all(parts):
        raise RunGrantError(
            f"Table '{table}' in '{claim_name}' must be 3-part format '<warehouse>.<namespace>.<table>'"
        )


def verify_run_grant(
    token: str,
    public_keys: Mapping[str, Any],
    *,
    now: float | None = None,
    leeway_seconds: float = 5,
    audience: str = "atlas-rt-run",
    issuer: str = "atlas-catalog",
) -> RunGrant:
    """Verify an ES256 compact JWS run grant against contract rules."""
    if not isinstance(token, str):
        raise RunGrantError("Token must be a string")

    parts = token.split(".")
    if len(parts) != 3:
        raise RunGrantError(f"Expected 3 segments in token, got {len(parts)}")

    header_b64, payload_b64, sig_b64 = parts

    # 1. Header validation
    try:
        header_bytes = _b64url_decode(header_b64)
        header = json.loads(header_bytes.decode("utf-8"))
    except Exception as exc:
        raise RunGrantError(f"Invalid token header: {exc}") from exc

    if not isinstance(header, dict):
        raise RunGrantError("Token header must be a JSON object")

    if header.get("alg") != "ES256":
        raise RunGrantError(f"Unsupported algorithm '{header.get('alg')}', only ES256 permitted")

    typ = header.get("typ")
    if typ is not None and typ != "JWT":
        raise RunGrantError(f"Unsupported header typ '{typ}', expected JWT")

    kid = header.get("kid")
    if not kid or not isinstance(kid, str):
        raise RunGrantError("Missing or invalid 'kid' in token header")

    if kid not in public_keys:
        raise RunGrantError(f"Unknown key ID '{kid}'")

    try:
        pub_key = _load_public_key(public_keys[kid], kid)
    except GrantTokenError as exc:
        raise RunGrantError(str(exc)) from exc

    # 2. Signature verification
    try:
        raw_sig = _b64url_decode(sig_b64)
    except Exception as exc:
        raise RunGrantError(f"Failed to decode signature: {exc}") from exc

    if len(raw_sig) != 64:
        raise RunGrantError(f"Signature must be exactly 64 bytes, got {len(raw_sig)}")

    r = int.from_bytes(raw_sig[:32], "big")
    s = int.from_bytes(raw_sig[32:], "big")
    try:
        dss_sig = encode_dss_signature(r, s)
    except Exception as exc:
        raise RunGrantError(f"Invalid signature format: {exc}") from exc

    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    try:
        pub_key.verify(dss_sig, signing_input, ec.ECDSA(hashes.SHA256()))
    except InvalidSignature:
        raise RunGrantError("Invalid token signature")
    except Exception as exc:
        raise RunGrantError(f"Signature verification failed: {exc}") from exc

    # 3. Payload decoding
    try:
        payload_bytes = _b64url_decode(payload_b64)
        claims = json.loads(payload_bytes.decode("utf-8"))
    except Exception as exc:
        raise RunGrantError(f"Invalid token payload JSON: {exc}") from exc

    if not isinstance(claims, dict):
        raise RunGrantError("Token claims must be a JSON object")

    # 4. Enforce every contract claim rule
    # typ
    if claims.get("typ") != "run":
        raise RunGrantError(
            f"Invalid 'typ' claim '{claims.get('typ')}', expected exactly 'run'"
        )

    # iss
    if claims.get("iss") != issuer:
        raise RunGrantError(
            f"Invalid 'iss' claim '{claims.get('iss')}', expected '{issuer}'"
        )

    # aud
    aud = claims.get("aud")
    if aud != audience:
        raise RunGrantError(
            f"Invalid 'aud' claim '{aud}', expected '{audience}'"
        )

    # sub, principal, ws, jti, release
    for field_name in ("sub", "principal", "ws", "jti", "release"):
        val = claims.get(field_name)
        if not val or not isinstance(val, str) or not val.strip():
            raise RunGrantError(f"Missing or invalid '{field_name}' claim in run grant")

    # plan_sha256
    plan_hash = claims.get("plan_sha256")
    if not isinstance(plan_hash, str) or not HEX64_PATTERN.match(plan_hash):
        raise RunGrantError(
            f"Invalid 'plan_sha256' claim: must be 64 lowercase hex characters, got '{plan_hash}'"
        )

    # trigger
    trigger = claims.get("trigger")
    if not isinstance(trigger, dict):
        raise RunGrantError("Missing or invalid 'trigger' claim, expected an object")
    trig_type = trigger.get("type")
    if trig_type not in VALID_TRIGGER_TYPES:
        raise RunGrantError(
            f"Invalid trigger type '{trig_type}', expected one of {sorted(VALID_TRIGGER_TYPES)}"
        )
    trig_id = trigger.get("id")
    if not trig_id or not isinstance(trig_id, str):
        raise RunGrantError("Invalid or missing 'id' in trigger claim")

    # Time validation: iat, nbf, exp
    current_time = time.time() if now is None else float(now)

    for time_claim in ("iat", "nbf", "exp"):
        if time_claim not in claims:
            raise RunGrantError(f"Missing required '{time_claim}' claim")

    iat_val = _parse_numeric_claim(claims["iat"], "iat")
    nbf_val = _parse_numeric_claim(claims["nbf"], "nbf")
    exp_val = _parse_numeric_claim(claims["exp"], "exp")

    if not float(iat_val).is_integer():
        raise RunGrantError("'iat' claim must be integer seconds")
    if not float(nbf_val).is_integer():
        raise RunGrantError("'nbf' claim must be integer seconds")
    if not float(exp_val).is_integer():
        raise RunGrantError("'exp' claim must be integer seconds")

    iat = int(iat_val)
    nbf = int(nbf_val)
    exp = int(exp_val)

    if iat > current_time + leeway_seconds:
        raise RunGrantError("Token 'iat' is in the future beyond leeway")

    # Contract: nbf <= iat + 5
    if nbf > iat + 5:
        raise RunGrantError(
            f"Token 'nbf' ({nbf}) exceeds 'iat + 5' ({iat + 5})"
        )

    if nbf > current_time + leeway_seconds:
        raise RunGrantError("Token is not yet valid ('nbf' is in the future)")

    if exp < current_time - leeway_seconds:
        raise RunGrantError("Token has expired")

    if exp < iat:
        raise RunGrantError("Token 'exp' must not be before 'iat'")

    # Contract: exp - iat from 60 to 86400
    lifetime = exp - iat
    if lifetime < 60 or lifetime > 86400:
        raise RunGrantError(
            f"Token lifetime of {lifetime}s is outside bounds [60, 86400]"
        )

    # reads validation
    reads = claims.get("reads", [])
    if not isinstance(reads, list):
        raise RunGrantError("Claim 'reads' must be a list")
    seen_read_tables: list[str] = []
    for item in reads:
        if not isinstance(item, dict):
            raise RunGrantError("Each entry in 'reads' must be an object")
        table = item.get("table")
        _validate_table_name(table, "reads")
        actions = item.get("actions")
        if actions != ["read"]:
            raise RunGrantError(f"Entry in 'reads' for table '{table}' must have actions ['read']")
        seen_read_tables.append(table)
    if seen_read_tables != sorted(seen_read_tables) or len(seen_read_tables) != len(set(seen_read_tables)):
        raise RunGrantError("Claim 'reads' must be sorted by table and unique")

    # writes validation
    writes = claims.get("writes", [])
    if not isinstance(writes, list):
        raise RunGrantError("Claim 'writes' must be a list")
    seen_write_tables: list[str] = []
    for item in writes:
        if not isinstance(item, dict):
            raise RunGrantError("Each entry in 'writes' must be an object")
        table = item.get("table")
        _validate_table_name(table, "writes")
        actions = item.get("actions")
        if actions != ["write"]:
            raise RunGrantError(f"Entry in 'writes' for table '{table}' must have actions ['write']")
        seen_write_tables.append(table)
    if seen_write_tables != sorted(seen_write_tables) or len(seen_write_tables) != len(set(seen_write_tables)):
        raise RunGrantError("Claim 'writes' must be sorted by table and unique")

    # connections validation
    conns = claims.get("connections", [])
    if not isinstance(conns, list):
        raise RunGrantError("Claim 'connections' must be a list")
    seen_conn_names: list[str] = []
    for item in conns:
        if not isinstance(item, dict):
            raise RunGrantError("Each entry in 'connections' must be an object")
        name = item.get("name")
        if not name or not isinstance(name, str):
            raise RunGrantError("Each entry in 'connections' must have a non-empty 'name'")
        actions = item.get("actions")
        if actions not in (["read"], ["write"], ["read", "write"]):
            raise RunGrantError(
                f"Entry in 'connections' for '{name}' has invalid actions '{actions}', "
                "expected ['read'], ['write'], or ['read', 'write']"
            )
        seen_conn_names.append(name)
    if seen_conn_names != sorted(seen_conn_names) or len(seen_conn_names) != len(set(seen_conn_names)):
        raise RunGrantError("Claim 'connections' must be sorted by name and unique")

    return RunGrant.parse(claims)


def require_plan_covered(grant: RunGrant, plan: RunPlan) -> None:
    """Verify that grant covers plan digest and all read, write and connection actions."""
    uncovered: list[str] = []

    actual_hash = plan_sha256(plan)
    if grant.plan_sha256 != actual_hash:
        uncovered.append(
            f"plan_sha256 digest mismatch: grant has '{grant.plan_sha256}', compiled plan is '{actual_hash}'"
        )

    # Check reads
    grant_reads: dict[str, set[str]] = {
        r.table: set(r.actions) for r in grant.reads
    }
    for plan_read in plan.reads:
        table = plan_read.table
        if table not in grant_reads:
            uncovered.append(f"uncovered read table: '{table}'")
        else:
            for act in plan_read.actions:
                if act not in grant_reads[table]:
                    uncovered.append(f"uncovered action '{act}' on read table '{table}'")

    # Check writes
    grant_writes: dict[str, set[str]] = {
        w.table: set(w.actions) for w in grant.writes
    }
    for plan_write in plan.writes:
        table = plan_write.table
        if table not in grant_writes:
            uncovered.append(f"uncovered write table: '{table}'")
        else:
            for act in plan_write.actions:
                if act not in grant_writes[table]:
                    uncovered.append(f"uncovered action '{act}' on write table '{table}'")

    # Check connections
    grant_conns: dict[str, set[str]] = {
        c.name: set(c.actions) for c in grant.connections
    }
    for plan_conn in plan.connections:
        name = plan_conn.name
        if name not in grant_conns:
            uncovered.append(f"uncovered connection: '{name}'")
        else:
            for act in plan_conn.actions:
                if act not in grant_conns[name]:
                    uncovered.append(f"uncovered action '{act}' on connection '{name}'")

    if uncovered:
        lines = "\n  - ".join(uncovered)
        raise RunGrantError(f"Plan is not covered by run grant:\n  - {lines}")
