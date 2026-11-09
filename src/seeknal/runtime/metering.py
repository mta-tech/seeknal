"""Atlas Runtime ACU metering: usage records, ACU calculation, hourly rollups, signing, and monthly exports."""

from __future__ import annotations

import csv
import io
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import (
    decode_dss_signature,
    encode_dss_signature,
)

from seeknal.serving.grant_token import (
    _b64url_decode,
    _load_public_key,
    sign_grant_token,
)

ALLOWED_KINDS = frozenset({"job", "session", "warehouse", "sandbox"})


class MeteringError(ValueError):
    """Raised when metering operations, verification, or validation fails."""


def _canonicalize_dict(obj: Any) -> Any:
    """Recursively sort dictionary keys for canonical JSON serialization."""
    if isinstance(obj, dict):
        return {k: _canonicalize_dict(v) for k, v in sorted(obj.items())}
    if isinstance(obj, list):
        return [_canonicalize_dict(v) for v in obj]
    return obj


@dataclass(frozen=True)
class UsageRecord:
    """Pod resource usage record."""

    pod_uid: str
    ws: str
    kind: str
    vcpu_requested: float
    memory_gib_requested: float
    start: datetime
    end: datetime
    labels: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.pod_uid, str) or not self.pod_uid:
            raise ValueError("pod_uid must be a non-empty string")
        if not isinstance(self.ws, str) or not self.ws:
            raise ValueError("ws must be a non-empty string")
        if self.kind not in ALLOWED_KINDS:
            raise ValueError(
                f"Unknown kind '{self.kind}', allowed kinds are: {sorted(ALLOWED_KINDS)}"
            )

        if (
            not isinstance(self.vcpu_requested, (int, float))
            or isinstance(self.vcpu_requested, bool)
            or not math.isfinite(self.vcpu_requested)
            or self.vcpu_requested < 0
        ):
            raise ValueError("vcpu_requested must be a finite number >= 0")

        if (
            not isinstance(self.memory_gib_requested, (int, float))
            or isinstance(self.memory_gib_requested, bool)
            or not math.isfinite(self.memory_gib_requested)
            or self.memory_gib_requested < 0
        ):
            raise ValueError("memory_gib_requested must be a finite number >= 0")

        if self.vcpu_requested == 0 and self.memory_gib_requested == 0:
            raise ValueError("At least one of vcpu_requested or memory_gib_requested must be > 0")

        if not isinstance(self.start, datetime) or not isinstance(self.end, datetime):
            raise ValueError("start and end must be datetime instances")
        if self.start.tzinfo is None or self.start.tzinfo.utcoffset(self.start) is None:
            raise ValueError("start datetime must be timezone-aware (UTC)")
        if self.end.tzinfo is None or self.end.tzinfo.utcoffset(self.end) is None:
            raise ValueError("end datetime must be timezone-aware (UTC)")

        start_utc = self.start.astimezone(timezone.utc)
        end_utc = self.end.astimezone(timezone.utc)
        if end_utc <= start_utc:
            raise ValueError(
                f"end must be strictly greater than start: start={start_utc}, end={end_utc}"
            )

        object.__setattr__(self, "start", start_utc)
        object.__setattr__(self, "end", end_utc)
        object.__setattr__(self, "vcpu_requested", float(self.vcpu_requested))
        object.__setattr__(self, "memory_gib_requested", float(self.memory_gib_requested))

        if self.labels is None or not isinstance(self.labels, Mapping):
            raise ValueError("labels must be a mapping")
        clean_labels = {str(k): str(v) for k, v in self.labels.items()}
        object.__setattr__(self, "labels", clean_labels)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> UsageRecord:
        """Construct UsageRecord from a dictionary."""
        start_val = data["start"]
        end_val = data["end"]
        if isinstance(start_val, str):
            start_dt = datetime.fromisoformat(start_val.replace("Z", "+00:00"))
        elif isinstance(start_val, datetime):
            start_dt = start_val
        else:
            raise ValueError(f"Invalid start datetime: {start_val}")

        if isinstance(end_val, str):
            end_dt = datetime.fromisoformat(end_val.replace("Z", "+00:00"))
        elif isinstance(end_val, datetime):
            end_dt = end_val
        else:
            raise ValueError(f"Invalid end datetime: {end_val}")

        return cls(
            pod_uid=str(data["pod_uid"]),
            ws=str(data["ws"]),
            kind=str(data["kind"]),
            vcpu_requested=float(data["vcpu_requested"]),
            memory_gib_requested=float(data["memory_gib_requested"]),
            start=start_dt,
            end=end_dt,
            labels=dict(data.get("labels", {})),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert UsageRecord to a dictionary."""
        return {
            "pod_uid": self.pod_uid,
            "ws": self.ws,
            "kind": self.kind,
            "vcpu_requested": self.vcpu_requested,
            "memory_gib_requested": self.memory_gib_requested,
            "start": self.start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": self.end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "labels": dict(self.labels),
        }


def acu_for(
    record: UsageRecord,
    weights: Mapping[str, float | Decimal],
) -> Decimal:
    """Calculate ACU for a usage record using Decimal arithmetic.

    Formula:
    seconds * max(vCPU_requested, GiB_requested / 4) * kind_weight / 14400
    Quantized to 6 decimals with ROUND_HALF_EVEN.
    """
    if record.kind not in weights:
        raise ValueError(f"Unknown kind '{record.kind}' in acu weights")

    seconds = Decimal(str((record.end - record.start).total_seconds()))
    vcpu = Decimal(str(record.vcpu_requested))
    mem = Decimal(str(record.memory_gib_requested))
    mem_div_4 = mem / Decimal(4)
    resource_factor = max(vcpu, mem_div_4)
    weight = Decimal(str(weights[record.kind]))

    raw_acu = (seconds * resource_factor * weight) / Decimal(14400)
    return raw_acu.quantize(Decimal("0.000001"), rounding=ROUND_HALF_EVEN)


def split_by_hour(record: UsageRecord) -> list[UsageRecord]:
    """Split a usage record at UTC hour boundaries into piecewise records."""
    pieces: list[UsageRecord] = []
    current_start = record.start
    while current_start < record.end:
        next_boundary = current_start.replace(
            minute=0, second=0, microsecond=0
        ) + timedelta(hours=1)
        current_end = min(record.end, next_boundary)
        pieces.append(
            UsageRecord(
                pod_uid=record.pod_uid,
                ws=record.ws,
                kind=record.kind,
                vcpu_requested=record.vcpu_requested,
                memory_gib_requested=record.memory_gib_requested,
                start=current_start,
                end=current_end,
                labels=record.labels,
            )
        )
        current_start = current_end
    return pieces


@dataclass(frozen=True)
class HourlyRollup:
    """Aggregated hourly compute usage and showback totals."""

    ws: str
    hour_start: datetime
    kind: str
    release: str
    acu: Decimal
    pod_seconds: Decimal
    record_count: int
    by_label: dict[str, Decimal] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.hour_start, datetime) or self.hour_start.tzinfo is None:
            raise ValueError("hour_start must be timezone-aware (UTC)")
        hour_start_utc = self.hour_start.astimezone(timezone.utc).replace(
            minute=0, second=0, microsecond=0
        )
        object.__setattr__(self, "hour_start", hour_start_utc)
        if not isinstance(self.acu, Decimal):
            object.__setattr__(self, "acu", Decimal(str(self.acu)))
        if not isinstance(self.pod_seconds, Decimal):
            object.__setattr__(self, "pod_seconds", Decimal(str(self.pod_seconds)))
        clean_by_label = {
            str(k): (v if isinstance(v, Decimal) else Decimal(str(v)))
            for k, v in self.by_label.items()
        }
        object.__setattr__(self, "by_label", clean_by_label)

    def to_dict(self) -> dict[str, Any]:
        """Convert HourlyRollup to serializable dictionary."""
        return {
            "ws": self.ws,
            "hour_start": self.hour_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "kind": self.kind,
            "release": self.release,
            "acu": str(self.acu),
            "pod_seconds": str(self.pod_seconds),
            "record_count": self.record_count,
            "by_label": {k: str(v) for k, v in sorted(self.by_label.items())},
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> HourlyRollup:
        """Construct HourlyRollup from dictionary."""
        hs_str = data["hour_start"]
        hs_dt = datetime.fromisoformat(hs_str.replace("Z", "+00:00"))
        by_label = {
            str(k): Decimal(str(v))
            for k, v in data.get("by_label", {}).items()
        }
        return cls(
            ws=str(data["ws"]),
            hour_start=hs_dt,
            kind=str(data["kind"]),
            release=str(data["release"]),
            acu=Decimal(str(data["acu"])),
            pod_seconds=Decimal(str(data["pod_seconds"])),
            record_count=int(data["record_count"]),
            by_label=by_label,
        )


def rollup(
    records: Sequence[UsageRecord],
    weights: Mapping[str, float | Decimal],
) -> list[HourlyRollup]:
    """Split records by hour, group by (ws, hour_start, kind, release) and sum."""
    groups: dict[
        tuple[str, datetime, str, str],
        dict[str, Any],
    ] = {}

    for record in records:
        for piece in split_by_hour(record):
            hour_start = piece.start.replace(minute=0, second=0, microsecond=0)
            release = str(piece.labels.get("release", ""))
            key = (piece.ws, hour_start, piece.kind, release)

            piece_acu = acu_for(piece, weights)
            delta = piece.end - piece.start
            sec_float = delta.total_seconds()
            if sec_float.is_integer():
                piece_sec = Decimal(int(sec_float))
            else:
                piece_sec = Decimal(str(sec_float))

            if key not in groups:
                groups[key] = {
                    "acu": Decimal(0),
                    "pod_seconds": Decimal(0),
                    "record_count": 0,
                    "by_label": {},
                }

            entry = groups[key]
            entry["acu"] += piece_acu
            entry["pod_seconds"] += piece_sec
            entry["record_count"] += 1

            if "principal" in piece.labels:
                pkey = f"principal:{piece.labels['principal']}"
                entry["by_label"][pkey] = entry["by_label"].get(pkey, Decimal(0)) + piece_acu
            if "user" in piece.labels:
                ukey = f"user:{piece.labels['user']}"
                entry["by_label"][ukey] = entry["by_label"].get(ukey, Decimal(0)) + piece_acu

    rollups: list[HourlyRollup] = []
    for (ws, hour_start, kind, release), data in sorted(
        groups.items(), key=lambda item: item[0]
    ):
        acu_quantized = data["acu"].quantize(Decimal("0.000001"), rounding=ROUND_HALF_EVEN)
        by_label_quantized = {
            k: v.quantize(Decimal("0.000001"), rounding=ROUND_HALF_EVEN)
            for k, v in sorted(data["by_label"].items())
        }
        rollups.append(
            HourlyRollup(
                ws=ws,
                hour_start=hour_start,
                kind=kind,
                release=release,
                acu=acu_quantized,
                pod_seconds=data["pod_seconds"],
                record_count=data["record_count"],
                by_label=by_label_quantized,
            )
        )

    return rollups


def sign_rollups(
    rollups: Sequence[HourlyRollup],
    private_key: Any,
    *,
    kid: str,
    cell_id: str,
    issued_at: int | float | datetime,
) -> str:
    """Sign hourly rollups as an ES256 compact JWS with canonical JSON claims."""
    if isinstance(issued_at, datetime):
        iat = int(issued_at.timestamp())
    elif isinstance(issued_at, (int, float)):
        iat = int(issued_at)
    else:
        raise ValueError("issued_at must be an integer, float, or datetime")

    raw_rollups = [r.to_dict() for r in rollups]
    claims: dict[str, Any] = {
        "typ": "acu_rollup",
        "iss": cell_id,
        "iat": iat,
        "rollups": raw_rollups,
    }

    canonical_claims = _canonicalize_dict(claims)
    return sign_grant_token(canonical_claims, private_key, kid=kid)


def verify_rollups(
    token: str,
    public_keys: Mapping[str, Any],
) -> list[HourlyRollup]:
    """Verify an ES256 compact JWS rollup token and return parsed HourlyRollup instances."""
    if not isinstance(token, str):
        raise MeteringError("Token must be a string")

    parts = token.split(".")
    if len(parts) != 3:
        raise MeteringError(f"Expected 3 segments in token, got {len(parts)}")

    header_b64, payload_b64, sig_b64 = parts

    # 1. Decode and check header
    try:
        header_bytes = _b64url_decode(header_b64)
        header = json.loads(header_bytes.decode("utf-8"))
    except Exception as exc:
        raise MeteringError(f"Invalid token header: {exc}") from exc

    if not isinstance(header, dict):
        raise MeteringError("Token header must be a JSON object")

    if header.get("alg") != "ES256":
        raise MeteringError(f"Unsupported algorithm '{header.get('alg')}', expected 'ES256'")

    kid = header.get("kid")
    if not kid or not isinstance(kid, str):
        raise MeteringError("Missing or invalid 'kid' in token header")

    if kid not in public_keys:
        raise MeteringError(f"Unknown key ID '{kid}'")

    try:
        pub_key = _load_public_key(public_keys[kid], kid)
    except Exception as exc:
        raise MeteringError(f"Failed to load public key for kid '{kid}': {exc}") from exc

    # 2. Decode signature and verify
    try:
        raw_sig = _b64url_decode(sig_b64)
    except Exception as exc:
        raise MeteringError(f"Failed to decode signature: {exc}") from exc

    if len(raw_sig) != 64:
        raise MeteringError(f"Signature must be exactly 64 bytes, got {len(raw_sig)}")

    r = int.from_bytes(raw_sig[:32], "big")
    s = int.from_bytes(raw_sig[32:], "big")
    try:
        dss_sig = encode_dss_signature(r, s)
    except Exception as exc:
        raise MeteringError(f"Invalid signature encoding: {exc}") from exc

    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    try:
        pub_key.verify(dss_sig, signing_input, ec.ECDSA(hashes.SHA256()))
    except InvalidSignature:
        raise MeteringError("Invalid token signature")
    except Exception as exc:
        raise MeteringError(f"Signature verification failed: {exc}") from exc

    # 3. Decode and validate payload
    try:
        payload_bytes = _b64url_decode(payload_b64)
        claims = json.loads(payload_bytes.decode("utf-8"))
    except Exception as exc:
        raise MeteringError(f"Invalid token payload JSON: {exc}") from exc

    if not isinstance(claims, dict):
        raise MeteringError("Token claims must be a JSON object")

    if claims.get("typ") != "acu_rollup":
        raise MeteringError(f"Wrong token typ '{claims.get('typ')}', expected 'acu_rollup'")

    rollups_data = claims.get("rollups")
    if not isinstance(rollups_data, list):
        raise MeteringError("Claims 'rollups' must be a list")

    rollups: list[HourlyRollup] = []
    for item in rollups_data:
        if not isinstance(item, dict):
            raise MeteringError("Rollup entry must be a dictionary")
        rollups.append(HourlyRollup.from_dict(item))

    return rollups


def export_monthly(
    rollups: Sequence[HourlyRollup],
    year: int,
    month: int,
) -> str:
    """Export rollups for the specified year and month as a CSV string."""
    y = int(year)
    m = int(month)

    matching = [
        r for r in rollups
        if r.hour_start.year == y and r.hour_start.month == m
    ]
    matching.sort(key=lambda r: (r.ws, r.hour_start, r.kind, r.release))

    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(
        ["ws", "hour_start", "kind", "release", "acu", "pod_seconds", "record_count"]
    )
    for r in matching:
        hs_str = r.hour_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        writer.writerow(
            [r.ws, hs_str, r.kind, r.release, str(r.acu), str(r.pod_seconds), r.record_count]
        )

    return output.getvalue()
