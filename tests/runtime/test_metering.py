"""Tests for Atlas Runtime ACU metering: formula, splitting, rollups, signing, exports, and CLI."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.runtime.__main__ import main
from seeknal.runtime.metering import (
    HourlyRollup,
    MeteringError,
    UsageRecord,
    acu_for,
    export_monthly,
    rollup,
    sign_rollups,
    split_by_hour,
    verify_rollups,
)

DEFAULT_WEIGHTS = {
    "job": 1.0,
    "session": 1.5,
    "warehouse": 2.0,
    "sandbox": 1.0,
}


def _make_key_pair():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    priv_pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    pub_pem = pub.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("utf-8")
    return priv, pub, priv_pem, pub_pem


def _make_manifest_file(tmp_path: Path) -> Path:
    data = {
        "apiVersion": "atlas.io/v1",
        "kind": "RuntimeRelease",
        "metadata": {"name": "atlas-rt/1.0.0", "channel": "stable"},
        "seeknal": {
            "version": "2.14.0",
            "commit": "a" * 40,
            "lock_sha256": "b" * 64,
        },
        "python": "3.11.9",
        "images": {
            "atlas-rt": {"digest": f"sha256:{'a' * 64}"},
            "atlas-rt-serving": {"digest": f"sha256:{'b' * 64}"},
        },
        "engines": {"duckdb": "1.4.4"},
        "contracts": {"grant": ["v2"], "cube_mode": ["jwt"]},
        "serving": {"grant_max_lifetime_seconds": 60},
        "acu_weights": DEFAULT_WEIGHTS,
        "licence_scan": "clean",
    }
    p = tmp_path / "manifest.yaml"
    p.write_text(yaml.dump(data), encoding="utf-8")
    return p


# ==============================================================================
# 1. Formula vectors
# ==============================================================================


def test_formula_job_4vcpu_16gib_3600s():
    start = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(seconds=3600)
    rec = UsageRecord(
        pod_uid="pod-1",
        ws="ws-1",
        kind="job",
        vcpu_requested=4.0,
        memory_gib_requested=16.0,
        start=start,
        end=end,
    )
    result = acu_for(rec, DEFAULT_WEIGHTS)
    assert result == Decimal("1.000000")


def test_formula_job_2vcpu_32gib_3600s_memory_dominates():
    start = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(seconds=3600)
    rec = UsageRecord(
        pod_uid="pod-2",
        ws="ws-1",
        kind="job",
        vcpu_requested=2.0,
        memory_gib_requested=32.0,
        start=start,
        end=end,
    )
    result = acu_for(rec, DEFAULT_WEIGHTS)
    assert result == Decimal("2.000000")


def test_formula_session_4vcpu_16gib_1800s():
    start = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(seconds=1800)
    rec = UsageRecord(
        pod_uid="pod-3",
        ws="ws-1",
        kind="session",
        vcpu_requested=4.0,
        memory_gib_requested=16.0,
        start=start,
        end=end,
    )
    result = acu_for(rec, DEFAULT_WEIGHTS)
    assert result == Decimal("0.750000")


def test_formula_warehouse_16vcpu_64gib_36000s():
    start = datetime(2026, 11, 9, 0, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(seconds=36000)
    rec = UsageRecord(
        pod_uid="pod-4",
        ws="ws-1",
        kind="warehouse",
        vcpu_requested=16.0,
        memory_gib_requested=64.0,
        start=start,
        end=end,
    )
    result = acu_for(rec, DEFAULT_WEIGHTS)
    assert result == Decimal("80.000000")


def test_formula_warehouse_4vcpu_16gib_36000s():
    start = datetime(2026, 11, 9, 0, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(seconds=36000)
    rec = UsageRecord(
        pod_uid="pod-5",
        ws="ws-1",
        kind="warehouse",
        vcpu_requested=4.0,
        memory_gib_requested=16.0,
        start=start,
        end=end,
    )
    result = acu_for(rec, DEFAULT_WEIGHTS)
    assert result == Decimal("20.000000")


def test_formula_sandbox_1vcpu_2gib_600s():
    start = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(seconds=600)
    rec = UsageRecord(
        pod_uid="pod-6",
        ws="ws-1",
        kind="sandbox",
        vcpu_requested=1.0,
        memory_gib_requested=2.0,
        start=start,
        end=end,
    )
    result = acu_for(rec, DEFAULT_WEIGHTS)
    assert result == Decimal("0.041667")


def test_formula_unknown_kind_raises():
    start = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(seconds=3600)
    rec = UsageRecord(
        pod_uid="pod-7",
        ws="ws-1",
        kind="job",
        vcpu_requested=1.0,
        memory_gib_requested=4.0,
        start=start,
        end=end,
    )
    with pytest.raises(ValueError, match="Unknown kind"):
        acu_for(rec, {"session": 1.5})


# ==============================================================================
# 2. Validation of UsageRecord
# ==============================================================================


def test_record_validation_end_not_greater_than_start():
    t = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="strictly greater"):
        UsageRecord(
            pod_uid="p1",
            ws="ws",
            kind="job",
            vcpu_requested=1.0,
            memory_gib_requested=4.0,
            start=t,
            end=t,
        )

    with pytest.raises(ValueError, match="strictly greater"):
        UsageRecord(
            pod_uid="p1",
            ws="ws",
            kind="job",
            vcpu_requested=1.0,
            memory_gib_requested=4.0,
            start=t,
            end=t - timedelta(seconds=1),
        )


def test_record_validation_requests():
    t1 = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    t2 = t1 + timedelta(seconds=100)

    # Both zero
    with pytest.raises(ValueError, match="At least one"):
        UsageRecord(
            pod_uid="p1",
            ws="ws",
            kind="job",
            vcpu_requested=0.0,
            memory_gib_requested=0.0,
            start=t1,
            end=t2,
        )

    # Negative
    with pytest.raises(ValueError, match=">= 0"):
        UsageRecord(
            pod_uid="p1",
            ws="ws",
            kind="job",
            vcpu_requested=-1.0,
            memory_gib_requested=4.0,
            start=t1,
            end=t2,
        )

    with pytest.raises(ValueError, match=">= 0"):
        UsageRecord(
            pod_uid="p1",
            ws="ws",
            kind="job",
            vcpu_requested=1.0,
            memory_gib_requested=-2.0,
            start=t1,
            end=t2,
        )

    # Non-finite
    with pytest.raises(ValueError, match="finite"):
        UsageRecord(
            pod_uid="p1",
            ws="ws",
            kind="job",
            vcpu_requested=float("nan"),
            memory_gib_requested=4.0,
            start=t1,
            end=t2,
        )

    with pytest.raises(ValueError, match="finite"):
        UsageRecord(
            pod_uid="p1",
            ws="ws",
            kind="job",
            vcpu_requested=1.0,
            memory_gib_requested=float("inf"),
            start=t1,
            end=t2,
        )


def test_record_validation_unknown_kind():
    t1 = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    t2 = t1 + timedelta(seconds=100)
    with pytest.raises(ValueError, match="Unknown kind"):
        UsageRecord(
            pod_uid="p1",
            ws="ws",
            kind="serving",
            vcpu_requested=1.0,
            memory_gib_requested=4.0,
            start=t1,
            end=t2,
        )


def test_record_validation_naive_datetimes():
    t_naive = datetime(2026, 11, 9, 10, 0, 0)
    t_aware = datetime(2026, 11, 9, 11, 0, 0, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="timezone-aware"):
        UsageRecord(
            pod_uid="p1",
            ws="ws",
            kind="job",
            vcpu_requested=1.0,
            memory_gib_requested=4.0,
            start=t_naive,
            end=t_aware,
        )

    with pytest.raises(ValueError, match="timezone-aware"):
        UsageRecord(
            pod_uid="p1",
            ws="ws",
            kind="job",
            vcpu_requested=1.0,
            memory_gib_requested=4.0,
            start=t_aware,
            end=t_naive,
        )


def test_record_non_empty_identity_and_labels():
    t1 = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    t2 = t1 + timedelta(seconds=100)

    with pytest.raises(ValueError, match="pod_uid"):
        UsageRecord(
            pod_uid="",
            ws="ws",
            kind="job",
            vcpu_requested=1.0,
            memory_gib_requested=4.0,
            start=t1,
            end=t2,
        )

    with pytest.raises(ValueError, match="ws"):
        UsageRecord(
            pod_uid="p1",
            ws="",
            kind="job",
            vcpu_requested=1.0,
            memory_gib_requested=4.0,
            start=t1,
            end=t2,
        )

    with pytest.raises(ValueError, match="labels"):
        UsageRecord(
            pod_uid="p1",
            ws="ws",
            kind="job",
            vcpu_requested=1.0,
            memory_gib_requested=4.0,
            start=t1,
            end=t2,
            labels=None,  # type: ignore[arg-type]
        )


def test_record_to_and_from_dict():
    t1 = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    t2 = t1 + timedelta(seconds=3600)
    rec = UsageRecord(
        pod_uid="p1",
        ws="ws-1",
        kind="job",
        vcpu_requested=4.0,
        memory_gib_requested=16.0,
        start=t1,
        end=t2,
        labels={"principal": "alice", "release": "atlas-rt/1.0.0"},
    )
    d = rec.to_dict()
    assert d["start"] == "2026-11-09T10:00:00Z"
    assert d["end"] == "2026-11-09T11:00:00Z"
    assert d["pod_uid"] == "p1"

    rec2 = UsageRecord.from_dict(d)
    assert rec2 == rec


# ==============================================================================
# 3. Hourly split preserves total
# ==============================================================================


def test_split_by_hour_within_single_hour():
    start = datetime(2026, 11, 9, 10, 15, 0, tzinfo=timezone.utc)
    end = datetime(2026, 11, 9, 10, 45, 0, tzinfo=timezone.utc)
    rec = UsageRecord(
        pod_uid="p1",
        ws="ws-1",
        kind="job",
        vcpu_requested=4.0,
        memory_gib_requested=16.0,
        start=start,
        end=end,
    )
    pieces = split_by_hour(rec)
    assert len(pieces) == 1
    assert pieces[0].start == start
    assert pieces[0].end == end


def test_split_by_hour_across_boundary_preserves_acu():
    # 10:15:00 to 12:45:00 -> spans 3 hours:
    # 10:15 -> 11:00 (45m = 2700s)
    # 11:00 -> 12:00 (60m = 3600s)
    # 12:00 -> 12:45 (45m = 2700s)
    # Total = 9000s
    start = datetime(2026, 11, 9, 10, 15, 0, tzinfo=timezone.utc)
    end = datetime(2026, 11, 9, 12, 45, 0, tzinfo=timezone.utc)
    rec = UsageRecord(
        pod_uid="pod-split",
        ws="ws-split",
        kind="session",
        vcpu_requested=2.0,
        memory_gib_requested=16.0,
        start=start,
        end=end,
        labels={"user": "bob", "release": "atlas-rt/1.0.0"},
    )

    whole_acu = acu_for(rec, DEFAULT_WEIGHTS)
    pieces = split_by_hour(rec)
    assert len(pieces) == 3

    assert pieces[0].start == start
    assert pieces[0].end == datetime(2026, 11, 9, 11, 0, 0, tzinfo=timezone.utc)
    assert pieces[1].start == datetime(2026, 11, 9, 11, 0, 0, tzinfo=timezone.utc)
    assert pieces[1].end == datetime(2026, 11, 9, 12, 0, 0, tzinfo=timezone.utc)
    assert pieces[2].start == datetime(2026, 11, 9, 12, 0, 0, tzinfo=timezone.utc)
    assert pieces[2].end == end

    for p in pieces:
        assert p.pod_uid == rec.pod_uid
        assert p.ws == rec.ws
        assert p.kind == rec.kind
        assert p.vcpu_requested == rec.vcpu_requested
        assert p.memory_gib_requested == rec.memory_gib_requested
        assert p.labels == rec.labels

    pieces_acu_sum = sum(acu_for(p, DEFAULT_WEIGHTS) for p in pieces)
    diff = abs(pieces_acu_sum - whole_acu)
    assert diff <= Decimal("0.000001")


def test_split_by_hour_exact_boundaries():
    start = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 11, 9, 12, 0, 0, tzinfo=timezone.utc)
    rec = UsageRecord(
        pod_uid="p-exact",
        ws="ws-1",
        kind="job",
        vcpu_requested=4.0,
        memory_gib_requested=16.0,
        start=start,
        end=end,
    )
    pieces = split_by_hour(rec)
    assert len(pieces) == 2
    assert pieces[0].start == start
    assert pieces[0].end == datetime(2026, 11, 9, 11, 0, 0, tzinfo=timezone.utc)
    assert pieces[1].start == datetime(2026, 11, 9, 11, 0, 0, tzinfo=timezone.utc)
    assert pieces[1].end == end


# ==============================================================================
# 4. Rollup grouping and showback keys
# ==============================================================================


def test_rollup_grouping_and_showback():
    t1 = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 11, 9, 11, 0, 0, tzinfo=timezone.utc)

    # Pod 1: job in ws-alpha, release rel-1, principal alice, 4 vCPU/16 GiB (1.0 ACU)
    rec1 = UsageRecord(
        pod_uid="p1",
        ws="ws-alpha",
        kind="job",
        vcpu_requested=4.0,
        memory_gib_requested=16.0,
        start=t1,
        end=t2,
        labels={"principal": "alice", "release": "rel-1"},
    )

    # Pod 2: job in ws-alpha, release rel-1, principal bob, 4 vCPU/16 GiB (1.0 ACU)
    rec2 = UsageRecord(
        pod_uid="p2",
        ws="ws-alpha",
        kind="job",
        vcpu_requested=4.0,
        memory_gib_requested=16.0,
        start=t1,
        end=t2,
        labels={"principal": "bob", "release": "rel-1"},
    )

    # Pod 3: session in ws-alpha, release rel-1, user charlie, 4 vCPU/16 GiB (1.5 ACU)
    rec3 = UsageRecord(
        pod_uid="p3",
        ws="ws-alpha",
        kind="session",
        vcpu_requested=4.0,
        memory_gib_requested=16.0,
        start=t1,
        end=t2,
        labels={"user": "charlie", "release": "rel-1"},
    )

    # Pod 4: job in ws-beta, release rel-2, principal dave, 4 vCPU/16 GiB (1.0 ACU)
    rec4 = UsageRecord(
        pod_uid="p4",
        ws="ws-beta",
        kind="job",
        vcpu_requested=4.0,
        memory_gib_requested=16.0,
        start=t1,
        end=t2,
        labels={"principal": "dave", "release": "rel-2"},
    )

    records = [rec3, rec1, rec4, rec2]
    rollups = rollup(records, DEFAULT_WEIGHTS)

    # We expect 3 rollups:
    # 1. ws-alpha, 10:00, job, rel-1: 2 records, total ACU 2.0, pod_seconds 7200
    # 2. ws-alpha, 10:00, session, rel-1: 1 record, total ACU 1.5, pod_seconds 3600
    # 3. ws-beta, 10:00, job, rel-2: 1 record, total ACU 1.0, pod_seconds 3600
    assert len(rollups) == 3

    r1 = rollups[0]
    assert r1.ws == "ws-alpha"
    assert r1.hour_start == t1
    assert r1.kind == "job"
    assert r1.release == "rel-1"
    assert r1.acu == Decimal("2.000000")
    assert r1.pod_seconds == Decimal("7200")
    assert r1.record_count == 2
    assert r1.by_label == {
        "principal:alice": Decimal("1.000000"),
        "principal:bob": Decimal("1.000000"),
    }

    r2 = rollups[1]
    assert r2.ws == "ws-alpha"
    assert r2.hour_start == t1
    assert r2.kind == "session"
    assert r2.release == "rel-1"
    assert r2.acu == Decimal("1.500000")
    assert r2.pod_seconds == Decimal("3600")
    assert r2.record_count == 1
    assert r2.by_label == {
        "user:charlie": Decimal("1.500000"),
    }

    r3 = rollups[2]
    assert r3.ws == "ws-beta"
    assert r3.hour_start == t1
    assert r3.kind == "job"
    assert r3.release == "rel-2"
    assert r3.acu == Decimal("1.000000")
    assert r3.pod_seconds == Decimal("3600")
    assert r3.record_count == 1
    assert r3.by_label == {
        "principal:dave": Decimal("1.000000"),
    }


def test_rollup_spanning_hours():
    t1 = datetime(2026, 11, 9, 10, 30, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 11, 9, 12, 30, 0, tzinfo=timezone.utc)
    rec = UsageRecord(
        pod_uid="p-span",
        ws="ws-1",
        kind="job",
        vcpu_requested=4.0,
        memory_gib_requested=16.0,
        start=t1,
        end=t2,
        labels={"principal": "alice", "release": "rel-1"},
    )
    rollups = rollup([rec], DEFAULT_WEIGHTS)
    assert len(rollups) == 3

    # Hour 10: 10:30 to 11:00 (1800s, 0.5 ACU)
    assert rollups[0].hour_start == datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    assert rollups[0].acu == Decimal("0.500000")
    assert rollups[0].pod_seconds == Decimal("1800")
    assert rollups[0].record_count == 1
    assert rollups[0].by_label == {"principal:alice": Decimal("0.500000")}

    # Hour 11: 11:00 to 12:00 (3600s, 1.0 ACU)
    assert rollups[1].hour_start == datetime(2026, 11, 9, 11, 0, 0, tzinfo=timezone.utc)
    assert rollups[1].acu == Decimal("1.000000")
    assert rollups[1].pod_seconds == Decimal("3600")
    assert rollups[1].record_count == 1
    assert rollups[1].by_label == {"principal:alice": Decimal("1.000000")}

    # Hour 12: 12:00 to 12:30 (1800s, 0.5 ACU)
    assert rollups[2].hour_start == datetime(2026, 11, 9, 12, 0, 0, tzinfo=timezone.utc)
    assert rollups[2].acu == Decimal("0.500000")
    assert rollups[2].pod_seconds == Decimal("1800")
    assert rollups[2].record_count == 1
    assert rollups[2].by_label == {"principal:alice": Decimal("0.500000")}


# ==============================================================================
# 5. Sign and verify round-trip
# ==============================================================================


def test_sign_and_verify_round_trip():
    priv, pub, _, _ = _make_key_pair()
    t = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    r1 = HourlyRollup(
        ws="ws-1",
        hour_start=t,
        kind="job",
        release="atlas-rt/1.0.0",
        acu=Decimal("1.000000"),
        pod_seconds=Decimal("3600"),
        record_count=1,
        by_label={"principal:alice": Decimal("1.000000")},
    )
    r2 = HourlyRollup(
        ws="ws-2",
        hour_start=t,
        kind="session",
        release="atlas-rt/1.0.0",
        acu=Decimal("0.750000"),
        pod_seconds=Decimal("1800"),
        record_count=1,
        by_label={"user:bob": Decimal("0.750000")},
    )

    token = sign_rollups(
        [r1, r2],
        priv,
        kid="cell-key-1",
        cell_id="cell-primary",
        issued_at=1700000000,
    )
    assert isinstance(token, str)
    assert len(token.split(".")) == 3

    recovered = verify_rollups(token, {"cell-key-1": pub})
    assert len(recovered) == 2
    assert recovered[0] == r1
    assert recovered[1] == r2


def test_verify_refuses_tampered_token():
    priv, pub, _, _ = _make_key_pair()
    t = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    r = HourlyRollup(
        ws="ws-1",
        hour_start=t,
        kind="job",
        release="atlas-rt/1.0.0",
        acu=Decimal("1.000000"),
        pod_seconds=Decimal("3600"),
        record_count=1,
    )
    token = sign_rollups(
        [r],
        priv,
        kid="key-1",
        cell_id="cell-1",
        issued_at=1700000000,
    )

    parts = token.split(".")
    # Tamper payload
    tampered_payload = parts[1][:-1] + ("A" if parts[1][-1] != "A" else "B")
    tampered_token = f"{parts[0]}.{tampered_payload}.{parts[2]}"

    with pytest.raises(MeteringError, match="Invalid token signature"):
        verify_rollups(tampered_token, {"key-1": pub})

    # Tamper signature
    tampered_sig = parts[2][:-1] + ("X" if parts[2][-1] != "X" else "Y")
    tampered_sig_token = f"{parts[0]}.{parts[1]}.{tampered_sig}"
    with pytest.raises(MeteringError, match="Invalid token signature"):
        verify_rollups(tampered_sig_token, {"key-1": pub})


def test_verify_refuses_wrong_typ():
    from seeknal.serving.grant_token import sign_grant_token

    priv, pub, _, _ = _make_key_pair()
    # Sign token with wrong typ "grant" instead of "acu_rollup"
    claims = {
        "typ": "grant",
        "iss": "cell-1",
        "iat": 1700000000,
        "rollups": [],
    }
    token = sign_grant_token(claims, priv, kid="key-1")

    with pytest.raises(MeteringError, match="Wrong token typ"):
        verify_rollups(token, {"key-1": pub})


def test_verify_refuses_unknown_kid():
    priv, pub, _, _ = _make_key_pair()
    t = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    r = HourlyRollup(
        ws="ws-1",
        hour_start=t,
        kind="job",
        release="rel",
        acu=Decimal("1.000000"),
        pod_seconds=Decimal("3600"),
        record_count=1,
    )
    token = sign_rollups([r], priv, kid="key-unknown", cell_id="c1", issued_at=1700000000)

    with pytest.raises(MeteringError, match="Unknown key ID"):
        verify_rollups(token, {"other-key": pub})


# ==============================================================================
# 6. Monthly CSV export (golden string)
# ==============================================================================


def test_export_monthly_exact_golden_string():
    t1 = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 11, 9, 11, 0, 0, tzinfo=timezone.utc)
    t_other_month = datetime(2026, 12, 1, 0, 0, 0, tzinfo=timezone.utc)

    r1 = HourlyRollup(
        ws="ws-prod-1",
        hour_start=t1,
        kind="job",
        release="atlas-rt/1.0.0",
        acu=Decimal("1.000000"),
        pod_seconds=Decimal("3600"),
        record_count=1,
        by_label={"principal:alice": Decimal("1.000000")},
    )
    r2 = HourlyRollup(
        ws="ws-prod-1",
        hour_start=t2,
        kind="session",
        release="atlas-rt/1.0.0",
        acu=Decimal("0.750000"),
        pod_seconds=Decimal("1800"),
        record_count=1,
        by_label={"user:bob": Decimal("0.750000")},
    )
    # Rollup from another month should be excluded
    r_dec = HourlyRollup(
        ws="ws-prod-1",
        hour_start=t_other_month,
        kind="job",
        release="atlas-rt/1.0.0",
        acu=Decimal("2.000000"),
        pod_seconds=Decimal("7200"),
        record_count=1,
    )

    csv_output = export_monthly([r2, r_dec, r1], 2026, 11)

    golden = (
        "ws,hour_start,kind,release,acu,pod_seconds,record_count\n"
        "ws-prod-1,2026-11-09T10:00:00Z,job,atlas-rt/1.0.0,1.000000,3600,1\n"
        "ws-prod-1,2026-11-09T11:00:00Z,session,atlas-rt/1.0.0,0.750000,1800,1\n"
    )
    assert csv_output == golden


# ==============================================================================
# 7. CLI tests: exit codes and outputs
# ==============================================================================


def test_cli_meter_default_json(tmp_path: Path, capsys):
    manifest_p = _make_manifest_file(tmp_path)
    records_p = tmp_path / "records.jsonl"
    record_line = json.dumps(
        {
            "pod_uid": "pod-cli-1",
            "ws": "ws-cli",
            "kind": "job",
            "vcpu_requested": 4.0,
            "memory_gib_requested": 16.0,
            "start": "2026-11-09T10:00:00Z",
            "end": "2026-11-09T11:00:00Z",
            "labels": {"principal": "svc:test", "release": "atlas-rt/1.0.0"},
        }
    )
    records_p.write_text(record_line + "\n", encoding="utf-8")

    exit_code = main(
        [
            "meter",
            "--records",
            str(records_p),
            "--manifest",
            str(manifest_p),
        ]
    )
    assert exit_code == 0

    out = capsys.readouterr().out
    data = json.loads(out)
    assert len(data) == 1
    assert data[0]["ws"] == "ws-cli"
    assert data[0]["kind"] == "job"
    assert data[0]["acu"] == "1.000000"
    assert data[0]["pod_seconds"] == "3600"
    assert data[0]["record_count"] == 1
    assert data[0]["by_label"] == {"principal:svc:test": "1.000000"}


def test_cli_meter_sign_key(tmp_path: Path, capsys):
    manifest_p = _make_manifest_file(tmp_path)
    records_p = tmp_path / "records.jsonl"
    records_p.write_text(
        json.dumps(
            {
                "pod_uid": "p1",
                "ws": "ws-1",
                "kind": "job",
                "vcpu_requested": 4.0,
                "memory_gib_requested": 16.0,
                "start": "2026-11-09T10:00:00Z",
                "end": "2026-11-09T11:00:00Z",
                "labels": {"release": "atlas-rt/1.0.0"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    _, pub, priv_pem, _ = _make_key_pair()
    key_p = tmp_path / "key.pem"
    key_p.write_text(priv_pem, encoding="utf-8")

    exit_code = main(
        [
            "meter",
            "--records",
            str(records_p),
            "--manifest",
            str(manifest_p),
            "--sign-key",
            str(key_p),
            "--kid",
            "kid-test-1",
            "--cell",
            "cell-xyz",
        ]
    )
    assert exit_code == 0

    token = capsys.readouterr().out.strip()
    assert len(token.split(".")) == 3

    # Verify that the generated token can be verified
    recovered = verify_rollups(token, {"kid-test-1": pub})
    assert len(recovered) == 1
    assert recovered[0].ws == "ws-1"
    assert recovered[0].acu == Decimal("1.000000")


def test_cli_meter_monthly_csv(tmp_path: Path, capsys):
    manifest_p = _make_manifest_file(tmp_path)
    records_p = tmp_path / "records.jsonl"
    records_p.write_text(
        json.dumps(
            {
                "pod_uid": "p1",
                "ws": "ws-1",
                "kind": "job",
                "vcpu_requested": 4.0,
                "memory_gib_requested": 16.0,
                "start": "2026-11-09T10:00:00Z",
                "end": "2026-11-09T11:00:00Z",
                "labels": {"release": "atlas-rt/1.0.0"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    exit_code = main(
        [
            "meter",
            "--records",
            str(records_p),
            "--manifest",
            str(manifest_p),
            "--month",
            "2026-11",
        ]
    )
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "ws,hour_start,kind,release,acu,pod_seconds,record_count\n" in out
    assert "ws-1,2026-11-09T10:00:00Z,job,atlas-rt/1.0.0,1.000000,3600,1\n" in out


def test_cli_meter_errors(tmp_path: Path, capsys):
    manifest_p = _make_manifest_file(tmp_path)

    # Missing records file -> exits 1
    exit_code = main(
        ["meter", "--records", str(tmp_path / "nonexistent.jsonl"), "--manifest", str(manifest_p)]
    )
    assert exit_code == 1

    # Missing manifest file -> exits 1
    records_p = tmp_path / "records.jsonl"
    records_p.write_text("{}\n", encoding="utf-8")
    exit_code = main(
        ["meter", "--records", str(records_p), "--manifest", str(tmp_path / "nonexistent.yaml")]
    )
    assert exit_code == 1

    # Invalid record json -> exits 1
    bad_records = tmp_path / "bad.jsonl"
    bad_records.write_text("not-a-json-line\n", encoding="utf-8")
    exit_code = main(
        ["meter", "--records", str(bad_records), "--manifest", str(manifest_p)]
    )
    assert exit_code == 1

    # Incompatible args (--sign-key and --month) -> exits 2
    exit_code = main(
        [
            "meter",
            "--records",
            str(records_p),
            "--manifest",
            str(manifest_p),
            "--sign-key",
            "some-key",
            "--kid",
            "k",
            "--cell",
            "c",
            "--month",
            "2026-11",
        ]
    )
    assert exit_code == 2

    # --sign-key missing --kid or --cell -> exits 2
    exit_code = main(
        [
            "meter",
            "--records",
            str(records_p),
            "--manifest",
            str(manifest_p),
            "--sign-key",
            "some-key",
        ]
    )
    assert exit_code == 2

    # Invalid month format -> exits 2
    exit_code = main(
        [
            "meter",
            "--records",
            str(records_p),
            "--manifest",
            str(manifest_p),
            "--month",
            "invalid-month",
        ]
    )
    assert exit_code == 2
