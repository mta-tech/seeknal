"""Tests for Seeknal Serving durable audit records, sinks, rotation/retention, and failure semantics."""

from __future__ import annotations

import asyncio
import base64
from concurrent.futures import ThreadPoolExecutor
import datetime
import errno
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import socket
import stat
import threading
import time
from typing import Any

from cryptography.hazmat.primitives.asymmetric import ec
import httpx
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.testclient import TestClient

from seeknal.serving.__main__ import main as cli_main
from seeknal.serving.adapters import DuckDBAdapter, EngineCancelled, QueryResult
from seeknal.serving.audit import (
    ALLOWED_AUDIT_FIELD_SET,
    ALLOWED_AUDIT_FIELDS,
    ALLOWED_AUDIT_ROUTES,
    AuditConfig,
    AuditManager,
    AuditRecord,
    AuditSink,
    AuditWriteError,
    AuditWriter,
    FileAuditSink,
    StdoutAuditSink,
    SyslogAuditSink,
    build_audit_record,
    build_audit_sink,
    build_minimal_audit_record,
    compute_sql_sha256,
    emit_audit_record,
    serialize_audit_record,
    verify_audit_config_offline,
)
from seeknal.serving.grant_token import sign_cancel_token, sign_grant_token
from seeknal.serving.http import _log_request, create_app
from seeknal.serving.placement import Placement
from seeknal.serving.server.app import build_server
from seeknal.serving.server.config import ServerConfig, ServerConfigError
from seeknal.serving.service import PreparedQuery, ServingPipeline


class FakeClock:
    """Deterministic controllable clock for rotation and retention tests."""

    def __init__(self, initial: float = 1_763_542_800.0) -> None:
        self._now = float(initial)

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> float:
        self._now += float(seconds)
        return self._now


def _sample_record(
    *,
    audit_id: str = "aud-001",
    route: str = "/v1/query",
    status: int = 200,
    code: str | None = None,
    sql: str | None = "SELECT id, name FROM orders WHERE id = $1",
    timestamp: str | float | None = "2026-11-19T09:00:00Z",
) -> AuditRecord:
    return build_audit_record(
        timestamp=timestamp,
        audit_id=audit_id,
        subject="user:alice",
        act_sub="sess-99",
        act_typ="ask_session",
        azp="atlas-ask",
        workload="ask",
        origin="ask",
        route=route,
        engine="duck",
        rule="analytics",
        tables=["orders"],
        status=status,
        code=code,
        elapsed_ms=12.3456,
        row_count=3 if status == 200 and route == "/v1/query" else None,
        truncated=False if status == 200 and route == "/v1/query" else None,
        publication_id="pub-orders-v1",
        revision=4,
        digest="sha256:abcdef0123456789",
        sql=sql,
    )


# ---------------------------------------------------------------------------
# 1. Record Schema & Strict Allowlist / Redaction Tests
# ---------------------------------------------------------------------------


def test_audit_record_strict_field_allowlist_and_planted_canaries_never_appear(tmp_path: Path):
    priv_key = ec.generate_private_key(ec.SECP256R1())
    planted_sql_literal = "SUPER_SECRET_SSN_999_00_1234"
    planted_sql = (
        f"SELECT id, secret_notes FROM customers "
        f"WHERE ssn = '{planted_sql_literal}' AND region = $1"
    )
    planted_param = "PLANTED_PARAM_CANARY_88421"
    planted_row_val = "PLANTED_ROW_CELL_CANARY_77319"
    planted_dsn = "postgresql://svc_prod:PLANTED_DSN_PASS_66200@db.internal:5432/prod"
    planted_secret = "PLANTED_API_SECRET_55109"

    claims = {
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "jti": "jti-allowlist-check",
        "sub": "user:bob",
        "act": {"sub": "ask-sess-07", "typ": "ask_session"},
        "azp": "seeknal-ask-client",
        "workload": "ask",
        "origin": "ask",
        "publication_id": "pub-cust-2",
        "revision": "rev-2026-11",
        "digest": "sha256:1122334455667788",
        "iat": 1_763_542_800,
        "exp": 1_763_543_000,
        "sql_sha256": hashlib.sha256(planted_sql.encode("utf-8")).hexdigest(),
    }
    planted_token = sign_grant_token(claims, priv_key, kid="kid-test-1")
    planted_auth_header = f"Bearer {planted_token}"

    record = build_audit_record(
        timestamp=datetime.datetime(2026, 11, 19, 9, 0, 0, tzinfo=datetime.timezone.utc),
        route="/v1/query",
        status=200,
        elapsed_ms=42.6789,
        logical_tables=["customers"],
        row_count=1,
        truncated=False,
        sql=planted_sql,
        claims=claims,
        # Planted sensitive material passed as extra arguments:
        token=planted_token,
        grant_token=planted_token,
        authorization=planted_auth_header,
        headers={"Authorization": planted_auth_header},
        params=[planted_param],
        rows=[[101, planted_row_val]],
        dsn=planted_dsn,
        secret=planted_secret,
        redacted_sql="SELECT id, secret_notes FROM customers WHERE ssn = ? AND region = ?",
    )

    # Also test serialization from a raw dictionary polluted with forbidden keys.
    polluted_dict = {
        **record.to_dict(),
        "sql": planted_sql,
        "sql_literals": [planted_sql_literal],
        "params": [planted_param],
        "rows": [[101, planted_row_val]],
        "grant_token": planted_token,
        "authorization": planted_auth_header,
        "dsn": planted_dsn,
        "secret": planted_secret,
    }

    audit_file = tmp_path / "audit.jsonl"
    file_sink = FileAuditSink(audit_file)
    stdout_buf = io.StringIO()
    stdout_sink = StdoutAuditSink(stream=stdout_buf)
    syslog_sink = SyslogAuditSink(hostname="node-1", procid="999")

    file_sink.write_record(record)
    file_sink.write_record(polluted_dict)
    file_sink.close()
    stdout_sink.write_record(polluted_dict)
    rfc5424_msg = syslog_sink.format_rfc5424(polluted_dict)

    file_text = audit_file.read_text(encoding="utf-8")
    stdout_text = stdout_buf.getvalue()
    combined_outputs = "\n".join([file_text, stdout_text, rfc5424_msg])

    for forbidden_canary in (
        planted_token,
        planted_auth_header,
        planted_sql,
        planted_sql_literal,
        planted_param,
        planted_row_val,
        planted_dsn,
        "PLANTED_DSN_PASS_66200",
        planted_secret,
        "secret_notes",
    ):
        assert forbidden_canary not in combined_outputs

    lines = file_text.strip().splitlines()
    assert len(lines) == 2
    parsed = json.loads(lines[0])
    assert list(parsed.keys()) == list(ALLOWED_AUDIT_FIELDS)
    assert set(parsed.keys()) == ALLOWED_AUDIT_FIELD_SET
    assert parsed["timestamp"] == "2026-11-19T09:00:00Z"
    assert parsed["audit_id"] == "jti-allowlist-check"
    assert parsed["subject"] == "user:bob"
    assert parsed["act_sub"] == "ask-sess-07"
    assert parsed["act_typ"] == "ask_session"
    assert parsed["azp"] == "seeknal-ask-client"
    assert parsed["workload"] == "ask"
    assert parsed["origin"] == "ask"
    assert parsed["route"] == "/v1/query"
    assert parsed["tables"] == ["customers"]
    assert parsed["status"] == 200
    assert parsed["code"] is None
    assert parsed["elapsed_ms"] == 42.68
    assert parsed["row_count"] == 1
    assert parsed["truncated"] is False
    assert parsed["publication_id"] == "pub-cust-2"
    assert parsed["revision"] == "rev-2026-11"
    assert parsed["digest"] == "sha256:1122334455667788"
    assert parsed["sql_sha256"] == hashlib.sha256(planted_sql.encode("utf-8")).hexdigest()


def test_value_level_scrubbing_and_strict_from_dict():
    priv_key = ec.generate_private_key(ec.SECP256R1())
    jwt_tok = sign_grant_token({"jti": "x", "sub": "u"}, priv_key, kid="k1")
    dsn_str = "postgresql://admin:hunter2@10.0.0.9:5432/analytics"

    # Even if a caller stuffs a token or DSN into an allowlisted string field, it is scrubbed.
    rec = AuditRecord(
        route="/v1/cancel",
        status=401,
        error_code="grant_token_error",
        subject=f"Bearer {jwt_tok}",
        origin=dsn_str,
        sql="",
    )
    assert rec.error_code == "grant_token_error"
    assert rec.code == "grant_token_error"
    assert rec.sql_sha256 is None
    assert rec.tables == []

    serialized = rec.to_json()
    assert jwt_tok not in serialized
    assert "hunter2" not in serialized
    assert dsn_str not in serialized

    # Raw SQL passed into sql_sha256 is rejected.
    with pytest.raises(ValueError, match="sql_sha256"):
        AuditRecord(route="/v1/explain", status=200, sql_sha256="SELECT 1")

    # strict=True rejects forbidden keys on from_dict.
    with pytest.raises(ValueError, match="Forbidden or unknown"):
        AuditRecord.from_dict(
            {"route": "/v1/query", "status": 200, "dsn": dsn_str},
            strict=True,
        )


def test_routes_and_error_code_behavior():
    assert "/v1/select" in ALLOWED_AUDIT_ROUTES
    for valid_route in ("/v1/query", "/v1/select", "/v1/explain", "/v1/cancel"):
        ok_rec = AuditRecord(route=valid_route, status=200, code="ignored_on_200")
        assert ok_rec.code is None

    err_rec = AuditRecord(route="/v1/explain", status=403, error_code="enforcement_error")
    assert err_rec.code == "enforcement_error"
    assert err_rec.error_code == "enforcement_error"

    with pytest.raises(ValueError, match="route"):
        AuditRecord(route="/healthz", status=200)


# ---------------------------------------------------------------------------
# 2. AuditConfig Validation Tests
# ---------------------------------------------------------------------------


def test_audit_config_from_dict_modes_aliases_and_unknown_keys():
    cfg_default = AuditConfig.from_dict({"path": "/var/log/seeknal/audit.jsonl"})
    assert cfg_default.sink == "file"
    assert cfg_default.on_failure == "refuse"
    assert cfg_default.mode == 0o600

    for raw_mode, expected in (
        ("0600", 0o600),
        ("0640", 0o640),
        ("0o600", 0o600),
        ("0o640", 0o640),
        (0o600, 0o600),
        (0o640, 0o640),
    ):
        c = AuditConfig.from_dict({"path": "/tmp/a.jsonl", "mode": raw_mode})
        assert c.mode == expected

    for bad_mode in ("0666", "0660", "0644", "0777", 0o666, 0o644, 0o620, 0o400, True):
        with pytest.raises(ServerConfigError):
            AuditConfig.from_dict(
                {"path": "/tmp/a.jsonl", "mode": bad_mode},
                error_cls=ServerConfigError,
            )

    # Aliases
    cfg_alias = AuditConfig.from_dict(
        {
            "sink": "file",
            "path": "/tmp/a.jsonl",
            "rotate_size_bytes": 4096,
            "rotate_seconds": 120,
            "retention_count": 5,
            "retention_age_seconds": 3600,
            "protocol": "tcp",
        },
        error_cls=ServerConfigError,
    )
    assert cfg_alias.max_bytes == 4096
    assert cfg_alias.rotate_size_bytes == 4096
    assert cfg_alias.rotate_interval_seconds == 120.0
    assert cfg_alias.rotate_seconds == 120.0
    assert cfg_alias.max_files == 5
    assert cfg_alias.retention_count == 5
    assert cfg_alias.max_age_seconds == 3600.0
    assert cfg_alias.retention_age_seconds == 3600.0
    assert cfg_alias.transport == "tcp"
    assert cfg_alias.protocol == "tcp"

    # Conflicting primary + alias rejected
    with pytest.raises(ServerConfigError, match="Cannot specify both"):
        AuditConfig.from_dict(
            {"path": "/tmp/a.jsonl", "max_bytes": 100, "rotate_size_bytes": 200},
            error_cls=ServerConfigError,
        )

    # Unknown key rejected with ServerConfigError
    with pytest.raises(ServerConfigError, match="Unknown key.*unexpected_option"):
        AuditConfig.from_dict(
            {"sink": "stdout", "unexpected_option": 123},
            error_cls=ServerConfigError,
        )

    # Missing path when sink == "file"
    with pytest.raises(ServerConfigError, match="audit.path is required"):
        AuditConfig.from_dict({"sink": "file"}, error_cls=ServerConfigError)


# ---------------------------------------------------------------------------
# 3. FileAuditSink: Permissions, Rotation, Retention, Concurrency
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("configured_mode,expected_mode", [("0600", 0o600), ("0640", 0o640)])
def test_file_sink_strict_permissions_active_and_rotated(
    tmp_path: Path,
    configured_mode: str,
    expected_mode: int,
):
    audit_path = tmp_path / "audit.jsonl"
    # Pre-create with overly permissive 0644 mode to verify tightening on open.
    audit_path.write_text("", encoding="utf-8")
    os.chmod(audit_path, 0o644)

    old_umask = os.umask(0o077)
    try:
        sink = FileAuditSink(audit_path, mode=configured_mode, max_bytes=300, fsync=False)
        sink.verify_writable()
        assert stat.S_IMODE(audit_path.stat().st_mode) == expected_mode

        for i in range(4):
            sink.write_record(_sample_record(audit_id=f"perm-{i}"))
        sink.close()
    finally:
        os.umask(old_umask)

    assert stat.S_IMODE(audit_path.stat().st_mode) == expected_mode
    rotated = sink.rotated_files()
    assert len(rotated) >= 1
    for rot_file in rotated:
        assert stat.S_IMODE(rot_file.stat().st_mode) == expected_mode


def test_file_sink_rotation_by_size(tmp_path: Path):
    audit_path = tmp_path / "audit.jsonl"
    one_line_len = len(_sample_record(audit_id="rec-00").to_json().encode("utf-8")) + 1
    # Allow at most 2 records per file before rotating on the 3rd.
    max_bytes = (one_line_len * 2) + 10

    sink = FileAuditSink(audit_path, max_bytes=max_bytes, fsync=False)
    for idx in range(5):
        sink.write_record(_sample_record(audit_id=f"rec-{idx:02d}"))
    sink.close()

    rotated = sink.rotated_files()
    assert len(rotated) == 2

    all_ids: list[str] = []
    for fpath in [*rotated, audit_path]:
        assert fpath.stat().st_size <= max_bytes
        for line in fpath.read_text(encoding="utf-8").splitlines():
            all_ids.append(json.loads(line)["audit_id"])

    assert all_ids == ["rec-00", "rec-01", "rec-02", "rec-03", "rec-04"]


def test_file_sink_rotation_by_time_with_fake_clock(tmp_path: Path):
    clock = FakeClock(1_763_542_800.0)
    audit_path = tmp_path / "audit.jsonl"
    sink = FileAuditSink(
        audit_path,
        rotate_interval_seconds=60.0,
        clock=clock,
        fsync=False,
    )

    sink.write_record(_sample_record(audit_id="t0"))
    clock.advance(30.0)
    sink.write_record(_sample_record(audit_id="t30"))
    assert len(sink.rotated_files()) == 0

    # Advance past 60s window -> next write rotates the previous 2-record segment.
    clock.advance(31.0)
    sink.write_record(_sample_record(audit_id="t61"))
    assert len(sink.rotated_files()) == 1

    # Advance another 60s -> rotates again.
    clock.advance(60.0)
    sink.write_record(_sample_record(audit_id="t121"))
    sink.close()

    rotated = sink.rotated_files()
    assert len(rotated) == 2
    seg0 = [json.loads(l)["audit_id"] for l in rotated[0].read_text(encoding="utf-8").splitlines()]
    seg1 = [json.loads(l)["audit_id"] for l in rotated[1].read_text(encoding="utf-8").splitlines()]
    active = [json.loads(l)["audit_id"] for l in audit_path.read_text(encoding="utf-8").splitlines()]
    assert seg0 == ["t0", "t30"]
    assert seg1 == ["t61"]
    assert active == ["t121"]


def test_file_sink_retention_by_count_and_age(tmp_path: Path):
    clock = FakeClock(1_763_542_800.0)
    audit_path = tmp_path / "audit.jsonl"
    unrelated_file = tmp_path / "keep_me.log"
    unrelated_file.write_text("do not touch\n", encoding="utf-8")

    # 1) Retention by count (max_files=3)
    sink_count = FileAuditSink(
        audit_path,
        rotate_interval_seconds=10.0,
        max_files=3,
        clock=clock,
        fsync=False,
    )
    for i in range(6):
        sink_count.write_record(_sample_record(audit_id=f"cnt-{i}"))
        clock.advance(15.0)
    sink_count.close()

    rotated_after_count = sink_count.rotated_files()
    assert len(rotated_after_count) == 3
    retained_ids = [
        json.loads(p.read_text(encoding="utf-8").strip())["audit_id"]
        for p in rotated_after_count
    ]
    # 6 writes with 15s steps caused 5 rotations (cnt-0..cnt-4) plus active cnt-5;
    # oldest two rotated (cnt-0, cnt-1) were pruned, leaving cnt-2, cnt-3, cnt-4.
    assert retained_ids == ["cnt-2", "cnt-3", "cnt-4"]
    assert unrelated_file.exists()

    # 2) Retention by age (max_age_seconds=100.0) with fake clock
    age_path = tmp_path / "audit_age.jsonl"
    sink_age = FileAuditSink(
        age_path,
        rotate_interval_seconds=10.0,
        max_age_seconds=100.0,
        clock=clock,
        fsync=False,
    )
    sink_age.write_record(_sample_record(audit_id="old-1"))
    clock.advance(20.0)
    sink_age.write_record(_sample_record(audit_id="old-2"))  # rotates old-1 at t+20
    clock.advance(20.0)
    sink_age.write_record(_sample_record(audit_id="recent-1"))  # rotates old-2 at t+40
    assert len(sink_age.rotated_files()) == 2

    # Advance 90s: at t+130, old-1 rotated at t+20 is 110s old (>100s -> pruned),
    # while old-2 rotated at t+40 is 90s old (<=100s -> kept), and recent-1 rotates now.
    clock.advance(90.0)
    sink_age.write_record(_sample_record(audit_id="active-now"))
    sink_age.close()

    age_rotated = sink_age.rotated_files()
    age_ids = [
        json.loads(p.read_text(encoding="utf-8").strip())["audit_id"]
        for p in age_rotated
    ]
    assert "old-1" not in age_ids
    assert age_ids == ["old-2", "recent-1"]
    assert unrelated_file.exists()


def test_file_sink_concurrent_thread_writers(tmp_path: Path):
    audit_path = tmp_path / "concurrent_audit.jsonl"
    sink = FileAuditSink(audit_path, max_bytes=4096, fsync=False)

    num_threads = 12
    records_per_thread = 25
    total_expected = num_threads * records_per_thread

    def _worker(tid: int) -> None:
        for seq in range(records_per_thread):
            sink.write_record(_sample_record(audit_id=f"thr-{tid:02d}-seq-{seq:03d}"))

    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        futures = [pool.submit(_worker, t) for t in range(num_threads)]
        for fut in futures:
            fut.result()

    sink.close()

    seen_ids: set[str] = set()
    for fpath in [*sink.rotated_files(), audit_path]:
        for raw_line in fpath.read_text(encoding="utf-8").splitlines():
            obj = json.loads(raw_line)
            assert set(obj.keys()) == ALLOWED_AUDIT_FIELD_SET
            seen_ids.add(obj["audit_id"])

    assert len(seen_ids) == total_expected


# ---------------------------------------------------------------------------
# 4. StdoutAuditSink Tests
# ---------------------------------------------------------------------------


def test_stdout_audit_sink_capsys_and_threads(capsys: pytest.CaptureFixture[str]):
    cfg = AuditConfig.from_dict({"sink": "stdout"})
    sink = build_audit_sink(cfg)
    assert isinstance(sink, StdoutAuditSink)

    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = [
            pool.submit(sink.write_record, _sample_record(audit_id=f"out-{i}"))
            for i in range(18)
        ]
        for f in futs:
            f.result()

    captured = capsys.readouterr().out.strip().splitlines()
    assert len(captured) == 18
    parsed_ids = {json.loads(line)["audit_id"] for line in captured}
    assert parsed_ids == {f"out-{i}" for i in range(18)}


# ---------------------------------------------------------------------------
# 5. SyslogAuditSink TCP (RFC 6587 Octet-Counting) & UDP Loopback Tests
# ---------------------------------------------------------------------------


def _parse_rfc6587_frames(data: bytes) -> list[str]:
    """Parse RFC 6587 octet-counted syslog frames: ``<len> <msg_bytes>``."""
    messages: list[str] = []
    idx = 0
    total = len(data)
    while idx < total:
        space_pos = data.find(b" ", idx)
        assert space_pos > idx, "Missing space after octet length in RFC 6587 frame"
        frame_len = int(data[idx:space_pos].decode("ascii"))
        msg_start = space_pos + 1
        msg_end = msg_start + frame_len
        assert msg_end <= total, "Truncated RFC 6587 syslog frame"
        messages.append(data[msg_start:msg_end].decode("utf-8"))
        idx = msg_end
    return messages


def test_syslog_tcp_rfc6587_octet_counting_loopback():
    received_chunks: list[bytes] = []
    done_event = threading.Event()

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("127.0.0.1", 0))
    server_sock.listen(4)
    port = server_sock.getsockname()[1]

    def _tcp_collector() -> None:
        server_sock.settimeout(5.0)
        try:
            conn, _ = server_sock.accept()
            with conn:
                conn.settimeout(5.0)
                while True:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    received_chunks.append(chunk)
        finally:
            done_event.set()

    thread = threading.Thread(target=_tcp_collector, daemon=True)
    thread.start()

    try:
        cfg = AuditConfig.from_dict(
            {
                "sink": "syslog",
                "host": "127.0.0.1",
                "port": port,
                "transport": "tcp",
                "facility": 16,
                "app_name": "seeknal-serving",
                "timeout_seconds": 2.0,
            }
        )
        sink = build_audit_sink(cfg)
        assert isinstance(sink, SyslogAuditSink)

        sink.write_record(_sample_record(audit_id="tcp-1", route="/v1/query"))
        sink.write_record(
            _sample_record(
                audit_id="tcp-2",
                route="/v1/explain",
                status=403,
                code="enforcement_error",
            )
        )
        sink.close()
        done_event.wait(timeout=5.0)
    finally:
        server_sock.close()

    raw_stream = b"".join(received_chunks)
    frames = _parse_rfc6587_frames(raw_stream)
    assert len(frames) == 2

    # Validate RFC 5424 header + allowlisted JSON body on both frames.
    # PRI = facility(16)*8 + severity(6) = 134 -> "<134>1 "
    for idx, frame_str in enumerate(frames, start=1):
        header_part, json_part = frame_str.split(" - ", 1)
        tokens = header_part.split(" ")
        assert len(tokens) == 6
        assert tokens[0] == "<134>1"
        assert tokens[1] == "2026-11-19T09:00:00Z"
        assert tokens[3] == "seeknal-serving"
        assert tokens[5] == "audit"

        payload = json.loads(json_part)
        assert payload["audit_id"] == f"tcp-{idx}"
        assert set(payload.keys()) == ALLOWED_AUDIT_FIELD_SET


def test_syslog_udp_loopback():
    udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_server.bind(("127.0.0.1", 0))
    udp_server.settimeout(3.0)
    port = udp_server.getsockname()[1]

    try:
        cfg = AuditConfig.from_dict(
            {
                "sink": "syslog",
                "host": "127.0.0.1",
                "port": port,
                "transport": "udp",
                "facility": 17,  # local1 -> 17*8 + 6 = 142
                "app_name": "seeknal-udp",
            }
        )
        sink = build_audit_sink(cfg)
        sink.write_record(_sample_record(audit_id="udp-001", route="/v1/cancel", sql=None))
        sink.close()

        datagram, _ = udp_server.recvfrom(65535)
    finally:
        udp_server.close()

    msg = datagram.decode("utf-8")
    assert msg.startswith("<142>1 2026-11-19T09:00:00Z ")
    header_part, json_part = msg.split(" - ", 1)
    assert header_part.split(" ")[3] == "seeknal-udp"
    parsed = json.loads(json_part)
    assert parsed["audit_id"] == "udp-001"
    assert parsed["route"] == "/v1/cancel"
    assert parsed["sql_sha256"] is None


# ---------------------------------------------------------------------------
# 6. Failure Semantics: Startup & Runtime ("refuse" vs "log")
# ---------------------------------------------------------------------------


def test_refuse_at_startup_unwritable_file_and_unreachable_tcp_syslog(tmp_path: Path):
    # 1) Non-existent parent directory refuses startup
    missing_parent_cfg = AuditConfig.from_dict(
        {
            "sink": "file",
            "on_failure": "refuse",
            "path": str(tmp_path / "no_such_dir" / "audit.jsonl"),
        }
    )
    with pytest.raises(AuditWriteError, match="parent directory"):
        build_audit_sink(missing_parent_cfg)

    # 2) Read-only directory refuses startup (skip chmod enforcement check only if running as root)
    if os.geteuid() != 0:
        ro_dir = tmp_path / "readonly_dir"
        ro_dir.mkdir()
        os.chmod(ro_dir, 0o500)
        try:
            ro_cfg = AuditConfig.from_dict(
                {
                    "sink": "file",
                    "on_failure": "refuse",
                    "path": str(ro_dir / "audit.jsonl"),
                }
            )
            with pytest.raises(AuditWriteError):
                build_audit_sink(ro_cfg)
        finally:
            os.chmod(ro_dir, 0o700)

    # 3) Unreachable TCP syslog listener refuses startup
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    closed_port = probe.getsockname()[1]
    probe.close()

    unreachable_syslog_cfg = AuditConfig.from_dict(
        {
            "sink": "syslog",
            "on_failure": "refuse",
            "host": "127.0.0.1",
            "port": closed_port,
            "transport": "tcp",
            "timeout_seconds": 0.5,
        }
    )
    with pytest.raises(AuditWriteError, match="unreachable"):
        build_audit_sink(unreachable_syslog_cfg)


class _FailingSink(AuditSink):
    """Deterministic broken sink simulating ENOSPC / destination outage at runtime."""

    def verify_writable(self) -> None:
        return None

    def write_record(self, record: AuditRecord | dict[str, Any]) -> None:
        raise OSError(errno.ENOSPC, "No space left on device")


def _build_pre_response_test_app(manager: AuditManager | None) -> Starlette:
    """Minimal Starlette app demonstrating pre-response audit emission contract."""

    async def handle_query(request: Request) -> Response:
        # Query result rows computed by engine, held until durable audit completes:
        computed_rows = [[1, "sensitive-row-1"], [2, "sensitive-row-2"]]
        record = _sample_record(audit_id="req-runtime-1", route="/v1/query", status=200)
        try:
            emit_audit_record(manager, record)
        except AuditWriteError as exc:
            return JSONResponse(exc.to_error_dict(), status_code=exc.status_code)

        return JSONResponse(
            {
                "columns": ["id", "val"],
                "rows": computed_rows,
                "audit_id": record.audit_id,
            },
            status_code=200,
        )

    return Starlette(routes=[Route("/v1/query", handle_query, methods=["POST"])])


def test_refuse_at_runtime_returns_503_audit_unavailable_with_no_rows():
    failures: list[int] = []
    manager = AuditManager(
        _FailingSink(),
        on_failure="refuse",
        on_write_failure=lambda: failures.append(1),
    )
    assert AuditWriter is AuditManager

    # Direct emit raises AuditWriteError with 503 audit_unavailable metadata.
    with pytest.raises(AuditWriteError) as exc_info:
        manager.emit(_sample_record(audit_id="direct-refuse"))
    assert exc_info.value.status_code == 503
    assert exc_info.value.code == "audit_unavailable"
    assert exc_info.value.error_code == "audit_unavailable"
    err_payload = exc_info.value.to_error_dict()
    assert err_payload["error"] == "audit_unavailable"
    assert "rows" not in err_payload
    assert len(failures) == 1

    # Pre-response HTTP flow returns 503 audit_unavailable and leaks NO rows.
    client = TestClient(_build_pre_response_test_app(manager))
    resp = client.post("/v1/query", json={"sql": "SELECT id, val FROM orders"})
    assert resp.status_code == 503
    body = resp.json()
    assert body["error"] == "audit_unavailable"
    assert "rows" not in body
    assert "sensitive-row-1" not in resp.text


def test_refuse_at_runtime_when_tcp_syslog_peer_drops():
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("127.0.0.1", 0))
    server_sock.listen(1)
    port = server_sock.getsockname()[1]

    accepted: list[socket.socket] = []

    def _accept_once() -> None:
        conn, _ = server_sock.accept()
        accepted.append(conn)

    t = threading.Thread(target=_accept_once, daemon=True)
    t.start()

    cfg = AuditConfig.from_dict(
        {
            "sink": "syslog",
            "on_failure": "refuse",
            "host": "127.0.0.1",
            "port": port,
            "transport": "tcp",
            "timeout_seconds": 0.5,
        }
    )
    manager = AuditManager.from_config(cfg)
    t.join(timeout=2.0)

    # Shut down collector completely after startup succeeded.
    for conn in accepted:
        conn.close()
    server_sock.close()
    time.sleep(0.02)

    with pytest.raises(AuditWriteError) as exc_info:
        manager.emit(_sample_record(audit_id="after-collector-drop"))
    assert exc_info.value.status_code == 503
    assert exc_info.value.code == "audit_unavailable"
    manager.close()


def test_log_mode_logs_error_increments_failure_counter_and_proceeds(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
):
    failure_counter = 0

    def _inc_failures() -> None:
        nonlocal failure_counter
        failure_counter += 1

    # 1) Startup failure in "log" mode logs ERROR, increments counter, and does not raise.
    bad_startup_cfg = AuditConfig.from_dict(
        {
            "sink": "file",
            "on_failure": "log",
            "path": str(tmp_path / "missing_dir" / "audit.jsonl"),
        }
    )
    with caplog.at_level(logging.ERROR, logger="seeknal.serving.audit"):
        caplog.clear()
        manager = AuditManager.from_config(
            bad_startup_cfg,
            on_write_failure=_inc_failures,
        )
        assert failure_counter == 1
        assert any(r.levelno == logging.ERROR for r in caplog.records)

        # 2) Runtime failure in "log" mode logs ERROR, increments counter, and allows HTTP 200.
        caplog.clear()
        client = TestClient(_build_pre_response_test_app(manager))
        resp = client.post("/v1/query", json={"sql": "SELECT id, val FROM orders"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["rows"] == [[1, "sensitive-row-1"], [2, "sensitive-row-2"]]
        assert failure_counter == 2
        assert any(
            r.levelno == logging.ERROR and "on_failure=log" in r.getMessage()
            for r in caplog.records
        )

    # 3) Absent audit config (manager=None) is a clean no-op.
    assert emit_audit_record(None, _sample_record(audit_id="noop")) is True


# ---------------------------------------------------------------------------
# 7. HTTP, ServerConfig, build_server & CLI Integration Tests
# ---------------------------------------------------------------------------


def _write_static_server_artifacts(
    tmp_path: Path,
) -> tuple[ec.EllipticCurvePrivateKey, str, str, str]:
    import base64

    priv = ec.generate_private_key(ec.SECP256R1())
    pub_nums = priv.public_key().public_numbers()
    x_b64 = (
        base64.urlsafe_b64encode(pub_nums.x.to_bytes(32, "big"))
        .rstrip(b"=")
        .decode("ascii")
    )
    y_b64 = (
        base64.urlsafe_b64encode(pub_nums.y.to_bytes(32, "big"))
        .rstrip(b"=")
        .decode("ascii")
    )
    jwks_file = tmp_path / "keys.jwks.json"
    jwks_file.write_text(
        json.dumps(
            {
                "keys": [
                    {
                        "kty": "EC",
                        "crv": "P-256",
                        "x": x_b64,
                        "y": y_b64,
                        "kid": "k-audit-int",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    placement_file = tmp_path / "placement.yaml"
    placement_file.write_text(
        """
engines:
  duck:
    kind: duckdb
    dialect: duckdb
tables:
  users:
    keys: [id]
    row_count: 10
    serve:
      duck: users
cubes: {}
""",
        encoding="utf-8",
    )

    schemas_file = tmp_path / "schemas.json"
    schemas_file.write_text(
        json.dumps({"users": ["id", "name", "region"]}),
        encoding="utf-8",
    )

    return priv, str(jwks_file), str(placement_file), str(schemas_file)


def _mint_grant(
    priv: ec.EllipticCurvePrivateKey,
    sql: str,
    *,
    jti: str,
    sub: str = "user:analyst",
    workload: str = "ask",
    origin: str = "ask",
    act: dict[str, str] | None = None,
    azp: str = "atlas-ask-ui",
    publication_id: str = "pub-users-01",
    revision: int | str = 11,
    digest: str = "sha256:feedface1234",
) -> str:
    now = time.time()
    claims: dict[str, Any] = {
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "typ": "grant",
        "sub": sub,
        "ws": "ws-prod",
        "jti": jti,
        "iat": now,
        "exp": now + 30.0,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": 50,
        "workload": workload,
        "azp": azp,
        "publication_id": publication_id,
        "revision": revision,
        "digest": digest,
        "tables": {
            "users": {
                "columns": ["id", "name", "region"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }
    if origin:
        claims["origin"] = origin
    if workload == "ask":
        claims["act"] = act or {"sub": "ask-session-77", "typ": "ask_session"}
    elif act is not None:
        claims["act"] = act
    return sign_grant_token(claims, priv, kid="k-audit-int")


def _mint_cancel(
    priv: ec.EllipticCurvePrivateKey,
    *,
    jti: str,
    sub: str = "user:analyst",
) -> str:
    now = time.time()
    return sign_cancel_token(
        {
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "typ": "cancel",
            "jti": jti,
            "sub": sub,
            "iat": now,
            "exp": now + 30.0,
        },
        priv,
        kid="k-audit-int",
    )


def test_server_config_audit_optional_and_validated(tmp_path: Path):
    _, jwks_file, placement_file, schemas_file = _write_static_server_artifacts(tmp_path)
    base_dict: dict[str, Any] = {
        "grant_keys": {"jwks_file": jwks_file},
        "placement": {
            "static": {
                "placement_file": placement_file,
                "schemas_file": schemas_file,
            }
        },
        "connections": {"duck": {"kind": "duckdb", "path": ":memory:"}},
    }

    # Absent audit section -> cfg.audit is None
    cfg_none = ServerConfig.from_dict(base_dict)
    assert cfg_none.audit is None

    # Explicit null audit section -> cfg.audit is None
    cfg_null = ServerConfig.from_dict({**base_dict, "audit": None})
    assert cfg_null.audit is None

    # Valid audit section
    audit_path = tmp_path / "srv_audit.jsonl"
    cfg_with_audit = ServerConfig.from_dict(
        {
            **base_dict,
            "audit": {
                "sink": "file",
                "on_failure": "refuse",
                "path": str(audit_path),
                "mode": "0640",
                "max_bytes": 1024 * 1024,
            },
        }
    )
    assert cfg_with_audit.audit is not None
    assert cfg_with_audit.audit.path == str(audit_path)
    assert cfg_with_audit.audit.mode == 0o640

    # Invalid/unknown key in audit raises ServerConfigError
    with pytest.raises(ServerConfigError, match="Unknown key"):
        ServerConfig.from_dict(
            {**base_dict, "audit": {"sink": "stdout", "bogus": True}}
        )


def test_build_server_http_end_to_end_query_explain_cancel_and_rejected(tmp_path: Path):
    import duckdb

    priv, jwks_file, placement_file, schemas_file = _write_static_server_artifacts(tmp_path)
    audit_path = tmp_path / "e2e_audit.jsonl"

    planted_lit = "SQL_LITERAL_CANARY_424242"
    planted_param = "SQL_PARAM_CANARY_535353"
    planted_row = "RESULT_ROW_CANARY_646464"

    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR, region VARCHAR)")
    con.execute(
        "INSERT INTO users VALUES (1, ?, ?)",
        [planted_row, planted_param],
    )

    cfg_dict = {
        "grant_keys": {"jwks_file": jwks_file},
        "placement": {
            "static": {
                "placement_file": placement_file,
                "schemas_file": schemas_file,
            }
        },
        "connections": {"duck": {"kind": "duckdb", "path": ":memory:"}},
        "audit": {
            "sink": "file",
            "on_failure": "refuse",
            "path": str(audit_path),
            "mode": "0600",
        },
    }

    handle = build_server(cfg_dict, connect_factories={"duck": lambda: con.cursor()})
    assert isinstance(handle.audit_sink, FileAuditSink)

    query_sql = (
        f"SELECT id, name FROM users "
        f"WHERE name != '{planted_lit}' AND region = $1"
    )
    tok_query = _mint_grant(priv, query_sql, jti="jti-e2e-q1")
    tok_explain = _mint_grant(priv, query_sql, jti="jti-e2e-e1")

    denied_sql = f"SELECT id, ungranted_col FROM users WHERE name != '{planted_lit}'"
    tok_denied = _mint_grant(priv, denied_sql, jti="jti-e2e-denied")

    tok_cancel_404 = _mint_cancel(priv, jti="jti-missing-cancel")

    with TestClient(handle.app) as client:
        # Probe endpoints must NOT emit audit records
        assert client.get("/healthz").status_code == 200
        assert client.get("/readyz").status_code == 200

        # 1) 200 /v1/query
        r_q = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_query}"},
            json={"sql": query_sql, "params": [planted_param]},
        )
        assert r_q.status_code == 200
        assert r_q.json()["rows"] == [[1, planted_row]]

        # 2) 401 grant_replayed on /v1/query
        r_replay = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_query}"},
            json={"sql": query_sql, "params": [planted_param]},
        )
        assert r_replay.status_code == 401
        assert r_replay.json()["code"] == "grant_replayed"

        # 3) 403 access_denied on /v1/query
        r_denied = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_denied}"},
            json={"sql": denied_sql},
        )
        assert r_denied.status_code == 403
        assert r_denied.json()["code"] == "access_denied"

        # 4) 401 grant_invalid (missing header) on /v1/query
        r_noauth = client.post("/v1/query", json={"sql": query_sql})
        assert r_noauth.status_code == 401
        assert r_noauth.json()["code"] == "grant_invalid"

        # 5) 200 /v1/explain
        r_exp = client.post(
            "/v1/explain",
            headers={"Authorization": f"Bearer {tok_explain}"},
            json={"sql": query_sql, "params": [planted_param]},
        )
        assert r_exp.status_code == 200

        # 6) 400 invalid_sql on /v1/explain
        r_bad_exp = client.post(
            "/v1/explain",
            headers={"Authorization": f"Bearer {tok_explain}"},
            content=b"{not-json",
        )
        assert r_bad_exp.status_code == 400
        assert r_bad_exp.json()["code"] == "invalid_sql"

        # 7) 404 not_found on /v1/cancel
        r_c404 = client.post(
            "/v1/cancel",
            headers={"Authorization": f"Bearer {tok_cancel_404}"},
        )
        assert r_c404.status_code == 404
        assert r_c404.json()["code"] == "not_found"

        # 8) 401 grant_invalid on /v1/cancel
        r_c401 = client.post("/v1/cancel")
        assert r_c401.status_code == 401
        assert r_c401.json()["code"] == "grant_invalid"

    # Lifespan shutdown closed file descriptor cleanly
    assert handle.audit_sink._fd is None

    raw_audit = audit_path.read_text(encoding="utf-8")
    for secret in (
        tok_query,
        tok_explain,
        tok_denied,
        tok_cancel_404,
        query_sql,
        denied_sql,
        planted_lit,
        planted_param,
        planted_row,
    ):
        assert secret not in raw_audit

    records = [json.loads(line) for line in raw_audit.splitlines()]
    assert len(records) == 8
    for rec in records:
        assert list(rec.keys()) == list(ALLOWED_AUDIT_FIELDS)

    # Check 200 /v1/query record
    q_rec = records[0]
    assert q_rec["route"] == "/v1/query"
    assert q_rec["status"] == 200
    assert q_rec["code"] is None
    assert q_rec["audit_id"] == "jti-e2e-q1"
    assert q_rec["subject"] == "user:analyst"
    assert q_rec["act_sub"] == "ask-session-77"
    assert q_rec["act_typ"] == "ask_session"
    assert q_rec["azp"] == "atlas-ask-ui"
    assert q_rec["workload"] == "ask"
    assert q_rec["origin"] == "ask"
    assert q_rec["engine"] == "duck"
    assert q_rec["tables"] == ["users"]
    assert q_rec["row_count"] == 1
    assert q_rec["truncated"] is False
    assert q_rec["publication_id"] == "pub-users-01"
    assert q_rec["revision"] == 11
    assert q_rec["digest"] == "sha256:feedface1234"
    assert q_rec["sql_sha256"] == hashlib.sha256(query_sql.encode("utf-8")).hexdigest()

    # Check 401 replay, 403 access_denied, 401 missing auth
    assert (records[1]["route"], records[1]["status"], records[1]["code"]) == (
        "/v1/query",
        401,
        "grant_replayed",
    )
    assert (records[2]["route"], records[2]["status"], records[2]["code"]) == (
        "/v1/query",
        403,
        "access_denied",
    )
    assert records[2]["tables"] == ["users"]
    assert records[2]["sql_sha256"] == hashlib.sha256(denied_sql.encode("utf-8")).hexdigest()
    assert (records[3]["route"], records[3]["status"], records[3]["code"]) == (
        "/v1/query",
        401,
        "grant_invalid",
    )
    assert records[3]["sql_sha256"] is None

    # Check 200 & 400 /v1/explain
    assert (records[4]["route"], records[4]["status"], records[4]["code"]) == (
        "/v1/explain",
        200,
        None,
    )
    assert records[4]["tables"] == ["users"]
    assert records[4]["row_count"] is None
    assert (records[5]["route"], records[5]["status"], records[5]["code"]) == (
        "/v1/explain",
        400,
        "invalid_sql",
    )

    # Check 404 & 401 /v1/cancel
    assert (records[6]["route"], records[6]["status"], records[6]["code"]) == (
        "/v1/cancel",
        404,
        "not_found",
    )
    assert records[6]["audit_id"] == "jti-missing-cancel"
    assert records[6]["sql_sha256"] is None
    assert (records[7]["route"], records[7]["status"], records[7]["code"]) == (
        "/v1/cancel",
        401,
        "grant_invalid",
    )


def test_cancel_202_emits_audit_record(tmp_path: Path):
    priv, jwks_file, _, _ = _write_static_server_artifacts(tmp_path)
    pub_keys = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_file},
            "placement": {"static": {"placement_file": "p", "schemas_file": "s"}},
            "connections": {"duck": {"kind": "duckdb", "path": ":memory:"}},
        },
        load_keys=False,
    ).load_grant_keys()

    placement = Placement.from_dict(
        {
            "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
            "tables": {"users": {"keys": ["id"], "row_count": 5, "serve": {"duck": "users"}}},
            "cubes": {},
        }
    )
    pipeline = ServingPipeline(
        placement=placement,
        schemas={"users": ["id", "name", "region"]},
        public_keys=pub_keys,
        require_grant_typ=True,
        max_grant_lifetime_seconds=60.0,
    )

    started = threading.Event()
    cancelled = threading.Event()

    class BlockingAdapter:
        def execute(
            self,
            prepared: PreparedQuery,
            *,
            timeout_seconds: float,
            on_cancel_handle: Any = None,
        ) -> QueryResult:
            if on_cancel_handle is not None:
                on_cancel_handle(cancelled.set)
            started.set()
            if cancelled.wait(timeout=5.0):
                raise EngineCancelled("cancelled")
            return QueryResult(columns=("id",), rows=[(1,)], truncated=False, engine="duck", elapsed_ms=1.0)

    audit_path = tmp_path / "cancel_202.jsonl"
    sink = FileAuditSink(audit_path, fsync=False)
    app = create_app(pipeline, {"duck": BlockingAdapter()}, audit_sink=sink)
    client = TestClient(app)

    sql = "SELECT id FROM users"
    q_tok = _mint_grant(priv, sql, jti="jti-live-cancel")
    c_tok = _mint_cancel(priv, jti="jti-live-cancel")

    q_res: list[Response] = []

    def _run_q() -> None:
        q_res.append(
            client.post(
                "/v1/query",
                headers={"Authorization": f"Bearer {q_tok}"},
                json={"sql": sql},
            )
        )

    t = threading.Thread(target=_run_q)
    t.start()
    assert started.wait(timeout=3.0)

    c_resp = client.post(
        "/v1/cancel",
        headers={"Authorization": f"Bearer {c_tok}"},
    )
    assert c_resp.status_code == 202
    t.join(timeout=3.0)
    assert q_res[0].status_code == 499
    sink.close()

    recs = [json.loads(l) for l in audit_path.read_text(encoding="utf-8").splitlines()]
    by_route = {r["route"]: r for r in recs}
    assert by_route["/v1/cancel"]["status"] == 202
    assert by_route["/v1/cancel"]["code"] is None
    assert by_route["/v1/cancel"]["audit_id"] == "jti-live-cancel"
    assert by_route["/v1/query"]["status"] == 499
    assert by_route["/v1/query"]["code"] == "query_cancelled"


def test_build_server_and_cli_refuse_startup_on_unwritable_audit(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
):
    _, jwks_file, placement_file, schemas_file = _write_static_server_artifacts(tmp_path)
    unwritable_audit = tmp_path / "does_not_exist_dir" / "audit.jsonl"

    cfg_yaml = tmp_path / "serving_bad_audit.yaml"
    cfg_yaml.write_text(
        f"""
listen: {{host: 127.0.0.1, port: 8765}}
grant_keys: {{jwks_file: {jwks_file}}}
placement:
  static:
    placement_file: {placement_file}
    schemas_file: {schemas_file}
connections:
  duck: {{kind: duckdb, path: ":memory:"}}
audit:
  sink: file
  on_failure: refuse
  path: {unwritable_audit}
""",
        encoding="utf-8",
    )

    with pytest.raises((ServerConfigError, AuditWriteError)):
        build_server(cfg_yaml)

    # Both --check and startup refuse with exit code 2 and no traceback
    assert cli_main(["--config", str(cfg_yaml), "--check"]) == 2
    err_check = capsys.readouterr().err
    assert "Error:" in err_check
    assert "Traceback" not in err_check

    assert cli_main(["--config", str(cfg_yaml)]) == 2
    err_start = capsys.readouterr().err
    assert "Error:" in err_start
    assert "Traceback" not in err_start

    # Unreachable TCP syslog also fails build_server with ServerConfigError
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    closed_port = probe.getsockname()[1]
    probe.close()

    syslog_cfg = {
        "grant_keys": {"jwks_file": jwks_file},
        "placement": {
            "static": {
                "placement_file": placement_file,
                "schemas_file": schemas_file,
            }
        },
        "connections": {"duck": {"kind": "duckdb", "path": ":memory:"}},
        "audit": {
            "sink": "syslog",
            "on_failure": "refuse",
            "host": "127.0.0.1",
            "port": closed_port,
            "transport": "tcp",
            "timeout_seconds": 0.4,
        },
    }
    with pytest.raises(ServerConfigError, match="unreachable"):
        build_server(syslog_cfg)


def test_create_app_runtime_refuse_vs_log_with_real_pipeline(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
):
    import duckdb

    priv, jwks_file, _, _ = _write_static_server_artifacts(tmp_path)
    pub_keys = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_file},
            "placement": {"static": {"placement_file": "p", "schemas_file": "s"}},
            "connections": {"duck": {"kind": "duckdb", "path": ":memory:"}},
        },
        load_keys=False,
    ).load_grant_keys()

    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR, region VARCHAR)")
    con.execute("INSERT INTO users VALUES (1, 'CONFIDENTIAL_ROW_VALUE', 'APAC')")

    placement = Placement.from_dict(
        {
            "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
            "tables": {"users": {"keys": ["id"], "row_count": 1, "serve": {"duck": "users"}}},
            "cubes": {},
        }
    )
    pipeline = ServingPipeline(
        placement=placement,
        schemas={"users": ["id", "name", "region"]},
        public_keys=pub_keys,
        require_grant_typ=True,
        max_grant_lifetime_seconds=60.0,
    )
    adapter = DuckDBAdapter(lambda: con.cursor())
    sql = "SELECT id, name FROM users"

    # 1) on_failure="refuse": returns 503 audit_unavailable with typed fields and NO rows!
    app_refuse = create_app(
        pipeline,
        {"duck": adapter},
        audit_sink=_FailingSink(),
        audit_on_failure="refuse",
    )
    client_refuse = TestClient(app_refuse)
    tok1 = _mint_grant(priv, sql, jti="jti-rt-refuse-01")
    r_refuse = client_refuse.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok1}"},
        json={"sql": sql},
    )
    assert r_refuse.status_code == 503
    b_refuse = r_refuse.json()
    assert b_refuse["error"] == "audit_unavailable"
    assert b_refuse["code"] == "audit_unavailable"
    assert b_refuse["audit_id"] == "jti-rt-refuse-01"
    assert "next_step" in b_refuse
    assert "rows" not in b_refuse
    assert "CONFIDENTIAL_ROW_VALUE" not in r_refuse.text

    # 2) on_failure="log": logs ERROR, invokes failure counter callback, and returns 200 with rows!
    failures = 0

    def _on_fail() -> None:
        nonlocal failures
        failures += 1

    app_log = create_app(
        pipeline,
        {"duck": adapter},
        audit_sink=_FailingSink(),
        audit_on_failure="log",
        audit_failure_callback=_on_fail,
    )
    client_log = TestClient(app_log)
    tok2 = _mint_grant(priv, sql, jti="jti-rt-log-02")
    with caplog.at_level(logging.ERROR):
        caplog.clear()
        r_log = client_log.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok2}"},
            json={"sql": sql},
        )
    assert r_log.status_code == 200
    assert r_log.json()["rows"] == [[1, "CONFIDENTIAL_ROW_VALUE"]]
    assert failures == 1
    assert any(
        rec.levelno == logging.ERROR and "on_failure=log" in rec.getMessage()
        for rec in caplog.records
    )


# ---------------------------------------------------------------------------
# 8. C-H1, C-M2, C-M3 Regression & Hardening Tests
# ---------------------------------------------------------------------------


def test_c_h1_blank_tables_dropped_deduped_truncated_and_capped():
    # Blank/whitespace-only table names (e.g. SELECT * FROM " ") are dropped instead of raising.
    rec_blank = build_audit_record(
        route="/v1/query",
        status=400,
        code="invalid_sql",
        sql='SELECT * FROM " "',
        tables=["", " ", "   \t\n  "],
    )
    assert rec_blank.tables == []

    # Preserves first-seen order, drops blanks, truncates >256 chars, caps at 64 entries.
    long_name = "t_" + ("x" * 300)
    many_tables = [
        " ",
        "orders",
        "",
        "customers",
        "orders",
        long_name,
        long_name + "_extra_beyond_256",  # truncates to same 256-char prefix -> deduped
        *[f"tbl_{idx:03d}" for idx in range(80)],
    ]
    rec_many = build_audit_record(
        route="/v1/explain",
        status=200,
        tables=many_tables,
    )
    assert len(rec_many.tables) == 64
    assert rec_many.tables[0] == "orders"
    assert rec_many.tables[1] == "customers"
    assert rec_many.tables[2] == long_name[:256]
    assert len(rec_many.tables[2]) == 256
    assert rec_many.tables[3] == "tbl_000"


def test_c_h1_lone_surrogates_in_tables_sql_and_fields_never_raise(tmp_path: Path):
    surrogate_sql = "SELECT * FROM t WHERE note = 'bad-\ud800-\udfff'"
    expected_sha = hashlib.sha256(
        surrogate_sql.encode("utf-8", errors="replace")
    ).hexdigest()
    assert compute_sql_sha256(surrogate_sql) == expected_sha

    rec = build_audit_record(
        route="/v1/query",
        status=400,
        code="invalid_sql_\ud800",
        sql=surrogate_sql,
        audit_id="jti-\ud800-01",
        subject="user:\udfffalice",
        tables=[" ", "tbl_\ud800_one", "tbl_\udfff_one"],
    )
    # Both surrogate variants map to \ufffd and deduplicate cleanly.
    assert rec.tables == ["tbl_\ufffd_one"]
    assert rec.sql_sha256 == expected_sha

    json_line = rec.to_json_line()
    encoded = json_line.encode("utf-8")
    decoded = json.loads(encoded.decode("utf-8"))
    assert decoded["tables"] == ["tbl_\ufffd_one"]
    assert decoded["subject"] == "user:\ufffdalice"
    assert decoded["audit_id"] == "jti-\ufffd-01"
    assert decoded["code"] == "invalid_sql_\ufffd"

    # Even if an attribute is mutated after init with a lone surrogate, to_json_line stays UTF-8 safe.
    rec.origin = "ask-\ud83d"
    mutated_bytes = rec.to_json_line().encode("utf-8")
    assert json.loads(mutated_bytes.decode("utf-8"))["origin"] == "ask-\ufffd"

    # Writing to FileAuditSink succeeds without UnicodeEncodeError.
    sink_path = tmp_path / "surrogate_audit.jsonl"
    sink = FileAuditSink(sink_path, max_bytes=4096, fsync=False)
    sink.write_record(rec)
    sink.close()
    on_disk = json.loads(sink_path.read_text(encoding="utf-8").strip())
    assert on_disk["sql_sha256"] == expected_sha


def test_c_h1_build_minimal_audit_record_fallback():
    min_rec = build_minimal_audit_record(
        route="/v1/query",
        status=500,
        code="internal_error",
        elapsed_ms=float("nan"),
        audit_id="jti-\ud800-fallback",
        timestamp="not-a-valid-iso-timestamp",
    )
    assert min_rec.route == "/v1/query"
    assert min_rec.status == 500
    assert min_rec.code == "internal_error"
    assert min_rec.elapsed_ms == 0.0
    assert min_rec.audit_id == "jti-\ufffd-fallback"
    assert min_rec.tables == []
    parsed = json.loads(min_rec.to_json_line().encode("utf-8").decode("utf-8"))
    assert set(parsed.keys()) == ALLOWED_AUDIT_FIELD_SET


def test_c_m2_startup_warning_when_unbounded_file_sink(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
):
    unbounded_path = tmp_path / "unbounded.jsonl"
    with caplog.at_level(logging.WARNING, logger="seeknal.serving.audit"):
        caplog.clear()
        cfg = AuditConfig.from_dict({"sink": "file", "path": str(unbounded_path)})
        sink = build_audit_sink(cfg, fsync=False)
        sink.close()

    warnings = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    assert str(unbounded_path) in warnings[0]
    assert (
        "has no max_bytes rotation and no retention (max_files/max_age_seconds) configured; "
        "audit log may grow unbounded"
    ) in warnings[0]

    # Configuring max_bytes or retention suppresses the unbounded warning.
    for bounded_opts in (
        {"max_bytes": 10240},
        {"max_files": 7},
        {"max_age_seconds": 3600},
    ):
        with caplog.at_level(logging.WARNING, logger="seeknal.serving.audit"):
            caplog.clear()
            b_cfg = AuditConfig.from_dict(
                {
                    "sink": "file",
                    "path": str(tmp_path / "bounded.jsonl"),
                    **bounded_opts,
                }
            )
            b_sink = build_audit_sink(b_cfg, fsync=False)
            b_sink.close()
        assert not [r for r in caplog.records if r.levelno == logging.WARNING]


def test_c_m3_symlink_victim_keeps_0644_and_receives_no_records(tmp_path: Path):
    victim = tmp_path / "victim.txt"
    victim.write_text("original-victim-content\n", encoding="utf-8")
    os.chmod(victim, 0o644)

    symlink_audit = tmp_path / "audit_symlink.jsonl"
    symlink_audit.symlink_to(victim)

    cfg = AuditConfig.from_dict(
        {"sink": "file", "path": str(symlink_audit), "mode": "0600", "max_bytes": 4096}
    )

    with pytest.raises(ValueError, match="symlink"):
        verify_audit_config_offline(cfg)

    with pytest.raises(AuditWriteError, match="symlink"):
        build_audit_sink(cfg, fsync=False)

    sink = FileAuditSink(symlink_audit, mode=0o600, max_bytes=4096, fsync=False)
    with pytest.raises(AuditWriteError, match="symlink"):
        sink.write_record(_sample_record(audit_id="sym-attempt"))
    sink.close()

    assert stat.S_IMODE(os.lstat(victim).st_mode) == 0o644
    assert victim.read_text(encoding="utf-8") == "original-victim-content\n"

    # Dangling symlink must also be rejected without creating target.
    dangling_target = tmp_path / "should_never_exist.txt"
    dangling_link = tmp_path / "dangling_audit.jsonl"
    dangling_link.symlink_to(dangling_target)
    dangling_cfg = AuditConfig.from_dict(
        {"sink": "file", "path": str(dangling_link), "max_bytes": 4096}
    )
    with pytest.raises(ValueError, match="symlink"):
        verify_audit_config_offline(dangling_cfg)
    with pytest.raises(AuditWriteError, match="symlink"):
        build_audit_sink(dangling_cfg, fsync=False)
    assert not dangling_target.exists()


def test_c_m3_hardlink_victim_keeps_0644_and_receives_no_records(tmp_path: Path):
    victim = tmp_path / "hardlink_victim.txt"
    victim.write_text("do-not-clobber\n", encoding="utf-8")
    os.chmod(victim, 0o644)

    hardlinked_audit = tmp_path / "audit_hardlink.jsonl"
    os.link(victim, hardlinked_audit)
    assert os.lstat(hardlinked_audit).st_nlink == 2

    cfg = AuditConfig.from_dict(
        {"sink": "file", "path": str(hardlinked_audit), "mode": "0600", "max_bytes": 4096}
    )

    with pytest.raises(ValueError, match="hard links"):
        verify_audit_config_offline(cfg)

    with pytest.raises(AuditWriteError, match=r"hard links.*st_nlink=2"):
        build_audit_sink(cfg, fsync=False)

    sink = FileAuditSink(hardlinked_audit, mode=0o600, max_bytes=4096, fsync=False)
    with pytest.raises(AuditWriteError, match=r"hard links.*st_nlink=2"):
        sink.write_record(_sample_record(audit_id="hl-attempt"))
    sink.close()

    assert stat.S_IMODE(os.lstat(victim).st_mode) == 0o644
    assert victim.read_text(encoding="utf-8") == "do-not-clobber\n"


def test_c_m3_wrong_uid_rejected_and_never_calls_path_chmod(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    audit_file = tmp_path / "foreign_uid.jsonl"
    audit_file.write_text("foreign-owner-data\n", encoding="utf-8")
    os.chmod(audit_file, 0o644)
    real_uid = os.getuid()

    cfg = AuditConfig.from_dict(
        {"sink": "file", "path": str(audit_file), "mode": "0600", "max_bytes": 4096}
    )

    monkeypatch.setattr(os, "getuid", lambda: real_uid + 1000)
    with pytest.raises(ValueError, match="uid"):
        verify_audit_config_offline(cfg)

    with pytest.raises(AuditWriteError, match="uid"):
        build_audit_sink(cfg, fsync=False)

    sink = FileAuditSink(audit_file, mode=0o600, max_bytes=4096, fsync=False)
    with pytest.raises(AuditWriteError, match="uid"):
        sink.write_record(_sample_record(audit_id="uid-attempt"))
    sink.close()
    monkeypatch.setattr(os, "getuid", lambda: real_uid)

    # Mode and contents of foreign-owned file remain untouched.
    assert stat.S_IMODE(os.lstat(audit_file).st_mode) == 0o644
    assert audit_file.read_text(encoding="utf-8") == "foreign-owner-data\n"

    # Verify FileAuditSink open + write + rotation NEVER calls path-based os.chmod.
    def _forbid_path_chmod(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("path-based os.chmod must never be called by FileAuditSink")

    monkeypatch.setattr(os, "chmod", _forbid_path_chmod)
    clean_path = tmp_path / "fchmod_only.jsonl"
    rot_sink = FileAuditSink(clean_path, mode=0o640, max_bytes=300, fsync=False)
    rot_sink.verify_writable()
    for idx in range(4):
        rot_sink.write_record(_sample_record(audit_id=f"fchmod-{idx}"))
    rot_sink.close()

    assert stat.S_IMODE(os.lstat(clean_path).st_mode) == 0o640
    assert len(rot_sink.rotated_files()) >= 1
    for rf in rot_sink.rotated_files():
        assert stat.S_IMODE(os.lstat(rf).st_mode) == 0o640


def test_c_m3_post_open_fstat_race_and_rotated_symlink_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    # 1) Simulate a TOCTOU race where a second hard link is added right as os.open runs:
    # fd must be closed WITHOUT calling fchmod and without writing any record.
    race_victim = tmp_path / "race_victim.txt"
    race_audit = tmp_path / "race_audit.jsonl"
    real_open = os.open

    def _racing_open(path: Any, flags: int, mode: int = 0o777, **kwargs: Any) -> int:
        # Open with 0o644 initially so we can prove fchmod(fd, 0o600) is NOT called!
        fd = real_open(path, flags, 0o644, **kwargs)
        if str(path) == str(race_audit) and not race_victim.exists():
            os.link(race_audit, race_victim)
            os.chmod(race_victim, 0o644)
        return fd

    old_umask = os.umask(0o000)
    try:
        monkeypatch.setattr(os, "open", _racing_open)
        race_sink = FileAuditSink(race_audit, mode=0o600, max_bytes=4096, fsync=False)
        with pytest.raises(AuditWriteError, match=r"hard links.*st_nlink=2"):
            race_sink.write_record(_sample_record(audit_id="race-hl"))
        race_sink.close()
    finally:
        os.umask(old_umask)
        monkeypatch.setattr(os, "open", real_open)

    assert stat.S_IMODE(os.lstat(race_victim).st_mode) == 0o644
    assert race_victim.read_text(encoding="utf-8") == ""

    # 2) Pre-planted symlink at rotated destination path during _rotate_unlocked is refused.
    clock = FakeClock(1_763_542_800.0)
    rot_audit = tmp_path / "rot_audit.jsonl"
    rot_victim = tmp_path / "rot_victim.txt"
    rot_victim.write_text("keep-rot-victim\n", encoding="utf-8")
    os.chmod(rot_victim, 0o644)

    rot_sink = FileAuditSink(
        rot_audit,
        mode=0o600,
        rotate_interval_seconds=10.0,
        max_files=5,
        clock=clock,
        fsync=False,
    )
    rot_sink.write_record(_sample_record(audit_id="before-rot"))

    next_epoch = clock.advance(15.0)
    ts_tag = datetime.datetime.fromtimestamp(
        next_epoch, tz=datetime.timezone.utc
    ).strftime("%Y%m%dT%H%M%S%fZ")
    predicted_rot = tmp_path / f"{rot_audit.name}.{ts_tag}.000001"
    predicted_rot.symlink_to(rot_victim)

    with pytest.raises(AuditWriteError, match="symlink"):
        rot_sink.write_record(_sample_record(audit_id="blocked-by-rot-symlink"))
    rot_sink.close()

    assert stat.S_IMODE(os.lstat(rot_victim).st_mode) == 0o644
    assert rot_victim.read_text(encoding="utf-8") == "keep-rot-victim\n"


# ---------------------------------------------------------------------------
# 9. Step 2 HTTP Audit Hardening Regression Tests (C-B1, C-H1, C-M1, C-M4, C-L1, C-L3)
# ---------------------------------------------------------------------------


def _build_minimal_pipeline_and_keys(
    tmp_path: Path,
) -> tuple[ec.EllipticCurvePrivateKey, ServingPipeline]:
    priv, jwks_file, _, _ = _write_static_server_artifacts(tmp_path)
    pub_keys = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_file},
            "placement": {"static": {"placement_file": "p", "schemas_file": "s"}},
            "connections": {"duck": {"kind": "duckdb", "path": ":memory:"}},
        },
        load_keys=False,
    ).load_grant_keys()

    placement = Placement.from_dict(
        {
            "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
            "tables": {
                "users": {
                    "keys": ["id"],
                    "row_count": 5,
                    "serve": {"duck": "users"},
                }
            },
            "cubes": {},
        }
    )
    pipeline = ServingPipeline(
        placement=placement,
        schemas={"users": ["id", "name", "region"]},
        public_keys=pub_keys,
        require_grant_typ=True,
        max_grant_lifetime_seconds=60.0,
    )
    return priv, pipeline


async def test_c_b1_large_unauthenticated_sql_returns_fast_and_healthz_stays_responsive(
    tmp_path: Path,
):
    import duckdb

    _, pipeline = _build_minimal_pipeline_and_keys(tmp_path)
    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR, region VARCHAR)")

    audit_path = tmp_path / "c_b1_audit.jsonl"
    sink = FileAuditSink(audit_path, max_bytes=1024 * 1024, fsync=False)
    app = create_app(pipeline, {"duck": DuckDBAdapter(lambda: con.cursor())}, audit_sink=sink)

    huge_sql = "SELECT * FROM " + ",".join(f"t{i}" for i in range(55000))
    body_bytes = json.dumps({"sql": huge_sql, "bogus": 1}).encode("utf-8")
    assert len(body_bytes) > 350_000

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        t0 = time.perf_counter()
        q_resp, h_resp = await asyncio.gather(
            ac.post(
                "/v1/query",
                headers={
                    "Authorization": "Bearer fake.token.sig",
                    "Content-Type": "application/json",
                },
                content=body_bytes,
            ),
            ac.get("/healthz"),
        )
        elapsed = time.perf_counter() - t0

    sink.close()

    assert elapsed < 1.5
    assert h_resp.status_code == 200
    assert h_resp.json() == {"status": "ok"}
    assert q_resp.status_code == 400
    q_body = q_resp.json()
    assert q_body["code"] == "invalid_sql"

    lines = audit_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["route"] == "/v1/query"
    assert rec["status"] == 400
    assert rec["code"] == "invalid_sql"
    assert rec["tables"] == []


def test_c_h1_whitespace_table_name_and_lone_surrogate_return_typed_response_and_one_audit_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    import duckdb

    priv, pipeline = _build_minimal_pipeline_and_keys(tmp_path)
    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR, region VARCHAR)")

    audit_path = tmp_path / "c_h1_http_audit.jsonl"
    sink = FileAuditSink(audit_path, max_bytes=1024 * 1024, fsync=False)
    app = create_app(pipeline, {"duck": DuckDBAdapter(lambda: con.cursor())}, audit_sink=sink)
    client = TestClient(app)

    ws_sql = 'SELECT * FROM " "'
    tok_ws = _mint_grant(priv, ws_sql, jti="jti-ws-1")

    surrogate_sql = 'SELECT * FROM "a\ud800"'
    surrogate_sha = hashlib.sha256(
        surrogate_sql.encode("utf-8", errors="replace")
    ).hexdigest()
    now = time.time()
    tok_surr = sign_grant_token(
        {
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "typ": "grant",
            "sub": "user:analyst",
            "ws": "ws-prod",
            "jti": "jti-surr-1",
            "iat": now,
            "exp": now + 30.0,
            "sql_sha256": surrogate_sha,
            "max_rows": 50,
            "tables": {
                "users": {
                    "columns": ["id"],
                    "masks": {},
                    "row_filters": [],
                    "actions": ["select"],
                }
            },
        },
        priv,
        kid="k-audit-int",
    )

    probes: list[tuple[dict[str, str], bytes]] = [
        # Probe 1a: whitespace table name with fake token + unknown field
        (
            {"Authorization": "Bearer fake.token.sig", "Content-Type": "application/json"},
            json.dumps({"sql": ws_sql, "bogus": 1}).encode("utf-8"),
        ),
        # Probe 1b: whitespace table name with valid signed token + unknown field
        (
            {"Authorization": f"Bearer {tok_ws}", "Content-Type": "application/json"},
            json.dumps({"sql": ws_sql, "bogus": 1}).encode("utf-8"),
        ),
        # Probe 2a: raw JSON bytes with lone surrogate (fake token)
        (
            {"Authorization": "Bearer fake.token.sig", "Content-Type": "application/json"},
            b'{"sql": "SELECT * FROM \\"a\\ud800\\""}',
        ),
        # Probe 2b: raw JSON bytes with lone surrogate (valid signed token)
        (
            {"Authorization": f"Bearer {tok_surr}", "Content-Type": "application/json"},
            b'{"sql": "SELECT * FROM \\"a\\ud800\\""}',
        ),
        # Probe 2c: raw JSON bytes with lone surrogate + bogus field (fake token)
        (
            {"Authorization": "Bearer fake.token.sig", "Content-Type": "application/json"},
            b'{"sql": "SELECT * FROM \\"a\\ud800\\"", "bogus": 1}',
        ),
        # Probe 2d: raw JSON bytes with lone surrogate + bogus field (valid signed token)
        (
            {"Authorization": f"Bearer {tok_surr}", "Content-Type": "application/json"},
            b'{"sql": "SELECT * FROM \\"a\\ud800\\"", "bogus": 1}',
        ),
    ]

    expected_count = 0
    for headers, raw_payload in probes:
        resp = client.post("/v1/query", headers=headers, content=raw_payload)
        assert resp.status_code >= 400
        body = resp.json()
        for required_key in ("error", "code", "message", "next_step"):
            assert required_key in body and isinstance(body[required_key], str)
        expected_count += 1
        current_lines = audit_path.read_text(encoding="utf-8").splitlines()
        assert len(current_lines) == expected_count

    # Also verify _finish fallback retry via build_minimal_audit_record if build_audit_record fails.
    import seeknal.serving.http as http_mod

    def _exploding_build_audit_record(**_kwargs: Any) -> AuditRecord:
        raise ValueError("simulated full record build failure")

    monkeypatch.setattr(http_mod, "build_audit_record", _exploding_build_audit_record)
    resp_fallback = client.post(
        "/v1/query",
        headers={"Authorization": "Bearer fake.token.sig"},
        json={"sql": "SELECT 1", "bogus": 1},
    )
    assert resp_fallback.status_code == 400
    fb_body = resp_fallback.json()
    for required_key in ("error", "code", "message", "next_step"):
        assert required_key in fb_body

    sink.close()
    final_lines = audit_path.read_text(encoding="utf-8").splitlines()
    assert len(final_lines) == expected_count + 1
    last_rec = json.loads(final_lines[-1])
    assert last_rec["route"] == "/v1/query"
    assert last_rec["status"] == 400
    assert last_rec["code"] == "invalid_sql"
    assert last_rec["tables"] == []


async def test_c_m1_slow_or_blackholed_audit_sink_does_not_block_event_loop_or_healthz(
    tmp_path: Path,
):
    import duckdb

    priv, pipeline = _build_minimal_pipeline_and_keys(tmp_path)
    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR, region VARCHAR)")
    con.execute("INSERT INTO users VALUES (1, 'SECRET_ROW_99', 'EU')")

    entered = threading.Event()
    calls_lock = threading.Lock()
    call_count = 0

    class _SlowFailingAuditSink(AuditSink):
        def verify_writable(self) -> None:
            return None

        def write_record(self, record: AuditRecord | dict[str, Any]) -> None:
            nonlocal call_count
            with calls_lock:
                call_count += 1
                idx = call_count
            if idx == 1:
                entered.set()
                time.sleep(0.6)
            raise AuditWriteError("audit destination timed out")

    app = create_app(
        pipeline,
        {"duck": DuckDBAdapter(lambda: con.cursor())},
        audit_sink=_SlowFailingAuditSink(),
        audit_on_failure="refuse",
    )

    sql = "SELECT id, name FROM users"
    tok = _mint_grant(priv, sql, jti="jti-slow-audit-01")

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        q_start = time.perf_counter()
        q_task = asyncio.create_task(
            ac.post(
                "/v1/query",
                headers={"Authorization": f"Bearer {tok}"},
                json={"sql": sql},
            )
        )

        # Wait until the threadpool worker enters the 0.6s blocking write_record call.
        while not entered.is_set():
            await asyncio.sleep(0.005)

        h_start = time.perf_counter()
        h_resp = await ac.get("/healthz")
        h_elapsed = time.perf_counter() - h_start

        q_resp = await q_task
        q_elapsed = time.perf_counter() - q_start

    assert h_resp.status_code == 200
    assert h_elapsed < 0.25
    assert q_resp.status_code == 503
    q_body = q_resp.json()
    assert q_body["code"] == "audit_unavailable"
    assert "rows" not in q_body
    assert "SECRET_ROW_99" not in q_resp.text
    assert q_elapsed < 1.5


def test_c_m4_log_request_prevents_log_forging_from_unverified_token_claims(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
):
    import duckdb

    _, pipeline = _build_minimal_pipeline_and_keys(tmp_path)
    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR, region VARCHAR)")

    app = create_app(pipeline, {"duck": DuckDBAdapter(lambda: con.cursor())})
    client = TestClient(app)

    hdr_b64 = (
        base64.urlsafe_b64encode(json.dumps({"alg": "ES256", "kid": "forged"}).encode("utf-8"))
        .rstrip(b"=")
        .decode("ascii")
    )
    forged_payload = {
        "azp": "x\nrequest audit_id=FORGED subject=admin status=200",
        "origin": "%s%s%d",
        "workload": "ask\r\nFORGED",
        "act": {"sub": "sess\nFORGED", "typ": "ask_session\t%s"},
    }
    pay_b64 = (
        base64.urlsafe_b64encode(json.dumps(forged_payload).encode("utf-8"))
        .rstrip(b"=")
        .decode("ascii")
    )
    unverified_jwt = f"{hdr_b64}.{pay_b64}.fakesignature"

    with caplog.at_level(logging.INFO, logger="seeknal.serving.http"):
        caplog.clear()
        resp = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {unverified_jwt}"},
            json={"sql": "SELECT id FROM users"},
        )
        assert resp.status_code == 401
        assert "FORGED" not in caplog.text
        for rec in caplog.records:
            msg = rec.getMessage()
            assert "\n" not in msg
            assert "\r" not in msg

        # Direct _log_request call with control chars and %-format tokens in verified claims/binding:
        # must never raise TypeError and must escape control characters on a single line.
        caplog.clear()
        _log_request(
            audit_id="jti-1\ninjected",
            subject="user%s%d\r\tname",
            status=200,
            elapsed_ms=1.23,
            claims={
                "azp": "azp-%s%s%d\nline2",
                "origin": "ask\rline3",
                "workload": "ask\x00\x7f",
            },
        )
        req_msgs = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
        assert len(req_msgs) == 1
        assert "\n" not in req_msgs[0]
        assert "\r" not in req_msgs[0]
        assert "\t" not in req_msgs[0]
        assert "%s%s%d" in req_msgs[0]
        assert "\\n" in req_msgs[0]


def test_c_l1_cancel_does_not_cancel_query_when_audit_refused(tmp_path: Path):
    priv, pipeline = _build_minimal_pipeline_and_keys(tmp_path)

    started = threading.Event()
    allow_finish = threading.Event()
    cancelled = threading.Event()

    class _ControllableAdapter:
        def execute(
            self,
            prepared: PreparedQuery,
            *,
            timeout_seconds: float,
            on_cancel_handle: Any = None,
        ) -> QueryResult:
            if on_cancel_handle is not None:
                on_cancel_handle(cancelled.set)
            started.set()
            allow_finish.wait(timeout=5.0)
            if cancelled.is_set():
                raise EngineCancelled("cancelled")
            return QueryResult(
                columns=("id",),
                rows=[(42,)],
                truncated=False,
                engine="duck",
                elapsed_ms=2.0,
            )

    class _CancelRefusingAuditSink(AuditSink):
        def __init__(self) -> None:
            self.records: list[AuditRecord] = []
            self._lock = threading.Lock()

        def verify_writable(self) -> None:
            return None

        def write_record(self, record: AuditRecord | dict[str, Any]) -> None:
            rec_obj = (
                record
                if isinstance(record, AuditRecord)
                else AuditRecord.from_dict(record)
            )
            if rec_obj.route == "/v1/cancel":
                raise AuditWriteError("cancel audit log unavailable")
            with self._lock:
                self.records.append(rec_obj)

    sink = _CancelRefusingAuditSink()
    app = create_app(
        pipeline,
        {"duck": _ControllableAdapter()},
        audit_sink=sink,
        audit_on_failure="refuse",
    )
    client = TestClient(app)

    sql = "SELECT id FROM users"
    q_tok = _mint_grant(priv, sql, jti="jti-c-l1-hold")
    c_tok = _mint_cancel(priv, jti="jti-c-l1-hold")

    q_responses: list[Response] = []

    def _run_query() -> None:
        q_responses.append(
            client.post(
                "/v1/query",
                headers={"Authorization": f"Bearer {q_tok}"},
                json={"sql": sql},
            )
        )

    t = threading.Thread(target=_run_query)
    t.start()
    assert started.wait(timeout=3.0)

    c_resp = client.post(
        "/v1/cancel",
        headers={"Authorization": f"Bearer {c_tok}"},
    )
    assert c_resp.status_code == 503
    assert c_resp.json()["code"] == "audit_unavailable"
    # Query must NOT have been cancelled because the cancel audit write was refused!
    assert not cancelled.is_set()

    allow_finish.set()
    t.join(timeout=3.0)
    assert len(q_responses) == 1
    assert q_responses[0].status_code == 200
    assert q_responses[0].json()["rows"] == [[42]]


async def test_c_l3_method_not_allowed_and_client_disconnect_are_typed_and_audited(
    tmp_path: Path,
):
    import duckdb

    _, pipeline = _build_minimal_pipeline_and_keys(tmp_path)
    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR, region VARCHAR)")

    audit_path = tmp_path / "c_l3_audit.jsonl"
    sink = FileAuditSink(audit_path, max_bytes=1024 * 1024, fsync=False)
    app = create_app(pipeline, {"duck": DuckDBAdapter(lambda: con.cursor())}, audit_sink=sink)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        for method, path in (
            ("GET", "/v1/query"),
            ("DELETE", "/v1/explain"),
            ("PUT", "/v1/cancel"),
            ("POST", "/healthz"),
        ):
            r405 = await ac.request(method, path)
            assert r405.status_code == 405
            b405 = r405.json()
            assert b405["error"] == "method_not_allowed"
            assert b405["code"] == "invalid_sql"
            assert "HTTP method not allowed" in b405["message"]
            assert "next_step" in b405

    records_after_405 = [
        json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()
    ]
    # Only the 3 /v1/* endpoints are audited; /healthz is never audited.
    assert len(records_after_405) == 3
    assert [(r["route"], r["status"], r["code"]) for r in records_after_405] == [
        ("/v1/query", 405, "invalid_sql"),
        ("/v1/explain", 405, "invalid_sql"),
        ("/v1/cancel", 405, "invalid_sql"),
    ]

    # Simulate ClientDisconnect during body read on /v1/query and /v1/cancel via raw ASGI call.
    async def _send_disconnected_post(path: str, headers: list[tuple[bytes, bytes]]) -> tuple[int, bytes]:
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode("ascii"),
            "query_string": b"",
            "headers": headers,
            "client": ("127.0.0.1", 50000),
            "server": ("test", 80),
        }
        status_holder: list[int] = []
        body_chunks: list[bytes] = []

        async def _receive() -> dict[str, Any]:
            return {"type": "http.disconnect"}

        async def _send(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.start":
                status_holder.append(message["status"])
            elif message["type"] == "http.response.body":
                body_chunks.append(message.get("body", b""))

        await app(scope, _receive, _send)
        return status_holder[0], b"".join(body_chunks)

    st_q, body_q = await _send_disconnected_post(
        "/v1/query",
        [(b"authorization", b"Bearer fake.token.sig")],
    )
    st_c, body_c = await _send_disconnected_post(
        "/v1/cancel",
        [(b"authorization", b"Bearer fake.token.sig")],
    )

    sink.close()

    for st_code, raw_b in ((st_q, body_q), (st_c, body_c)):
        assert st_code == 499
        parsed_b = json.loads(raw_b.decode("utf-8"))
        assert parsed_b["error"] == "query_cancelled"
        assert parsed_b["code"] == "query_cancelled"
        assert "message" in parsed_b
        assert "next_step" in parsed_b

    all_records = [
        json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()
    ]
    assert len(all_records) == 5
    assert (all_records[3]["route"], all_records[3]["status"], all_records[3]["code"]) == (
        "/v1/query",
        499,
        "query_cancelled",
    )
    assert (all_records[4]["route"], all_records[4]["status"], all_records[4]["code"]) == (
        "/v1/cancel",
        499,
        "query_cancelled",
    )


def test_c_n1_oversized_udp_64x256_tables_emits_no_minimal_record_and_returns_503(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    import duckdb

    priv, jwks_file, _, _ = _write_static_server_artifacts(tmp_path)
    pub_keys = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_file},
            "placement": {"static": {"placement_file": "p", "schemas_file": "s"}},
            "connections": {"duck": {"kind": "duckdb", "path": ":memory:"}},
        },
        load_keys=False,
    ).load_grant_keys()

    # Build 64 tables each with a 256-character name
    table_names = [f"t{idx:02d}_" + ("x" * 252) for idx in range(64)]
    assert len(table_names) == 64
    assert all(len(t) == 256 for t in table_names)

    con = duckdb.connect()
    for idx, tname in enumerate(table_names):
        con.execute(f'CREATE TABLE "{tname}" (id INT, val VARCHAR)')
        if idx == 0:
            con.execute(f"INSERT INTO \"{tname}\" VALUES (1, 'CONFIDENTIAL_UDP_ROW')")

    placement = Placement.from_dict(
        {
            "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
            "tables": {
                tname: {"keys": ["id"], "row_count": 1, "serve": {"duck": tname}}
                for tname in table_names
            },
            "cubes": {},
        }
    )
    schemas = {tname: ["id", "val"] for tname in table_names}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys=pub_keys,
        require_grant_typ=True,
        max_grant_lifetime_seconds=60.0,
    )

    udp_collector = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_collector.bind(("127.0.0.1", 0))
    udp_collector.settimeout(0.2)
    udp_port = udp_collector.getsockname()[1]

    try:
        syslog_sink = SyslogAuditSink(
            host="127.0.0.1",
            port=udp_port,
            transport="udp",
            timeout_seconds=1.0,
        )
        syslog_sink.verify_writable()
        assert syslog_sink._sock is not None

        # Simulate OS UDP datagram MTU limit where oversized datagram raises EMSGSIZE
        # while a small minimal record (< 1024 bytes) WOULD have succeeded if retried!
        real_sock = syslog_sink._sock

        class _MaxDatagramSocketProxy:
            def __init__(self, inner: socket.socket) -> None:
                self._inner = inner

            def settimeout(self, val: float | None) -> None:
                self._inner.settimeout(val)

            def sendto(self, data: bytes, addr: Any) -> int:
                if len(data) > 9216:
                    raise OSError(errno.EMSGSIZE, "Message too long")
                return self._inner.sendto(data, addr)

            def close(self) -> None:
                self._inner.close()

        syslog_sink._sock = _MaxDatagramSocketProxy(real_sock)  # type: ignore[assignment]

        app = create_app(
            pipeline,
            {"duck": DuckDBAdapter(lambda: con.cursor())},
            audit_sink=syslog_sink,
            audit_on_failure="refuse",
        )
        client = TestClient(app)

        union_branches = [f'SELECT id, val FROM "{tname}"' for tname in table_names]
        big_sql = " UNION ALL ".join(union_branches)
        now = time.time()
        tok = sign_grant_token(
            {
                "iss": "atlas-catalog",
                "aud": "seeknal-serving",
                "typ": "grant",
                "sub": "user:analyst",
                "ws": "ws-prod",
                "jti": "jti-udp-emsgsize-01",
                "iat": now,
                "exp": now + 30.0,
                "sql_sha256": hashlib.sha256(big_sql.encode("utf-8")).hexdigest(),
                "max_rows": 50,
                "tables": {
                    tname: {
                        "columns": ["id", "val"],
                        "masks": {},
                        "row_filters": [],
                        "actions": ["select"],
                    }
                    for tname in table_names
                },
            },
            priv,
            kid="k-audit-int",
        )

        with caplog.at_level(logging.ERROR):
            caplog.clear()
            resp = client.post(
                "/v1/query",
                headers={"Authorization": f"Bearer {tok}"},
                json={"sql": big_sql},
            )

        assert resp.status_code == 503
        body = resp.json()
        assert body["code"] == "audit_unavailable"
        assert body["audit_id"] == "jti-udp-emsgsize-01"
        assert "rows" not in body
        assert "CONFIDENTIAL_UDP_ROW" not in resp.text

        # Confirm NO fallback minimal UDP datagram was sent!
        with pytest.raises(socket.timeout):
            udp_collector.recvfrom(65535)

        assert any(
            rec.levelno == logging.ERROR
            and "code=audit_unavailable" in rec.getMessage()
            and "Message too long" in rec.getMessage()
            and "detail=" in rec.getMessage()
            and "audit_id=jti-udp-emsgsize-01" in rec.getMessage()
            for rec in caplog.records
        )
        syslog_sink.close()
    finally:
        udp_collector.close()
        con.close()


def test_c_n1_f_l2_log_mode_and_build_fallback_log_detail_and_audit_id(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    import duckdb
    import seeknal.serving.http as http_mod

    priv, pipeline = _build_minimal_pipeline_and_keys(tmp_path)
    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR, region VARCHAR)")
    con.execute("INSERT INTO users VALUES (1, 'Alice', 'US')")

    try:
        # 1) Direct audit.py log mode includes detail= (scrubbed) and audit_id=
        class _LeakyFailSink(AuditSink):
            def verify_writable(self) -> None:
                raise AuditWriteError(
                    "disk error password='secret_startup_pw_1234'",
                    audit_id="aud-init-1",
                )

            def write_record(self, record: AuditRecord | dict[str, Any]) -> None:
                raise OSError(
                    errno.EIO,
                    "io error password='secret_runtime_pw_5678'",
                )

        mgr = AuditManager(_LeakyFailSink(), on_failure="log")
        with caplog.at_level(logging.ERROR, logger="seeknal.serving.audit"):
            caplog.clear()
            mgr.verify_writable()
            ok = mgr.emit(_sample_record(audit_id="aud-log-direct-1"))
            assert ok is False

        audit_msgs = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert len(audit_msgs) == 2
        assert "detail=" in audit_msgs[0] and "audit_id=aud-init-1" in audit_msgs[0]
        assert "secret_startup_pw_1234" not in audit_msgs[0]
        assert "detail=" in audit_msgs[1] and "audit_id=aud-log-direct-1" in audit_msgs[1]
        assert "secret_runtime_pw_5678" not in audit_msgs[1]

        # 2) HTTP log mode (structured & callable) logs detail= (scrubbed) and audit_id=
        sql = "SELECT id, name FROM users"
        tok1 = _mint_grant(priv, sql, jti="jti-http-log-struct")
        app_log_struct = create_app(
            pipeline,
            {"duck": DuckDBAdapter(lambda: con.cursor())},
            audit_sink=_LeakyFailSink(),
            audit_on_failure="log",
        )
        with caplog.at_level(logging.ERROR):
            caplog.clear()
            r1 = TestClient(app_log_struct).post(
                "/v1/query",
                headers={"Authorization": f"Bearer {tok1}"},
                json={"sql": sql},
            )
        assert r1.status_code == 200
        assert any(
            rec.levelno == logging.ERROR
            and "on_failure=log" in rec.getMessage()
            and "detail=" in rec.getMessage()
            and "audit_id=jti-http-log-struct" in rec.getMessage()
            and "secret_runtime_pw_5678" not in rec.getMessage()
            for rec in caplog.records
        )

        # 3) Build/serialize error fallback logs ERROR with detail= and audit_id=
        audit_file = tmp_path / "build_fallback.jsonl"
        file_sink = FileAuditSink(audit_file, max_bytes=4096, fsync=False)
        app_fb = create_app(
            pipeline,
            {"duck": DuckDBAdapter(lambda: con.cursor())},
            audit_sink=file_sink,
            audit_on_failure="refuse",
        )

        def _bad_build(**_kw: Any) -> AuditRecord:
            raise ValueError("bad record field password='build_secret_pw_9999'")

        monkeypatch.setattr(http_mod, "build_audit_record", _bad_build)
        tok2 = _mint_grant(priv, sql, jti="jti-build-fb-01")
        with caplog.at_level(logging.ERROR):
            caplog.clear()
            r_fb = TestClient(app_fb).post(
                "/v1/query",
                headers={"Authorization": f"Bearer {tok2}"},
                json={"sql": sql},
            )
        file_sink.close()
        assert r_fb.status_code == 200
        assert any(
            rec.levelno == logging.ERROR
            and "falling back to minimal record" in rec.getMessage()
            and "detail=ValueError:" in rec.getMessage()
            and "audit_id=jti-build-fb-01" in rec.getMessage()
            and "build_secret_pw_9999" not in rec.getMessage()
            for rec in caplog.records
        )
    finally:
        con.close()


def test_c_m3_cached_fd_rechecked_on_unlink_and_replace(tmp_path: Path):
    import duckdb

    priv, pipeline = _build_minimal_pipeline_and_keys(tmp_path)
    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR, region VARCHAR)")
    con.execute("INSERT INTO users VALUES (1, 'ROW_SECRET_M3', 'US')")

    try:
        audit_dir = tmp_path / "m3_dir"
        audit_dir.mkdir()
        audit_path = audit_dir / "audit.jsonl"

        sink = FileAuditSink(audit_path, mode=0o600, max_bytes=1024 * 1024, fsync=False)
        app = create_app(
            pipeline,
            {"duck": DuckDBAdapter(lambda: con.cursor())},
            audit_sink=sink,
            audit_on_failure="refuse",
        )
        client = TestClient(app)
        sql = "SELECT id, name FROM users"

        # 1) Initial write opens and caches self._fd
        tok1 = _mint_grant(priv, sql, jti="jti-m3-init")
        r1 = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok1}"},
            json={"sql": sql},
        )
        assert r1.status_code == 200
        assert sink._fd is not None
        first_ino = os.lstat(audit_path).st_ino

        # 2) Unlink case (writable parent): next write detects missing path, closes orphan fd,
        #    recreates self._path with 0600, and writes the record there (not to unlinked inode).
        os.unlink(audit_path)
        assert not audit_path.exists()

        tok2 = _mint_grant(priv, sql, jti="jti-m3-after-unlink")
        r2 = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok2}"},
            json={"sql": sql},
        )
        assert r2.status_code == 200
        assert audit_path.exists()
        assert stat.S_IMODE(os.lstat(audit_path).st_mode) == 0o600
        lines_after_unlink = [
            json.loads(l) for l in audit_path.read_text(encoding="utf-8").splitlines()
        ]
        assert [r["audit_id"] for r in lines_after_unlink] == ["jti-m3-after-unlink"]

        # 3) Replace with new regular file (e.g. external logrotate mv + create):
        #    next write detects st_ino mismatch, closes old fd, and appends to new file.
        recreated_ino = os.lstat(audit_path).st_ino
        archived_path = audit_dir / "archived_external.jsonl"
        os.replace(audit_path, archived_path)
        replacement_tmp = audit_dir / "new_audit.tmp"
        replacement_tmp.write_text("", encoding="utf-8")
        os.chmod(replacement_tmp, 0o644)
        os.replace(replacement_tmp, audit_path)
        replaced_ino = os.lstat(audit_path).st_ino
        assert replaced_ino != recreated_ino

        tok3 = _mint_grant(priv, sql, jti="jti-m3-after-replace")
        r3 = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok3}"},
            json={"sql": sql},
        )
        assert r3.status_code == 200
        assert stat.S_IMODE(os.lstat(audit_path).st_mode) == 0o600
        assert [
            json.loads(l)["audit_id"]
            for l in audit_path.read_text(encoding="utf-8").splitlines()
        ] == ["jti-m3-after-replace"]
        # Old archived file did NOT receive the post-replace record!
        assert [
            json.loads(l)["audit_id"]
            for l in archived_path.read_text(encoding="utf-8").splitlines()
        ] == ["jti-m3-after-unlink"]

        # 4) Replace active audit file with a symlink while fd is cached -> refuses with 503 and NO rows!
        victim = tmp_path / "m3_symlink_victim.txt"
        victim.write_text("untouched\n", encoding="utf-8")
        os.chmod(victim, 0o644)
        sym_tmp = audit_dir / "sym.tmp"
        sym_tmp.symlink_to(victim)
        os.replace(sym_tmp, audit_path)

        tok4 = _mint_grant(priv, sql, jti="jti-m3-replace-sym")
        r4 = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok4}"},
            json={"sql": sql},
        )
        assert r4.status_code == 503
        assert r4.json()["code"] == "audit_unavailable"
        assert "ROW_SECRET_M3" not in r4.text
        assert victim.read_text(encoding="utf-8") == "untouched\n"
        assert stat.S_IMODE(os.lstat(victim).st_mode) == 0o644

        # Restore clean file, cache fd again, then add hardlink while fd is open -> refuses 503!
        os.unlink(audit_path)
        tok5 = _mint_grant(priv, sql, jti="jti-m3-clean-again")
        assert (
            client.post(
                "/v1/query",
                headers={"Authorization": f"Bearer {tok5}"},
                json={"sql": sql},
            ).status_code
            == 200
        )
        hl_path = audit_dir / "hardlink_after_open.jsonl"
        os.link(audit_path, hl_path)
        tok6 = _mint_grant(priv, sql, jti="jti-m3-hardlink-open")
        r6 = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok6}"},
            json={"sql": sql},
        )
        assert r6.status_code == 503
        assert r6.json()["code"] == "audit_unavailable"
        assert "ROW_SECRET_M3" not in r6.text
        os.unlink(hl_path)

        # 5) Unlink + unwritable directory -> reopening fails -> refuses 503 with NO rows!
        if os.geteuid() != 0:
            tok7 = _mint_grant(priv, sql, jti="jti-m3-pre-ro")
            assert (
                client.post(
                    "/v1/query",
                    headers={"Authorization": f"Bearer {tok7}"},
                    json={"sql": sql},
                ).status_code
                == 200
            )
            os.unlink(audit_path)
            os.chmod(audit_dir, 0o500)
            try:
                tok8 = _mint_grant(priv, sql, jti="jti-m3-unwritable-reopen")
                r8 = client.post(
                    "/v1/query",
                    headers={"Authorization": f"Bearer {tok8}"},
                    json={"sql": sql},
                )
                assert r8.status_code == 503
                assert r8.json()["code"] == "audit_unavailable"
                assert "ROW_SECRET_M3" not in r8.text
            finally:
                os.chmod(audit_dir, 0o700)

        sink.close()
    finally:
        con.close()


def test_c_n4_fsync_eio_single_record_and_partial_write_reopen_trailing_newline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    import duckdb

    priv, pipeline = _build_minimal_pipeline_and_keys(tmp_path)
    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR, region VARCHAR)")
    con.execute("INSERT INTO users VALUES (1, 'CONFIDENTIAL_N4_ROW', 'APAC')")

    try:
        # 1) fsync EIO in refuse mode: bytes were written before fsync failed, so ONLY 1 record
        #    line is in the file (no minimal duplicate appended!), fd is closed, and HTTP returns 503.
        fsync_audit_path = tmp_path / "fsync_eio.jsonl"
        fsync_sink = FileAuditSink(fsync_audit_path, max_bytes=1024 * 1024, fsync=True)
        app_fsync = create_app(
            pipeline,
            {"duck": DuckDBAdapter(lambda: con.cursor())},
            audit_sink=fsync_sink,
            audit_on_failure="refuse",
        )
        client_fsync = TestClient(app_fsync)
        sql = "SELECT id, name FROM users"

        real_fsync = os.fsync
        fail_fsync = True

        def _flaky_fsync(fd: int) -> None:
            if fail_fsync:
                raise OSError(errno.EIO, "Input/output error")
            real_fsync(fd)

        monkeypatch.setattr(os, "fsync", _flaky_fsync)
        tok_eio = _mint_grant(priv, sql, jti="jti-n4-fsync-eio")
        r_eio = client_fsync.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_eio}"},
            json={"sql": sql},
        )
        assert r_eio.status_code == 503
        assert r_eio.json()["code"] == "audit_unavailable"
        assert "rows" not in r_eio.json()
        assert "CONFIDENTIAL_N4_ROW" not in r_eio.text
        assert fsync_sink._fd is None

        lines_after_eio = fsync_audit_path.read_text(encoding="utf-8").splitlines()
        assert len(lines_after_eio) == 1
        assert json.loads(lines_after_eio[0])["audit_id"] == "jti-n4-fsync-eio"

        # Once fsync recovers, the next write reopens cleanly without inserting a blank line.
        fail_fsync = False
        tok_after_eio = _mint_grant(priv, sql, jti="jti-n4-after-eio")
        r_ok = client_fsync.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_after_eio}"},
            json={"sql": sql},
        )
        assert r_ok.status_code == 200
        fsync_sink.close()
        monkeypatch.setattr(os, "fsync", real_fsync)

        lines_recovered = fsync_audit_path.read_text(encoding="utf-8").splitlines()
        assert len(lines_recovered) == 2
        assert [json.loads(l)["audit_id"] for l in lines_recovered] == [
            "jti-n4-fsync-eio",
            "jti-n4-after-eio",
        ]

        # 2) Partial write without trailing '\n': first write writes partial bytes then fails;
        #    fd is closed, 503 is returned, and next reopen writes b'\n' first so the next record
        #    starts cleanly on a new line!
        partial_path = tmp_path / "partial_write.jsonl"
        partial_sink = FileAuditSink(partial_path, max_bytes=1024 * 1024, fsync=True)
        app_partial = create_app(
            pipeline,
            {"duck": DuckDBAdapter(lambda: con.cursor())},
            audit_sink=partial_sink,
            audit_on_failure="refuse",
        )
        client_partial = TestClient(app_partial)

        real_write = os.write
        inject_partial = True
        partial_step = 0

        def _partial_os_write(fd: int, data: Any) -> int:
            nonlocal partial_step
            raw_bytes = bytes(data)
            if inject_partial and len(raw_bytes) > 10:
                partial_step += 1
                if partial_step == 1:
                    # Write only first 32 bytes (no trailing '\n')
                    return real_write(fd, raw_bytes[:32])
                raise OSError(errno.ENOSPC, "No space left on device")
            return real_write(fd, raw_bytes)

        monkeypatch.setattr(os, "write", _partial_os_write)
        tok_part = _mint_grant(priv, sql, jti="jti-n4-partial-1")
        r_part = client_partial.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_part}"},
            json={"sql": sql},
        )
        assert r_part.status_code == 503
        assert r_part.json()["code"] == "audit_unavailable"
        assert "CONFIDENTIAL_N4_ROW" not in r_part.text
        assert partial_sink._fd is None
        assert not partial_path.read_bytes().endswith(b"\n")

        # Now restore os.write: next request reopens, heals missing '\n', and writes on line 2!
        inject_partial = False
        monkeypatch.setattr(os, "write", real_write)

        tok_healed = _mint_grant(priv, sql, jti="jti-n4-healed-2")
        r_healed = client_partial.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_healed}"},
            json={"sql": sql},
        )
        assert r_healed.status_code == 200
        partial_sink.close()

        raw_lines = partial_path.read_text(encoding="utf-8").splitlines()
        assert len(raw_lines) == 2
        # Line 0 is the partial fragment; Line 1 is a complete valid JSON audit record!
        healed_rec = json.loads(raw_lines[1])
        assert healed_rec["audit_id"] == "jti-n4-healed-2"
        assert healed_rec["status"] == 200
    finally:
        con.close()


async def test_c_n5_blackholed_audit_sink_isolates_threadpool_fails_fast_and_trips_readyz(
    tmp_path: Path,
):
    import duckdb
    from starlette.concurrency import run_in_threadpool
    from seeknal.serving.ops import OpsConfig, ReadinessProbe, build_public_extra_routes, create_ops_app

    priv, pipeline = _build_minimal_pipeline_and_keys(tmp_path)
    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR, region VARCHAR)")
    con.execute("INSERT INTO users VALUES (1, 'CONFIDENTIAL_N5_ROW', 'US')")

    first_entered = threading.Event()
    unblock_sink = threading.Event()
    observed_threads: list[str] = []
    obs_lock = threading.Lock()

    class _BlackHoledAuditSink(AuditSink):
        timeout_seconds = 0.15

        def verify_writable(self) -> None:
            return None

        def write_record(self, record: AuditRecord | dict[str, Any]) -> None:
            with obs_lock:
                observed_threads.append(threading.current_thread().name)
            first_entered.set()
            unblock_sink.wait(timeout=5.0)
            raise AuditWriteError("black-holed sink unblocked")

    sink = _BlackHoledAuditSink()
    duck_adapter = DuckDBAdapter(lambda: con.cursor())
    readiness = ReadinessProbe(
        {"duck": duck_adapter},
        audit_sink=sink,
        audit_on_failure="refuse",
        timeout_seconds=0.1,
        cache_seconds=5.0,
    )
    ops_app = create_ops_app(
        ops_config=OpsConfig(enabled=True),
        readiness=readiness,
    )
    app = create_app(
        pipeline,
        {"duck": duck_adapter},
        extra_routes=build_public_extra_routes(
            OpsConfig(public_readyz=True),
            readiness=readiness,
        ),
        audit_sink=sink,
        audit_on_failure="refuse",
        audit_timeout_seconds=0.15,
        audit_max_concurrency=2,
    )

    sql = "SELECT id, name FROM users"
    pub_transport = httpx.ASGITransport(app=app)
    ops_transport = httpx.ASGITransport(app=ops_app)

    try:
        async with (
            httpx.AsyncClient(transport=pub_transport, base_url="http://pub") as pub_ac,
            httpx.AsyncClient(transport=ops_transport, base_url="http://ops") as ops_ac,
        ):
            # Initially ready before sink stalls
            r_ready_0 = await ops_ac.get("/readyz")
            assert r_ready_0.status_code == 200
            assert r_ready_0.json() == {"ready": True, "engines": {"duck": "ok"}}

            # Fire 6 concurrent requests (3x audit_max_concurrency=2) into the black-holed sink
            burst_tasks = [
                asyncio.create_task(
                    pub_ac.post(
                        "/v1/query",
                        headers={
                            "Authorization": f"Bearer {_mint_grant(priv, sql, jti=f'jti-n5-burst-{i}')}"
                        },
                        json={"sql": sql},
                    )
                )
                for i in range(6)
            ]

            while not first_entered.is_set():
                await asyncio.sleep(0.005)

            # 1) While the audit worker pool is saturated & wedged, run_in_threadpool(pipeline.prepare, ...)
            #    for other requests still makes fast progress on the unstarved shared threadpool!
            prep_tok = _mint_grant(priv, sql, jti="jti-n5-prep-progress")
            prep_t0 = time.perf_counter()
            prepared_results = await asyncio.gather(
                *[
                    run_in_threadpool(pipeline.prepare, sql, prep_tok)
                    for _ in range(8)
                ]
            )
            prep_elapsed = time.perf_counter() - prep_t0
            assert len(prepared_results) == 8
            assert prep_elapsed < 0.25

            # 2) Wait for initial burst to hit the 0.15s audit deadline
            burst_resps = await asyncio.gather(*burst_tasks)
            for br in burst_resps:
                assert br.status_code == 503
                assert br.json()["code"] == "audit_unavailable"
                assert "CONFIDENTIAL_N5_ROW" not in br.text

            # Confirm only dedicated audit worker threads were used (capped at max_concurrency=2)
            with obs_lock:
                assert 1 <= len(observed_threads) <= 2
                assert all(
                    name.startswith("seeknal-audit-io-") for name in observed_threads
                )

            # 3) Subsequent audit writes fail fast immediately while sink is stalled!
            ff_tok = _mint_grant(priv, sql, jti="jti-n5-failfast")
            ff_t0 = time.perf_counter()
            ff_resp = await pub_ac.post(
                "/v1/query",
                headers={"Authorization": f"Bearer {ff_tok}"},
                json={"sql": sql},
            )
            ff_elapsed = time.perf_counter() - ff_t0
            assert ff_resp.status_code == 503
            assert ff_resp.json()["code"] == "audit_unavailable"
            assert "CONFIDENTIAL_N5_ROW" not in ff_resp.text
            assert ff_elapsed < 0.05

            # 4) Ops /readyz and public /readyz report 503 with reason="audit_sink"!
            ops_ready_503 = await ops_ac.get("/readyz")
            assert ops_ready_503.status_code == 503
            assert ops_ready_503.json() == {
                "ready": False,
                "reason": "audit_sink",
                "engines": {"duck": "ok"},
            }

            pub_ready_503 = await pub_ac.get("/readyz")
            assert pub_ready_503.status_code == 503
            assert pub_ready_503.json() == {
                "ready": False,
                "reason": "audit_sink",
            }
    finally:
        unblock_sink.set()
        con.close()


# ---------------------------------------------------------------------------
# 10. Multi-source Postgres / Lakebase / Select API §8 Audit Fields Tests
# ---------------------------------------------------------------------------


def test_run3_new_audit_fields_defaults_roundtrip_and_sanitization():
    new_fields = (
        "api",
        "engine_kind",
        "storage_class",
        "account",
        "lakebase_endpoint",
        "cold_start",
        "access_pattern",
        "guard",
        "filter_columns",
        "effective_max_rows",
        "paged",
    )
    for nf in new_fields:
        assert nf in ALLOWED_AUDIT_FIELDS
        assert nf in ALLOWED_AUDIT_FIELD_SET

    # 1. Default values are always present in to_dict() and JSON as None
    minimal = AuditRecord(route="/v1/select", status=200)
    min_dict = minimal.to_dict()
    assert set(min_dict.keys()) == ALLOWED_AUDIT_FIELD_SET
    for nf in new_fields:
        assert nf in min_dict
        assert min_dict[nf] is None

    # 2. Full population via build_audit_record and round-trip via AuditRecord.from_dict
    rec = build_audit_record(
        timestamp="2026-11-19T12:00:00Z",
        route="/v1/select",
        status=200,
        elapsed_ms=8.5,
        engine="lb_ops",
        engine_kind="lakebase",
        storage_class="object_storage",
        account="interactive",
        lakebase_endpoint={
            "project_id": "prj_0123456789abcdefghij",
            "branch_id": "br_0123456789abcdefghij",
            "endpoint_id": "ep_0123456789abcdefghij",
        },
        cold_start={"woke": True, "wait_ms": 2450.5},
        rule="filter",
        access_pattern="filter",
        guard="enforce",
        filter_columns=["customer_id", "as_of_date", "customer_id"],
        effective_max_rows=50,
        paged=True,
        tables=["ops.ops.ticket_events"],
        row_count=10,
        truncated=False,
        claims={"jti": "jti-select-1", "sub": "user:analyst", "api": "select.v1"},
        sql_sha256="a" * 64,
    )
    d = rec.to_dict()
    assert d["route"] == "/v1/select"
    assert d["api"] == "select.v1"
    assert d["engine"] == "lb_ops"
    assert d["engine_kind"] == "lakebase"
    assert d["storage_class"] == "object_storage"
    assert d["account"] == "interactive"
    assert d["lakebase_endpoint"] == {
        "project_id": "prj_0123456789abcdefghij",
        "branch_id": "br_0123456789abcdefghij",
        "endpoint_id": "ep_0123456789abcdefghij",
    }
    assert d["cold_start"] == {"woke": True, "wait_ms": 2450.5}
    assert d["access_pattern"] == "filter"
    assert d["guard"] == "enforce"
    assert d["filter_columns"] == ["customer_id", "as_of_date"]
    assert d["effective_max_rows"] == 50
    assert d["paged"] is True

    roundtripped = AuditRecord.from_dict(json.loads(rec.to_json_line()), strict=True)
    assert roundtripped.to_dict() == d

    # cold_start also accepts bool or non-negative number; lakebase_endpoint accepts string
    rec_bool_cs = AuditRecord(
        route="/v1/query",
        status=200,
        lakebase_endpoint="ep_0123456789abcdefghij",
        cold_start=False,
        effective_max_rows=0,
        paged=False,
    )
    assert rec_bool_cs.lakebase_endpoint == "ep_0123456789abcdefghij"
    assert rec_bool_cs.cold_start is False
    assert rec_bool_cs.effective_max_rows == 0
    assert rec_bool_cs.paged is False

    rec_num_cs = AuditRecord(route="/v1/query", status=200, cold_start=3.125)
    assert rec_num_cs.cold_start == 3.125

    # 3. Secret scrubbing on string fields, lakebase_endpoint, cold_start, filter_columns
    priv_key = ec.generate_private_key(ec.SECP256R1())
    jwt_tok = sign_grant_token({"jti": "x", "sub": "u"}, priv_key, kid="k1")
    dsn_str = "postgresql://svc:PLANTED_AUDIT_DSN_PW@pg.internal:5432/cbn"

    scrubbed_rec = AuditRecord(
        route="/v1/select",
        status=422,
        code="filter_required",
        api=f"Bearer {jwt_tok}",
        engine_kind=dsn_str,
        storage_class=dsn_str,
        account=f"Bearer {jwt_tok}",
        access_pattern=dsn_str,
        guard=f"Bearer {jwt_tok}",
        lakebase_endpoint={"endpoint_id": dsn_str},
        cold_start={"note": f"Bearer {jwt_tok}"},
        filter_columns=["customer_id", f"Bearer {jwt_tok}"],
    )
    serialized = scrubbed_rec.to_json_line()
    for secret in (jwt_tok, dsn_str, "PLANTED_AUDIT_DSN_PW"):
        assert secret not in serialized

    # 4. Type validation & filter_columns NEVER accepts mappings or filter values
    with pytest.raises(ValueError, match="filter_columns"):
        AuditRecord(
            route="/v1/select",
            status=200,
            filter_columns={"customer_id": "C-SECRET-1001"},  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="filter_columns"):
        AuditRecord(
            route="/v1/select",
            status=200,
            filter_columns=["customer_id='C-SECRET-1001'"],
        )
    with pytest.raises(ValueError, match="filter_columns"):
        AuditRecord(
            route="/v1/select",
            status=200,
            filter_columns="customer_id",  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="effective_max_rows"):
        AuditRecord(route="/v1/select", status=200, effective_max_rows=True)
    with pytest.raises(ValueError, match="effective_max_rows"):
        AuditRecord(route="/v1/select", status=200, effective_max_rows=-1)
    with pytest.raises(ValueError, match="paged"):
        AuditRecord(route="/v1/select", status=200, paged="yes")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="lakebase_endpoint"):
        AuditRecord(route="/v1/select", status=200, lakebase_endpoint={"dsn": dsn_str})
    with pytest.raises(ValueError, match="cold_start"):
        AuditRecord(route="/v1/select", status=200, cold_start={"token": "secret"})









