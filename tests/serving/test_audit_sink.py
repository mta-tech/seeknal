"""Tests for Seeknal Serving durable audit records, sinks, rotation/retention, and failure semantics."""

from __future__ import annotations

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
    emit_audit_record,
    serialize_audit_record,
)
from seeknal.serving.grant_token import sign_cancel_token, sign_grant_token
from seeknal.serving.http import create_app
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
    for valid_route in ("/v1/query", "/v1/explain", "/v1/cancel"):
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

