"""Tests for Cube adapter, token signing, watchdog deadline, cancellation, and security context."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import threading
import time
from typing import Any

import pytest

from seeknal.serving.adapters import (
    CubeAdapter,
    CubeSecurityConfig,
    DBAPIAdapter,
    EngineCancelled,
    EngineError,
    EngineTimeout,
    build_adapter,
    make_cube_context_token,
)
from seeknal.serving.service import PreparedQuery


_DEFAULT = object()

def _sample_prepared_query(
    *,
    cube_security_context: Any = _DEFAULT,
    sql: str = "SELECT province, total_amount FROM claims_cube",
) -> PreparedQuery:
    if cube_security_context is _DEFAULT:
        cube_security_context = {
            "subject": "analyst@atlas.org",
            "masks": {"claims_cube": {"patient_nik": "hash"}},
            "row_filters": {"claims_cube": [{"column": "province", "op": "eq", "values": ["JB"]}]},
        }

    return PreparedQuery(
        engine="cube",
        engine_kind="cube",
        sql=sql,
        params=(),
        paramstyle="format",
        rule="semantic",
        reason="Semantic cube mapping",
        row_limit=100,
        masked_columns=("claims_cube.patient_nik",),
        redacted_sql=sql,
        subject="analyst@atlas.org",
        workspace="ws-cube",
        audit_id="audit-cube-test-01",
        cube_security_context=cube_security_context,
        workload="interactive",
    )


def test_make_cube_context_token_valid_hs256_jwt():
    cfg = CubeSecurityConfig(
        secret=b"my-super-secret-key-32-bytes-long!",
        issuer="seeknal-serving",
        audience="cube-sql",
        ttl_seconds=45,
        sql_user="seeknal",
    )
    prep = _sample_prepared_query()
    fixed_now = 1700000000.0

    token = make_cube_context_token(prep, cfg, now=fixed_now)

    parts = token.split(".")
    assert len(parts) == 3, f"Expected 3 parts in JWT, got {len(parts)}"
    h_b64, p_b64, sig_b64 = parts

    # Independent HMAC verification with stdlib only
    signing_input = f"{h_b64}.{p_b64}".encode("ascii")
    expected_sig = hmac.new(cfg.secret, signing_input, hashlib.sha256).digest()
    expected_sig_b64 = base64.urlsafe_b64encode(expected_sig).rstrip(b"=").decode("ascii")
    assert sig_b64 == expected_sig_b64

    # Decode header
    h_pad = h_b64 + "=" * ((4 - len(h_b64) % 4) % 4)
    header = json.loads(base64.urlsafe_b64decode(h_pad).decode("utf-8"))
    assert header == {"alg": "HS256", "typ": "JWT"}

    # Decode payload
    p_pad = p_b64 + "=" * ((4 - len(p_b64) % 4) % 4)
    payload = json.loads(base64.urlsafe_b64decode(p_pad).decode("utf-8"))
    assert payload["iss"] == "seeknal-serving"
    assert payload["aud"] == "cube-sql"
    assert payload["sub"] == "analyst@atlas.org"
    assert payload["audit_id"] == "audit-cube-test-01"
    assert payload["iat"] == 1700000000
    assert payload["exp"] == 1700000045
    assert payload["exp"] - payload["iat"] == 45
    assert payload["masks"] == {"claims_cube": {"patient_nik": "hash"}}
    assert payload["row_filters"] == {
        "claims_cube": [{"column": "province", "op": "eq", "values": ["JB"]}]
    }


def test_cube_security_config_ttl_and_types():
    with pytest.raises(ValueError, match="ttl_seconds"):
        CubeSecurityConfig(secret=b"secret", ttl_seconds=0)

    with pytest.raises(ValueError, match="ttl_seconds"):
        CubeSecurityConfig(secret=b"secret", ttl_seconds=301)

    with pytest.raises(ValueError, match="secret"):
        CubeSecurityConfig(secret="not-bytes")  # type: ignore


def test_cube_adapter_connect_receives_user_token_timeout():
    cfg = CubeSecurityConfig(
        secret=b"test-secret",
        issuer="seeknal-serving",
        audience="cube-sql",
        ttl_seconds=60,
        sql_user="custom_cube_user",
    )
    prep = _sample_prepared_query()

    recorded: dict[str, Any] = {}

    class FakeCursor:
        def __init__(self):
            self.description = [("province", 25), ("total_amount", 23)]
        def execute(self, sql, params):
            pass
        def fetchmany(self, limit):
            return [("JB", 100)]
        def close(self):
            pass

    class FakeConn:
        def cursor(self):
            return FakeCursor()
        def cancel(self):
            pass
        def close(self):
            pass

    def fake_connect(*args, **kwargs):
        recorded.update(kwargs)
        return FakeConn()

    adapter = CubeAdapter(fake_connect, cfg, connect_timeout_seconds=12)
    res = adapter.execute(prep, timeout_seconds=5)

    assert recorded["user"] == "custom_cube_user"
    assert recorded["connect_timeout"] == 12
    # Password must be the valid HS256 JWT
    token = recorded["password"]
    parts = token.split(".")
    assert len(parts) == 3
    # Verify token signature
    sig = base64.urlsafe_b64encode(
        hmac.new(b"test-secret", f"{parts[0]}.{parts[1]}".encode("ascii"), hashlib.sha256).digest()
    ).rstrip(b"=").decode("ascii")
    assert parts[2] == sig

    assert res.engine == "cube"
    assert res.columns == ("province", "total_amount")
    assert res.rows == [("JB", 100)]


def test_token_not_in_logs_or_exceptions(caplog):
    cfg = CubeSecurityConfig(secret=b"secret-key-123456", sql_user="seeknal")
    prep = _sample_prepared_query()
    token = make_cube_context_token(prep, cfg)

    # 1. Connection error that attempts to leak password
    def leaky_connect(*args, **kwargs):
        pwd = kwargs.get("password")
        raise RuntimeError(f"Connection failed to host cube:4000 with password {pwd}")

    adapter = CubeAdapter(leaky_connect, cfg)

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(EngineError) as exc_info:
            adapter.execute(prep, timeout_seconds=5)

    # Token must not appear in exception string, repr, or cause
    err_str = str(exc_info.value)
    err_repr = repr(exc_info.value)
    assert token not in err_str
    assert token not in err_repr
    assert exc_info.value.__cause__ is None or token not in str(exc_info.value.__cause__)
    assert token not in caplog.text


def test_watchdog_fires_conn_cancel_and_raises_engine_timeout():
    cfg = CubeSecurityConfig(secret=b"secret")
    prep = _sample_prepared_query()

    class BlockingConn:
        def __init__(self):
            self.cancel_called = False
            self.unblock = threading.Event()

        def cursor(self):
            return BlockingCursor(self)

        def cancel(self):
            self.cancel_called = True
            self.unblock.set()

        def close(self):
            pass

    class BlockingCursor:
        def __init__(self, conn):
            self.conn = conn
            self.description = [("col", 23)]

        def execute(self, sql, params):
            self.conn.unblock.wait()

        def close(self):
            pass

    conn = BlockingConn()
    adapter = CubeAdapter(lambda **kw: conn, cfg)

    with pytest.raises(EngineTimeout):
        adapter.execute(prep, timeout_seconds=0.1)

    assert conn.cancel_called is True


def test_cancel_handle_works_and_raises_engine_cancelled():
    cfg = CubeSecurityConfig(secret=b"secret")
    prep = _sample_prepared_query()

    class BlockingConn:
        def __init__(self):
            self.cancel_called = False
            self.unblock = threading.Event()

        def cursor(self):
            return BlockingCursor(self)

        def cancel(self):
            self.cancel_called = True
            self.unblock.set()

        def close(self):
            pass

    class BlockingCursor:
        def __init__(self, conn):
            self.conn = conn
            self.description = [("col", 23)]

        def execute(self, sql, params):
            self.conn.unblock.wait()

        def close(self):
            pass

    conn = BlockingConn()
    adapter = CubeAdapter(lambda **kw: conn, cfg)

    cancel_fn_holder = []
    def on_cancel(fn):
        cancel_fn_holder.append(fn)

    error_holder = []
    def run_query():
        try:
            adapter.execute(prep, timeout_seconds=10, on_cancel_handle=on_cancel)
        except Exception as exc:
            error_holder.append(exc)

    t = threading.Thread(target=run_query)
    t.start()

    # Wait for cancel handle to be registered
    for _ in range(50):
        if cancel_fn_holder:
            break
        time.sleep(0.02)

    assert len(cancel_fn_holder) == 1
    # Trigger cancellation
    cancel_fn_holder[0]()
    t.join(timeout=2.0)

    assert conn.cancel_called is True
    assert len(error_holder) == 1
    assert isinstance(error_holder[0], EngineCancelled)


def test_missing_security_context_raises_engine_error():
    cfg = CubeSecurityConfig(secret=b"secret")
    prep = _sample_prepared_query(cube_security_context=None)

    adapter = CubeAdapter(lambda **kw: None, cfg)

    with pytest.raises(EngineError, match="cube_security_context"):
        adapter.execute(prep, timeout_seconds=5)


def test_build_adapter_with_and_without_security():
    cfg = CubeSecurityConfig(secret=b"secret")

    def dummy_connect(**kw):
        pass

    # With security -> returns CubeAdapter
    ad_with = build_adapter("cube", dummy_connect, security=cfg)
    assert isinstance(ad_with, CubeAdapter)

    # Without security -> returns DBAPIAdapter
    ad_without = build_adapter("cube", dummy_connect)
    assert isinstance(ad_without, DBAPIAdapter)
    assert ad_without._kind == "cube"


def test_dbapi_adapter_cube_watchdog_timeout_and_cancel():
    prep = _sample_prepared_query()

    class BlockingConn:
        def __init__(self):
            self.cancel_called = False
            self.unblock = threading.Event()

        def cursor(self):
            return BlockingCursor(self)

        def cancel(self):
            self.cancel_called = True
            self.unblock.set()

        def close(self):
            pass

    class BlockingCursor:
        def __init__(self, conn):
            self.conn = conn
            self.description = [("col", 23)]

        def execute(self, sql, params):
            self.conn.unblock.wait()

        def close(self):
            pass

    # 1. Watchdog timeout on DBAPIAdapter (kind="cube")
    conn1 = BlockingConn()
    db_adapter1 = DBAPIAdapter(lambda: conn1, kind="cube")
    with pytest.raises(EngineTimeout):
        db_adapter1.execute(prep, timeout_seconds=0.1)
    assert conn1.cancel_called is True

    # 2. Cancel handle on DBAPIAdapter (kind="cube")
    conn2 = BlockingConn()
    db_adapter2 = DBAPIAdapter(lambda: conn2, kind="cube")
    cancel_fn_holder = []
    def on_cancel(fn):
        cancel_fn_holder.append(fn)

    error_holder = []
    def run_query():
        try:
            db_adapter2.execute(prep, timeout_seconds=10, on_cancel_handle=on_cancel)
        except Exception as exc:
            error_holder.append(exc)

    t = threading.Thread(target=run_query)
    t.start()

    for _ in range(50):
        if cancel_fn_holder:
            break
        time.sleep(0.02)

    assert len(cancel_fn_holder) == 1
    cancel_fn_holder[0]()
    t.join(timeout=2.0)

    assert conn2.cancel_called is True
    assert len(error_holder) == 1
    assert isinstance(error_holder[0], EngineCancelled)
