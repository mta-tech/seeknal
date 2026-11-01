"""Tests for grant token payload typ validation."""

from __future__ import annotations

import hashlib
import time

import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving.grant_token import (
    GrantTokenError,
    sign_cancel_token,
    sign_grant_token,
    verify_grant_token,
)
from seeknal.serving.placement import Placement
from seeknal.serving.service import ServingPipeline


@pytest.fixture
def ec_keys():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    return priv, pub


def _make_claims(sql: str = "SELECT 1", **kwargs) -> dict:
    now = time.time()
    claims = {
        "sub": "analyst@example.com",
        "ws": "analytics",
        "jti": "jti-typ-01",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 60,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": 1000,
        "tables": {
            "users": {
                "columns": ["id"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }
    claims.update(kwargs)
    return claims


def test_verify_grant_token_require_grant_typ_off(ec_keys):
    priv, pub = ec_keys
    public_keys = {"k1": pub}

    # 1. No typ claim is accepted when require_grant_typ=False
    claims_no_typ = _make_claims()
    assert "typ" not in claims_no_typ
    token_no_typ = sign_grant_token(claims_no_typ, priv, kid="k1")
    verified = verify_grant_token(token_no_typ, public_keys, require_grant_typ=False)
    assert verified["sub"] == "analyst@example.com"

    # Default is require_grant_typ=False
    verified_default = verify_grant_token(token_no_typ, public_keys)
    assert verified_default["sub"] == "analyst@example.com"

    # 2. typ: "grant" is accepted
    claims_grant = _make_claims(typ="grant")
    token_grant = sign_grant_token(claims_grant, priv, kid="k1")
    verified_grant = verify_grant_token(token_grant, public_keys, require_grant_typ=False)
    assert verified_grant["typ"] == "grant"

    # 3. typ: "cancel" is always refused
    claims_cancel = _make_claims(typ="cancel")
    token_cancel = sign_cancel_token(claims_cancel, priv, kid="k1")
    with pytest.raises(GrantTokenError, match="Cancel token cannot be used to run a query"):
        verify_grant_token(token_cancel, public_keys, require_grant_typ=False)


def test_verify_grant_token_require_grant_typ_on(ec_keys):
    priv, pub = ec_keys
    public_keys = {"k1": pub}

    # 1. typ: "grant" is accepted
    claims_grant = _make_claims(typ="grant")
    token_grant = sign_grant_token(claims_grant, priv, kid="k1")
    verified = verify_grant_token(token_grant, public_keys, require_grant_typ=True)
    assert verified["typ"] == "grant"

    # 2. Missing typ is refused
    claims_no_typ = _make_claims()
    assert "typ" not in claims_no_typ
    token_no_typ = sign_grant_token(claims_no_typ, priv, kid="k1")
    with pytest.raises(GrantTokenError, match="Missing or invalid 'typ' claim, expected 'grant'"):
        verify_grant_token(token_no_typ, public_keys, require_grant_typ=True)

    # 3. Wrong typ is refused
    claims_wrong = _make_claims(typ="access")
    token_wrong = sign_grant_token(claims_wrong, priv, kid="k1")
    with pytest.raises(GrantTokenError, match="Missing or invalid 'typ' claim, expected 'grant'"):
        verify_grant_token(token_wrong, public_keys, require_grant_typ=True)

    # 4. Cancel typ is refused
    claims_cancel = _make_claims(typ="cancel")
    token_cancel = sign_cancel_token(claims_cancel, priv, kid="k1")
    with pytest.raises(GrantTokenError, match="Cancel token cannot be used to run a query"):
        verify_grant_token(token_cancel, public_keys, require_grant_typ=True)


def test_serving_pipeline_require_grant_typ(ec_keys):
    priv, pub = ec_keys
    placement = Placement.from_dict({
        "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
        "tables": {"users": {"keys": ["id"], "row_count": 10, "serve": {"duck": "users"}}},
        "cubes": {},
    })
    schemas = {"users": ["id"]}

    # Pipeline with require_grant_typ=False
    pipeline_loose = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"k1": pub},
        require_grant_typ=False,
    )

    sql = "SELECT id FROM users"
    token_no_typ = sign_grant_token(_make_claims(sql=sql), priv, kid="k1")
    token_grant = sign_grant_token(_make_claims(sql=sql, typ="grant"), priv, kid="k1")
    token_cancel = sign_cancel_token(_make_claims(sql=sql, typ="cancel"), priv, kid="k1")

    # Loose pipeline accepts both no typ and typ="grant"
    prep1 = pipeline_loose.prepare(sql, token_no_typ)
    assert prep1.engine == "duck"

    prep2 = pipeline_loose.prepare(sql, token_grant)
    assert prep2.engine == "duck"

    with pytest.raises(GrantTokenError):
        pipeline_loose.prepare(sql, token_cancel)

    # Pipeline with require_grant_typ=True
    pipeline_strict = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"k1": pub},
        require_grant_typ=True,
    )

    # Strict pipeline accepts typ="grant"
    prep_strict = pipeline_strict.prepare(sql, token_grant)
    assert prep_strict.engine == "duck"

    # Strict pipeline rejects token without typ
    with pytest.raises(GrantTokenError, match="expected 'grant'"):
        pipeline_strict.prepare(sql, token_no_typ)

    # Strict pipeline rejects token with typ="cancel"
    with pytest.raises(GrantTokenError):
        pipeline_strict.prepare(sql, token_cancel)
