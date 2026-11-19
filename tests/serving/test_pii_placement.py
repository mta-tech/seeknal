"""Tests for PII placement invariants, discovery propagation, and PII cross-engine query guards."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import httpx
import pytest

from seeknal.serving.discovery.build import discover
from seeknal.serving.discovery.config import DiscoveryConfig, DiscoveryConfigError
from seeknal.serving.placement import (
    Placement,
    PlacementError,
    Policy,
    TableEntry,
    TableServe,
)
from seeknal.serving.router import RoutingError
from seeknal.serving.startup_validation import (
    PII_CROSS_ENGINE_CODE,
    PiiCrossEngineError,
    check_pii_query_mixing,
)


def _mock_lakekeeper_client(*, tables: tuple[str, ...] = ("patients", "provinces")) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/config":
            return httpx.Response(200, json={})
        if path == "/v1/namespaces":
            return httpx.Response(200, json={"namespaces": [["clinical"]]})
        if path == "/v1/namespaces/clinical/tables":
            return httpx.Response(
                200,
                json={"identifiers": [{"name": t} for t in tables]},
            )
        for t in tables:
            if path == f"/v1/namespaces/clinical/tables/{t}":
                return httpx.Response(
                    200,
                    json={
                        "metadata": {
                            "current-schema-id": 0,
                            "schemas": [
                                {
                                    "schema-id": 0,
                                    "fields": [
                                        {"id": 1, "name": "id"},
                                        {"id": 2, "name": "full_name"},
                                    ],
                                    "identifier-field-ids": [1],
                                }
                            ],
                            "current-snapshot-id": 700,
                            "snapshots": [
                                {
                                    "snapshot-id": 700,
                                    "timestamp-ms": 1777636800000,
                                    "summary": {"total-records": "1200"},
                                }
                            ],
                        }
                    },
                )
        return httpx.Response(404)

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_pii_defaults_on_table_and_policy():
    p = Placement.from_dict(
        {
            "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
            "tables": {"public.users": {"keys": ["id"], "serve": {"lb": "public.users"}}},
        }
    )
    tbl = p.find_table("public.users")
    assert tbl is not None
    assert tbl.pii is False
    assert p.policy.pii_engines == ()
    assert p.policy == Policy()


@pytest.mark.parametrize("bad_pii", ["true", "false", 1, 0, None, ["true"], {}])
def test_pii_non_boolean_rejected_in_placement(bad_pii):
    with pytest.raises(PlacementError, match="pii.*boolean"):
        Placement.from_dict(
            {
                "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                "tables": {
                    "public.users": {
                        "keys": ["id"],
                        "pii": bad_pii,
                        "serve": {"lb": "public.users"},
                    }
                },
            }
        )


@pytest.mark.parametrize(
    "bad_pii_engines",
    ["duck", 42, True, None, {"duck": True}, [""], ["   "], [123], [True]],
)
def test_policy_pii_engines_invalid_type_or_item_rejected(bad_pii_engines):
    with pytest.raises(PlacementError, match="pii_engines"):
        Placement.from_dict(
            {
                "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
                "tables": {"t": {"keys": ["id"], "serve": {"duck": "t"}}},
                "policy": {"pii_engines": bad_pii_engines},
            }
        )


def test_policy_pii_engines_unknown_engine_rejected():
    with pytest.raises(PlacementError, match="ghost_duck"):
        Placement.from_dict(
            {
                "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                "tables": {"t": {"keys": ["id"], "serve": {"lb": "public.t"}}},
                "policy": {"pii_engines": ["ghost_duck"]},
            }
        )


def test_pii_table_allowed_on_single_lakebase_or_starrocks_engine():
    for kind, dialect in (("lakebase", "postgres"), ("starrocks", "starrocks")):
        p = Placement.from_dict(
            {
                "engines": {"eng": {"kind": kind, "dialect": dialect}},
                "tables": {
                    "claims.patients": {
                        "keys": ["patient_id"],
                        "pii": True,
                        "serve": {"eng": "phys.patients"},
                    }
                },
            }
        )
        entry = p.find_table("claims.patients")
        assert entry is not None
        assert entry.pii is True
        assert len(entry.serves) == 1


def test_pii_table_with_two_serves_rejected():
    with pytest.raises(PlacementError, match=r"PII table 'claims\.patients' must have exactly one serve, got 2"):
        Placement.from_dict(
            {
                "engines": {
                    "lb": {"kind": "lakebase", "dialect": "postgres"},
                    "sr": {"kind": "starrocks", "dialect": "starrocks"},
                },
                "tables": {
                    "claims.patients": {
                        "keys": ["patient_id"],
                        "pii": True,
                        "serve": {
                            "lb": "public.patients",
                            "sr": "lakekeeper.claims.patients",
                        },
                    }
                },
            }
        )


def test_pii_table_on_duckdb_local_file_and_iceberg_direct_rejected_without_pii_engines():
    # Both local_file and iceberg_direct DuckDB engines have kind == 'duckdb' in Placement
    for eng_name, phys_name in (
        ("duck_local", "main.patients"),
        ("duck_ice", "lake.clinical.patients"),
    ):
        with pytest.raises(
            PlacementError,
            match=rf"PII table 'clinical\.patients' cannot be served on duckdb engine '{eng_name}' unless listed in policy\.pii_engines",
        ):
            Placement.from_dict(
                {
                    "engines": {eng_name: {"kind": "duckdb", "dialect": "duckdb"}},
                    "tables": {
                        "clinical.patients": {
                            "keys": ["id"],
                            "pii": True,
                            "serve": {
                                eng_name: {
                                    "name": phys_name,
                                    "snapshot_id": 700 if eng_name == "duck_ice" else None,
                                }
                            },
                        }
                    },
                }
            )


def test_pii_table_on_duckdb_allowed_when_listed_in_policy_pii_engines():
    p = Placement.from_dict(
        {
            "engines": {
                "duck_approved": {"kind": "duckdb", "dialect": "duckdb"},
                "duck_other": {"kind": "duckdb", "dialect": "duckdb"},
            },
            "tables": {
                "clinical.patients": {
                    "keys": ["id"],
                    "pii": True,
                    "serve": {"duck_approved": "lake.clinical.patients"},
                }
            },
            "policy": {"pii_engines": ["duck_approved"]},
        }
    )
    tbl = p.find_table("clinical.patients")
    assert tbl is not None
    assert tbl.pii is True
    assert p.policy.pii_engines == ("duck_approved",)

    # Unlisted duckdb sibling is still rejected
    with pytest.raises(PlacementError, match="duck_other"):
        Placement.from_dict(
            {
                "engines": {
                    "duck_approved": {"kind": "duckdb", "dialect": "duckdb"},
                    "duck_other": {"kind": "duckdb", "dialect": "duckdb"},
                },
                "tables": {
                    "clinical.patients": {
                        "keys": ["id"],
                        "pii": True,
                        "serve": {"duck_other": "main.patients"},
                    }
                },
                "policy": {"pii_engines": ["duck_approved"]},
            }
        )


def test_discovery_carries_pii_from_synced_tables_and_pii_tables():
    sqlite_conn = sqlite3.connect(":memory:")
    sqlite_conn.execute(
        "CREATE TABLE _lakebase_sync_state (target_table TEXT PRIMARY KEY, snapshot_id INTEGER, synced_at TEXT)"
    )
    sqlite_conn.execute(
        "INSERT INTO _lakebase_sync_state VALUES (?, ?, ?)",
        ("public.patients", 700, "2026-05-01T12:00:00+00:00"),
    )
    sqlite_conn.commit()

    # 1. Carried via lakebase.synced_tables.<logical>.pii = True on single Lakebase engine
    cfg_lb = {
        "default_catalog": "lakekeeper",
        "lakekeeper": {"url": "https://lk.test", "warehouse": "wh"},
        "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
        "lakebase": {
            "engine": "lb",
            "synced_tables": {
                "lakekeeper.clinical.patients": {
                    "target": "public.patients",
                    "primary_key": ["id"],
                    "pii": True,
                }
            },
        },
    }
    res_lb = discover(
        cfg_lb,
        http_client=_mock_lakekeeper_client(tables=("patients",)),
        lakebase_connect=lambda: sqlite_conn,
        now=datetime(2026, 5, 1, 12, 1, 0, tzinfo=timezone.utc),
    )
    assert res_lb.placement["tables"]["lakekeeper.clinical.patients"]["pii"] is True
    pl_lb = Placement.from_dict(res_lb.placement)
    assert pl_lb.tables["lakekeeper.clinical.patients"].pii is True

    # 2. If StarRocks + Lakebase both serve a PII table in discovery -> rejected with PlacementError
    cfg_two_serves = {
        "default_catalog": "lakekeeper",
        "lakekeeper": {"url": "https://lk.test", "warehouse": "wh"},
        "engines": {
            "sr": {"kind": "starrocks", "dialect": "starrocks"},
            "lb": {"kind": "lakebase", "dialect": "postgres"},
        },
        "lakebase": {
            "engine": "lb",
            "synced_tables": {
                "lakekeeper.clinical.patients": {
                    "target": "public.patients",
                    "primary_key": ["id"],
                    "pii": True,
                }
            },
        },
    }
    with pytest.raises(PlacementError, match="must have exactly one serve, got 2"):
        discover(
            cfg_two_serves,
            http_client=_mock_lakekeeper_client(tables=("patients",)),
            lakebase_connect=lambda: sqlite_conn,
            now=datetime(2026, 5, 1, 12, 1, 0, tzinfo=timezone.utc),
        )


def test_discovery_pii_on_duckdb_iceberg_direct_requires_pii_engines():
    base_cfg = {
        "default_catalog": "lakekeeper",
        "lakekeeper": {"url": "https://lk.test", "warehouse": "wh"},
        "engines": {
            "duck_ice": {
                "kind": "duckdb",
                "dialect": "duckdb",
                "profile": "iceberg_direct",
                "alias": "lake",
            }
        },
        "pii_tables": ["lakekeeper.clinical.patients"],
    }

    # Refused when duck_ice is not in pii_engines
    with pytest.raises(PlacementError, match="duck_ice.*policy.pii_engines"):
        discover(
            base_cfg,
            http_client=_mock_lakekeeper_client(),
            now=datetime(2026, 5, 1, 12, 1, 0, tzinfo=timezone.utc),
        )

    # Allowed when duck_ice is listed in pii_engines
    approved_cfg = {**base_cfg, "pii_engines": ["duck_ice"]}
    res = discover(
        approved_cfg,
        http_client=_mock_lakekeeper_client(),
        now=datetime(2026, 5, 1, 12, 1, 0, tzinfo=timezone.utc),
    )
    assert res.placement["tables"]["lakekeeper.clinical.patients"]["pii"] is True
    assert "pii" not in res.placement["tables"]["lakekeeper.clinical.provinces"]
    assert res.placement["policy"]["pii_engines"] == ["duck_ice"]

    pl = Placement.from_dict(res.placement)
    assert pl.tables["lakekeeper.clinical.patients"].pii is True
    assert pl.tables["lakekeeper.clinical.provinces"].pii is False
    assert pl.policy.pii_engines == ("duck_ice",)


def test_discovery_config_validation_for_pii_fields():
    with pytest.raises(DiscoveryConfigError, match="pii.*boolean"):
        DiscoveryConfig.from_dict(
            {
                "default_catalog": "lk",
                "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                "lakebase": {
                    "engine": "lb",
                    "synced_tables": {"lk.ns.t": {"target": "public.t", "pii": "yes"}},
                },
            }
        )

    with pytest.raises(DiscoveryConfigError, match="pii_tables"):
        DiscoveryConfig.from_dict({"default_catalog": "lk", "pii_tables": "lk.ns.t"})

    with pytest.raises(DiscoveryConfigError, match="pii_engines"):
        DiscoveryConfig.from_dict({"default_catalog": "lk", "pii_engines": "duck"})

    with pytest.raises(DiscoveryConfigError, match="Conflicting"):
        DiscoveryConfig.from_dict(
            {
                "default_catalog": "lk",
                "pii_engines": ["duck_a"],
                "policy": {"pii_engines": ["duck_b"]},
            }
        )


def test_check_pii_query_mixing_allows_same_engine_and_rejects_cross_engine_and_bad_hints():
    pii_lb = TableEntry(
        name="clinical.patients",
        keys=("id",),
        row_count=1000,
        serves=(TableServe(engine="lb", physical_name="public.patients"),),
        pii=True,
    )
    pii_lb_2 = TableEntry(
        name="clinical.ssn_vault",
        keys=("id",),
        row_count=1000,
        serves=(TableServe(engine="lb", physical_name="public.ssn_vault"),),
        pii=True,
    )
    pii_sr = TableEntry(
        name="clinical.audit_identities",
        keys=("id",),
        row_count=50000,
        serves=(TableServe(engine="sr", physical_name="lk.clinical.audit_identities"),),
        pii=True,
    )
    shared_ref = TableEntry(
        name="ref.provinces",
        keys=("code",),
        row_count=38,
        serves=(
            TableServe(engine="lb", physical_name="public.provinces"),
            TableServe(engine="sr", physical_name="lk.ref.provinces"),
        ),
        pii=False,
    )
    sr_only_fact = TableEntry(
        name="claims.fact_claims",
        keys=("claim_id",),
        row_count=5_000_000,
        serves=(TableServe(engine="sr", physical_name="lk.claims.fact_claims"),),
        pii=False,
    )

    # Allowed cases
    check_pii_query_mixing([])
    check_pii_query_mixing([shared_ref, sr_only_fact], hint="sr")
    check_pii_query_mixing([pii_lb])
    check_pii_query_mixing([pii_lb], hint="lb")
    check_pii_query_mixing([pii_lb, pii_lb_2])
    check_pii_query_mixing([pii_lb, shared_ref])
    check_pii_query_mixing([pii_lb, shared_ref], hint="lb")

    # Rejected with typed RoutingError (.code == 'pii_cross_engine'):
    # 1. PII table on lb joined with table only served on sr (no hint, hint='lb', hint='sr')
    for hint_val in (None, "lb", "sr"):
        with pytest.raises(RoutingError) as exc_info:
            check_pii_query_mixing([pii_lb, sr_only_fact], hint=hint_val)
        assert isinstance(exc_info.value, PiiCrossEngineError)
        assert exc_info.value.code == PII_CROSS_ENGINE_CODE
        assert "pii_cross_engine" == exc_info.value.code

    # 2. Single PII table on lb with engine hint targeting sr
    with pytest.raises(RoutingError) as exc_info:
        check_pii_query_mixing([pii_lb], hint="sr")
    assert exc_info.value.code == PII_CROSS_ENGINE_CODE

    # 3. PII table on lb + shared table with engine hint targeting sr
    with pytest.raises(RoutingError) as exc_info:
        check_pii_query_mixing([pii_lb, shared_ref], hint="sr")
    assert exc_info.value.code == PII_CROSS_ENGINE_CODE

    # 4. Two PII tables on different engines
    with pytest.raises(RoutingError) as exc_info:
        check_pii_query_mixing([pii_lb, pii_sr])
    assert exc_info.value.code == PII_CROSS_ENGINE_CODE


def test_discovery_carries_pii_from_iceberg_table_attribute(monkeypatch):
    from dataclasses import dataclass

    import seeknal.serving.discovery.build as build_mod

    @dataclass(frozen=True)
    class TaggedIcebergTable:
        namespace: str
        name: str
        columns: tuple[str, ...]
        keys: tuple[str, ...]
        row_count: int | None
        current_snapshot_id: int | None
        snapshot_ms: int | None
        pii: bool = False

    class FakeLakekeeperSource:
        def __init__(self, *args, **kwargs):
            self.warnings: list[str] = []

        def discover(self):
            return [
                TaggedIcebergTable(
                    namespace="clinical",
                    name="patients",
                    columns=("id", "ssn"),
                    keys=("id",),
                    row_count=500,
                    current_snapshot_id=10,
                    snapshot_ms=1777636800000,
                    pii=True,
                )
            ]

    monkeypatch.setattr(build_mod, "LakekeeperSource", FakeLakekeeperSource)

    res = discover(
        {
            "default_catalog": "lakekeeper",
            "lakekeeper": {"url": "https://lk.test", "warehouse": "wh"},
            "engines": {"sr": {"kind": "starrocks", "dialect": "starrocks"}},
        },
        http_client=httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(404))),
        now=datetime(2026, 5, 1, 12, 1, 0, tzinfo=timezone.utc),
    )
    assert res.placement["tables"]["lakekeeper.clinical.patients"]["pii"] is True
    pl = Placement.from_dict(res.placement)
    assert pl.tables["lakekeeper.clinical.patients"].pii is True


def test_router_enforces_pii_cross_engine_and_restricts_feasible_engines():
    from seeknal.serving.router import Router

    placement = Placement.from_dict(
        {
            "engines": {
                "lb": {"kind": "lakebase", "dialect": "postgres"},
                "sr": {"kind": "starrocks", "dialect": "starrocks"},
            },
            "tables": {
                "clinical.patients": {
                    "keys": ["id"],
                    "row_count": 2_000_000,
                    "pii": True,
                    "serve": {"lb": "public.patients"},
                },
                "claims.fact_claims": {
                    "keys": ["claim_id"],
                    "row_count": 5_000_000,
                    "serve": {"sr": "lk.claims.fact_claims"},
                },
                "ref.provinces": {
                    "keys": ["code"],
                    "row_count": 38,
                    "serve": {"lb": "public.provinces", "sr": "lk.ref.provinces"},
                },
            },
        }
    )
    router = Router(placement)

    # 1. Joining PII table on 'lb' with 'claims.fact_claims' only on 'sr' -> pii_cross_engine
    join_cross_sql = (
        "SELECT p.id, c.claim_id FROM clinical.patients p "
        "JOIN claims.fact_claims c ON p.id = c.claim_id"
    )
    for hint_val in (None, "lb", "sr"):
        with pytest.raises(RoutingError) as exc_info:
            router.route(join_cross_sql, hint=hint_val)
        assert exc_info.value.code == "pii_cross_engine"

    with pytest.raises(RoutingError) as exc_comment:
        router.route("/* seeknal:engine=sr */ " + join_cross_sql)
    assert exc_comment.value.code == "pii_cross_engine"

    # 2. Single PII table forced via hint or SQL comment to another engine -> pii_cross_engine
    with pytest.raises(RoutingError) as exc_single_hint:
        router.route("SELECT id FROM clinical.patients", hint="sr")
    assert exc_single_hint.value.code == "pii_cross_engine"

    with pytest.raises(RoutingError) as exc_single_comment:
        router.route("/* seeknal:engine=sr */ SELECT id FROM clinical.patients")
    assert exc_single_comment.value.code == "pii_cross_engine"

    # 3. Joining PII table on 'lb' with 'ref.provinces' (served on both 'lb' and 'sr')
    # restricts feasible engines strictly to ('lb',) and routes to 'lb' without hint
    decision = router.route(
        "SELECT p.id, r.code FROM clinical.patients p JOIN ref.provinces r ON p.id = r.code"
    )
    assert decision.engine == "lb"
    assert decision.feasible == ("lb",)
    assert "sr" in decision.rejected


def test_http_query_and_explain_return_422_pii_cross_engine_before_any_engine_call():
    import hashlib
    import time
    from cryptography.hazmat.primitives.asymmetric import ec
    from starlette.testclient import TestClient

    from seeknal.serving.grant_token import sign_grant_token
    from seeknal.serving.http import create_app
    from seeknal.serving.service import ServingPipeline

    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    placement = Placement.from_dict(
        {
            "engines": {
                "lb": {"kind": "lakebase", "dialect": "postgres"},
                "sr": {"kind": "starrocks", "dialect": "starrocks"},
            },
            "tables": {
                "clinical.patients": {
                    "keys": ["id"],
                    "row_count": 500,
                    "pii": True,
                    "serve": {"lb": "public.patients"},
                },
                "claims.fact_claims": {
                    "keys": ["claim_id"],
                    "row_count": 10000,
                    "serve": {"sr": "lk.claims.fact_claims"},
                },
            },
        }
    )
    schemas = {
        "clinical.patients": ["id", "ssn"],
        "claims.fact_claims": ["claim_id", "patient_id", "amount"],
    }
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"pii-test-kid": pub},
    )

    adapter_calls: list[str] = []

    class NeverCalledAdapter:
        def __init__(self, name: str) -> None:
            self.name = name

        def execute(self, *args, **kwargs):
            adapter_calls.append(self.name)
            raise AssertionError(f"Engine adapter '{self.name}' must not be called on pii_cross_engine")

    app = create_app(
        pipeline,
        {"lb": NeverCalledAdapter("lb"), "sr": NeverCalledAdapter("sr")},
    )
    client = TestClient(app)

    def _mint(sql_text: str, jti: str) -> str:
        now = time.time()
        return sign_grant_token(
            {
                "sub": "analyst@atlas.example",
                "ws": "ws-pii",
                "jti": jti,
                "iss": "atlas-catalog",
                "aud": "seeknal-serving",
                "iat": now,
                "exp": now + 60.0,
                "typ": "grant",
                "sql_sha256": hashlib.sha256(sql_text.encode("utf-8")).hexdigest(),
                "max_rows": 100,
                "tables": {
                    "clinical.patients": {
                        "columns": ["id"],
                        "masks": {"ssn": "redact"},
                        "row_filters": [],
                        "actions": ["select"],
                    },
                    "claims.fact_claims": {
                        "columns": ["claim_id", "patient_id", "amount"],
                        "masks": {},
                        "row_filters": [],
                        "actions": ["select"],
                    },
                },
            },
            priv,
            kid="pii-test-kid",
        )

    cross_sql = (
        "SELECT p.id, f.amount FROM clinical.patients p "
        "JOIN claims.fact_claims f ON p.id = f.patient_id"
    )

    idx = 0
    for endpoint in ("/v1/query", "/v1/explain"):
        for hint_val in (None, "lb", "sr"):
            idx += 1
            jti = f"jti-pii-http-{idx}"
            tok = _mint(cross_sql, jti)
            req_body: dict = {"sql": cross_sql}
            if hint_val is not None:
                req_body["hint"] = hint_val

            resp = client.post(
                endpoint,
                headers={"Authorization": f"Bearer {tok}"},
                json=req_body,
            )
            assert resp.status_code == 422
            payload = resp.json()
            assert payload["code"] == "pii_cross_engine"
            assert payload["error"] == "routing_error"
            assert isinstance(payload["next_step"], str) and payload["next_step"].strip()
            assert payload["audit_id"] == jti

    # Single PII table with hint targeting 'sr' also returns 422 pii_cross_engine on both endpoints
    single_sql = "SELECT id FROM clinical.patients"
    for ep_idx, endpoint in enumerate(("/v1/query", "/v1/explain"), start=100):
        jti = f"jti-pii-single-hint-{ep_idx}"
        tok = _mint(single_sql, jti)
        resp = client.post(
            endpoint,
            headers={"Authorization": f"Bearer {tok}"},
            json={"sql": single_sql, "hint": "sr"},
        )
        assert resp.status_code == 422
        payload = resp.json()
        assert payload["code"] == "pii_cross_engine"
        assert payload["error"] == "routing_error"
        assert isinstance(payload["next_step"], str) and payload["next_step"].strip()
        assert payload["audit_id"] == jti

    assert adapter_calls == []


