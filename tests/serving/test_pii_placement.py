"""Tests for PII placement invariants, discovery propagation, and PII cross-engine query guards."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import httpx
import pytest

from seeknal.serving.discovery.build import discover
from seeknal.serving.discovery.config import (
    DiscoveryConfig,
    DiscoveryConfigError,
    DiscoveryError,
)
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

    # 2. If StarRocks + Lakebase both could serve a synced PII table in discovery ->
    # single-serve selection picks synced Lakebase ('lb') so Placement has 1 serve
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
    res_single = discover(
        cfg_two_serves,
        http_client=_mock_lakekeeper_client(tables=("patients",)),
        lakebase_connect=lambda: sqlite_conn,
        now=datetime(2026, 5, 1, 12, 1, 0, tzinfo=timezone.utc),
    )
    assert list(res_single.placement["tables"]["lakekeeper.clinical.patients"]["serve"].keys()) == ["lb"]
    pl_single = Placement.from_dict(res_single.placement)
    assert len(pl_single.tables["lakekeeper.clinical.patients"].serves) == 1
    assert pl_single.tables["lakekeeper.clinical.patients"].serves[0].engine == "lb"


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


def test_discovery_unmatched_pii_tables_and_synced_pii_raise_discovery_error():
    # E-H1: typo in pii_tables and/or synced_tables(pii=True) must raise DiscoveryError naming every unmatched entry
    cfg_typo = {
        "default_catalog": "lakekeeper",
        "lakekeeper": {"url": "https://lk.test", "warehouse": "wh"},
        "engines": {
            "sr": {"kind": "starrocks", "dialect": "starrocks"},
            "lb": {"kind": "lakebase", "dialect": "postgres"},
        },
        "lakebase": {
            "engine": "lb",
            "synced_tables": {
                "lakekeeper.clinical.patients_synced_typo": {
                    "target": "public.patients",
                    "primary_key": ["id"],
                    "pii": True,
                }
            },
        },
        "pii_tables": [
            "lakekeeper.clinical.patients",
            "lakekeeper.clinical.patients_typo_one",
            "lakekeeper.clinical.patients_typo_two",
        ],
    }
    with pytest.raises(DiscoveryError) as exc_info:
        discover(
            cfg_typo,
            http_client=_mock_lakekeeper_client(tables=("patients",)),
            now=datetime(2026, 5, 1, 12, 1, 0, tzinfo=timezone.utc),
        )
    msg = str(exc_info.value)
    assert "lakekeeper.clinical.patients_typo_one" in msg
    assert "lakekeeper.clinical.patients_typo_two" in msg
    assert "lakekeeper.clinical.patients_synced_typo" in msg


@pytest.mark.parametrize(
    "bad_pii_name",
    [
        "othercat.clinical.patients",  # wrong catalog prefix
        "clinical.patients",  # missing catalog prefix (2-part)
        "lakekeeper.clinical.sub.patients",  # 4-part
        'lakekeeper."clinical".patients',  # double quotes
        "'lakekeeper.clinical.patients'",  # single quotes
        " lakekeeper.clinical.patients",  # leading whitespace
        "lakekeeper.clinical.patients ",  # trailing whitespace
        "lakekeeper.clinical .patients",  # internal whitespace
        "lakekeeper.clinical.patients-v2",  # invalid identifier segment
        "lakekeeper.1clinical.patients",  # segment starting with digit
    ],
)
def test_discovery_rejects_wrong_catalog_quoted_and_malformed_pii_names(bad_pii_name):
    # Rejected in DiscoveryConfig.from_dict and discover() for both pii_tables and synced_tables
    with pytest.raises(DiscoveryConfigError):
        DiscoveryConfig.from_dict(
            {
                "default_catalog": "lakekeeper",
                "lakekeeper": {"url": "https://lk.test", "warehouse": "wh"},
                "engines": {"sr": {"kind": "starrocks", "dialect": "starrocks"}},
                "pii_tables": [bad_pii_name],
            }
        )

    with pytest.raises(DiscoveryConfigError):
        DiscoveryConfig.from_dict(
            {
                "default_catalog": "lakekeeper",
                "lakekeeper": {"url": "https://lk.test", "warehouse": "wh"},
                "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                "lakebase": {
                    "engine": "lb",
                    "synced_tables": {
                        bad_pii_name: {"target": "public.patients", "pii": True}
                    },
                },
            }
        )

    with pytest.raises(DiscoveryConfigError):
        discover(
            {
                "default_catalog": "lakekeeper",
                "lakekeeper": {"url": "https://lk.test", "warehouse": "wh"},
                "engines": {"sr": {"kind": "starrocks", "dialect": "starrocks"}},
                "pii_tables": [bad_pii_name],
            },
            http_client=_mock_lakekeeper_client(tables=("patients",)),
        )


def test_discovery_rejects_pii_tables_when_lakekeeper_missing():
    with pytest.raises(DiscoveryConfigError, match="lakekeeper"):
        DiscoveryConfig.from_dict(
            {
                "default_catalog": "lakekeeper",
                "engines": {"sr": {"kind": "starrocks", "dialect": "starrocks"}},
                "pii_tables": ["lakekeeper.clinical.patients"],
            }
        )


def test_static_placement_top_and_policy_pii_tables_fold_into_table_entry_and_router():
    from seeknal.serving.router import Router

    # 1. Both top-level pii_tables and policy.pii_tables fold into TableEntry.pii=True (case-insensitively)
    for container_kind in ("top", "policy"):
        raw: dict = {
            "engines": {
                "lb": {"kind": "lakebase", "dialect": "postgres"},
                "sr": {"kind": "starrocks", "dialect": "starrocks"},
            },
            "tables": {
                "clinical.patients": {
                    "keys": ["id"],
                    "serve": {"lb": "public.patients"},
                },
                "claims.fact_claims": {
                    "keys": ["claim_id"],
                    "serve": {"sr": "lk.claims.fact_claims"},
                },
            },
        }
        if container_kind == "top":
            raw["pii_tables"] = ["CLINICAL.PATIENTS"]
        else:
            raw["policy"] = {"pii_tables": ["CLINICAL.PATIENTS"]}

        pl = Placement.from_dict(raw)
        tbl = pl.find_table("clinical.patients")
        assert tbl is not None and tbl.pii is True

        # Router enforces pii_cross_engine automatically
        router = Router(pl)
        with pytest.raises(RoutingError) as exc_join:
            router.route(
                "SELECT p.id, f.claim_id FROM clinical.patients p "
                "JOIN claims.fact_claims f ON p.id = f.claim_id"
            )
        assert exc_join.value.code == "pii_cross_engine"

        with pytest.raises(RoutingError) as exc_hint:
            router.route("SELECT id FROM clinical.patients", hint="sr")
        assert exc_hint.value.code == "pii_cross_engine"

    # 2. Refused when folded PII table is on DuckDB without pii_engines
    for container_kind in ("top", "policy"):
        raw_duck: dict = {
            "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
            "tables": {
                "clinical.patients": {
                    "keys": ["id"],
                    "serve": {"duck": "main.patients"},
                }
            },
        }
        if container_kind == "top":
            raw_duck["pii_tables"] = ["clinical.patients"]
        else:
            raw_duck["policy"] = {"pii_tables": ["clinical.patients"]}
        with pytest.raises(PlacementError, match="policy.pii_engines"):
            Placement.from_dict(raw_duck)

    # 3. Refused when folded PII table has two serves
    for container_kind in ("top", "policy"):
        raw_two: dict = {
            "engines": {
                "lb": {"kind": "lakebase", "dialect": "postgres"},
                "sr": {"kind": "starrocks", "dialect": "starrocks"},
            },
            "tables": {
                "clinical.patients": {
                    "keys": ["id"],
                    "serve": {"lb": "public.patients", "sr": "lk.clinical.patients"},
                }
            },
        }
        if container_kind == "top":
            raw_two["pii_tables"] = ["clinical.patients"]
        else:
            raw_two["policy"] = {"pii_tables": ["clinical.patients"]}
        with pytest.raises(PlacementError, match="must have exactly one serve, got 2"):
            Placement.from_dict(raw_two)

    # 4. Unknown table or invalid type in pii_tables / policy.pii_tables raises PlacementError
    with pytest.raises(PlacementError, match="Unknown table 'clinical.ghost' in pii_tables"):
        Placement.from_dict(
            {
                "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                "tables": {"clinical.patients": {"keys": ["id"], "serve": {"lb": "public.patients"}}},
                "pii_tables": ["clinical.ghost"],
            }
        )
    for bad_list in ("clinical.patients", b"clinical.patients", {"a": 1}, [""], [123]):
        with pytest.raises(PlacementError, match="pii_tables"):
            Placement.from_dict(
                {
                    "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                    "tables": {"clinical.patients": {"keys": ["id"], "serve": {"lb": "public.patients"}}},
                    "policy": {"pii_tables": bad_list},
                }
            )


def test_placement_from_yaml_rejects_duplicate_table_keys_and_duplicate_pii_keys(tmp_path):
    # E-M1: Duplicate table key in placement.yaml (first pii: true, second pii: false)
    dup_tbl_yaml = tmp_path / "dup_table.yaml"
    dup_tbl_yaml.write_text(
        "engines:\n"
        "  lb: {kind: lakebase, dialect: postgres}\n"
        "  duck: {kind: duckdb, dialect: duckdb}\n"
        "tables:\n"
        "  clinical.patients:\n"
        "    keys: [id]\n"
        "    pii: true\n"
        "    serve:\n"
        "      lb: public.patients\n"
        "  clinical.patients:\n"
        "    keys: [id]\n"
        "    pii: false\n"
        "    serve:\n"
        "      duck: main.patients\n",
        encoding="utf-8",
    )
    with pytest.raises(PlacementError, match="duplicate YAML key 'clinical.patients'"):
        Placement.from_yaml(dup_tbl_yaml)

    # Duplicate pii: key inside the same table mapping (pii: true followed by pii: false)
    dup_pii_yaml = tmp_path / "dup_pii.yaml"
    dup_pii_yaml.write_text(
        "engines:\n"
        "  duck: {kind: duckdb, dialect: duckdb}\n"
        "tables:\n"
        "  clinical.patients:\n"
        "    keys: [id]\n"
        "    pii: true\n"
        "    pii: false\n"
        "    serve:\n"
        "      duck: main.patients\n",
        encoding="utf-8",
    )
    with pytest.raises(PlacementError, match="duplicate YAML key 'pii'"):
        Placement.from_yaml(dup_pii_yaml)


def test_discovery_pii_single_serve_selection_priority():
    # E-M2: Verify priority order:
    # 1. Synced Lakebase serve ('lb') when synced
    # 2. First non-DuckDB serve ('sr') when not synced to Lakebase
    # 3. First serve in cfg.pii_engines when only DuckDB serves exist
    # 4. Unapproved DuckDB serve kept so Placement.from_dict raises PlacementError
    sqlite_conn = sqlite3.connect(":memory:")
    sqlite_conn.execute(
        "CREATE TABLE _lakebase_sync_state (target_table TEXT PRIMARY KEY, snapshot_id INTEGER, synced_at TEXT)"
    )
    sqlite_conn.execute(
        "INSERT INTO _lakebase_sync_state VALUES (?, ?, ?)",
        ("public.patients", 700, "2026-05-01T12:00:00+00:00"),
    )
    sqlite_conn.commit()
    now_dt = datetime(2026, 5, 1, 12, 1, 0, tzinfo=timezone.utc)

    # Priority 1: Lakebase chosen when synced (even with StarRocks + DuckDB present)
    res_1 = discover(
        {
            "default_catalog": "lakekeeper",
            "lakekeeper": {"url": "https://lk.test", "warehouse": "wh"},
            "engines": {
                "sr": {"kind": "starrocks", "dialect": "starrocks"},
                "duck_ice": {
                    "kind": "duckdb",
                    "dialect": "duckdb",
                    "profile": "iceberg_direct",
                    "alias": "lake",
                },
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
            "pii_engines": ["duck_ice"],
        },
        http_client=_mock_lakekeeper_client(tables=("patients", "provinces")),
        lakebase_connect=lambda: sqlite_conn,
        now=now_dt,
    )
    assert list(res_1.placement["tables"]["lakekeeper.clinical.patients"]["serve"].keys()) == ["lb"]
    # Non-PII table still gets all non-Lakebase serves
    assert set(res_1.placement["tables"]["lakekeeper.clinical.provinces"]["serve"].keys()) == {
        "sr",
        "duck_ice",
    }

    # Priority 2: StarRocks chosen when PII table is NOT synced to Lakebase (even with DuckDB present)
    res_2 = discover(
        {
            "default_catalog": "lakekeeper",
            "lakekeeper": {"url": "https://lk.test", "warehouse": "wh"},
            "engines": {
                "duck_ice": {
                    "kind": "duckdb",
                    "dialect": "duckdb",
                    "profile": "iceberg_direct",
                    "alias": "lake",
                },
                "sr": {"kind": "starrocks", "dialect": "starrocks"},
                "lb": {"kind": "lakebase", "dialect": "postgres"},
            },
            "lakebase": {"engine": "lb", "synced_tables": {}},
            "pii_tables": ["lakekeeper.clinical.patients"],
            "pii_engines": ["duck_ice"],
        },
        http_client=_mock_lakekeeper_client(tables=("patients",)),
        lakebase_connect=lambda: sqlite_conn,
        now=now_dt,
    )
    assert list(res_2.placement["tables"]["lakekeeper.clinical.patients"]["serve"].keys()) == ["sr"]

    # Priority 3: Approved DuckDB in pii_engines chosen when multiple DuckDB iceberg_direct engines exist
    res_3 = discover(
        {
            "default_catalog": "lakekeeper",
            "lakekeeper": {"url": "https://lk.test", "warehouse": "wh"},
            "engines": {
                "duck_unapproved": {
                    "kind": "duckdb",
                    "dialect": "duckdb",
                    "profile": "iceberg_direct",
                    "alias": "lakea",
                },
                "duck_approved": {
                    "kind": "duckdb",
                    "dialect": "duckdb",
                    "profile": "iceberg_direct",
                    "alias": "lakeb",
                },
            },
            "pii_tables": ["lakekeeper.clinical.patients"],
            "pii_engines": ["duck_approved"],
        },
        http_client=_mock_lakekeeper_client(tables=("patients",)),
        now=now_dt,
    )
    assert list(res_3.placement["tables"]["lakekeeper.clinical.patients"]["serve"].keys()) == [
        "duck_approved"
    ]

    # Priority 4: Multiple unapproved DuckDB serves -> keeps single unapproved serve and raises PlacementError
    with pytest.raises(PlacementError, match="policy.pii_engines"):
        discover(
            {
                "default_catalog": "lakekeeper",
                "lakekeeper": {"url": "https://lk.test", "warehouse": "wh"},
                "engines": {
                    "duck_a": {
                        "kind": "duckdb",
                        "dialect": "duckdb",
                        "profile": "iceberg_direct",
                        "alias": "lakea",
                    },
                    "duck_b": {
                        "kind": "duckdb",
                        "dialect": "duckdb",
                        "profile": "iceberg_direct",
                        "alias": "lakeb",
                    },
                },
                "pii_tables": ["lakekeeper.clinical.patients"],
            },
            http_client=_mock_lakekeeper_client(tables=("patients",)),
            now=now_dt,
        )


def test_cube_pii_requires_pii_engines_and_enforces_pii_cross_engine_in_router():
    from seeknal.serving.router import Router

    # 1. Default pii is False on CubeEntry
    pl_default = Placement.from_dict(
        {
            "engines": {"cube_eng": {"kind": "cube", "dialect": "postgres"}},
            "cubes": {
                "patient_cube": {
                    "engine": "cube_eng",
                    "measures": ["cnt"],
                    "dimensions": ["province"],
                }
            },
        }
    )
    assert pl_default.cubes["patient_cube"].pii is False

    # 2. Non-boolean pii on cube rejected
    with pytest.raises(PlacementError, match="pii.*boolean"):
        Placement.from_dict(
            {
                "engines": {"cube_eng": {"kind": "cube", "dialect": "postgres"}},
                "cubes": {
                    "patient_cube": {
                        "engine": "cube_eng",
                        "measures": ["cnt"],
                        "dimensions": ["province"],
                        "pii": "yes",
                    }
                },
            }
        )

    # 3. pii: true on cube refused unless cube engine is in policy.pii_engines
    with pytest.raises(
        PlacementError,
        match="PII cube 'patient_cube' engine 'cube_eng' must be listed in policy.pii_engines",
    ):
        Placement.from_dict(
            {
                "engines": {"cube_eng": {"kind": "cube", "dialect": "postgres"}},
                "cubes": {
                    "patient_cube": {
                        "engine": "cube_eng",
                        "measures": ["cnt"],
                        "dimensions": ["province"],
                        "pii": True,
                    }
                },
            }
        )

    # Also refused when folded via pii_tables without pii_engines
    with pytest.raises(
        PlacementError,
        match="PII cube 'patient_cube' engine 'cube_eng' must be listed in policy.pii_engines",
    ):
        Placement.from_dict(
            {
                "engines": {"cube_eng": {"kind": "cube", "dialect": "postgres"}},
                "cubes": {
                    "patient_cube": {
                        "engine": "cube_eng",
                        "measures": ["cnt"],
                        "dimensions": ["province"],
                    }
                },
                "pii_tables": ["patient_cube"],
            }
        )

    # 4. Accepted when cube engine is listed in policy.pii_engines; router enforces pii_cross_engine
    pl = Placement.from_dict(
        {
            "engines": {
                "cube_pii": {"kind": "cube", "dialect": "postgres"},
                "cube_pub": {"kind": "cube", "dialect": "postgres"},
                "lb": {"kind": "lakebase", "dialect": "postgres"},
            },
            "tables": {
                "ref.provinces": {
                    "keys": ["code"],
                    "serve": {"lb": "public.provinces"},
                }
            },
            "cubes": {
                "patient_cube": {
                    "engine": "cube_pii",
                    "measures": ["cnt"],
                    "dimensions": ["province"],
                    "pii": True,
                },
                "public_cube": {
                    "engine": "cube_pub",
                    "measures": ["cnt"],
                    "dimensions": ["province"],
                },
            },
            "policy": {"pii_engines": ["cube_pii"]},
        }
    )
    assert pl.cubes["patient_cube"].pii is True
    router = Router(pl)

    # Direct query to PII cube (with or without matching hint) routes to cube_pii
    dec = router.route("SELECT province, cnt FROM patient_cube")
    assert dec.engine == "cube_pii"
    dec_hint = router.route("SELECT province, cnt FROM patient_cube", hint="cube_pii")
    assert dec_hint.engine == "cube_pii"

    # Mixing PII cube and table -> pii_cross_engine
    with pytest.raises(RoutingError) as exc_mix_tbl:
        router.route(
            "SELECT c.cnt FROM patient_cube c JOIN ref.provinces p ON c.province = p.code"
        )
    assert exc_mix_tbl.value.code == "pii_cross_engine"

    # Mixing PII cube with cube on another engine -> pii_cross_engine
    with pytest.raises(RoutingError) as exc_mix_cubes:
        router.route(
            "SELECT a.cnt FROM patient_cube a JOIN public_cube b ON a.province = b.province"
        )
    assert exc_mix_cubes.value.code == "pii_cross_engine"

    # Hint targeting different engine on PII cube -> pii_cross_engine
    for hint_sql_or_arg in (
        ("SELECT province, cnt FROM patient_cube", "lb"),
        ("/* seeknal:engine=lb */ SELECT province, cnt FROM patient_cube", None),
    ):
        with pytest.raises(RoutingError) as exc_bad_hint:
            router.route(hint_sql_or_arg[0], hint=hint_sql_or_arg[1])
        assert exc_bad_hint.value.code == "pii_cross_engine"


def test_e_m2_discovery_initial_refresh_failure_exits_2_before_serving(
    tmp_path, monkeypatch, capsys
):
    import base64
    import json
    from unittest.mock import MagicMock

    import yaml
    from cryptography.hazmat.primitives.asymmetric import ec

    import seeknal.serving.server.app as app_mod
    from seeknal.serving.__main__ import main as serving_main

    priv = ec.generate_private_key(ec.SECP256R1())
    pn = priv.public_key().public_numbers()
    jwk = {
        "kty": "EC",
        "crv": "P-256",
        "x": base64.urlsafe_b64encode(pn.x.to_bytes(32, "big")).rstrip(b"=").decode("ascii"),
        "y": base64.urlsafe_b64encode(pn.y.to_bytes(32, "big")).rstrip(b"=").decode("ascii"),
        "kid": "em2-kid",
    }
    jwks_path = tmp_path / "grants.jwks.json"
    jwks_path.write_text(json.dumps({"keys": [jwk]}), encoding="utf-8")

    lb_secret = "SECRET_LB_PW_EM2_778899"
    sr_secret = "SECRET_SR_PW_EM2_112233"
    lk_secret = "SECRET_LK_OAUTH_EM2_445566"
    s3_secret = "SECRET_S3_KEY_EM2_990011"
    monkeypatch.setenv(
        "EM2_LB_DSN", f"postgresql://svc:{lb_secret}@lakebase.internal:5432/atlas"
    )
    monkeypatch.setenv("EM2_SR_PW", sr_secret)
    monkeypatch.setenv("EM2_LK_ID", "lk-id")
    monkeypatch.setenv("EM2_LK_SECRET", lk_secret)
    monkeypatch.setenv("EM2_S3_KEY", "AKIAEXAMPLE")
    monkeypatch.setenv("EM2_S3_SECRET", s3_secret)
    monkeypatch.setenv("SEEKNAL_CURSOR_KEY", "em2-cursor-signing-key-32-bytes-ok")

    ext_dir = tmp_path / "duck_exts"
    ext_dir.mkdir()

    # Empty _lakebase_sync_state -> 'lb' serve is omitted at runtime so only unapproved 'duck_ice' remains!
    empty_sqlite = sqlite3.connect(":memory:")
    empty_sqlite.execute(
        "CREATE TABLE _lakebase_sync_state (target_table TEXT PRIMARY KEY, snapshot_id INTEGER, synced_at TEXT)"
    )
    empty_sqlite.commit()

    def _fake_run_discovery_unapproved_duck(disc_data, *, client=None, lb_connect=None, env=None):
        return discover(
            disc_data,
            http_client=_mock_lakekeeper_client(tables=("patients",)),
            lakebase_connect=lambda: empty_sqlite,
            env=env or {},
            now=datetime(2026, 5, 1, 12, 1, 0, tzinfo=timezone.utc),
        )

    monkeypatch.setattr(app_mod, "_run_discovery", _fake_run_discovery_unapproved_duck)

    # 1. PII table falls back to unapproved DuckDB serve during initial discovery refresh -> exit 2, never serve
    cfg_unapproved_pii = tmp_path / "serving_disc_unapproved_pii.yaml"
    cfg_unapproved_pii.write_text(
        yaml.safe_dump(
            {
                "listen": {"host": "127.0.0.1", "port": 8765},
                "grant_keys": {"jwks_file": str(jwks_path)},
                "placement": {
                    "discovery": {
                        "default_catalog": "lakekeeper",
                        "lakekeeper": {
                            "url": "https://lk.internal:8181",
                            "warehouse": "main",
                        },
                        "engines": {
                            "lb": {"kind": "lakebase", "dialect": "postgres"},
                            "duck_ice": {
                                "kind": "duckdb",
                                "dialect": "duckdb",
                                "profile": "iceberg_direct",
                                "alias": "lake",
                            },
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
                },
                "connections": {
                    "lb": {
                        "kind": "lakebase",
                        "dsn_env": "EM2_LB_DSN",
                        "storage_class": "local",
                    },
                    "duck_ice": {
                        "kind": "duckdb",
                        "profile": "iceberg_direct",
                        "extension_directory": str(ext_dir),
                        "extensions": ["httpfs", "avro", "iceberg"],
                        "allowed_s3_prefixes": ["s3://atlas-wh/clinical/"],
                        "recycle_seconds": 120,
                        "iceberg_catalogs": {
                            "lake": {
                                "endpoint": "https://lk.internal:8181",
                                "warehouse": "main",
                                "token_uri": "https://idp.internal/token",
                                "client_id_env": "EM2_LK_ID",
                                "client_secret_env": "EM2_LK_SECRET",
                                "access_delegation": "vended",
                                "s3": {
                                    "endpoint": "s3.internal:9000",
                                    "region": "us-east-1",
                                    "key_id_env": "EM2_S3_KEY",
                                    "secret_env": "EM2_S3_SECRET",
                                    "use_ssl": True,
                                },
                            }
                        },
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    capsys.readouterr()
    fake_uvicorn_1 = MagicMock()
    rc_1 = serving_main(["--config", str(cfg_unapproved_pii)], run=fake_uvicorn_1)
    assert rc_1 == 2
    assert not fake_uvicorn_1.called
    cap_1 = capsys.readouterr()
    assert "Error: Initial discovery refresh failed" in cap_1.err
    assert "duck_ice" in cap_1.err
    assert "policy.pii_engines" in cap_1.err
    for secret in (lb_secret, sr_secret, lk_secret, s3_secret, "Traceback"):
        assert secret not in cap_1.err
        assert secret not in cap_1.out

    # 2. Unmatched pii_tables entry in live discovery refresh -> exit 2, never serve, no secret leak
    cfg_unmatched_pii = tmp_path / "serving_disc_unmatched_pii.yaml"
    cfg_unmatched_pii.write_text(
        yaml.safe_dump(
            {
                "listen": {"host": "127.0.0.1", "port": 8765},
                "grant_keys": {"jwks_file": str(jwks_path)},
                "placement": {
                    "discovery": {
                        "default_catalog": "lakekeeper",
                        "lakekeeper": {
                            "url": "https://lk.internal:8181",
                            "warehouse": "main",
                        },
                        "engines": {
                            "sr": {"kind": "starrocks", "dialect": "starrocks"},
                        },
                        "pii_tables": ["lakekeeper.clinical.patients_nonexistent"],
                    }
                },
                "connections": {
                    "sr": {
                        "kind": "starrocks",
                        "storage_class": "local",
                        "accounts": {
                            "interactive": {
                                "host": "sr.internal",
                                "port": 9030,
                                "user": "svc",
                                "password_env": "EM2_SR_PW",
                            }
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    fake_uvicorn_2 = MagicMock()
    rc_2 = serving_main(["--config", str(cfg_unmatched_pii)], run=fake_uvicorn_2)
    assert rc_2 == 2
    assert not fake_uvicorn_2.called
    cap_2 = capsys.readouterr()
    assert "Error: Initial discovery refresh failed" in cap_2.err
    assert "lakekeeper.clinical.patients_nonexistent" in cap_2.err
    for secret in (lb_secret, sr_secret, lk_secret, s3_secret, "Traceback"):
        assert secret not in cap_2.err
        assert secret not in cap_2.out

    # 3. Valid discovery PII table with StarRocks + synced Lakebase -> single serve 'lb' chosen, server starts (rc 0)
    synced_sqlite = sqlite3.connect(":memory:")
    synced_sqlite.execute(
        "CREATE TABLE _lakebase_sync_state (target_table TEXT PRIMARY KEY, snapshot_id INTEGER, synced_at TEXT)"
    )
    synced_sqlite.execute(
        "INSERT INTO _lakebase_sync_state VALUES (?, ?, ?)",
        ("public.patients", 700, "2026-05-01T12:00:00+00:00"),
    )
    synced_sqlite.commit()

    monkeypatch.setattr(
        app_mod,
        "_run_discovery",
        lambda disc_data, *, client=None, lb_connect=None, env=None: discover(
            disc_data,
            http_client=_mock_lakekeeper_client(tables=("patients",)),
            lakebase_connect=lambda: synced_sqlite,
            env=env or {},
            now=datetime(2026, 5, 1, 12, 1, 0, tzinfo=timezone.utc),
        ),
    )

    cfg_valid_pii = tmp_path / "serving_disc_valid_pii.yaml"
    cfg_valid_pii.write_text(
        yaml.safe_dump(
            {
                "listen": {"host": "127.0.0.1", "port": 8765},
                "grant_keys": {"jwks_file": str(jwks_path)},
                "placement": {
                    "discovery": {
                        "default_catalog": "lakekeeper",
                        "lakekeeper": {
                            "url": "https://lk.internal:8181",
                            "warehouse": "main",
                        },
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
                },
                "connections": {
                    "lb": {
                        "kind": "lakebase",
                        "dsn_env": "EM2_LB_DSN",
                        "storage_class": "local",
                    },
                    "sr": {
                        "kind": "starrocks",
                        "accounts": {
                            "interactive": {
                                "host": "sr.internal",
                                "port": 9030,
                                "user": "svc",
                                "password_env": "EM2_SR_PW",
                            }
                        },
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    fake_uvicorn_3 = MagicMock()
    rc_3 = serving_main(["--config", str(cfg_valid_pii)], run=fake_uvicorn_3)
    assert rc_3 == 0
    assert fake_uvicorn_3.called




