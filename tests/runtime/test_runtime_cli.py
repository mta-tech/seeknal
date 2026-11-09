"""Tests for seeknal.runtime CLI commands (validate and resolve)."""

import copy
import hashlib
from pathlib import Path
import pytest
import yaml

from seeknal.runtime.__main__ import main


def make_manifest_yaml(name: str, channel: str = "stable") -> str:
    digest_hex = hashlib.sha256(name.encode()).hexdigest()
    data = {
        "apiVersion": "atlas.io/v1",
        "kind": "RuntimeRelease",
        "metadata": {
            "name": name,
            "channel": channel,
        },
        "seeknal": {
            "version": "2.14.0",
            "commit": "a" * 40,
            "lock_sha256": "b" * 64,
        },
        "python": "3.11.9",
        "images": {
            "atlas-rt": {
                "digest": f"sha256:{digest_hex}",
            },
            "atlas-rt-serving": {
                "digest": f"sha256:{'1' * 64}",
            },
        },
        "engines": {
            "duckdb": "1.4.4",
        },
        "contracts": {
            "grant": ["v2"],
            "cube_mode": ["jwt"],
        },
        "serving": {
            "grant_max_lifetime_seconds": 60,
        },
        "acu_weights": {
            "job": 1.0,
            "session": 1.5,
            "warehouse": 2.0,
            "sandbox": 1.0,
        },
        "licence_scan": "clean",
    }
    return yaml.dump(data)


# --- validate command tests ---


def test_cli_validate_single_valid(capsys):
    valid_file = "src/seeknal/runtime/examples/atlas-rt-1.0.0.yaml"
    exit_code = main(["validate", valid_file])
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "OK atlas-rt/1.0.0" in out


def test_cli_validate_multiple_valid(tmp_path: Path, capsys):
    f1 = tmp_path / "rt-1.yaml"
    f2 = tmp_path / "rt-2.yaml"
    f1.write_text(make_manifest_yaml("atlas-rt/1.0.1"))
    f2.write_text(make_manifest_yaml("atlas-rt/1.0.2"))

    exit_code = main(["validate", str(f1), str(f2)])
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "OK atlas-rt/1.0.1" in out
    assert "OK atlas-rt/1.0.2" in out


def test_cli_validate_invalid_manifest(tmp_path: Path, capsys):
    bad = tmp_path / "bad.yaml"
    bad.write_text("apiVersion: atlas.io/v999\nkind: Unknown")

    exit_code = main(["validate", str(bad)])
    assert exit_code == 1

    out = capsys.readouterr().out
    assert "ERR" in out
    assert "apiVersion" in out


def test_cli_validate_mixed(tmp_path: Path, capsys):
    good = tmp_path / "good.yaml"
    bad = tmp_path / "bad.yaml"
    good.write_text(make_manifest_yaml("atlas-rt/1.0.0"))
    bad.write_text("invalid: yaml: doc")

    exit_code = main(["validate", str(good), str(bad)])
    assert exit_code == 1

    out = capsys.readouterr().out
    assert "OK atlas-rt/1.0.0" in out
    assert "ERR" in out


def test_cli_validate_missing_file(capsys):
    exit_code = main(["validate", "nonexistent-file.yaml"])
    assert exit_code == 1

    out = capsys.readouterr().out
    assert "ERR" in out
    assert "file not found" in out


# --- resolve command tests ---


def test_cli_resolve_line_pin(tmp_path: Path, capsys):
    # Setup project
    proj_dir = tmp_path / "project"
    proj_dir.mkdir()
    (proj_dir / "seeknal_project.yml").write_text(
        yaml.dump({"runtime": "atlas-rt/1.0"})
    )

    # Setup releases
    rel_dir = tmp_path / "releases"
    rel_dir.mkdir()
    (rel_dir / "rt-1.0.0.yaml").write_text(make_manifest_yaml("atlas-rt/1.0.0"))
    (rel_dir / "rt-1.0.1.yaml").write_text(make_manifest_yaml("atlas-rt/1.0.1"))

    exit_code = main(
        ["resolve", "--project", str(proj_dir), "--releases", str(rel_dir)]
    )
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "atlas-rt/1.0.1" in out
    assert "ghcr.io/mta-tech/atlas-rt@sha256:" in out


def test_cli_resolve_env_override(tmp_path: Path, capsys):
    proj_dir = tmp_path / "project"
    proj_dir.mkdir()
    (proj_dir / "seeknal_project.yml").write_text(
        yaml.dump(
            {
                "runtime": "atlas-rt/1.0",
                "environments": {
                    "prod": {"runtime": "atlas-rt/1.0.5"},
                },
            }
        )
    )

    rel_dir = tmp_path / "releases"
    rel_dir.mkdir()
    (rel_dir / "rt-1.0.1.yaml").write_text(make_manifest_yaml("atlas-rt/1.0.1"))
    (rel_dir / "rt-1.0.5.yaml").write_text(
        make_manifest_yaml("atlas-rt/1.0.5", channel="preview")
    )

    exit_code = main(
        [
            "resolve",
            "--project",
            str(proj_dir),
            "--releases",
            str(rel_dir),
            "--env",
            "prod",
        ]
    )
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "atlas-rt/1.0.5" in out


def test_cli_resolve_custom_registry(tmp_path: Path, capsys):
    proj_dir = tmp_path / "project"
    proj_dir.mkdir()
    (proj_dir / "seeknal_project.yml").write_text(
        yaml.dump({"runtime": "atlas-rt/1.0.0"})
    )

    rel_dir = tmp_path / "releases"
    rel_dir.mkdir()
    (rel_dir / "rt-1.0.0.yaml").write_text(make_manifest_yaml("atlas-rt/1.0.0"))

    exit_code = main(
        [
            "resolve",
            "--project",
            str(proj_dir),
            "--releases",
            str(rel_dir),
            "--registry",
            "registry.internal.corp/seeknal",
        ]
    )
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "registry.internal.corp/seeknal/atlas-rt@sha256:" in out


def test_cli_resolve_no_pin_exits_2(tmp_path: Path, capsys):
    proj_dir = tmp_path / "project"
    proj_dir.mkdir()
    (proj_dir / "seeknal_project.yml").write_text(yaml.dump({"name": "unpinned_proj"}))

    rel_dir = tmp_path / "releases"
    rel_dir.mkdir()

    exit_code = main(
        ["resolve", "--project", str(proj_dir), "--releases", str(rel_dir)]
    )
    assert exit_code == 2

    captured = capsys.readouterr()
    assert "no runtime pin" in (captured.out + captured.err)


def test_cli_resolve_unresolvable_pin_exits_1(tmp_path: Path, capsys):
    proj_dir = tmp_path / "project"
    proj_dir.mkdir()
    (proj_dir / "seeknal_project.yml").write_text(
        yaml.dump({"runtime": "atlas-rt/9.9"})
    )

    rel_dir = tmp_path / "releases"
    rel_dir.mkdir()
    (rel_dir / "rt-1.0.0.yaml").write_text(make_manifest_yaml("atlas-rt/1.0.0"))

    exit_code = main(
        ["resolve", "--project", str(proj_dir), "--releases", str(rel_dir)]
    )
    assert exit_code == 1


def test_cli_resolve_skips_invalid_manifest(tmp_path: Path, capsys):
    proj_dir = tmp_path / "project"
    proj_dir.mkdir()
    (proj_dir / "seeknal_project.yml").write_text(
        yaml.dump({"runtime": "atlas-rt/1.0.0"})
    )

    rel_dir = tmp_path / "releases"
    rel_dir.mkdir()
    (rel_dir / "rt-1.0.0.yaml").write_text(make_manifest_yaml("atlas-rt/1.0.0"))
    (rel_dir / "broken.yaml").write_text("invalid_manifest_content")

    exit_code = main(
        ["resolve", "--project", str(proj_dir), "--releases", str(rel_dir)]
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "atlas-rt/1.0.0" in captured.out
    assert "broken.yaml" in captured.err


def test_cli_resolve_missing_releases_dir(tmp_path: Path, capsys):
    proj_dir = tmp_path / "project"
    proj_dir.mkdir()
    (proj_dir / "seeknal_project.yml").write_text(
        yaml.dump({"runtime": "atlas-rt/1.0.0"})
    )

    exit_code = main(
        [
            "resolve",
            "--project",
            str(proj_dir),
            "--releases",
            str(tmp_path / "nonexistent"),
        ]
    )
    assert exit_code == 1


def test_cli_invalid_arguments():
    # Calling with invalid arguments returns non-zero (2)
    exit_code = main(["unknown_subcommand"])
    assert exit_code == 2


# --- plan command tests ---


def test_cli_plan_success(tmp_path: Path, capsys):
    proj_dir = tmp_path / "project"
    sources_dir = proj_dir / "seeknal" / "sources"
    sources_dir.mkdir(parents=True)
    (sources_dir / "raw.yml").write_text("""\
kind: source
name: raw
source: iceberg
table: warehouse.raw.data
""")

    transforms_dir = proj_dir / "seeknal" / "transforms"
    transforms_dir.mkdir(parents=True)
    (transforms_dir / "agg.yml").write_text("""\
kind: transform
name: agg
transform: SELECT * FROM source.raw
inputs:
  - ref: source.raw
materializations:
  - type: iceberg
    table: warehouse.analytics.summary
    enabled: true
""")

    exit_code = main(["plan", "--project", str(proj_dir)])
    assert exit_code == 0

    captured = capsys.readouterr()
    assert '"reads"' in captured.out
    assert '"writes"' in captured.out
    assert "warehouse.raw.data" in captured.out
    assert "warehouse.analytics.summary" in captured.out
    assert "plan_sha256=" in captured.err


def test_cli_plan_with_env_and_out(tmp_path: Path, capsys):
    proj_dir = tmp_path / "project"
    sources_dir = proj_dir / "seeknal" / "sources"
    sources_dir.mkdir(parents=True)
    (sources_dir / "raw.yml").write_text("""\
kind: source
name: raw
source: iceberg
table: warehouse.raw.data
""")

    transforms_dir = proj_dir / "seeknal" / "transforms"
    transforms_dir.mkdir(parents=True)
    (transforms_dir / "agg.yml").write_text("""\
kind: transform
name: agg
transform: SELECT * FROM source.raw
inputs:
  - ref: source.raw
materializations:
  - type: iceberg
    table: warehouse.analytics.summary
    enabled: true
""")

    out_file = tmp_path / "out_plan.json"
    exit_code = main(
        [
            "plan",
            "--project",
            str(proj_dir),
            "--env",
            "staging",
            "--out",
            str(out_file),
        ]
    )
    assert exit_code == 0
    assert out_file.is_file()
    content = out_file.read_text("utf-8")
    assert "warehouse.staging_analytics.summary" in content

    captured = capsys.readouterr()
    assert "plan_sha256=" in captured.err


def test_cli_plan_error_exits_2(tmp_path: Path, capsys):
    proj_dir = tmp_path / "project"
    sources_dir = proj_dir / "seeknal" / "sources"
    sources_dir.mkdir(parents=True)
    (sources_dir / "bad.yml").write_text("""\
kind: source
name: bad
source: iceberg
table: not_three_parts
""")

    exit_code = main(["plan", "--project", str(proj_dir)])
    assert exit_code == 2

    captured = capsys.readouterr()
    assert "ERR:" in captured.err


# --- verify-grant command tests ---


def _create_jwks_and_grant(now: int, plan_hash: str = "a" * 64):
    import base64
    import json
    from cryptography.hazmat.primitives.asymmetric import ec
    from seeknal.serving.grant_token import sign_grant_token

    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    kid = "key-test-cli"

    numbers = pub.public_numbers()
    x_bytes = numbers.x.to_bytes(32, "big")
    y_bytes = numbers.y.to_bytes(32, "big")
    x_b64 = base64.urlsafe_b64encode(x_bytes).rstrip(b"=").decode("ascii")
    y_b64 = base64.urlsafe_b64encode(y_bytes).rstrip(b"=").decode("ascii")

    jwks = {
        "keys": [
            {
                "kty": "EC",
                "crv": "P-256",
                "kid": kid,
                "x": x_b64,
                "y": y_b64,
            }
        ]
    }

    claims = {
        "typ": "run",
        "iss": "atlas-catalog",
        "aud": "atlas-rt-run",
        "sub": "user:cli-test",
        "principal": "svc:cli:test",
        "ws": "ws-cli",
        "jti": "550e8400-e29b-41d4-a716-446655440000",
        "release": "rel-cli",
        "plan_sha256": plan_hash,
        "trigger": {"type": "user", "id": "tester"},
        "iat": now,
        "nbf": now,
        "exp": now + 600,
        "reads": [{"table": "warehouse.raw.data", "actions": ["read"]}],
        "writes": [{"table": "warehouse.analytics.summary", "actions": ["write"]}],
        "connections": [],
    }

    token = sign_grant_token(claims, priv, kid=kid)
    return token, json.dumps(jwks)


def test_cli_verify_grant_success(tmp_path: Path, capsys):
    now = 1700000000
    token, jwks_json = _create_jwks_and_grant(now)

    grant_file = tmp_path / "grant.jwt"
    grant_file.write_text(token)
    jwks_file = tmp_path / "jwks.json"
    jwks_file.write_text(jwks_json)

    # Monkeypatch time if necessary, or pass a fresh token with current timestamp
    import time
    curr_now = int(time.time())
    token_curr, jwks_curr = _create_jwks_and_grant(curr_now)
    grant_file.write_text(token_curr)
    jwks_file.write_text(jwks_curr)

    exit_code = main(
        ["verify-grant", "--grant", str(grant_file), "--jwks", str(jwks_file)]
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "OK" in captured.out


def test_cli_verify_grant_with_plan(tmp_path: Path, capsys):
    import time
    import json
    from seeknal.runtime.run_plan import RunPlan, TableAccess, plan_sha256

    plan = RunPlan(
        reads=(TableAccess(table="warehouse.raw.data", actions=("read",)),),
        writes=(TableAccess(table="warehouse.analytics.summary", actions=("write",)),),
        connections=(),
        nodes=(),
    )
    plan_digest = plan_sha256(plan)

    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps(plan.to_dict()))

    curr_now = int(time.time())
    token, jwks_json = _create_jwks_and_grant(curr_now, plan_hash=plan_digest)

    grant_file = tmp_path / "grant.jwt"
    grant_file.write_text(token)
    jwks_file = tmp_path / "jwks.json"
    jwks_file.write_text(jwks_json)

    exit_code = main(
        [
            "verify-grant",
            "--grant",
            str(grant_file),
            "--jwks",
            str(jwks_file),
            "--plan",
            str(plan_file),
        ]
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "OK" in captured.out


def test_cli_verify_grant_uncovered_plan_exits_1(tmp_path: Path, capsys):
    import time
    import json
    from seeknal.runtime.run_plan import RunPlan, TableAccess

    plan = RunPlan(
        reads=(),
        writes=(TableAccess(table="warehouse.uncovered.sink", actions=("write",)),),
        connections=(),
        nodes=(),
    )
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps(plan.to_dict()))

    curr_now = int(time.time())
    token, jwks_json = _create_jwks_and_grant(curr_now)

    grant_file = tmp_path / "grant.jwt"
    grant_file.write_text(token)
    jwks_file = tmp_path / "jwks.json"
    jwks_file.write_text(jwks_json)

    exit_code = main(
        [
            "verify-grant",
            "--grant",
            str(grant_file),
            "--jwks",
            str(jwks_file),
            "--plan",
            str(plan_file),
        ]
    )
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "ERR:" in captured.err


def test_cli_verify_grant_missing_files_exits_1(tmp_path: Path):
    exit_code = main(
        [
            "verify-grant",
            "--grant",
            str(tmp_path / "nonexistent.jwt"),
            "--jwks",
            str(tmp_path / "jwks.json"),
        ]
    )
    assert exit_code == 1
