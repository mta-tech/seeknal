"""Tests for DuckDB iceberg_direct configuration model and validation."""

from __future__ import annotations

import json
import os
import stat
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving.server.config import (
    DuckDBConnectionConfig,
    IcebergCatalogConfig,
    IcebergS3Config,
    ServerConfig,
    ServerConfigError,
)


@pytest.fixture
def pub_pem_file(tmp_path):
    priv = ec.generate_private_key(ec.SECP256R1())
    pub_pem = (
        priv.public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode("ascii")
    )
    p = tmp_path / "pub.pem"
    p.write_text(pub_pem, encoding="utf-8")
    return str(p)


@pytest.fixture
def static_placement_files(tmp_path):
    pf = tmp_path / "placement.yaml"
    pf.write_text(
        "engines:\n"
        "  duck_ice:\n"
        "    kind: duckdb\n"
        "    dialect: duckdb\n"
        "tables:\n"
        "  lake.ns.events:\n"
        "    keys: [id]\n"
        "    row_count: 100\n"
        "    serve:\n"
        "      duck_ice: lake.ns.events\n"
        "cubes: {}\n",
        encoding="utf-8",
    )
    sf = tmp_path / "schemas.json"
    sf.write_text(json.dumps({"lake.ns.events": ["id", "ts"]}), encoding="utf-8")
    return str(pf), str(sf)


@pytest.fixture
def secrets_env_file(tmp_path):
    p = tmp_path / "iceberg.env"
    p.write_text(
        "LK_CLIENT_ID=fake_client_id_value\n"
        "LK_CLIENT_SECRET=fake_client_secret_value_do_not_leak\n"
        "S3_ACCESS_KEY_ID=fake_s3_key_id\n"
        "S3_SECRET_ACCESS_KEY=fake_s3_secret_key_do_not_leak\n",
        encoding="utf-8",
    )
    os.chmod(p, stat.S_IRUSR | stat.S_IWUSR)
    return str(p)


def _base_iceberg_direct_config(
    tmp_path,
    pub_pem_file: str,
    placement_files: tuple[str, str],
    secrets_file: str | None = None,
) -> dict[str, Any]:
    pf, sf = placement_files
    ext_dir = tmp_path / "extensions"
    ext_dir.mkdir(exist_ok=True)
    temp_dir = tmp_path / "duck_tmp"
    temp_dir.mkdir(exist_ok=True)

    d: dict[str, Any] = {
        "grant_keys": {"pem_files": {"k1": pub_pem_file}},
        "placement": {
            "static": {
                "placement_file": pf,
                "schemas_file": sf,
            }
        },
        "connections": {
            "duck_ice": {
                "kind": "duckdb",
                "profile": "iceberg_direct",
                "extension_directory": str(ext_dir),
                "extensions": ["httpfs", "avro", "iceberg"],
                "allowed_s3_prefixes": [
                    "s3://warehouse-bucket/analytics/",
                    "s3://warehouse-bucket/shared/curated/",
                ],
                "recycle_seconds": 240,
                "token_lifetime_seconds": 300,
                "temp_directory": str(temp_dir),
                "max_temp_directory_size": "2GB",
                "memory_limit": "1GB",
                "threads": 4,
                "max_concurrent_cursors": 16,
                "iceberg_catalogs": {
                    "lake": {
                        "endpoint": "https://lakekeeper.internal.example/catalog",
                        "warehouse": "main_warehouse",
                        "token_uri": "https://idp.internal.example/oauth2/token",
                        "client_id_env": "LK_CLIENT_ID",
                        "client_secret_env": "LK_CLIENT_SECRET",
                        "scope": "lakekeeper:read",
                        "access_delegation": "vended",
                        "s3": {
                            "endpoint": "s3.internal.example:9000",
                            "region": "ap-southeast-1",
                            "url_style": "path",
                            "key_id_env": "S3_ACCESS_KEY_ID",
                            "secret_env": "S3_SECRET_ACCESS_KEY",
                            "use_ssl": True,
                        },
                    }
                },
            }
        },
    }
    if secrets_file is not None:
        d["secrets_env_file"] = secrets_file
    return d


def test_valid_iceberg_direct_config_and_secrets_env_resolution(
    tmp_path,
    pub_pem_file,
    static_placement_files,
    secrets_env_file,
):
    raw = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    cfg = ServerConfig.from_dict(raw)
    conn = cfg.connections["duck_ice"]
    assert isinstance(conn, DuckDBConnectionConfig)
    assert conn.profile == "iceberg_direct"
    assert conn.path == ":memory:"
    assert conn.extensions == ("httpfs", "avro", "iceberg")
    assert conn.allowed_s3_prefixes == (
        "s3://warehouse-bucket/analytics/",
        "s3://warehouse-bucket/shared/curated/",
    )
    assert conn.recycle_seconds == 240.0
    assert conn.token_lifetime_seconds == 300.0
    assert conn.external_access_after_attach == "off"
    assert conn.memory_limit == "1GB"
    assert conn.threads == 4
    assert conn.max_temp_directory_size == "2GB"
    assert conn.max_concurrent_cursors == 16

    assert "lake" in conn.iceberg_catalogs
    cat = conn.iceberg_catalogs["lake"]
    assert isinstance(cat, IcebergCatalogConfig)
    assert cat.alias == "lake"
    assert cat.endpoint == "https://lakekeeper.internal.example/catalog"
    assert cat.warehouse == "main_warehouse"
    assert cat.token_uri == "https://idp.internal.example/oauth2/token"
    assert cat.client_id_env == "LK_CLIENT_ID"
    assert cat.client_secret_env == "LK_CLIENT_SECRET"
    assert cat.scope == "lakekeeper:read"
    assert cat.access_delegation == "vended"
    assert isinstance(cat.s3, IcebergS3Config)
    assert cat.s3.endpoint == "s3.internal.example:9000"
    assert cat.s3.region == "ap-southeast-1"
    assert cat.s3.url_style == "path"
    assert cat.s3.key_id_env == "S3_ACCESS_KEY_ID"
    assert cat.s3.secret_env == "S3_SECRET_ACCESS_KEY"
    assert cat.s3.use_ssl is True

    # Referenced env vars include all 4 env names and resolve via secrets_env_file
    refs = cfg.get_referenced_env_vars()
    for var_name in (
        "LK_CLIENT_ID",
        "LK_CLIENT_SECRET",
        "S3_ACCESS_KEY_ID",
        "S3_SECRET_ACCESS_KEY",
    ):
        assert var_name in refs

    preflight = cfg.validate_runtime_wiring(env={})
    assert preflight.mode == "static"


def test_local_file_default_profile_unchanged(
    tmp_path,
    pub_pem_file,
    static_placement_files,
):
    pf, sf = static_placement_files
    d = {
        "grant_keys": {"pem_files": {"k1": pub_pem_file}},
        "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
        "connections": {
            "duck_ice": {
                "kind": "duckdb",
                "path": str(tmp_path / "local.duckdb"),
                "allowed_directories": [str(tmp_path) + "/"],
            }
        },
    }
    cfg = ServerConfig.from_dict(d)
    conn = cfg.connections["duck_ice"]
    assert isinstance(conn, DuckDBConnectionConfig)
    assert conn.profile == "local_file"
    assert conn.path == str(tmp_path / "local.duckdb")


@pytest.mark.parametrize(
    "pg_key",
    [
        "postgres",
        "postgresql",
        "postgres_dsn",
        "postgres_attach",
        "attach",
        "lakebase",
        "pg",
    ],
)
def test_postgres_and_attach_keys_rejected(
    tmp_path,
    pub_pem_file,
    static_placement_files,
    secrets_env_file,
    pg_key,
):
    raw = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    raw["connections"]["duck_ice"][pg_key] = "anything"
    with pytest.raises(ServerConfigError, match="forbids Postgres/attach key"):
        ServerConfig.from_dict(raw)


def test_unknown_keys_rejected_at_every_level(
    tmp_path,
    pub_pem_file,
    static_placement_files,
    secrets_env_file,
):
    # Connection level
    raw1 = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    raw1["connections"]["duck_ice"]["extra_option"] = 123
    with pytest.raises(ServerConfigError, match="Unknown key"):
        ServerConfig.from_dict(raw1)

    # Path is forbidden in iceberg_direct
    raw_path = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    raw_path["connections"]["duck_ice"]["path"] = "/tmp/cache.duckdb"
    with pytest.raises(ServerConfigError, match="forbids 'path'"):
        ServerConfig.from_dict(raw_path)

    # Catalog level
    raw2 = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    raw2["connections"]["duck_ice"]["iceberg_catalogs"]["lake"]["unknown_cat_field"] = "x"
    with pytest.raises(ServerConfigError, match="Unknown key"):
        ServerConfig.from_dict(raw2)

    # S3 level
    raw3 = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    raw3["connections"]["duck_ice"]["iceberg_catalogs"]["lake"]["s3"]["unknown_s3_field"] = "x"
    with pytest.raises(ServerConfigError, match="Unknown key"):
        ServerConfig.from_dict(raw3)


@pytest.mark.parametrize(
    "bad_exts",
    [
        [],
        ["postgres_scanner"],
        ["httpfs", "sqlite_scanner"],
        ["iceberg", "fts"],
        [123],
    ],
)
def test_extensions_allowlist_validation(
    tmp_path,
    pub_pem_file,
    static_placement_files,
    secrets_env_file,
    bad_exts,
):
    raw = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    raw["connections"]["duck_ice"]["extensions"] = bad_exts
    with pytest.raises(ServerConfigError):
        ServerConfig.from_dict(raw)


@pytest.mark.parametrize(
    "bad_ext_dir",
    [
        "",
        "relative/extensions",
        "/opt/duckdb/../extensions",
    ],
)
def test_extension_directory_must_be_absolute_normalized(
    tmp_path,
    pub_pem_file,
    static_placement_files,
    secrets_env_file,
    bad_ext_dir,
):
    raw = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    raw["connections"]["duck_ice"]["extension_directory"] = bad_ext_dir
    with pytest.raises(ServerConfigError):
        ServerConfig.from_dict(raw)


@pytest.mark.parametrize(
    "bad_alias",
    [
        "Lake",
        "1lake",
        "lake-cat",
        "lake.cat",
        "lake;DROP TABLE x",
        "system",
        "temp",
        "memory",
        "main",
        "a" * 33,
    ],
)
def test_catalog_alias_validation(
    tmp_path,
    pub_pem_file,
    static_placement_files,
    secrets_env_file,
    bad_alias,
):
    raw = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    cat_cfg = raw["connections"]["duck_ice"]["iceberg_catalogs"].pop("lake")
    raw["connections"]["duck_ice"]["iceberg_catalogs"][bad_alias] = cat_cfg
    with pytest.raises(ServerConfigError):
        ServerConfig.from_dict(raw)


@pytest.mark.parametrize(
    "bad_prefixes",
    [
        [],
        ["http://bucket/prefix/"],
        ["s3://bucket/prefix_without_trailing_slash"],
        ["s3://bucket/"],
        ["s3://bucket/../escape/"],
        ["s3://bucket/double//slash/"],
    ],
)
def test_allowed_s3_prefixes_validation(
    tmp_path,
    pub_pem_file,
    static_placement_files,
    secrets_env_file,
    bad_prefixes,
):
    raw = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    raw["connections"]["duck_ice"]["allowed_s3_prefixes"] = bad_prefixes
    with pytest.raises(ServerConfigError):
        ServerConfig.from_dict(raw)


@pytest.mark.parametrize(
    "recycle_s,conn_tl,cat_tl",
    [
        (0, None, None),
        (-10, None, None),
        (300, 300, None),
        (350, 300, None),
        (200, 300, 180),
        (180, None, 180),
    ],
)
def test_recycle_seconds_and_token_lifetime_bounds(
    tmp_path,
    pub_pem_file,
    static_placement_files,
    secrets_env_file,
    recycle_s,
    conn_tl,
    cat_tl,
):
    raw = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    conn_raw = raw["connections"]["duck_ice"]
    conn_raw["recycle_seconds"] = recycle_s
    if conn_tl is None:
        conn_raw.pop("token_lifetime_seconds", None)
    else:
        conn_raw["token_lifetime_seconds"] = conn_tl
    if cat_tl is not None:
        conn_raw["iceberg_catalogs"]["lake"]["token_lifetime_seconds"] = cat_tl
    with pytest.raises(ServerConfigError):
        ServerConfig.from_dict(raw)


def test_yaml_literal_secret_keys_and_values_rejected_without_leaking(
    tmp_path,
    pub_pem_file,
    static_placement_files,
    secrets_env_file,
):
    sentinel = "SUPER_SECRET_LITERAL_CANARY_VALUE_999"

    # 1. Literal client_secret key in catalog
    raw1 = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    raw1["connections"]["duck_ice"]["iceberg_catalogs"]["lake"]["client_secret"] = sentinel
    with pytest.raises(ServerConfigError) as exc1:
        ServerConfig.from_dict(raw1)
    assert sentinel not in str(exc1.value)

    # 2. Literal secret key in s3
    raw2 = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    raw2["connections"]["duck_ice"]["iceberg_catalogs"]["lake"]["s3"]["secret"] = sentinel
    with pytest.raises(ServerConfigError) as exc2:
        ServerConfig.from_dict(raw2)
    assert sentinel not in str(exc2.value)

    # 3. Putting a secret URI/value into client_secret_env instead of an env var name
    raw3 = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    raw3["connections"]["duck_ice"]["iceberg_catalogs"]["lake"]["client_secret_env"] = (
        f"https://user:{sentinel}@example.com"
    )
    with pytest.raises(ServerConfigError) as exc3:
        ServerConfig.from_dict(raw3)
    assert sentinel not in str(exc3.value)


def test_secrets_env_scoped_to_referenced_vars_and_hidden_from_repr(
    tmp_path,
    pub_pem_file,
    static_placement_files,
):
    sec_path = tmp_path / "multi_engine.env"
    duck_secret = "DUCK_ICEBERG_SECRET_CANARY_111"
    other_engine_secret = "STARROCKS_ADMIN_PASSWORD_CANARY_222"
    sec_path.write_text(
        f"LK_CLIENT_ID=fake_client_id\n"
        f"LK_CLIENT_SECRET={duck_secret}\n"
        f"S3_ACCESS_KEY_ID=fake_s3_key\n"
        f"S3_SECRET_ACCESS_KEY={duck_secret}\n"
        f"OTHER_ENGINE_PASSWORD={other_engine_secret}\n",
        encoding="utf-8",
    )
    os.chmod(sec_path, stat.S_IRUSR | stat.S_IWUSR)

    raw = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, str(sec_path)
    )
    cfg = ServerConfig.from_dict(raw)
    conn = cfg.connections["duck_ice"]

    # 1. Unreferenced variables from secrets_env_file are NOT copied into DuckDBConnectionConfig.secrets_env
    assert "OTHER_ENGINE_PASSWORD" not in conn.secrets_env
    assert set(conn.secrets_env.keys()) == {
        "LK_CLIENT_ID",
        "LK_CLIENT_SECRET",
        "S3_ACCESS_KEY_ID",
        "S3_SECRET_ACCESS_KEY",
    }

    # 2. Neither repr(conn) nor repr(cfg) contains any secret values
    assert duck_secret not in repr(conn)
    assert other_engine_secret not in repr(conn)
    assert duck_secret not in repr(cfg)
    assert other_engine_secret not in repr(cfg)


def test_iceberg_direct_requires_avro_when_iceberg_configured(
    tmp_path,
    pub_pem_file,
    static_placement_files,
    secrets_env_file,
):
    # 1. Missing 'avro' when 'iceberg' is configured raises exact required ServerConfigError
    for exts_without_avro in (["httpfs", "iceberg"], ["iceberg"]):
        raw = _base_iceberg_direct_config(
            tmp_path, pub_pem_file, static_placement_files, secrets_env_file
        )
        raw["connections"]["duck_ice"]["extensions"] = exts_without_avro
        with pytest.raises(
            ServerConfigError,
            match="DuckDB iceberg_direct connection 'duck_ice' requires 'avro' in 'extensions' when 'iceberg' is configured",
        ):
            ServerConfig.from_dict(raw)

    # 2. Invalid extension message lists allowlist {'avro', 'httpfs', 'iceberg'}
    raw_bad = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    raw_bad["connections"]["duck_ice"]["extensions"] = ["httpfs", "avro", "parquet"]
    with pytest.raises(
        ServerConfigError,
        match=r"DuckDB iceberg_direct connection 'duck_ice' extension 'parquet' is not in allowlist \{'avro', 'httpfs', 'iceberg'\}",
    ):
        ServerConfig.from_dict(raw_bad)


def test_iceberg_direct_orders_avro_before_iceberg(
    tmp_path,
    pub_pem_file,
    static_placement_files,
    secrets_env_file,
):
    from seeknal.serving.duckdb_policy import plan_iceberg_direct

    for input_exts, expected_exts in (
        (["httpfs", "iceberg", "avro"], ("httpfs", "avro", "iceberg")),
        (["iceberg", "avro", "httpfs"], ("avro", "iceberg", "httpfs")),
        (["iceberg", "httpfs", "avro"], ("avro", "iceberg", "httpfs")),
        (["httpfs", "avro", "iceberg"], ("httpfs", "avro", "iceberg")),
        (["avro", "httpfs", "iceberg"], ("avro", "httpfs", "iceberg")),
        (["iceberg", "avro"], ("avro", "iceberg")),
    ):
        raw = _base_iceberg_direct_config(
            tmp_path, pub_pem_file, static_placement_files, secrets_env_file
        )
        raw["connections"]["duck_ice"]["extensions"] = input_exts
        cfg = ServerConfig.from_dict(raw)
        conn = cfg.connections["duck_ice"]
        assert conn.extensions == expected_exts
        assert conn.extensions.index("avro") < conn.extensions.index("iceberg")

        steps = plan_iceberg_direct(conn)
        step_names = [s.name for s in steps]
        assert step_names.index("b_load_extension_avro") < step_names.index(
            "b_load_extension_iceberg"
        )


def test_iceberg_direct_fails_early_when_avro_file_missing_from_extension_directory(
    tmp_path,
    pub_pem_file,
    static_placement_files,
    secrets_env_file,
):
    from seeknal.serving.startup_validation import StartupValidationError, validate_startup

    populated_ext_dir = tmp_path / "ext_populated"
    plat_dir = populated_ext_dir / "v2.0.0" / "linux_amd64"
    plat_dir.mkdir(parents=True)
    (plat_dir / "httpfs.duckdb_extension").write_bytes(b"fake-httpfs")
    (plat_dir / "iceberg.duckdb_extension").write_bytes(b"fake-iceberg")

    raw = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    raw["connections"]["duck_ice"]["extension_directory"] = str(populated_ext_dir)
    raw["connections"]["duck_ice"]["extensions"] = ["httpfs", "avro", "iceberg"]

    # 1. ServerConfig.from_dict fails early naming avro.duckdb_extension and extension_directory
    with pytest.raises(ServerConfigError) as exc_cfg:
        ServerConfig.from_dict(raw)
    assert "avro.duckdb_extension" in str(exc_cfg.value)
    assert str(populated_ext_dir) in str(exc_cfg.value)

    # 2. Startup validation fails early naming avro.duckdb_extension
    with pytest.raises(StartupValidationError) as exc_val:
        validate_startup(raw, env={})
    assert "avro.duckdb_extension" in str(exc_val.value)
    assert str(populated_ext_dir) in str(exc_val.value)

    # 3. Synthetic non-existent path on host is allowed during unit config parsing
    raw_synthetic = _base_iceberg_direct_config(
        tmp_path, pub_pem_file, static_placement_files, secrets_env_file
    )
    raw_synthetic["connections"]["duck_ice"]["extension_directory"] = (
        "/opt/nonexistent_synthetic_duckdb_exts"
    )
    cfg_syn = ServerConfig.from_dict(raw_synthetic)
    assert cfg_syn.connections["duck_ice"].extensions == ("httpfs", "avro", "iceberg")

    # 4. Once avro.duckdb_extension is present in populated extension_directory, both succeed
    (plat_dir / "avro.duckdb_extension").write_bytes(b"fake-avro")
    cfg_ok = ServerConfig.from_dict(raw)
    assert cfg_ok.connections["duck_ice"].extensions == ("httpfs", "avro", "iceberg")
    rep = validate_startup(raw, env={})
    assert rep.ok is True


