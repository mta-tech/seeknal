"""Tests for DuckDB policy hardening, probe conformance checks, and CLI."""

from __future__ import annotations

import json
import os
import subprocess
import sys

import duckdb
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving import ServerConfig, build_server
from seeknal.serving.duckdb_policy import (
    DuckDBHolder,
    connect_hardened,
    harden_connection,
    main,
    probe,
)


@pytest.fixture
def temp_duckdb_file(tmp_path):
    db_file = tmp_path / "sample.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE users (id INT, name VARCHAR)")
    con.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
    con.close()
    return str(db_file)


def test_probe_passes_on_hardened_file(temp_duckdb_file):
    conn = connect_hardened(temp_duckdb_file, read_only=True)
    try:
        results = probe(conn)
        assert results["external_access_disabled"] is True
        assert results["config_locked"] is True
        assert results["read_text_outside_refused"] is True
        assert results["copy_to_refused"] is True
        assert results["attach_refused"] is True
        assert results["install_refused"] is True
        assert results["load_refused"] is True
        assert results["select_ok"] is True
        assert all(results.values())
    finally:
        conn.close()


def test_probe_fails_on_plain_connection(temp_duckdb_file):
    conn = duckdb.connect(temp_duckdb_file, read_only=True)
    try:
        results = probe(conn)
        assert results["external_access_disabled"] is False
        assert results["config_locked"] is False
        assert results["select_ok"] is True
    finally:
        conn.close()


def test_allowed_directories_permits_inside_refuses_outside(tmp_path):
    allowed_dir = tmp_path / "allowed"
    outside_dir = tmp_path / "outside"
    allowed_dir.mkdir()
    outside_dir.mkdir()

    inside_parquet = allowed_dir / "inside.parquet"
    outside_parquet = outside_dir / "outside.parquet"

    db_file = tmp_path / "policy_test.duckdb"
    con_init = duckdb.connect(str(db_file))
    con_init.execute("CREATE TABLE t (x INT)")
    con_init.execute("INSERT INTO t VALUES (10), (20)")
    con_init.execute(f"COPY t TO '{inside_parquet}' (FORMAT PARQUET)")
    con_init.execute(f"COPY t TO '{outside_parquet}' (FORMAT PARQUET)")
    con_init.close()

    allowed_path_str = str(allowed_dir) + "/"
    conn = connect_hardened(str(db_file), read_only=True, allowed_directories=[allowed_path_str])
    try:
        # File inside allowed_directories succeeds
        rows = conn.execute(f"SELECT * FROM read_parquet('{inside_parquet}')").fetchall()
        assert rows == [(10,), (20,)]

        # File outside allowed_directories is refused with duckdb.Error
        with pytest.raises(duckdb.Error):
            conn.execute(f"SELECT * FROM read_parquet('{outside_parquet}')")
    finally:
        conn.close()


@pytest.mark.parametrize(
    "bad_dir",
    [
        "relative/path/",
        "/path/without/slash",
        "/path/../normalized/",
        "/path//double/slash/",
        "",
        123,
    ],
)
def test_harden_connection_validates_allowed_directories(bad_dir):
    conn = duckdb.connect(":memory:")
    with pytest.raises(ValueError):
        harden_connection(conn, allowed_directories=[bad_dir])
    conn.close()


def test_harden_connection_validates_temp_directory(tmp_path):
    allowed_dir = tmp_path / "allowed"
    allowed_dir.mkdir()
    allowed_str = str(allowed_dir) + "/"

    inside_dir = allowed_dir / "temp"
    inside_dir.mkdir()

    outside_dir = tmp_path / "outside_temp"
    outside_dir.mkdir()

    conn = duckdb.connect(":memory:")
    # temp_directory inside allowed_directories raises ValueError
    with pytest.raises(ValueError, match="must not be inside allowed directory"):
        harden_connection(conn, allowed_directories=[allowed_str], temp_directory=str(inside_dir))

    # relative path raises ValueError
    with pytest.raises(ValueError, match="must be an absolute path"):
        harden_connection(conn, allowed_directories=[allowed_str], temp_directory="rel/temp")

    # non-existent path raises ValueError
    with pytest.raises(ValueError, match="does not exist"):
        harden_connection(conn, allowed_directories=[allowed_str], temp_directory=str(tmp_path / "non_existent"))

    # valid outside path succeeds
    conn2 = duckdb.connect(":memory:")
    harden_connection(conn2, allowed_directories=[allowed_str], temp_directory=str(outside_dir), max_temp_directory_size="256MB")
    res = probe(conn2)
    assert all(res.values())
    conn2.close()


def test_build_server_yields_hardened_duckdb_connection(temp_duckdb_file, tmp_path):
    DuckDBHolder.reset()
    try:
        pl_file = tmp_path / "placement.yaml"
        pl_file.write_text(
            "engines:\n  duck:\n    kind: duckdb\n    dialect: duckdb\n"
            "tables:\n  users:\n    keys: [id]\n    row_count: 2\n    serve:\n      duck: users\n"
            "cubes: {}\n",
            encoding="utf-8",
        )
        sc_file = tmp_path / "schemas.json"
        sc_file.write_text('{"users": ["id", "name"]}', encoding="utf-8")

        priv = ec.generate_private_key(ec.SECP256R1())
        pub_pem = priv.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        ).decode("ascii")
        pub_file = tmp_path / "pub.pem"
        pub_file.write_text(pub_pem, encoding="utf-8")

        config = {
            "grant_keys": {
                "pem_files": {
                    "key-1": str(pub_file),
                }
            },
            "placement": {
                "static": {
                    "placement_file": str(pl_file),
                    "schemas_file": str(sc_file),
                }
            },
            "connections": {
                "duck": {
                    "kind": "duckdb",
                    "path": temp_duckdb_file,
                    "memory_limit": "256MB",
                    "threads": 2,
                }
            },
        }

        server = build_server(config, cursor_key="test-duckdb-policy-cursor-key-32b")
        adapter = server.adapters["duck"]
        # Adapter._connect should return a hardened cursor from DuckDBHolder
        conn = adapter._connect()
        try:
            results = probe(conn)
            assert all(results.values())
            assert results["external_access_disabled"] is True
            assert results["config_locked"] is True
        finally:
            conn.close()
    finally:
        DuckDBHolder.reset()


def test_cli_probe_exit_codes_and_output(temp_duckdb_file, capsys):
    # 1. Hardened connection probe succeeds with exit 0
    exit_code = main(["probe", "--path", temp_duckdb_file])
    assert exit_code == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["select_ok"] is True
    assert payload["external_access_disabled"] is True
    assert payload["config_locked"] is True
    assert all(payload.values())
