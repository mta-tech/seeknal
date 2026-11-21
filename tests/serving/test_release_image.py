"""Unit tests for the lock-only, air-gapped atlas-rt-serving release image."""

from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
import os
import re
import shutil
import sys
import tomllib
import types
import urllib.request
from pathlib import Path

import duckdb
import pytest

from seeknal.serving.buildinfo import EXPECTED_DUCKDB_VERSION, write_build_info

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_script_module(module_name: str, rel_path: str):
    script_path = REPO_ROOT / rel_path
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


if not (REPO_ROOT / "docker/serving-image/prefetch.py").exists():
    pytest.skip("docker/ not present in this checkout", allow_module_level=True)

prefetch_mod = _load_script_module(
    "serving_image_prefetch", "docker/serving-image/prefetch.py"
)
check_env_mod = _load_script_module(
    "serving_image_check_env", "docker/serving-image/check_serving_env.py"
)


@pytest.fixture(autouse=True)
def _forbid_network(monkeypatch):
    """Guarantee zero network calls across all tests in this module."""

    def _blocked_urlopen(*args, **kwargs):
        raise AssertionError(f"Network access is forbidden in tests: {args!r}")

    monkeypatch.setattr(urllib.request, "urlopen", _blocked_urlopen)


def _make_fake_ext_lock(
    tmp_path: Path,
    *,
    duckdb_version: str = "2.0.0",
    ext_dir: Path | None = None,
    payloads: dict[tuple[str, str], bytes] | None = None,
    entry_order: list[tuple[str, str]] | None = None,
) -> tuple[Path, dict[tuple[str, str], bytes], dict[tuple[str, str], bytes]]:
    if payloads is None:
        payloads = {
            ("linux_amd64", "httpfs"): b"FAKE-HTTPFS-AMD64-BINARY",
            ("linux_amd64", "avro"): b"FAKE-AVRO-AMD64-BINARY",
            ("linux_amd64", "iceberg"): b"FAKE-ICEBERG-AMD64-BINARY",
            ("linux_arm64", "httpfs"): b"FAKE-HTTPFS-ARM64-BINARY",
            ("linux_arm64", "avro"): b"FAKE-AVRO-ARM64-BINARY",
            ("linux_arm64", "iceberg"): b"FAKE-ICEBERG-ARM64-BINARY",
        }
    target_ext_dir = ext_dir or (tmp_path / "duckdb_extensions")
    gz_map: dict[tuple[str, str], bytes] = {}
    ordered_keys = entry_order if entry_order is not None else list(payloads.keys())
    entries = []
    for plat, name in ordered_keys:
        raw_bytes = payloads[(plat, name)]
        gz_bytes = gzip.compress(raw_bytes, mtime=0)
        gz_map[(plat, name)] = gz_bytes
        gz_sha = hashlib.sha256(gz_bytes).hexdigest()
        raw_sha = hashlib.sha256(raw_bytes).hexdigest()
        entries.append(
            {
                "platform": plat,
                "name": name,
                "source_url": f"https://extensions.duckdb.org/v{duckdb_version}/{plat}/{name}.duckdb_extension.gz",
                "sha256": gz_sha,
                "gz_sha256": gz_sha,
                "raw_sha256": raw_sha,
                "decompressed_sha256": raw_sha,
                "gz_bytes": len(gz_bytes),
            }
        )

    lock_path = tmp_path / "extensions.lock.json"
    lock_path.write_text(
        json.dumps(
            {
                "duckdb_version": duckdb_version,
                "extension_directory": str(target_ext_dir),
                "extensions": entries,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return lock_path, payloads, gz_map


def _make_fake_uv_lock_with_build_backend(
    tmp_path: Path,
    *,
    duckdb_version: str = "2.0.0",
    include_hatchling: bool = True,
) -> tuple[Path, dict[str, bytes], dict[str, str]]:
    """Create a synthetic uv.lock with duckdb + locked hatchling build backend wheels."""
    pkg_specs = [
        (
            "duckdb",
            duckdb_version,
            f"duckdb-{duckdb_version}-cp311-cp311-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl",
            [],
        ),
        ("packaging", "25.0", "packaging-25.0-py3-none-any.whl", []),
        ("pathspec", "1.0.3", "pathspec-1.0.3-py3-none-any.whl", []),
        ("pluggy", "1.6.0", "pluggy-1.6.0-py3-none-any.whl", []),
        (
            "trove-classifiers",
            "2025.5.9.12",
            "trove_classifiers-2025.5.9.12-py3-none-any.whl",
            [],
        ),
    ]
    if include_hatchling:
        pkg_specs.append(
            (
                "hatchling",
                "1.27.0",
                "hatchling-1.27.0-py3-none-any.whl",
                ["packaging", "pathspec", "pluggy", "trove-classifiers"],
            )
        )

    url_to_bytes: dict[str, bytes] = {}
    pkg_to_sha: dict[str, str] = {}
    blocks = [
        'version = 1\nrevision = 5\nrequires-python = ">=3.11"\n',
    ]

    for name, ver, filename, deps in pkg_specs:
        payload = f"PK\x03\x04FAKE_WHEEL_{name}_{ver}".encode("utf-8")
        sha = hashlib.sha256(payload).hexdigest()
        url = f"https://files.pythonhosted.org/packages/fake/{filename}"
        url_to_bytes[url] = payload
        pkg_to_sha[name] = sha
        deps_toml = ""
        if deps:
            dep_items = ", ".join(f'{{ name = "{d}" }}' for d in deps)
            deps_toml = f"dependencies = [{dep_items}]\n"
        blocks.append(
            f'[[package]]\nname = "{name}"\nversion = "{ver}"\n{deps_toml}'
            f'wheels = [\n    {{ url = "{url}", hash = "sha256:{sha}", size = {len(payload)} }},\n]\n'
        )

    blocks.append(
        '[[package]]\nname = "seeknal"\nversion = "2.14.0"\n\n'
        '[package.dev-dependencies]\nserving = [\n    { name = "duckdb" },\n]\n'
    )

    lock_path = tmp_path / "uv.lock"
    lock_path.write_text("\n".join(blocks), encoding="utf-8")
    return lock_path, url_to_bytes, pkg_to_sha


def test_pyproject_and_uv_lock_duckdb_2_0_0_and_serving_group():
    pyproject_data = tomllib.loads(
        (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )
    # [project].dependencies keeps duckdb>=1.4.4 floor untouched
    proj_deps = pyproject_data["project"]["dependencies"]
    assert "duckdb>=1.4.4" in proj_deps

    # [dependency-groups].serving has the 14 minimal packages
    expected_serving_group = {
        "attrs",
        "cryptography",
        "duckdb",
        "httpx",
        "psycopg2-binary",
        "pygments",
        "pymysql",
        "python-multipart",
        "pyyaml",
        "rich",
        "sniffio",
        "sqlglot",
        "starlette",
        "uvicorn",
    }
    serving_group = set(pyproject_data["dependency-groups"]["serving"])
    assert serving_group == expected_serving_group

    # uv.lock pins duckdb == 2.0.0
    with open(REPO_ROOT / "uv.lock", "rb") as handle:
        lock_data = tomllib.load(handle)
    duckdb_pkgs = [p for p in lock_data["package"] if p["name"] == "duckdb"]
    assert len(duckdb_pkgs) == 1
    assert duckdb_pkgs[0]["version"] == "2.0.0" == EXPECTED_DUCKDB_VERSION


def test_extensions_lock_json_pins_exact_duckdb_2_0_0_digests():
    lock_path = REPO_ROOT / "docker" / "serving-image" / "extensions.lock.json"
    manifest = prefetch_mod.load_extensions_manifest(lock_path)

    assert manifest.duckdb_version == "2.0.0"
    assert manifest.extension_directory == "/opt/seeknal/duckdb_extensions"

    by_key = {(e.platform, e.name): e for e in manifest.entries}
    assert set(by_key.keys()) == {
        ("linux_amd64", "httpfs"),
        ("linux_amd64", "avro"),
        ("linux_amd64", "iceberg"),
        ("linux_arm64", "httpfs"),
        ("linux_arm64", "avro"),
        ("linux_arm64", "iceberg"),
    }

    assert (
        by_key[("linux_amd64", "httpfs")].gz_sha256
        == "ee6c7d7e60a5da1b6f6bf6156b4298b55ddce0496a5bfe628bd6ab6d47eb8e3e"
    )
    assert (
        by_key[("linux_amd64", "httpfs")].raw_sha256
        == "77e6c429aa04f25da7962572f12b19113e0ce46080fa1365ad66017aadcdb68d"
    )
    assert by_key[("linux_amd64", "httpfs")].gz_bytes == 8657469

    assert (
        by_key[("linux_amd64", "avro")].gz_sha256
        == "35447cb42ade2f723f53416ee336dc9ee30ba3584f290e01165398c80f56fd92"
    )
    assert (
        by_key[("linux_amd64", "avro")].raw_sha256
        == "3f37797ca9a4ef3b5a07e65278c653fe0a32f7edea1498c801022fed49afedc7"
    )
    assert by_key[("linux_amd64", "avro")].gz_bytes == 8625644

    assert (
        by_key[("linux_amd64", "iceberg")].gz_sha256
        == "ddc8e8f4af60e6e3accffa1b38043c328c810db91215d6299f29cc933655bf1a"
    )
    assert (
        by_key[("linux_amd64", "iceberg")].raw_sha256
        == "b93676e544e5c6843915fb3392e3b3c35859c065fa6ace7a26cb46ca4394e56d"
    )
    assert by_key[("linux_amd64", "iceberg")].gz_bytes == 17879202

    assert (
        by_key[("linux_arm64", "httpfs")].gz_sha256
        == "f524888fc7d74cc0c6c4a8f7ed5beedac5b12e71cb426503f882c72ce6afbd43"
    )
    assert (
        by_key[("linux_arm64", "httpfs")].raw_sha256
        == "53f7aea1a0448ea62ef9b63f95da37c5766070084f14f14110e275e76382e7b2"
    )
    assert by_key[("linux_arm64", "httpfs")].gz_bytes == 8620240

    assert (
        by_key[("linux_arm64", "avro")].gz_sha256
        == "1d240802771737ae14695b9d544ecac170832f38dda9a3edcb674ee1d546c2ca"
    )
    assert (
        by_key[("linux_arm64", "avro")].raw_sha256
        == "3e8a61c8ae51851a8c70c49a5972fd3c3302c3ffa798f3286f7dca0746975607"
    )
    assert by_key[("linux_arm64", "avro")].gz_bytes == 7637646

    assert (
        by_key[("linux_arm64", "iceberg")].gz_sha256
        == "dc5fc010b24a044ec2dc34294d8d251f481560cfbd5819c22f1fb1b6cc0032ae"
    )
    assert (
        by_key[("linux_arm64", "iceberg")].raw_sha256
        == "5616c30201099564326826d223aeea19b59f15744a981b074a906fb2b9dd4f44"
    )
    assert by_key[("linux_arm64", "iceberg")].gz_bytes == 16161197


def test_a_avro_ordered_before_iceberg_with_fakes(tmp_path, monkeypatch):
    """A-avro: extensions.lock.json pins avro before iceberg and check_baked_extensions enforces load order."""
    lock_path = REPO_ROOT / "docker" / "serving-image" / "extensions.lock.json"
    raw_manifest = json.loads(lock_path.read_text(encoding="utf-8"))
    raw_entries = raw_manifest["extensions"]

    for plat, expected_url, expected_gz, expected_raw, expected_bytes in [
        (
            "linux_amd64",
            "https://extensions.duckdb.org/v2.0.0/linux_amd64/avro.duckdb_extension.gz",
            "35447cb42ade2f723f53416ee336dc9ee30ba3584f290e01165398c80f56fd92",
            "3f37797ca9a4ef3b5a07e65278c653fe0a32f7edea1498c801022fed49afedc7",
            8625644,
        ),
        (
            "linux_arm64",
            "https://extensions.duckdb.org/v2.0.0/linux_arm64/avro.duckdb_extension.gz",
            "1d240802771737ae14695b9d544ecac170832f38dda9a3edcb674ee1d546c2ca",
            "3e8a61c8ae51851a8c70c49a5972fd3c3302c3ffa798f3286f7dca0746975607",
            7637646,
        ),
    ]:
        plat_names = [e["name"] for e in raw_entries if e["platform"] == plat]
        assert plat_names.index("avro") < plat_names.index("iceberg")
        avro_item = next(
            e for e in raw_entries if e["platform"] == plat and e["name"] == "avro"
        )
        assert avro_item["source_url"] == expected_url
        assert avro_item["sha256"] == expected_gz
        assert avro_item["gz_sha256"] == expected_gz
        assert avro_item["raw_sha256"] == expected_raw
        assert avro_item["decompressed_sha256"] == expected_raw
        assert avro_item["gz_bytes"] == expected_bytes

    # Even if lockfile entries list iceberg BEFORE avro, check_baked_extensions loads avro BEFORE iceberg
    ext_dir = tmp_path / "shuffled_ext"
    shuffled_order = [
        ("linux_amd64", "iceberg"),
        ("linux_amd64", "httpfs"),
        ("linux_amd64", "avro"),
    ]
    shuffled_payloads = {
        ("linux_amd64", "iceberg"): b"FAKE-ICEBERG",
        ("linux_amd64", "httpfs"): b"FAKE-HTTPFS",
        ("linux_amd64", "avro"): b"FAKE-AVRO",
    }
    shuffled_lock, raw_map, _ = _make_fake_ext_lock(
        tmp_path,
        ext_dir=ext_dir,
        payloads=shuffled_payloads,
        entry_order=shuffled_order,
    )
    for (plat, name), raw_bytes in raw_map.items():
        dest = ext_dir / "v2.0.0" / plat / f"{name}.duckdb_extension"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(raw_bytes)

    load_sequence: list[str] = []
    check_env_mod.check_baked_extensions(
        shuffled_lock,
        extension_directory=ext_dir,
        platform="linux_amd64",
        extension_loader=lambda _root, ext_name: load_sequence.append(ext_name),
    )
    assert load_sequence == ["httpfs", "avro", "iceberg"]
    assert load_sequence.index("avro") < load_sequence.index("iceberg")

    # Verify _default_extension_loader loads avro before iceberg on the connection
    executed_sql: list[str] = []

    class _FakeConn:
        def execute(self, sql: str, params=None):
            executed_sql.append(sql)
            return self

        def fetchone(self):
            return (True,)

        def close(self):
            pass

    monkeypatch.setattr(
        check_env_mod.duckdb, "connect", lambda *_a, **_kw: _FakeConn()
    )
    check_env_mod._default_extension_loader(ext_dir, "iceberg")
    load_stmts = [s for s in executed_sql if s.startswith("LOAD ")]
    assert load_stmts == ["LOAD avro", "LOAD iceberg"]

    # Missing avro.duckdb_extension on disk fails early BEFORE any extension_loader call
    avro_on_disk = ext_dir / "v2.0.0" / "linux_amd64" / "avro.duckdb_extension"
    avro_on_disk.unlink()
    early_loads: list[str] = []
    with pytest.raises(
        check_env_mod.EnvCheckError,
        match=r"avro\.duckdb_extension is missing from extension_directory",
    ):
        check_env_mod.check_baked_extensions(
            shuffled_lock,
            extension_directory=ext_dir,
            platform="linux_amd64",
            extension_loader=lambda _root, ext_name: early_loads.append(ext_name),
        )
    assert early_loads == []

    # Missing avro entry in extensions.lock.json also fails early BEFORE any extension_loader call
    no_avro_lock, _, _ = _make_fake_ext_lock(
        tmp_path / "no_avro_lock_dir" if (tmp_path / "no_avro_lock_dir").exists() else tmp_path,
        ext_dir=ext_dir,
        payloads={
            ("linux_amd64", "httpfs"): b"FAKE-HTTPFS",
            ("linux_amd64", "iceberg"): b"FAKE-ICEBERG",
        },
    )
    with pytest.raises(
        check_env_mod.EnvCheckError,
        match=r"missing required 'avro' extension entry",
    ):
        check_env_mod.check_baked_extensions(
            no_avro_lock,
            extension_directory=ext_dir,
            platform="linux_amd64",
            extension_loader=lambda _root, ext_name: early_loads.append(ext_name),
        )
    assert early_loads == []


def test_a_avro_real_duckdb_extension_load(tmp_path):
    """A-avro: real DuckDB 2.0.0 extension test when SEEKNAL_TEST_DUCKDB_EXTENSION_DIR is provided."""
    ext_dir_env = os.environ.get("SEEKNAL_TEST_DUCKDB_EXTENSION_DIR", "").strip()
    if not ext_dir_env:
        pytest.skip("SEEKNAL_TEST_DUCKDB_EXTENSION_DIR is not set")

    ext_dir = Path(ext_dir_env)
    plat = check_env_mod.detect_duckdb_platform()
    plat_dir = ext_dir / "v2.0.0" / plat
    required_files = [
        plat_dir / "httpfs.duckdb_extension",
        plat_dir / "avro.duckdb_extension",
        plat_dir / "iceberg.duckdb_extension",
    ]
    if not plat_dir.is_dir() or not all(p.is_file() for p in required_files):
        pytest.skip(
            f"SEEKNAL_TEST_DUCKDB_EXTENSION_DIR={ext_dir} does not contain real v2.0.0/{plat} extension files"
        )

    escaped_dir = str(ext_dir).replace("'", "''")
    lock_path = REPO_ROOT / "docker" / "serving-image" / "extensions.lock.json"

    # 1. With avro present in SEEKNAL_TEST_DUCKDB_EXTENSION_DIR:
    #    LOAD avro, LOAD iceberg, LOAD httpfs succeed and duckdb_extensions() reports loaded=true
    con_ok = duckdb.connect(":memory:")
    try:
        con_ok.execute("SET autoinstall_known_extensions = false")
        con_ok.execute("SET autoload_known_extensions = false")
        con_ok.execute(
            "SET custom_extension_repository = '/nonexistent_extension_repository'"
        )
        con_ok.execute(f"SET extension_directory = '{escaped_dir}'")
        con_ok.execute("LOAD avro")
        con_ok.execute("LOAD iceberg")
        con_ok.execute("LOAD httpfs")
        loaded_rows = dict(
            con_ok.execute(
                "SELECT extension_name, loaded FROM duckdb_extensions() "
                "WHERE extension_name IN ('avro', 'iceberg', 'httpfs')"
            ).fetchall()
        )
        assert loaded_rows == {"avro": True, "iceberg": True, "httpfs": True}
    finally:
        con_ok.close()

    # Even when LOAD iceberg is run first with avro present in extension_directory,
    # DuckDB 2.0.0 loads avro from extension_directory and both report loaded=true
    con_auto_avro = duckdb.connect(":memory:")
    try:
        con_auto_avro.execute("SET autoinstall_known_extensions = false")
        con_auto_avro.execute("SET autoload_known_extensions = false")
        con_auto_avro.execute(
            "SET custom_extension_repository = '/nonexistent_extension_repository'"
        )
        con_auto_avro.execute(f"SET extension_directory = '{escaped_dir}'")
        con_auto_avro.execute("LOAD iceberg")
        loaded_rows = dict(
            con_auto_avro.execute(
                "SELECT extension_name, loaded FROM duckdb_extensions() "
                "WHERE extension_name IN ('avro', 'iceberg')"
            ).fetchall()
        )
        assert loaded_rows == {"avro": True, "iceberg": True}
    finally:
        con_auto_avro.close()

    verified = check_env_mod.check_baked_extensions(
        lock_path,
        extension_directory=ext_dir,
        platform=plat,
    )
    assert len(verified) == 3

    # 2. With a directory missing avro.duckdb_extension:
    #    LOAD iceberg fails with duckdb.Error and check_baked_extensions fails early naming avro
    no_avro_ext_dir = tmp_path / "no_avro_ext"
    no_avro_plat_dir = no_avro_ext_dir / "v2.0.0" / plat
    no_avro_plat_dir.mkdir(parents=True, exist_ok=True)
    for ext_filename in ("httpfs.duckdb_extension", "iceberg.duckdb_extension"):
        shutil.copy2(plat_dir / ext_filename, no_avro_plat_dir / ext_filename)

    escaped_no_avro = str(no_avro_ext_dir).replace("'", "''")
    con_fail = duckdb.connect(":memory:")
    try:
        con_fail.execute("SET autoinstall_known_extensions = false")
        con_fail.execute("SET autoload_known_extensions = false")
        con_fail.execute(
            "SET custom_extension_repository = '/nonexistent_extension_repository'"
        )
        con_fail.execute(f"SET extension_directory = '{escaped_no_avro}'")
        with pytest.raises(duckdb.Error):
            con_fail.execute("LOAD iceberg")
    finally:
        con_fail.close()

    with pytest.raises(
        check_env_mod.EnvCheckError,
        match=r"avro\.duckdb_extension is missing from extension_directory",
    ):
        check_env_mod.check_baked_extensions(
            lock_path,
            extension_directory=no_avro_ext_dir,
            platform=plat,
        )


def test_a_m1_dockerfile_offline_path_only_when_offline_eq_1():
    """A-M1: Dockerfile.serving takes the offline vendor branch strictly when OFFLINE=1."""
    dockerfile = (REPO_ROOT / "docker" / "Dockerfile.serving").read_text(
        encoding="utf-8"
    )
    assert 'if [ "${OFFLINE}" = "1" ]; then \\' in dockerfile
    assert '-d "docker/serving-image/vendor' not in dockerfile
    assert "|| [ -d" not in dockerfile


def test_a_m2_offline_installs_source_with_no_build_isolation_and_locked_hatchling(
    tmp_path,
):
    """A-M2: Offline path installs copied src/ with --no-build-isolation and hash-checked hatchling backend."""
    dockerfile = (REPO_ROOT / "docker" / "Dockerfile.serving").read_text(
        encoding="utf-8"
    )

    # 1. Dockerfile never installs from vendor/project and uses --no-build-isolation on copied source
    assert "vendor/project" not in dockerfile
    idx_verify = dockerfile.index("python3 docker/serving-image/prefetch.py verify-vendor")
    idx_wheels = dockerfile.index(
        "uv pip install --system --offline --no-index --require-hashes \\\n"
        "            --find-links docker/serving-image/vendor/wheels \\\n"
        "            -r docker/serving-image/vendor/requirements.txt;"
    )
    idx_src_install = dockerfile.index(
        "uv pip install --system --offline --no-index --no-deps --no-build-isolation .;"
    )
    idx_buildinfo = dockerfile.index('write_build_info("/opt/seeknal/buildinfo.json"')
    assert idx_verify < idx_wheels < idx_src_install < idx_buildinfo
    assert 'source_dir="src/seeknal"' in dockerfile

    # 2. prefetch.py includes locked build backend in vendor/requirements.txt and vendor/wheels
    assert set(prefetch_mod.BUILD_BACKEND_PACKAGES) == {
        "hatchling",
        "packaging",
        "pathspec",
        "pluggy",
        "trove-classifiers",
    }

    fake_uv_lock, wheel_urls, pkg_shas = _make_fake_uv_lock_with_build_backend(tmp_path)
    ext_lock, _, gz_map = _make_fake_ext_lock(tmp_path)
    url_map = {
        **wheel_urls,
        **{
            f"https://extensions.duckdb.org/v2.0.0/{plat}/{name}.duckdb_extension.gz": data
            for (plat, name), data in gz_map.items()
        },
    }

    vendor_dir = tmp_path / "vendor"

    def _fake_export(out_file: Path) -> None:
        out_file.write_text(
            f"duckdb==2.0.0 \\\n    --hash=sha256:{pkg_shas['duckdb']}\n",
            encoding="utf-8",
        )

    wheels, exts = prefetch_mod.prefetch_all(
        vendor_dir,
        uv_lock_path=fake_uv_lock,
        extensions_lock_path=ext_lock,
        platforms=["linux_amd64"],
        fetcher=lambda url: url_map[url],
        export_runner=_fake_export,
    )
    assert not (vendor_dir / "project").exists()
    assert len(exts) == 3
    vendored_pkg_names = {p.name.split("-")[0].replace("_", "-") for p in wheels}
    assert vendored_pkg_names == {
        "duckdb",
        "hatchling",
        "packaging",
        "pathspec",
        "pluggy",
        "trove-classifiers",
    }

    parsed_reqs = prefetch_mod.verify_requirements_file(
        vendor_dir / "requirements.txt", fake_uv_lock
    )
    for backend_pkg in prefetch_mod.BUILD_BACKEND_PACKAGES:
        assert backend_pkg in parsed_reqs
        assert parsed_reqs[backend_pkg][1] == (pkg_shas[backend_pkg],)

    prefetch_mod.verify_vendor_directory(
        vendor_dir,
        uv_lock_path=fake_uv_lock,
        extensions_lock_path=ext_lock,
        platform="linux_amd64",
    )

    # Reject unchecked vendor/project directory if present
    (vendor_dir / "project").mkdir()
    with pytest.raises(prefetch_mod.PrefetchError, match="vendor/project"):
        prefetch_mod.verify_vendor_directory(
            vendor_dir,
            uv_lock_path=fake_uv_lock,
            extensions_lock_path=ext_lock,
            platform="linux_amd64",
        )
    (vendor_dir / "project").rmdir()

    # Reject vendor/requirements.txt missing hatchling
    reqs_file = vendor_dir / "requirements.txt"
    original_reqs_text = reqs_file.read_text(encoding="utf-8")
    reqs_without_hatchling = "\n".join(
        line
        for line in original_reqs_text.splitlines()
        if "hatchling==" not in line and pkg_shas["hatchling"] not in line
    )
    reqs_file.write_text(reqs_without_hatchling + "\n", encoding="utf-8")
    with pytest.raises(prefetch_mod.PrefetchError, match="hatchling"):
        prefetch_mod.verify_vendor_directory(
            vendor_dir,
            uv_lock_path=fake_uv_lock,
            extensions_lock_path=ext_lock,
            platform="linux_amd64",
        )
    reqs_file.write_text(original_reqs_text, encoding="utf-8")

    # Reject missing hatchling in uv.lock
    (tmp_path / "incomplete").mkdir(exist_ok=True)
    incomplete_lock, _, _ = _make_fake_uv_lock_with_build_backend(
        tmp_path / "incomplete", include_hatchling=False
    )
    with pytest.raises(prefetch_mod.PrefetchError, match="hatchling"):
        prefetch_mod.select_serving_wheels(
            incomplete_lock, platforms=["linux_amd64"], include_build_backend=True
        )

    # 3. /buildinfo write_build_info refuses to record a git_sha when installed code != copied src/
    copied_src = tmp_path / "copied_src" / "seeknal"
    installed_pkg = tmp_path / "site_packages" / "seeknal"
    copied_src.mkdir(parents=True)
    installed_pkg.mkdir(parents=True)
    (copied_src / "__init__.py").write_text("VERSION = 'from-copied-src'\n", encoding="utf-8")
    (installed_pkg / "__init__.py").write_text("VERSION = 'stale-prebuilt-wheel'\n", encoding="utf-8")

    bad_buildinfo = tmp_path / "bad_buildinfo.json"
    with pytest.raises(RuntimeError, match="does not match copied source"):
        write_build_info(
            bad_buildinfo,
            git_sha="deadbeef1234",
            lock_path=fake_uv_lock,
            source_dir=copied_src,
            installed_package_dir=installed_pkg,
        )
    assert not bad_buildinfo.exists()


def test_a_m3_dockerfile_has_no_syntax_directive():
    """A-M3: Dockerfile.serving has no # syntax= directive that would trigger air-gapped pulls."""
    dockerfile = (REPO_ROOT / "docker" / "Dockerfile.serving").read_text(
        encoding="utf-8"
    )
    for line in dockerfile.splitlines():
        assert not re.match(
            r"^\s*#\s*syntax\s*=", line, re.IGNORECASE
        ), f"Forbidden # syntax= directive found in Dockerfile.serving: {line!r}"


def test_a_l2_duckdb_version_four_way_agreement_and_any_single_mismatch_fails(
    tmp_path, monkeypatch
):
    """A-L2: runtime, buildinfo, accel settings, and uv.lock must all agree on DuckDB version."""
    base_uv_lock = REPO_ROOT / "uv.lock"
    locked_versions = check_env_mod._load_locked_versions(base_uv_lock)
    valid_installed = {
        pkg: locked_versions[check_env_mod.normalize_pep503(pkg)]
        for pkg in check_env_mod.KEY_PACKAGES
    }

    ext_dir = tmp_path / "baked_ext"
    ext_lock, raw_map, _ = _make_fake_ext_lock(tmp_path, ext_dir=ext_dir)
    for (plat, name), raw_bytes in raw_map.items():
        dest = ext_dir / "v2.0.0" / plat / f"{name}.duckdb_extension"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(raw_bytes)

    # All 4 agree on 2.0.0 -> passes
    check_env_mod.verify_serving_environment(
        uv_lock_path=base_uv_lock,
        extensions_lock_path=ext_lock,
        extension_directory=ext_dir,
        platform="linux_amd64",
        installed_versions_provider=lambda: dict(valid_installed),
        runtime_duckdb_version="2.0.0",
        expected_duckdb_version="2.0.0",
        accel_expected_duckdb_version="2.0.0",
        extension_loader=lambda _r, _n: None,
        pip_check_runner=lambda: None,
    )

    # Mismatch 1: runtime_duckdb_version disagrees
    with pytest.raises(check_env_mod.EnvCheckError, match="runtime version mismatch"):
        check_env_mod.verify_serving_environment(
            uv_lock_path=base_uv_lock,
            extensions_lock_path=ext_lock,
            extension_directory=ext_dir,
            platform="linux_amd64",
            installed_versions_provider=lambda: dict(valid_installed),
            runtime_duckdb_version="2.0.1",
            expected_duckdb_version="2.0.0",
            accel_expected_duckdb_version="2.0.0",
            extension_loader=lambda _r, _n: None,
            pip_check_runner=lambda: None,
        )

    # Mismatch 2: buildinfo expected_duckdb_version disagrees
    with pytest.raises(check_env_mod.EnvCheckError):
        check_env_mod.verify_serving_environment(
            uv_lock_path=base_uv_lock,
            extensions_lock_path=ext_lock,
            extension_directory=ext_dir,
            platform="linux_amd64",
            installed_versions_provider=lambda: dict(valid_installed),
            runtime_duckdb_version="2.0.0",
            expected_duckdb_version="2.0.1",
            accel_expected_duckdb_version="2.0.0",
            extension_loader=lambda _r, _n: None,
            pip_check_runner=lambda: None,
        )

    # Mismatch 3: accel settings DEFAULT_EXPECTED_DUCKDB_VERSION disagrees
    with pytest.raises(check_env_mod.EnvCheckError, match="accel settings"):
        check_env_mod.verify_serving_environment(
            uv_lock_path=base_uv_lock,
            extensions_lock_path=ext_lock,
            extension_directory=ext_dir,
            platform="linux_amd64",
            installed_versions_provider=lambda: dict(valid_installed),
            runtime_duckdb_version="2.0.0",
            expected_duckdb_version="2.0.0",
            accel_expected_duckdb_version="2.0.1",
            extension_loader=lambda _r, _n: None,
            pip_check_runner=lambda: None,
        )

    # Mismatch 4: uv.lock duckdb version disagrees (while runtime, buildinfo, accel are 2.0.0)
    drifted_uv_lock = tmp_path / "drifted_uv.lock"
    drifted_uv_lock.write_text(
        base_uv_lock.read_text(encoding="utf-8").replace(
            'name = "duckdb"\nversion = "2.0.0"',
            'name = "duckdb"\nversion = "2.0.1"',
            1,
        ),
        encoding="utf-8",
    )
    with pytest.raises(check_env_mod.EnvCheckError, match="uv.lock"):
        check_env_mod.verify_serving_environment(
            uv_lock_path=drifted_uv_lock,
            extensions_lock_path=ext_lock,
            extension_directory=ext_dir,
            platform="linux_amd64",
            installed_versions_provider=lambda: dict(valid_installed, duckdb="2.0.1"),
            runtime_duckdb_version="2.0.0",
            expected_duckdb_version="2.0.0",
            accel_expected_duckdb_version="2.0.0",
            extension_loader=lambda _r, _n: None,
            pip_check_runner=lambda: None,
        )

    # Unimportable seeknal.serving.accel.settings fails closed when no override is passed
    monkeypatch.setitem(sys.modules, "seeknal.serving.accel.settings", None)
    with pytest.raises(
        check_env_mod.EnvCheckError,
        match="Failed to import seeknal.serving.accel.settings",
    ):
        check_env_mod.verify_serving_environment(
            uv_lock_path=base_uv_lock,
            extensions_lock_path=ext_lock,
            extension_directory=ext_dir,
            platform="linux_amd64",
            installed_versions_provider=lambda: dict(valid_installed),
            runtime_duckdb_version="2.0.0",
            expected_duckdb_version="2.0.0",
            extension_loader=lambda _r, _n: None,
            pip_check_runner=lambda: None,
        )


def test_a_l3_extension_directory_env_fallback_before_manifest(tmp_path, monkeypatch):
    """A-L3: SEEKNAL_EXTENSION_DIRECTORY takes precedence over manifest when --extension-directory is omitted."""
    manifest_dir = tmp_path / "manifest_unused_dir"
    env_ext_dir = tmp_path / "env_override_ext_dir"
    cli_ext_dir = tmp_path / "cli_override_ext_dir"

    ext_lock, raw_map, _ = _make_fake_ext_lock(tmp_path, ext_dir=manifest_dir)
    for root in (env_ext_dir, cli_ext_dir):
        for (plat, name), raw_bytes in raw_map.items():
            dest = root / "v2.0.0" / plat / f"{name}.duckdb_extension"
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(raw_bytes)

    seen_roots: list[Path] = []
    monkeypatch.setenv("SEEKNAL_EXTENSION_DIRECTORY", str(env_ext_dir))

    # Without extension_directory arg, SEEKNAL_EXTENSION_DIRECTORY wins over manifest_dir
    verified_env = check_env_mod.check_baked_extensions(
        ext_lock,
        extension_directory=None,
        platform="linux_amd64",
        extension_loader=lambda root, _name: seen_roots.append(root),
    )
    assert all(p.is_relative_to(env_ext_dir) for p in verified_env)
    assert seen_roots == [env_ext_dir, env_ext_dir, env_ext_dir]

    # Explicit extension_directory still wins over SEEKNAL_EXTENSION_DIRECTORY
    seen_roots.clear()
    verified_cli = check_env_mod.check_baked_extensions(
        ext_lock,
        extension_directory=cli_ext_dir,
        platform="linux_amd64",
        extension_loader=lambda root, _name: seen_roots.append(root),
    )
    assert all(p.is_relative_to(cli_ext_dir) for p in verified_cli)
    assert seen_roots == [cli_ext_dir, cli_ext_dir, cli_ext_dir]


def test_docs_and_readme_avro_layer_size_duckdb_2_0_and_release_notes():
    """Verify docs/serving/image.md and docker/serving-image/README.md stay aligned."""
    image_doc = (REPO_ROOT / "docs" / "serving" / "image.md").read_text(encoding="utf-8")
    readme_doc = (
        REPO_ROOT / "docker" / "serving-image" / "README.md"
    ).read_text(encoding="utf-8")

    for doc_name, content in [
        ("docs/serving/image.md", image_doc),
        ("docker/serving-image/README.md", readme_doc),
    ]:
        assert "avro" in content, doc_name
        assert "extensions: [httpfs, avro, iceberg]" in content, doc_name
        assert "COPY --from=builder" in content and "rm -rf" in content, doc_name
        assert "builder" in content and "runtime" in content, doc_name
        assert "cannot be read by pre-2.0 DuckDB tools" in content, doc_name
        assert "## Release notes" in content, doc_name
        assert "2.0.1" in content, doc_name
        assert "python -m seeknal.serving" in content, doc_name
        assert "validate --config" in content, doc_name
        assert "--version" in content, doc_name

        # Verify the 3 backlog known limitations in the ## Release notes section
        release_notes_sec = content.split("## Release notes", 1)[1]
        assert (
            "a discovery refresh fails as a whole (the previous catalog is kept) "
            "when one PII table loses its only safe serve, or when two Lakekeeper "
            "tables differ only by case; planned fix: omit just that table and add "
            "a metric for consecutive failures"
        ) in release_notes_sec, doc_name
        assert (
            "in discovery mode, cubes cannot be marked PII; PII cubes are static-only"
        ) in release_notes_sec, doc_name
        assert (
            "validate cannot detect a typo in discovery pii_tables offline; "
            "start refuses it, because the first refresh fails"
        ) in release_notes_sec, doc_name


def test_dockerfile_serving_lock_only_offline_and_verification_contract():
    dockerfile = (REPO_ROOT / "docker" / "Dockerfile.serving").read_text(
        encoding="utf-8"
    )

    # 1. Pinned uv 0.9.18 base image and two-stage builder -> runtime build (no # syntax=)
    assert not re.search(
        r"^\s*#\s*syntax\s*=", dockerfile, re.MULTILINE | re.IGNORECASE
    )
    assert (
        "ARG BASE_IMAGE=ghcr.io/astral-sh/uv:0.9.18-python3.11-bookworm-slim"
        in dockerfile
    )
    stage_matches = list(
        re.finditer(r"^FROM\s+\$\{BASE_IMAGE\}\s+AS\s+(\S+)\s*$", dockerfile, re.MULTILINE)
    )
    assert [m.group(1) for m in stage_matches] == ["builder", "runtime"]
    builder_stage = dockerfile[stage_matches[0].end() : stage_matches[1].start()]
    runtime_stage = dockerfile[stage_matches[1].end() :]

    # 2. Builder stage: online lock-only sequence in strict order
    idx_export = builder_stage.index(
        "uv export --locked --only-group serving --no-emit-project --format requirements-txt -o /tmp/requirements.txt"
    )
    idx_install_deps = builder_stage.index(
        "uv pip install --system --require-hashes -r /tmp/requirements.txt"
    )
    idx_install_pkg = builder_stage.index("uv pip install --system --no-deps .")
    assert idx_export < idx_install_deps < idx_install_pkg

    # 3. Builder stage: offline (--network=none) vendor verification & install sequence
    assert 'if [ "${OFFLINE}" = "1" ]; then' in builder_stage
    assert "prefetch.py verify-vendor" in builder_stage
    assert "--offline --no-index --require-hashes" in builder_stage
    assert "--find-links docker/serving-image/vendor/wheels" in builder_stage
    assert "--no-build-isolation ." in builder_stage
    assert "prefetch.py install-extensions" in builder_stage
    assert "/opt/seeknal/duckdb_extensions" in builder_stage

    # 4. Builder stage writes buildinfo.json and cleans up caches, docs/cli, and vendor
    idx_buildinfo = builder_stage.index('write_build_info("/opt/seeknal/buildinfo.json"')
    idx_builder_cleanup = builder_stage.index(
        "rm -rf /root/.cache /tmp/* docs/cli docker/serving-image/vendor"
    )
    assert idx_install_pkg < idx_buildinfo < idx_builder_cleanup
    assert "check_serving_env.py" not in builder_stage

    # 5. Final runtime stage: copies ONLY site-packages, extensions, buildinfo from builder
    #    and uv.lock + extensions.lock.json + check_serving_env.py (NEVER vendor)
    assert (
        "COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages"
        in runtime_stage
    )
    assert (
        "COPY --from=builder /opt/seeknal/duckdb_extensions /opt/seeknal/duckdb_extensions"
        in runtime_stage
    )
    assert (
        "COPY --from=builder /opt/seeknal/buildinfo.json /opt/seeknal/buildinfo.json"
        in runtime_stage
    )
    assert "vendor" not in runtime_stage
    for copy_line in re.findall(r"^COPY\s+(.+)$", runtime_stage, re.MULTILINE):
        tokens = [t for t in copy_line.split() if not t.startswith("--")]
        for src in tokens[:-1]:
            assert src not in {"docker/serving-image", "docker/serving-image/", "src", "src/"}
            assert "vendor" not in src

    idx_check_env = runtime_stage.index(
        "python3 docker/serving-image/check_serving_env.py"
    )
    idx_etc = runtime_stage.index("mkdir -p /etc/seeknal")
    idx_chmod_etc = runtime_stage.index("chmod 755 /etc/seeknal")
    idx_chmod_ext = runtime_stage.index(
        "chmod -R a+rX /opt/seeknal/duckdb_extensions /opt/seeknal/buildinfo.json"
    )
    idx_runtime_cleanup = runtime_stage.index(
        "rm -rf /root/.cache /tmp/* uv.lock docker/serving-image"
    )
    assert (
        idx_check_env
        < idx_etc
        < idx_chmod_etc
        < idx_chmod_ext
        < idx_runtime_cleanup
    )

    # 6. Runtime contract preserved in final stage
    assert "USER 10001:10001" in runtime_stage
    assert "EXPOSE 8765" in runtime_stage
    assert 'ENTRYPOINT ["python", "-m", "seeknal.serving"]' in runtime_stage
    assert 'CMD ["--config", "/etc/seeknal/serving.yaml"]' in runtime_stage


def test_prefetch_extension_hash_verification_and_offline_install(tmp_path):
    ext_dir = tmp_path / "opt_ext"
    lock_path, raw_map, gz_map = _make_fake_ext_lock(tmp_path, ext_dir=ext_dir)
    manifest = prefetch_mod.load_extensions_manifest(lock_path)
    amd64_httpfs = next(
        e for e in manifest.entries if (e.platform, e.name) == ("linux_amd64", "httpfs")
    )

    # Valid compressed + decompressed hashes pass
    decompressed = prefetch_mod.verify_and_decompress_extension(
        gz_map[("linux_amd64", "httpfs")], amd64_httpfs
    )
    assert decompressed == raw_map[("linux_amd64", "httpfs")]

    # Bad compressed hash fails closed
    tampered_gz = gz_map[("linux_amd64", "httpfs")][:-1] + bytes(
        [gz_map[("linux_amd64", "httpfs")][-1] ^ 0xFF]
    )
    with pytest.raises(prefetch_mod.HashMismatchError, match="Compressed SHA-256"):
        prefetch_mod.verify_and_decompress_extension(tampered_gz, amd64_httpfs)

    # Matching gz_sha256 + gz_bytes but mismatched raw_sha256 fails closed
    wrong_raw_entry = prefetch_mod.ExtensionEntry(
        platform=amd64_httpfs.platform,
        name=amd64_httpfs.name,
        source_url=amd64_httpfs.source_url,
        gz_sha256=amd64_httpfs.gz_sha256,
        raw_sha256="0" * 64,
        gz_bytes=amd64_httpfs.gz_bytes,
    )
    with pytest.raises(prefetch_mod.HashMismatchError, match="Decompressed SHA-256"):
        prefetch_mod.verify_and_decompress_extension(
            gz_map[("linux_amd64", "httpfs")], wrong_raw_entry
        )

    # Prefetch into vendor directory via fake fetcher, then offline install without fetcher
    url_to_bytes = {
        f"https://extensions.duckdb.org/v2.0.0/{plat}/{name}.duckdb_extension.gz": data
        for (plat, name), data in gz_map.items()
    }
    vendor_dir = tmp_path / "vendor"
    written_gz = prefetch_mod.prefetch_extensions(
        vendor_dir,
        extensions_lock_path=lock_path,
        platforms=["linux_amd64"],
        fetcher=lambda url: url_to_bytes[url],
    )
    assert len(written_gz) == 3

    def _forbidden_fetcher(url: str) -> bytes:
        raise AssertionError(f"Offline install must not fetch {url}")

    installed = prefetch_mod.install_extensions(
        extensions_lock_path=lock_path,
        extension_directory=ext_dir,
        platform="linux_amd64",
        vendor_extensions_dir=vendor_dir / "extensions",
        offline=True,
        fetcher=_forbidden_fetcher,
    )
    assert [p.relative_to(ext_dir).as_posix() for p in installed] == [
        "v2.0.0/linux_amd64/httpfs.duckdb_extension",
        "v2.0.0/linux_amd64/avro.duckdb_extension",
        "v2.0.0/linux_amd64/iceberg.duckdb_extension",
    ]
    assert (
        ext_dir / "v2.0.0" / "linux_amd64" / "httpfs.duckdb_extension"
    ).read_bytes() == raw_map[("linux_amd64", "httpfs")]

    # Offline install fails closed if vendor archive for requested platform is missing
    with pytest.raises(prefetch_mod.PrefetchError, match="Offline mode enabled"):
        prefetch_mod.install_extensions(
            extensions_lock_path=lock_path,
            extension_directory=ext_dir,
            platform="linux_arm64",
            vendor_extensions_dir=vendor_dir / "extensions",
            offline=True,
            fetcher=_forbidden_fetcher,
        )


def test_prefetch_wheels_and_verify_vendor_with_fake_uv_lock(tmp_path):
    fake_uv_lock, wheel_urls, pkg_shas = _make_fake_uv_lock_with_build_backend(tmp_path)
    ext_lock, _, gz_map = _make_fake_ext_lock(tmp_path)
    url_map = {
        **wheel_urls,
        **{
            f"https://extensions.duckdb.org/v2.0.0/{plat}/{name}.duckdb_extension.gz": data
            for (plat, name), data in gz_map.items()
        },
    }

    vendor_dir = tmp_path / "vendor"

    def _fake_export(out_file: Path) -> None:
        out_file.write_text(
            f"duckdb==2.0.0 \\\n    --hash=sha256:{pkg_shas['duckdb']}\n",
            encoding="utf-8",
        )

    wheels, exts = prefetch_mod.prefetch_all(
        vendor_dir,
        uv_lock_path=fake_uv_lock,
        extensions_lock_path=ext_lock,
        platforms=["linux_amd64"],
        fetcher=lambda url: url_map[url],
        export_runner=_fake_export,
    )
    assert len(wheels) == 6
    assert len(exts) == 3

    # Vendor verification passes on untampered artifacts
    prefetch_mod.verify_vendor_directory(
        vendor_dir,
        uv_lock_path=fake_uv_lock,
        extensions_lock_path=ext_lock,
        platform="linux_amd64",
    )

    # Corrupt wheel bytes -> verify_wheel_directory fails closed
    original_wheel_bytes = wheels[0].read_bytes()
    wheels[0].write_bytes(b"PK\x03\x04TAMPERED_BYTES________123")
    with pytest.raises(prefetch_mod.HashMismatchError):
        prefetch_mod.verify_wheel_directory(vendor_dir / "wheels", fake_uv_lock)

    # Restore valid wheel, add unexpected wheel -> fails closed
    wheels[0].write_bytes(original_wheel_bytes)
    rogue = vendor_dir / "wheels" / "rogue-0.1.0-py3-none-any.whl"
    rogue.write_bytes(b"rogue")
    with pytest.raises(prefetch_mod.PrefetchError, match="Unrecognized artifact"):
        prefetch_mod.verify_wheel_directory(vendor_dir / "wheels", fake_uv_lock)


def test_check_serving_env_passes_when_locked_and_fails_on_any_violation(
    tmp_path, monkeypatch
):
    monkeypatch.delenv("SEEKNAL_EXTENSION_DIRECTORY", raising=False)
    uv_lock = REPO_ROOT / "uv.lock"
    locked_versions = check_env_mod._load_locked_versions(uv_lock)
    valid_installed = {
        pkg: locked_versions[check_env_mod.normalize_pep503(pkg)]
        for pkg in check_env_mod.KEY_PACKAGES
    }
    valid_installed["seeknal"] = "2.14.0"

    ext_dir = tmp_path / "baked_ext"
    ext_lock, raw_map, _ = _make_fake_ext_lock(tmp_path, ext_dir=ext_dir)
    for (plat, name), raw_bytes in raw_map.items():
        dest = ext_dir / "v2.0.0" / plat / f"{name}.duckdb_extension"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(raw_bytes)

    loaded_calls: list[tuple[Path, str]] = []

    def _fake_loader(root: Path, ext_name: str) -> None:
        loaded_calls.append((root, ext_name))

    pip_checked = False

    def _fake_pip_check() -> None:
        nonlocal pip_checked
        pip_checked = True

    # 1. Happy path passes (including real seeknal.serving.accel.settings import)
    check_env_mod.verify_serving_environment(
        uv_lock_path=uv_lock,
        extensions_lock_path=ext_lock,
        extension_directory=ext_dir,
        platform="linux_amd64",
        installed_versions_provider=lambda: dict(valid_installed),
        runtime_duckdb_version="2.0.0",
        expected_duckdb_version="2.0.0",
        extension_loader=_fake_loader,
        pip_check_runner=_fake_pip_check,
    )
    assert pip_checked is True
    assert [name for _, name in loaded_calls] == ["httpfs", "avro", "iceberg"]

    # 2. Key package version drift fails
    drifted = dict(valid_installed, starlette="0.0.1")
    with pytest.raises(check_env_mod.EnvCheckError, match="Version mismatch"):
        check_env_mod.verify_serving_environment(
            uv_lock_path=uv_lock,
            extensions_lock_path=ext_lock,
            extension_directory=ext_dir,
            platform="linux_amd64",
            installed_versions_provider=lambda: drifted,
            runtime_duckdb_version="2.0.0",
            extension_loader=_fake_loader,
            pip_check_runner=_fake_pip_check,
        )

    # 3. Forbidden package installed fails
    polluted = dict(valid_installed)
    polluted["pydantic-ai-slim"] = "1.108.0"
    with pytest.raises(check_env_mod.EnvCheckError, match="Forbidden non-serving"):
        check_env_mod.verify_serving_environment(
            uv_lock_path=uv_lock,
            extensions_lock_path=ext_lock,
            extension_directory=ext_dir,
            platform="linux_amd64",
            installed_versions_provider=lambda: polluted,
            runtime_duckdb_version="2.0.0",
            extension_loader=_fake_loader,
            pip_check_runner=_fake_pip_check,
        )

    # 4. Runtime duckdb.__version__ mismatch fails
    with pytest.raises(check_env_mod.EnvCheckError, match="DuckDB runtime version"):
        check_env_mod.verify_serving_environment(
            uv_lock_path=uv_lock,
            extensions_lock_path=ext_lock,
            extension_directory=ext_dir,
            platform="linux_amd64",
            installed_versions_provider=lambda: dict(valid_installed),
            runtime_duckdb_version="1.4.4",
            extension_loader=_fake_loader,
            pip_check_runner=_fake_pip_check,
        )

    # 5. Agreement with seeknal.serving.accel.settings when importable
    fake_accel_pkg = types.ModuleType("seeknal.serving.accel")
    fake_accel_settings = types.ModuleType("seeknal.serving.accel.settings")
    fake_accel_settings.DEFAULT_EXPECTED_DUCKDB_VERSION = "2.0.0"
    monkeypatch.setitem(sys.modules, "seeknal.serving.accel", fake_accel_pkg)
    monkeypatch.setitem(
        sys.modules, "seeknal.serving.accel.settings", fake_accel_settings
    )
    check_env_mod.check_duckdb_version_pin(
        runtime_duckdb_version="2.0.0", locked_duckdb_version="2.0.0"
    )

    fake_accel_settings.DEFAULT_EXPECTED_DUCKDB_VERSION = "1.4.4"
    with pytest.raises(check_env_mod.EnvCheckError, match="disagreement"):
        check_env_mod.check_duckdb_version_pin(
            runtime_duckdb_version="2.0.0", locked_duckdb_version="2.0.0"
        )

    # 6. Tampered baked .duckdb_extension raw_sha256 fails closed
    httpfs_file = ext_dir / "v2.0.0" / "linux_amd64" / "httpfs.duckdb_extension"
    httpfs_file.write_bytes(b"CORRUPTED_EXTENSION_BINARY")
    with pytest.raises(check_env_mod.EnvCheckError, match="SHA-256 mismatch"):
        check_env_mod.check_baked_extensions(
            ext_lock,
            extension_directory=ext_dir,
            platform="linux_amd64",
            extension_loader=_fake_loader,
        )

    # 7. Real DuckDB _default_extension_loader fails closed offline on missing extension
    with pytest.raises(check_env_mod.EnvCheckError, match="DuckDB failed to LOAD"):
        check_env_mod._default_extension_loader(tmp_path / "empty_ext_dir", "httpfs")


def test_select_serving_wheels_against_real_uv_lock():
    wheels = prefetch_mod.select_serving_wheels(
        REPO_ROOT / "uv.lock", platforms=["linux_amd64", "linux_arm64"]
    )
    selected_pkgs = {w.package_name for w in wheels}
    for key_pkg in check_env_mod.KEY_PACKAGES:
        assert prefetch_mod.normalize_pep503(key_pkg) in selected_pkgs
    for forbidden_pkg in check_env_mod.FORBIDDEN_PACKAGES:
        assert prefetch_mod.normalize_pep503(forbidden_pkg) not in selected_pkgs

    duckdb_wheels = [w for w in wheels if w.package_name == "duckdb"]
    assert all(w.version == "2.0.0" for w in duckdb_wheels)
    assert any("x86_64" in w.filename for w in duckdb_wheels)
    assert any("aarch64" in w.filename for w in duckdb_wheels)


def test_cli_entrypoints_return_nonzero_on_errors(tmp_path, capsys):
    # prefetch CLI verify-vendor fails with exit code 1 when vendor directory is missing
    rc = prefetch_mod.main(
        [
            "verify-vendor",
            "--vendor-dir",
            str(tmp_path / "missing_vendor"),
            "--uv-lock",
            str(REPO_ROOT / "uv.lock"),
            "--extensions-lock",
            str(REPO_ROOT / "docker" / "serving-image" / "extensions.lock.json"),
        ]
    )
    assert rc == 1
    assert "ERROR:" in capsys.readouterr().err

    # check_serving_env CLI fails with exit code 1 when baked extensions are absent
    rc_env = check_env_mod.main(
        [
            "--uv-lock",
            str(REPO_ROOT / "uv.lock"),
            "--extensions-lock",
            str(REPO_ROOT / "docker" / "serving-image" / "extensions.lock.json"),
            "--extension-directory",
            str(tmp_path / "nonexistent_duckdb_ext"),
            "--platform",
            "linux_amd64",
        ]
    )
    assert rc_env == 1
    assert "SERVING ENV CHECK FAILED:" in capsys.readouterr().err
