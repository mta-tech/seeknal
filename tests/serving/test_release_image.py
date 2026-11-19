"""Unit tests for the lock-only, air-gapped atlas-rt-serving release image."""

from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
import sys
import tomllib
import types
import urllib.request
from pathlib import Path

import pytest

from seeknal.serving.buildinfo import EXPECTED_DUCKDB_VERSION

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_script_module(module_name: str, rel_path: str):
    script_path = REPO_ROOT / rel_path
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


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
) -> tuple[Path, dict[tuple[str, str], bytes], dict[tuple[str, str], bytes]]:
    if payloads is None:
        payloads = {
            ("linux_amd64", "httpfs"): b"FAKE-HTTPFS-AMD64-BINARY",
            ("linux_amd64", "iceberg"): b"FAKE-ICEBERG-AMD64-BINARY",
            ("linux_arm64", "httpfs"): b"FAKE-HTTPFS-ARM64-BINARY",
            ("linux_arm64", "iceberg"): b"FAKE-ICEBERG-ARM64-BINARY",
        }
    target_ext_dir = ext_dir or (tmp_path / "duckdb_extensions")
    gz_map: dict[tuple[str, str], bytes] = {}
    entries = []
    for (plat, name), raw_bytes in payloads.items():
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
    duckdb_pkgs = [
        p for p in lock_data["package"] if p["name"] == "duckdb"
    ]
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
        ("linux_amd64", "iceberg"),
        ("linux_arm64", "httpfs"),
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
        by_key[("linux_arm64", "iceberg")].gz_sha256
        == "dc5fc010b24a044ec2dc34294d8d251f481560cfbd5819c22f1fb1b6cc0032ae"
    )
    assert (
        by_key[("linux_arm64", "iceberg")].raw_sha256
        == "5616c30201099564326826d223aeea19b59f15744a981b074a906fb2b9dd4f44"
    )
    assert by_key[("linux_arm64", "iceberg")].gz_bytes == 16161197


def test_dockerfile_serving_lock_only_offline_and_verification_contract():
    dockerfile = (REPO_ROOT / "docker" / "Dockerfile.serving").read_text(
        encoding="utf-8"
    )

    # 1. Pinned uv 0.9.18 base image
    assert (
        "ARG BASE_IMAGE=ghcr.io/astral-sh/uv:0.9.18-python3.11-bookworm-slim"
        in dockerfile
    )

    # 2. Online lock-only sequence in strict order
    idx_export = dockerfile.index(
        "uv export --locked --only-group serving --no-emit-project --format requirements-txt -o /tmp/requirements.txt"
    )
    idx_install_deps = dockerfile.index(
        "uv pip install --system --require-hashes -r /tmp/requirements.txt"
    )
    idx_install_pkg = dockerfile.index("uv pip install --system --no-deps .")
    assert idx_export < idx_install_deps < idx_install_pkg

    # 3. Offline (--network=none) vendor verification & install sequence
    assert "prefetch.py verify-vendor" in dockerfile
    assert "--offline --no-index --require-hashes" in dockerfile
    assert "--find-links docker/serving-image/vendor/wheels" in dockerfile
    assert "prefetch.py install-extensions" in dockerfile
    assert "/opt/seeknal/duckdb_extensions" in dockerfile

    # 4. Build metadata JSON and fail-closed check_serving_env.py before cleanup
    idx_buildinfo = dockerfile.index('write_build_info("/opt/seeknal/buildinfo.json"')
    idx_check_env = dockerfile.index("python3 docker/serving-image/check_serving_env.py")
    idx_cleanup = dockerfile.index("rm -rf /root/.cache")
    assert idx_install_pkg < idx_buildinfo < idx_check_env < idx_cleanup

    # 5. Runtime contract preserved
    assert "USER 10001:10001" in dockerfile
    assert "EXPOSE 8765" in dockerfile
    assert 'ENTRYPOINT ["python", "-m", "seeknal.serving"]' in dockerfile
    assert 'CMD ["--config", "/etc/seeknal/serving.yaml"]' in dockerfile


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
    assert len(written_gz) == 2

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
    wheel_bytes = b"PK\x03\x04FAKE_DUCKDB_WHEEL_CONTENT"
    wheel_sha = hashlib.sha256(wheel_bytes).hexdigest()
    wheel_name = "duckdb-2.0.0-cp311-cp311-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl"
    wheel_url = f"https://files.pythonhosted.org/packages/e4/a1/{wheel_name}"

    fake_uv_lock = tmp_path / "uv.lock"
    fake_uv_lock.write_text(
        f"""
version = 1
revision = 5
requires-python = ">=3.11"

[[package]]
name = "duckdb"
version = "2.0.0"
wheels = [
    {{ url = "{wheel_url}", hash = "sha256:{wheel_sha}", size = {len(wheel_bytes)} }},
]

[[package]]
name = "seeknal"
version = "2.14.0"

[package.dev-dependencies]
serving = [
    {{ name = "duckdb" }},
]
""",
        encoding="utf-8",
    )

    ext_lock, _, gz_map = _make_fake_ext_lock(tmp_path)
    url_map = {
        wheel_url: wheel_bytes,
        **{
            f"https://extensions.duckdb.org/v2.0.0/{plat}/{name}.duckdb_extension.gz": data
            for (plat, name), data in gz_map.items()
        },
    }

    vendor_dir = tmp_path / "vendor"

    def _fake_export(out_file: Path) -> None:
        out_file.write_text(
            f"duckdb==2.0.0 \\\n    --hash=sha256:{wheel_sha}\n",
            encoding="utf-8",
        )

    wheels, exts = prefetch_mod.prefetch_all(
        vendor_dir,
        uv_lock_path=fake_uv_lock,
        extensions_lock_path=ext_lock,
        platforms=["linux_amd64"],
        fetcher=lambda url: url_map[url],
        export_runner=_fake_export,
        build_project_wheel=False,
    )
    assert len(wheels) == 1
    assert len(exts) == 2

    # Vendor verification passes on untampered artifacts
    prefetch_mod.verify_vendor_directory(
        vendor_dir,
        uv_lock_path=fake_uv_lock,
        extensions_lock_path=ext_lock,
        platform="linux_amd64",
    )

    # Corrupt wheel bytes -> verify_wheel_directory fails closed
    wheels[0].write_bytes(b"PK\x03\x04TAMPERED_BYTES________123")
    with pytest.raises(prefetch_mod.HashMismatchError):
        prefetch_mod.verify_wheel_directory(vendor_dir / "wheels", fake_uv_lock)

    # Restore valid wheel, add unexpected wheel -> fails closed
    wheels[0].write_bytes(wheel_bytes)
    rogue = vendor_dir / "wheels" / "rogue-0.1.0-py3-none-any.whl"
    rogue.write_bytes(b"rogue")
    with pytest.raises(prefetch_mod.PrefetchError, match="Unrecognized artifact"):
        prefetch_mod.verify_wheel_directory(vendor_dir / "wheels", fake_uv_lock)


def test_check_serving_env_passes_when_locked_and_fails_on_any_violation(
    tmp_path, monkeypatch
):
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

    # 1. Happy path passes
    check_env_mod.verify_serving_environment(
        uv_lock_path=uv_lock,
        extensions_lock_path=ext_lock,
        extension_directory=ext_dir,
        platform="linux_amd64",
        installed_versions_provider=lambda: dict(valid_installed),
        runtime_duckdb_version="2.0.0",
        expected_duckdb_version="2.0.0",
        accel_expected_duckdb_version=None,
        extension_loader=_fake_loader,
        pip_check_runner=_fake_pip_check,
    )
    assert pip_checked is True
    assert [name for _, name in loaded_calls] == ["httpfs", "iceberg"]

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
    check_env_mod.check_duckdb_version_pin(runtime_duckdb_version="2.0.0")

    fake_accel_settings.DEFAULT_EXPECTED_DUCKDB_VERSION = "1.4.4"
    with pytest.raises(check_env_mod.EnvCheckError, match="disagreement"):
        check_env_mod.check_duckdb_version_pin(runtime_duckdb_version="2.0.0")

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

