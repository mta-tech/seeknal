from __future__ import annotations

import io
import shutil
import sys
import tarfile
import types
from pathlib import Path

import pytest
from sqlalchemy.engine import Engine
from starlette.testclient import TestClient

# In partial-tree test checkouts where top-level seeknal submodules (e.g. seeknal.validation)
# are not present, pre-seed the seeknal package and stubs so report_server tests can run.
_REPO_SRC = Path(__file__).resolve().parents[2] / "src"
if "seeknal" not in sys.modules and not (_REPO_SRC / "seeknal" / "validation.py").exists():
    _seeknal_pkg = types.ModuleType("seeknal")
    _seeknal_pkg.__path__ = [str(_REPO_SRC / "seeknal")]
    _seeknal_pkg.__version__ = "2.14.0"
    sys.modules["seeknal"] = _seeknal_pkg

if "seeknal.core_utils" not in sys.modules and not (_REPO_SRC / "seeknal" / "core_utils.py").exists():
    _core_utils = types.ModuleType("seeknal.core_utils")
    for _attr in ("Later", "is_notebook", "to_snake", "pretty_returns", "get_df_schema", "check_is_dict_same"):
        setattr(_core_utils, _attr, None)
    sys.modules["seeknal.core_utils"] = _core_utils

if "seeknal.models" not in sys.modules and not (_REPO_SRC / "seeknal" / "models.py").exists():
    from sqlalchemy import MetaData as _SA_MetaData

    _models_mod = types.ModuleType("seeknal.models")
    _models_mod.metadata = _SA_MetaData()  # type: ignore[attr-defined]
    sys.modules["seeknal.models"] = _models_mod

from seeknal.report_server.app import create_app
from seeknal.report_server.config import ServerConfig
from seeknal.report_server.models import get_engine
from seeknal.report_server.storage import LocalFilesystemStorage
from seeknal.utils.path_security import is_insecure_path


@pytest.fixture()
def test_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    data_dir = Path.cwd() / ".seeknal" / "test-tmp" / tmp_path.name
    shutil.rmtree(data_dir, ignore_errors=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    if is_insecure_path(str(data_dir)):
        # If the repository checkout itself resides under /tmp (e.g. isolated worker run root),
        # allow data_dir under this checkout for fixture setup.
        real_is_insecure = is_insecure_path
        checkout_prefix = str(Path.cwd() / ".seeknal" / "test-tmp")

        def _allow_test_tmp(p: str) -> bool:
            if str(p).startswith(checkout_prefix):
                return False
            return real_is_insecure(p)

        monkeypatch.setattr("seeknal.report_server.config.is_insecure_path", _allow_test_tmp)
        monkeypatch.setattr("seeknal.report_server.storage.is_insecure_path", _allow_test_tmp)
    try:
        yield ServerConfig(
            data_dir=data_dir,
            auth_mode="api_key",
            api_keys=["test-key-123"],
        )
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)
        test_tmp_root = Path.cwd() / ".seeknal" / "test-tmp"
        if test_tmp_root.exists() and not any(test_tmp_root.iterdir()):
            test_tmp_root.rmdir()
        seeknal_root = Path.cwd() / ".seeknal"
        if seeknal_root.exists() and not any(seeknal_root.iterdir()):
            seeknal_root.rmdir()


@pytest.fixture()
def test_engine(test_config: ServerConfig) -> Engine:
    return get_engine(test_config.data_dir)


@pytest.fixture()
def test_storage(test_config: ServerConfig) -> LocalFilesystemStorage:
    return LocalFilesystemStorage(test_config.data_dir / "assets")


@pytest.fixture()
def test_app(test_config: ServerConfig):
    return create_app(test_config)


@pytest.fixture()
def test_client(test_app) -> TestClient:
    return TestClient(test_app, raise_server_exceptions=True)


def _make_tarball(tmp_path: Path, members: list[tuple], filename: str = "test.tar.gz") -> Path:
    """Build a .tar.gz file at tmp_path/{filename}.

    members: list of (arcname, content_bytes, type) where type is
    'file', 'absolute', 'symlink', 'hardlink', 'chrdev', 'blkdev', or 'fifo'.
    """
    out = tmp_path / filename
    with tarfile.open(out, "w:gz") as tf:
        for item in members:
            arcname, content, kind = item
            if kind == "file":
                data = content if isinstance(content, bytes) else content.encode()
                info = tarfile.TarInfo(name=arcname)
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
            elif kind == "absolute":
                data = content if isinstance(content, bytes) else content.encode()
                info = tarfile.TarInfo(name=arcname)
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
            elif kind == "symlink":
                info = tarfile.TarInfo(name=arcname)
                info.type = tarfile.SYMTYPE
                info.linkname = content.decode() if isinstance(content, bytes) else content
                tf.addfile(info)
            elif kind == "hardlink":
                info = tarfile.TarInfo(name=arcname)
                info.type = tarfile.LNKTYPE
                info.linkname = content.decode() if isinstance(content, bytes) else content
                tf.addfile(info)
            elif kind == "chrdev":
                info = tarfile.TarInfo(name=arcname)
                info.type = tarfile.CHRTYPE
                info.devmajor = 1
                info.devminor = 3
                tf.addfile(info)
            elif kind == "blkdev":
                info = tarfile.TarInfo(name=arcname)
                info.type = tarfile.BLKTYPE
                info.devmajor = 8
                info.devminor = 0
                tf.addfile(info)
            elif kind == "fifo":
                info = tarfile.TarInfo(name=arcname)
                info.type = tarfile.FIFOTYPE
                tf.addfile(info)
    return out


@pytest.fixture()
def honest_tarball(tmp_path: Path) -> Path:
    members = [
        ("index.html", b"<html><body>Report</body></html>", "file"),
        ("_app/asset.js", b"console.log('hello');", "file"),
        ("data.duckdb", b"\x00\x01\x02\x03", "file"),
    ]
    return _make_tarball(tmp_path, members)


@pytest.fixture()
def malicious_tarball_path_traversal(tmp_path: Path) -> Path:
    members = [
        ("../../etc/passwd", b"root:x:0:0:root:/root:/bin/bash", "file"),
    ]
    return _make_tarball(tmp_path, members)


@pytest.fixture()
def malicious_tarball_absolute(tmp_path: Path) -> Path:
    out = tmp_path / "absolute.tar.gz"
    with tarfile.open(out, "w:gz") as tf:
        content = b"root:x:0:0:root:/root:/bin/bash"
        info = tarfile.TarInfo(name="/etc/passwd")
        info.size = len(content)
        tf.addfile(info, io.BytesIO(content))
    return out


@pytest.fixture()
def malicious_tarball_symlink(tmp_path: Path) -> Path:
    members = [
        ("evil_link", "/etc/passwd", "symlink"),
    ]
    return _make_tarball(tmp_path, members)
