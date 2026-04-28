from __future__ import annotations

import io
import shutil
import tarfile
from pathlib import Path

import pytest
from sqlalchemy.engine import Engine
from starlette.testclient import TestClient

from seeknal.report_server.app import create_app
from seeknal.report_server.config import ServerConfig
from seeknal.report_server.models import get_engine
from seeknal.report_server.storage import LocalFilesystemStorage


@pytest.fixture()
def test_config(tmp_path: Path) -> ServerConfig:
    data_dir = Path.cwd() / ".seeknal" / "test-tmp" / tmp_path.name
    shutil.rmtree(data_dir, ignore_errors=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    return ServerConfig(
        data_dir=data_dir,
        auth_mode="api_key",
        api_keys=["test-key-123"],
    )


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


def _make_tarball(tmp_path: Path, members: list[tuple]) -> Path:
    """Build a .tar.gz file at tmp_path/test.tar.gz.

    members: list of (arcname, content_bytes, type) where type is
    'file', 'absolute', 'symlink', or 'hardlink'.
    """
    out = tmp_path / "test.tar.gz"
    with tarfile.open(out, "w:gz") as tf:
        for item in members:
            arcname, content, kind = item
            if kind == "file":
                data = content if isinstance(content, bytes) else content.encode()
                info = tarfile.TarInfo(name=arcname)
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
            elif kind == "absolute":
                info = tarfile.TarInfo(name=arcname)
                info.size = len(content)
                tf.addfile(info, io.BytesIO(content))
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
