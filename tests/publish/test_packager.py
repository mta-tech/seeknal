from __future__ import annotations

import tarfile
from pathlib import Path

import pytest

from seeknal.publish.exceptions import PackageTooLargeError
from seeknal.publish.packager import package_build


class TestPackageBuild:
    def test_roundtrip_3_file_tree(self, tmp_path: Path):
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        (build_dir / "index.html").write_text("<html><body>Report</body></html>")
        (build_dir / "style.css").write_text("body { margin: 0; }")
        (build_dir / "data.duckdb").write_bytes(b"\x00\x01\x02\x03duck")

        tarball = package_build(build_dir)

        assert tarball.exists()
        assert tarball.suffix in (".gz",)

        extract_dir = tmp_path / "extracted"
        extract_dir.mkdir()
        with tarfile.open(tarball, "r:gz") as tf:
            tf.extractall(extract_dir)

        assert (extract_dir / "index.html").exists()
        assert (extract_dir / "style.css").exists()
        assert (extract_dir / "data.duckdb").exists()

        assert (extract_dir / "index.html").read_text() == "<html><body>Report</body></html>"
        assert (extract_dir / "style.css").read_text() == "body { margin: 0; }"
        assert (extract_dir / "data.duckdb").read_bytes() == b"\x00\x01\x02\x03duck"

        tarball.unlink(missing_ok=True)

    def test_rejects_oversized_build(self, tmp_path: Path):
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        big_file = build_dir / "big.bin"
        big_file.write_bytes(b"x" * 200)

        with pytest.raises(PackageTooLargeError):
            package_build(build_dir, max_bytes=100)

    def test_returns_path_object(self, tmp_path: Path):
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        (build_dir / "index.html").write_text("hi")

        result = package_build(build_dir)
        assert isinstance(result, Path)
        result.unlink(missing_ok=True)

    def test_nested_directories_included(self, tmp_path: Path):
        build_dir = tmp_path / "build"
        (build_dir / "_app").mkdir(parents=True)
        (build_dir / "index.html").write_text("root")
        (build_dir / "_app" / "bundle.js").write_text("js")

        tarball = package_build(build_dir)

        extract_dir = tmp_path / "extracted"
        extract_dir.mkdir()
        with tarfile.open(tarball, "r:gz") as tf:
            tf.extractall(extract_dir)

        assert (extract_dir / "index.html").exists()
        assert (extract_dir / "_app" / "bundle.js").exists()

        tarball.unlink(missing_ok=True)
