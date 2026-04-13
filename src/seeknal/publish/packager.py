from __future__ import annotations

import tarfile
import tempfile
from pathlib import Path

from seeknal.publish.exceptions import PackageTooLargeError


def package_build(build_dir: Path, max_bytes: int = 250 * 1024 * 1024) -> Path:
    tmp = tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False)
    tmp.close()
    tarball_path = Path(tmp.name)

    cumulative = 0
    with tarfile.open(tarball_path, mode="w:gz") as tf:
        for file_path in sorted(build_dir.rglob("*")):
            if not file_path.is_file():
                continue
            file_size = file_path.stat().st_size
            cumulative += file_size
            if cumulative > max_bytes:
                tarball_path.unlink(missing_ok=True)
                raise PackageTooLargeError(
                    f"Build directory exceeds {max_bytes} bytes uncompressed "
                    f"(stopped at {file_path.relative_to(build_dir)})"
                )
            tf.add(file_path, arcname=str(file_path.relative_to(build_dir)))

    return tarball_path
