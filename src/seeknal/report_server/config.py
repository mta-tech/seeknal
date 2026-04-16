from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from seeknal.utils.path_security import is_insecure_path


@dataclass
class ServerConfig:
    data_dir: Path = field(default_factory=lambda: Path.home() / ".seeknal" / "report-server")
    auth_mode: str = "open"
    api_keys: list[str] = field(default_factory=list)
    max_upload_bytes: int = 100 * 1024 * 1024  # 100 MB
    cors_origins: list[str] = field(default_factory=list)

    @classmethod
    def from_env(cls) -> ServerConfig:
        data_dir = Path(os.environ.get("SEEKNAL_REPORT_SERVER_DATA_DIR", Path.home() / ".seeknal" / "report-server"))
        auth_mode = os.environ.get("SEEKNAL_REPORT_SERVER_AUTH_MODE", "open")
        raw_keys = os.environ.get("SEEKNAL_REPORT_SERVER_KEYS", "")
        api_keys = [k.strip() for k in raw_keys.split(",") if k.strip()] if raw_keys else []
        max_upload_bytes = int(os.environ.get("SEEKNAL_REPORT_SERVER_MAX_UPLOAD_BYTES", str(100 * 1024 * 1024)))
        raw_origins = os.environ.get("SEEKNAL_REPORT_SERVER_CORS_ORIGINS", "")
        cors_origins = [o.strip() for o in raw_origins.split(",") if o.strip()] if raw_origins else []
        return cls(
            data_dir=data_dir,
            auth_mode=auth_mode,
            api_keys=api_keys,
            max_upload_bytes=max_upload_bytes,
            cors_origins=cors_origins,
        )

    def validate(self) -> None:
        if is_insecure_path(str(self.data_dir)):
            raise ValueError(
                f"Insecure data_dir detected: '{self.data_dir}'. "
                "Use a secure location such as ~/.seeknal/report-server"
            )
        if self.auth_mode not in ("open", "api_key"):
            raise ValueError(
                f"Invalid auth_mode '{self.auth_mode}'. Must be 'open' or 'api_key'."
            )
