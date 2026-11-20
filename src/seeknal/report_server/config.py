from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from seeknal.utils.path_security import is_insecure_path

DEFAULT_MAX_UPLOAD_BYTES = 100 * 1024 * 1024  # 100 MB
DEFAULT_MAX_TAR_MEMBERS = 10_000
DEFAULT_MAX_UNPACKED_BYTES = 512 * 1024 * 1024  # 512 MiB
DEFAULT_MAX_PATH_DEPTH = 32
DEFAULT_MAX_MEMBER_NAME_LENGTH = 1024
DEFAULT_MAX_CONCURRENT_PUBLISHES = 4

_POSITIVE_INT_FIELDS: tuple[tuple[str, str], ...] = (
    ("max_upload_bytes", "SEEKNAL_REPORT_SERVER_MAX_UPLOAD_BYTES"),
    ("max_tar_members", "SEEKNAL_REPORT_SERVER_MAX_TAR_MEMBERS"),
    ("max_unpacked_bytes", "SEEKNAL_REPORT_SERVER_MAX_UNPACKED_BYTES"),
    ("max_path_depth", "SEEKNAL_REPORT_SERVER_MAX_PATH_DEPTH"),
    ("max_member_name_length", "SEEKNAL_REPORT_SERVER_MAX_MEMBER_NAME_LENGTH"),
    ("max_concurrent_publishes", "SEEKNAL_REPORT_SERVER_MAX_CONCURRENT_PUBLISHES"),
)


def _parse_int_env(env_var: str, default: int) -> int:
    raw = os.environ.get(env_var)
    if raw is None:
        return default
    stripped = raw.strip()
    try:
        value = int(stripped)
    except ValueError as exc:
        raise ValueError(
            f"Invalid {env_var}='{raw}': must be a positive integer."
        ) from exc
    if value <= 0:
        raise ValueError(
            f"Invalid {env_var}='{raw}': must be a positive integer."
        )
    return value


@dataclass
class ServerConfig:
    """Configuration for the Seeknal Report Server.

    By default, ``auth_mode`` is ``"api_key"`` and requires at least one key in
    ``api_keys`` (configured via ``SEEKNAL_REPORT_SERVER_KEYS``). To run without
    authentication for local development, explicitly opt in with
    ``auth_mode="open"`` or ``SEEKNAL_REPORT_SERVER_AUTH_MODE=open``.
    """

    data_dir: Path = field(default_factory=lambda: Path.home() / ".seeknal" / "report-server")
    auth_mode: str = "api_key"
    api_keys: list[str] = field(default_factory=list)
    max_upload_bytes: int = DEFAULT_MAX_UPLOAD_BYTES
    max_tar_members: int = DEFAULT_MAX_TAR_MEMBERS
    max_unpacked_bytes: int = DEFAULT_MAX_UNPACKED_BYTES
    max_path_depth: int = DEFAULT_MAX_PATH_DEPTH
    max_member_name_length: int = DEFAULT_MAX_MEMBER_NAME_LENGTH
    max_concurrent_publishes: int = DEFAULT_MAX_CONCURRENT_PUBLISHES
    cors_origins: list[str] = field(default_factory=list)

    @classmethod
    def from_env(cls) -> ServerConfig:
        data_dir = Path(os.environ.get("SEEKNAL_REPORT_SERVER_DATA_DIR", Path.home() / ".seeknal" / "report-server"))
        auth_mode = os.environ.get("SEEKNAL_REPORT_SERVER_AUTH_MODE", "api_key")
        raw_keys = os.environ.get("SEEKNAL_REPORT_SERVER_KEYS", "")
        api_keys = [k.strip() for k in raw_keys.split(",") if k.strip()] if raw_keys else []
        max_upload_bytes = _parse_int_env("SEEKNAL_REPORT_SERVER_MAX_UPLOAD_BYTES", DEFAULT_MAX_UPLOAD_BYTES)
        max_tar_members = _parse_int_env("SEEKNAL_REPORT_SERVER_MAX_TAR_MEMBERS", DEFAULT_MAX_TAR_MEMBERS)
        max_unpacked_bytes = _parse_int_env("SEEKNAL_REPORT_SERVER_MAX_UNPACKED_BYTES", DEFAULT_MAX_UNPACKED_BYTES)
        max_path_depth = _parse_int_env("SEEKNAL_REPORT_SERVER_MAX_PATH_DEPTH", DEFAULT_MAX_PATH_DEPTH)
        max_member_name_length = _parse_int_env(
            "SEEKNAL_REPORT_SERVER_MAX_MEMBER_NAME_LENGTH", DEFAULT_MAX_MEMBER_NAME_LENGTH
        )
        max_concurrent_publishes = _parse_int_env(
            "SEEKNAL_REPORT_SERVER_MAX_CONCURRENT_PUBLISHES", DEFAULT_MAX_CONCURRENT_PUBLISHES
        )
        raw_origins = os.environ.get("SEEKNAL_REPORT_SERVER_CORS_ORIGINS", "")
        cors_origins = [o.strip() for o in raw_origins.split(",") if o.strip()] if raw_origins else []
        return cls(
            data_dir=data_dir,
            auth_mode=auth_mode,
            api_keys=api_keys,
            max_upload_bytes=max_upload_bytes,
            max_tar_members=max_tar_members,
            max_unpacked_bytes=max_unpacked_bytes,
            max_path_depth=max_path_depth,
            max_member_name_length=max_member_name_length,
            max_concurrent_publishes=max_concurrent_publishes,
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
        if any(not isinstance(k, str) or not k.strip() for k in self.api_keys):
            raise ValueError(
                "Invalid SEEKNAL_REPORT_SERVER_KEYS: API keys must not contain blank or whitespace-only entries."
            )
        if self.auth_mode == "api_key" and not self.api_keys:
            raise ValueError(
                "report_server auth_mode='api_key' requires at least one API key: "
                "set SEEKNAL_REPORT_SERVER_KEYS, or explicitly opt in to unauthenticated "
                "mode with SEEKNAL_REPORT_SERVER_AUTH_MODE=open."
            )
        for attr_name, env_var in _POSITIVE_INT_FIELDS:
            val = getattr(self, attr_name)
            if isinstance(val, bool) or not isinstance(val, int) or val <= 0:
                raise ValueError(
                    f"Invalid {attr_name} ({env_var}): {val!r} must be a positive integer."
                )
