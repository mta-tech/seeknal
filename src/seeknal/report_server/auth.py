from __future__ import annotations

import hmac

from starlette.requests import Request

from seeknal.report_server.config import ServerConfig


class AuthError(Exception):
    pass


async def require_publish_auth(request: Request, config: ServerConfig) -> None:
    if config.auth_mode == "open":
        return

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise AuthError("Missing or malformed Authorization header")

    provided = auth_header[len("Bearer "):]
    if not provided.strip():
        raise AuthError("Invalid API key")

    for key in config.api_keys:
        if key and key.strip() and hmac.compare_digest(key.encode(), provided.encode()):
            return

    raise AuthError("Invalid API key")
