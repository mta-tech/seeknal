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
    for key in config.api_keys:
        if hmac.compare_digest(key.encode(), provided.encode()):
            return

    raise AuthError("Invalid API key")
