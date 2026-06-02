"""
Seeknal CLI - Keycloak/OIDC authentication.

A command group for logging the engine user in against the Atlas Keycloak realm
using the OAuth 2.0 Authorization Code flow with PKCE (S256) over a localhost
loopback redirect. The resulting tokens are written to the seeknal credentials
file so the governance gate and ``seeknal gov`` commands can attribute requests
to the logged-in user.

Usage:
    seeknal auth login
    seeknal auth login --no-browser
    seeknal auth status
    seeknal auth logout

The Keycloak ``seeknal-cli`` client is public (no secret) with PKCE S256 and the
standard authorization-code flow enabled; RFC 8628 device flow is *not* enabled,
so login uses a browser-based loopback redirect on ``http://127.0.0.1:8765/callback``.
"""

from __future__ import annotations

import base64
import hashlib
import http.server
import json
import os
import secrets
import threading
import urllib.parse
import webbrowser
from pathlib import Path
from typing import Any, Optional

import httpx
import typer

from seeknal.integrations.atlas_governance import (
    credentials_path,
    user_email_from_credentials,
    user_sub_from_credentials,
)
from seeknal.ui.output import echo_error, echo_info, echo_success, echo_warning

auth_app = typer.Typer(
    name="auth",
    help="Authenticate against the Atlas Keycloak realm (login, status, logout).",
)

#: Default Keycloak issuer for the Atlas realm (overridable via KEYCLOAK_ISSUER).
_DEFAULT_ISSUER = "http://localhost:8080/realms/atlas"
#: Default public OIDC client id (overridable via SEEKNAL_OIDC_CLIENT_ID).
_DEFAULT_CLIENT_ID = "seeknal-cli"
#: Default loopback redirect port; the registered redirect URIs use 8765.
_DEFAULT_REDIRECT_PORT = 8765
#: Seconds to wait for the OIDC discovery / token endpoints before giving up.
_HTTP_TIMEOUT_SECONDS = 30.0
#: Seconds to wait for the browser to complete the redirect before aborting login.
_CALLBACK_TIMEOUT_SECONDS = 300.0


def _issuer() -> str:
    """Return the configured Keycloak issuer URL (trailing slash stripped)."""

    return (os.getenv("KEYCLOAK_ISSUER", "").strip() or _DEFAULT_ISSUER).rstrip("/")


def _client_id() -> str:
    """Return the configured public OIDC client id."""

    return os.getenv("SEEKNAL_OIDC_CLIENT_ID", "").strip() or _DEFAULT_CLIENT_ID


def _redirect_port() -> int:
    """Return the loopback redirect port (env override falls back to the default)."""

    raw = os.getenv("SEEKNAL_OIDC_REDIRECT_PORT", "").strip()
    if not raw:
        return _DEFAULT_REDIRECT_PORT
    try:
        return int(raw)
    except ValueError:
        return _DEFAULT_REDIRECT_PORT


def _redirect_uri(port: int) -> str:
    """Build the loopback redirect URI for ``port`` (matches Keycloak config)."""

    return f"http://127.0.0.1:{port}/callback"


def _pkce_pair() -> tuple[str, str]:
    """Generate a PKCE ``(code_verifier, code_challenge)`` pair using S256.

    The verifier is a high-entropy URL-safe random string (RFC 7636 §4.1) and the
    challenge is the base64url-encoded SHA-256 of the verifier with padding removed
    (RFC 7636 §4.2). Both are returned as ASCII strings.

    Returns:
        A ``(code_verifier, code_challenge)`` tuple.
    """

    verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode("ascii")
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return verifier, challenge


def _discover_endpoints(issuer: str, *, client: httpx.Client | None = None) -> dict[str, Any]:
    """Fetch the OIDC discovery document from ``<issuer>/.well-known/openid-configuration``.

    Args:
        issuer: The Keycloak realm issuer URL.
        client: Optional pre-built httpx client (used in tests). When ``None`` a
            one-shot request is made.

    Returns:
        The parsed discovery document, which carries ``authorization_endpoint`` and
        ``token_endpoint`` among other fields.

    Raises:
        httpx.HTTPError: If the discovery request fails.
    """

    url = f"{issuer}/.well-known/openid-configuration"
    if client is not None:
        response = client.get(url, timeout=_HTTP_TIMEOUT_SECONDS)
    else:
        response = httpx.get(url, timeout=_HTTP_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def _build_authorize_url(
    endpoints: dict[str, Any],
    *,
    client_id: str,
    redirect_uri: str,
    code_challenge: str,
    state: str,
    scope: str = "openid email profile",
) -> str:
    """Build the OIDC authorization-endpoint URL for the PKCE code flow.

    Args:
        endpoints: The OIDC discovery document (must contain ``authorization_endpoint``).
        client_id: The public OIDC client id.
        redirect_uri: The loopback redirect URI registered with Keycloak.
        code_challenge: The S256 PKCE challenge derived from the verifier.
        state: Opaque CSRF state echoed back on the redirect.
        scope: Space-delimited OIDC scopes (default requests an id token + email/profile).

    Returns:
        The fully-qualified authorization URL to open in the browser.
    """

    authorization_endpoint = endpoints["authorization_endpoint"]
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": scope,
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    return f"{authorization_endpoint}?{urllib.parse.urlencode(params)}"


def _exchange_code(
    token_endpoint: str,
    *,
    code: str,
    code_verifier: str,
    client_id: str,
    redirect_uri: str,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Exchange an authorization ``code`` for tokens at the token endpoint.

    Uses ``grant_type=authorization_code`` with the PKCE ``code_verifier``. The
    client is public (no secret), so no client authentication is sent.

    Args:
        token_endpoint: The OIDC token endpoint URL.
        code: The authorization code captured from the loopback redirect.
        code_verifier: The PKCE verifier matching the challenge sent on authorize.
        client_id: The public OIDC client id.
        redirect_uri: The redirect URI used on the authorize request (must match).
        client: Optional pre-built httpx client (used in tests).

    Returns:
        The parsed token response (``access_token``, ``refresh_token``, ...).

    Raises:
        httpx.HTTPError: If the token request fails.
    """

    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "code_verifier": code_verifier,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    if client is not None:
        response = client.post(
            token_endpoint, data=data, headers=headers, timeout=_HTTP_TIMEOUT_SECONDS
        )
    else:
        response = httpx.post(
            token_endpoint, data=data, headers=headers, timeout=_HTTP_TIMEOUT_SECONDS
        )
    response.raise_for_status()
    return response.json()


def _write_credentials(tokens: dict[str, Any]) -> Path:
    """Persist the OIDC token response to the seeknal credentials file.

    Writes ``access_token``, ``refresh_token``, ``expires_in`` and
    ``refresh_expires_in`` as JSON. The parent directory is created mode ``0700``
    and the file itself is written mode ``0600`` so the tokens are not world- or
    group-readable.

    Args:
        tokens: The token response from :func:`_exchange_code`.

    Returns:
        The path the credentials were written to.
    """

    path = credentials_path()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    payload = {
        "access_token": tokens.get("access_token"),
        "refresh_token": tokens.get("refresh_token"),
        "expires_in": tokens.get("expires_in"),
        "refresh_expires_in": tokens.get("refresh_expires_in"),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.chmod(path, 0o600)
    return path


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    """Single-shot HTTP handler that captures the OIDC redirect query params."""

    # Populated on the server instance by ``_capture_authorization_code``.
    server_version = "seeknal-auth/1.0"

    def do_GET(self) -> None:  # noqa: N802 (http.server API)
        """Record the ``code``/``state``/``error`` from the redirect and respond."""

        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        self.server.oidc_result = {  # type: ignore[attr-defined]
            "code": query.get("code", [None])[0],
            "state": query.get("state", [None])[0],
            "error": query.get("error", [None])[0],
            "error_description": query.get("error_description", [None])[0],
        }
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        body = (
            "<html><body><h2>Seeknal login complete.</h2>"
            "<p>You may close this tab and return to the terminal.</p></body></html>"
        )
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, *args: Any) -> None:  # noqa: D401 (silence default logging)
        """Suppress the default stderr request logging."""


def _capture_authorization_code(port: int, *, timeout: float = _CALLBACK_TIMEOUT_SECONDS) -> dict:
    """Run a one-shot loopback HTTP server and return the captured redirect params.

    Starts an :class:`http.server.HTTPServer` bound to ``127.0.0.1:port`` in a
    background thread, services exactly one request (the OIDC redirect), and shuts
    down. The returned dict carries ``code``/``state``/``error``.

    Args:
        port: The loopback port to bind (must match the registered redirect URI).
        timeout: Seconds to wait for the redirect before giving up.

    Returns:
        The captured redirect parameters, or an ``error`` of ``"timeout"`` if no
        request arrived in time.
    """

    server = http.server.HTTPServer(("127.0.0.1", port), _CallbackHandler)
    server.oidc_result = None  # type: ignore[attr-defined]

    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()
    thread.join(timeout=timeout)
    server.server_close()

    result = getattr(server, "oidc_result", None)
    if not result:
        return {"code": None, "state": None, "error": "timeout"}
    return result


@auth_app.command("login")
def login(
    no_browser: bool = typer.Option(
        False,
        "--no-browser",
        help="Print the authorization URL instead of opening a browser.",
    ),
) -> None:
    """Log in via the Keycloak Authorization Code + PKCE loopback flow.

    Discovers the OIDC endpoints from the issuer well-known document, generates a
    PKCE verifier/challenge and CSRF state, opens (or prints) the authorization URL,
    captures the redirect on the loopback port, exchanges the code for tokens, and
    writes them to the credentials file. Prints the logged-in subject and email on
    success; exits with code 1 on any failure.
    """

    issuer = _issuer()
    client_id = _client_id()
    port = _redirect_port()
    redirect_uri = _redirect_uri(port)

    try:
        endpoints = _discover_endpoints(issuer)
    except httpx.HTTPError as exc:
        echo_error(f"OIDC discovery failed for {issuer}: {exc}")
        raise typer.Exit(1)

    token_endpoint = endpoints.get("token_endpoint")
    if not token_endpoint or "authorization_endpoint" not in endpoints:
        echo_error("OIDC discovery document is missing authorization/token endpoints.")
        raise typer.Exit(1)

    verifier, challenge = _pkce_pair()
    state = secrets.token_urlsafe(24)
    authorize_url = _build_authorize_url(
        endpoints,
        client_id=client_id,
        redirect_uri=redirect_uri,
        code_challenge=challenge,
        state=state,
    )

    if no_browser:
        echo_info("Open this URL in your browser to log in:")
        typer.echo(authorize_url)
    else:
        echo_info(f"Opening browser for login (listening on {redirect_uri})...")
        webbrowser.open(authorize_url)

    result = _capture_authorization_code(port)

    if result.get("error"):
        detail = result.get("error_description") or result["error"]
        echo_error(f"Login failed: {detail}")
        raise typer.Exit(1)
    if result.get("state") != state:
        echo_error("Login failed: state mismatch (possible CSRF); aborting.")
        raise typer.Exit(1)
    code = result.get("code")
    if not code:
        echo_error("Login failed: no authorization code returned.")
        raise typer.Exit(1)

    try:
        tokens = _exchange_code(
            token_endpoint,
            code=code,
            code_verifier=verifier,
            client_id=client_id,
            redirect_uri=redirect_uri,
        )
    except httpx.HTTPError as exc:
        echo_error(f"Token exchange failed: {exc}")
        raise typer.Exit(1)

    if not tokens.get("access_token"):
        echo_error("Token exchange returned no access token.")
        raise typer.Exit(1)

    path = _write_credentials(tokens)

    sub = user_sub_from_credentials()
    email = user_email_from_credentials()
    echo_success(f"Logged in as {email or sub or '(unknown)'}")
    echo_info(f"sub: {sub or '(none)'} | credentials: {path}")


@auth_app.command("status")
def status() -> None:
    """Print whether the user is authenticated, with their sub and email.

    Reads the stored access token's claims. Exits with code 1 (and prints
    ``authenticated: no``) when no usable credentials are present.
    """

    sub = user_sub_from_credentials()
    email = user_email_from_credentials()
    if not sub and not email:
        echo_warning("authenticated: no")
        raise typer.Exit(1)
    echo_success("authenticated: yes")
    echo_info(f"sub: {sub or '(none)'}")
    echo_info(f"email: {email or '(none)'}")


@auth_app.command("logout")
def logout() -> None:
    """Delete the seeknal credentials file, logging the user out locally.

    A missing credentials file is treated as already logged out (success).
    """

    path = credentials_path()
    try:
        path.unlink()
    except FileNotFoundError:
        echo_info("Already logged out (no credentials file).")
        return
    except OSError as exc:
        echo_error(f"Failed to remove credentials: {exc}")
        raise typer.Exit(1)
    echo_success(f"Logged out (removed {path}).")
