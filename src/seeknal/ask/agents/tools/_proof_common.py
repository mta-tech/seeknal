"""Shared helpers for the Proof Editor tool suite.

Covers URL parsing, sanitization, and network conventions used by the
publish/read/edit tools. Each tool wires its own approval gate on top.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qs, urlparse

# Strip ANSI escapes, control chars, and DEL — prevents a hostile Proof server
# from injecting terminal escape sequences into the TUI via its response body.
_UNSAFE_CHARS = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")


def sanitize(value: Any, *, max_len: int = 500) -> str:
    """Scrub untrusted string data before echoing to the TUI."""
    text = str(value) if value is not None else ""
    text = _UNSAFE_CHARS.sub("", text)
    if len(text) > max_len:
        text = text[:max_len] + "…"
    return text


def mask_secret(secret: str) -> str:
    """Return a hint-only preview of a secret — never echo it in full."""
    if not secret:
        return ""
    if len(secret) <= 8:
        return "****"
    return f"{secret[:4]}…{secret[-4:]}"


def validate_base_url(base_url: str) -> str | None:
    """Return an error message if base_url is not a safe http(s) URL."""
    try:
        parsed = urlparse(base_url)
    except ValueError as exc:
        return f"Error: invalid base URL: {exc}"
    if parsed.scheme not in ("http", "https"):
        return (
            f"Error: base URL must use http or https, got scheme "
            f"'{parsed.scheme or '(empty)'}' in '{sanitize(base_url, max_len=120)}'."
        )
    if not parsed.netloc:
        return (
            f"Error: base URL is missing a host: '{sanitize(base_url, max_len=120)}'."
        )
    return None


_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}


def upgrade_scheme_if_remote(base_url: str) -> str:
    """Upgrade http:// → https:// for non-local hosts.

    Hosted Proof servers (memokami.exe.xyz, proofeditor.ai) redirect http to
    https at the edge. httpx follows 301/302 by converting POST into GET per
    RFC, which silently drops the request body. Eagerly upgrading the scheme
    before any POST avoids that trap. Localhost / 127.0.0.1 are left alone so
    self-hosted development servers on plain http still work.
    """
    try:
        parsed = urlparse(base_url)
    except ValueError:
        return base_url
    if parsed.scheme != "http":
        return base_url
    host = (parsed.hostname or "").lower()
    if host in _LOCAL_HOSTS:
        return base_url
    upgraded = parsed._replace(scheme="https")
    return upgraded.geturl()


def parse_proof_url(url: str) -> dict[str, str] | str:
    """Parse a Proof share URL into its components.

    Accepts:
      https://host/d/<slug>?token=<token>
      https://host/d/<slug>
      <slug>   (bare slug — caller must supply base_url separately)

    Returns a dict `{base, slug, token}` on success, or an error message string
    on failure. `token` may be empty if the URL didn't include one.
    """
    if not url or not url.strip():
        return "Error: proof URL/slug cannot be empty."

    url = url.strip()

    # Bare slug (no scheme, no slashes)
    if "://" not in url and "/" not in url:
        return {"base": "", "slug": url, "token": ""}

    try:
        parsed = urlparse(url)
    except ValueError as exc:
        return f"Error: invalid proof URL: {exc}"

    if parsed.scheme not in ("http", "https"):
        return (
            f"Error: proof URL must use http or https, got scheme "
            f"'{parsed.scheme or '(empty)'}'."
        )
    if not parsed.netloc:
        return f"Error: proof URL is missing a host: '{sanitize(url, max_len=120)}'."

    # Extract slug from path — expected shape: /d/<slug>[/...]
    path_parts = [p for p in parsed.path.split("/") if p]
    slug = ""
    if len(path_parts) >= 2 and path_parts[0] == "d":
        slug = path_parts[1]
    elif len(path_parts) >= 1:
        # Fall back to last non-empty path segment
        slug = path_parts[-1]

    if not slug:
        return (
            f"Error: could not extract slug from proof URL "
            f"'{sanitize(url, max_len=120)}'. Expected shape: .../d/<slug>?token=..."
        )

    query = parse_qs(parsed.query)
    token_values = query.get("token", [])
    token = token_values[0] if token_values else ""

    base = f"{parsed.scheme}://{parsed.netloc}"
    return {"base": base, "slug": slug, "token": token}
