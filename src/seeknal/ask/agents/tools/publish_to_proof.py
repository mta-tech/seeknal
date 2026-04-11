"""Publish memo to Proof Editor (https://proofeditor.ai/) as a shareable document.

The agent uses this tool after a confirmation gate — mirroring the evidence.dev
report flow — to push a markdown memo to a Proof SDK server and return a
shareable URL the user can hand to others.

See: https://github.com/EveryInc/proof-sdk (POST /documents endpoint).
"""

from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import urlparse

import httpx

_VALID_ROLES = {"viewer", "commenter", "editor"}
_DEFAULT_BASE_URL = "https://memokami.exe.xyz"
_DEFAULT_OWNER_ID = "agent:seeknal"
_TIMEOUT_SECONDS = 30.0
_MAX_ERROR_BODY = 200

# Endpoint fallback: hosted Proof servers may use /documents OR /share/markdown
# as the publish alias. Try /documents first (richer response); on 404 fall
# back to /share/markdown. Works for memokami.exe.xyz, proofeditor.ai, and any
# self-hosted proof-sdk instance.
_PUBLISH_PATHS = ("/documents", "/share/markdown")


def resolve_proof_base_url(override: str | None = None) -> str:
    """Resolve the Proof base URL from (in priority order):
    1. Explicit override parameter
    2. PROOF_BASE_URL environment variable
    3. Built-in default (_DEFAULT_BASE_URL)
    """
    if override and override.strip():
        return override.strip()
    env = os.environ.get("PROOF_BASE_URL", "").strip()
    if env:
        return env
    return _DEFAULT_BASE_URL

# Strip ANSI escapes, control chars, and DEL — prevents a hostile Proof server
# from injecting terminal escape sequences into the TUI via its response body.
_UNSAFE_CHARS = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")


def _sanitize(value: Any, *, max_len: int = 500) -> str:
    """Scrub untrusted string data before echoing to the TUI."""
    text = str(value) if value is not None else ""
    text = _UNSAFE_CHARS.sub("", text)
    if len(text) > max_len:
        text = text[:max_len] + "…"
    return text


def _mask_secret(secret: str) -> str:
    """Return a hint-only preview of a secret — never echo it in full."""
    if not secret:
        return ""
    if len(secret) <= 8:
        return "****"
    return f"{secret[:4]}…{secret[-4:]}"


def _validate_base_url(base_url: str) -> str | None:
    """Return an error message if base_url is not a safe http(s) URL."""
    try:
        parsed = urlparse(base_url)
    except ValueError as exc:
        return f"Error: invalid PROOF_BASE_URL: {exc}"
    if parsed.scheme not in ("http", "https"):
        return (
            f"Error: PROOF_BASE_URL must use http or https, got scheme "
            f"'{parsed.scheme or '(empty)'}' in '{_sanitize(base_url, max_len=120)}'."
        )
    if not parsed.netloc:
        return (
            f"Error: PROOF_BASE_URL is missing a host: "
            f"'{_sanitize(base_url, max_len=120)}'."
        )
    return None


def _do_publish(
    title: str,
    content: str,
    role: str,
    base_url: str,
    api_key: str | None,
) -> str:
    """Core publish logic separated from the approval gate for testability."""
    if not title or not title.strip():
        return "Error: memo title cannot be empty."
    if not content or not content.strip():
        return "Error: memo content cannot be empty."
    if role not in _VALID_ROLES:
        # role is agent-controlled — sanitize before echoing
        return (
            f"Error: role must be one of {sorted(_VALID_ROLES)}, "
            f"got '{_sanitize(role, max_len=40)}'."
        )

    url_error = _validate_base_url(base_url)
    if url_error:
        return url_error

    payload: dict[str, Any] = {
        "markdown": content,
        "title": title.strip(),
        "role": role,
        "ownerId": _DEFAULT_OWNER_ID,
    }
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    base = base_url.rstrip("/")
    response = None
    attempted_urls: list[str] = []

    try:
        with httpx.Client(timeout=_TIMEOUT_SECONDS, follow_redirects=True) as client:
            for path in _PUBLISH_PATHS:
                url = f"{base}{path}"
                attempted_urls.append(url)
                response = client.post(url, json=payload, headers=headers)
                # 404 on one path just means the server uses the other
                # alias — try the next one. Any other status is final.
                if response.status_code != 404:
                    break
    except httpx.TimeoutException:
        return (
            f"Error: Proof Editor request timed out after {_TIMEOUT_SECONDS:.0f}s "
            f"(PROOF_BASE_URL={_sanitize(base_url, max_len=120)})."
        )
    except httpx.RequestError as exc:
        last_url = attempted_urls[-1] if attempted_urls else base
        return (
            f"Error: failed to reach Proof Editor at "
            f"{_sanitize(last_url, max_len=120)}: {_sanitize(str(exc), max_len=200)}"
        )

    assert response is not None  # loop always assigns or raises
    final_url = attempted_urls[-1] if attempted_urls else base

    if response.status_code in (401, 403):
        return (
            "Error: Proof Editor rejected the request (auth). "
            "Set PROOF_API_KEY if the target server requires it."
        )
    if response.status_code == 404:
        tried = ", ".join(_sanitize(u, max_len=120) for u in attempted_urls)
        return (
            "Error: Proof Editor publish endpoint not found. "
            f"Tried: [{tried}]. "
            "Check PROOF_BASE_URL — the server may not expose POST /documents "
            "or POST /share/markdown."
        )
    if response.status_code == 429:
        return "Error: Proof Editor rate-limited the request. Retry in a moment."
    if response.status_code >= 400:
        snippet = _sanitize(response.text or "", max_len=_MAX_ERROR_BODY)
        return (
            f"Error: Proof Editor returned HTTP {response.status_code} "
            f"from {_sanitize(final_url, max_len=120)}. {snippet}"
        )

    try:
        data = response.json()
    except ValueError:
        return (
            f"Error: Proof Editor returned a non-JSON response "
            f"(HTTP {response.status_code})."
        )

    if not isinstance(data, dict):
        return "Error: Proof Editor response was not a JSON object."

    share_url = data.get("shareUrl") or data.get("tokenUrl")
    if not share_url:
        safe_keys = ", ".join(_sanitize(k, max_len=40) for k in sorted(data.keys()))
        return (
            "Error: Proof Editor response missing shareUrl. "
            f"Response keys: [{safe_keys}]"
        )

    slug = _sanitize(data.get("slug", ""), max_len=80)
    safe_share_url = _sanitize(share_url, max_len=500)
    owner_secret = data.get("ownerSecret", "")

    lines = [
        "Memo published to Proof Editor!",
        "",
        f"Title: {_sanitize(title.strip(), max_len=200)}",
        f"Share link: {safe_share_url}",
    ]
    token_url = data.get("tokenUrl")
    if token_url and token_url != share_url:
        lines.append(f"Token link (pre-auth): {_sanitize(token_url, max_len=500)}")
    if slug:
        lines.append(f"Slug: {slug}")
    if owner_secret:
        lines.append(
            f"Owner secret (preview): {_mask_secret(_sanitize(owner_secret, max_len=200))} "
            "— full value saved to Proof; re-request via the shareUrl owner flow if needed."
        )
    return "\n".join(lines)


async def publish_to_proof(
    title: str,
    content: str,
    role: str = "commenter",
    base_url: str | None = None,
) -> str:
    """Publish a markdown memo to a Proof Editor server and return a shareable URL.

    Use this tool when the user wants to share a memo, summary, or write-up with
    other people. The output is a public link that recipients can open in any
    browser — no account required on their end.

    The tool requires explicit user confirmation — just like `generate_report`.
    Before calling this tool you MUST use `ask_user` with these exact options:
    `Continue analysis`, `Publish memo to Proof`, `Done for now`, `Type your own`.
    Only call `publish_to_proof` after the user explicitly selects `Publish memo to Proof`.

    Environment:
        PROOF_BASE_URL: Proof Editor base URL. Default:
            https://memokami.exe.xyz. Override for other deployments such as
            https://proofeditor.ai or http://localhost:4000.
        PROOF_API_KEY: Bearer token, only required if the target server is
            configured with PROOF_SHARE_MARKDOWN_AUTH_MODE=api_key.

    Args:
        title: Memo title shown at the top of the Proof document.
        content: The memo body as Markdown. Headings, lists, tables, and
            fenced code blocks are all rendered natively by Proof.
        role: Access role granted to anyone with the link —
            `viewer`, `commenter`, or `editor`. Defaults to `commenter`.
        base_url: Optional per-call override for the Proof base URL. Takes
            precedence over PROOF_BASE_URL. Pass this when the user explicitly
            specifies a different Proof host for a single publish.
    """
    from seeknal.ask.agents.tools._context import require_proof_publish_approval

    guard_error = require_proof_publish_approval("publish_to_proof")
    if guard_error:
        return guard_error

    resolved_base_url = resolve_proof_base_url(base_url)
    api_key = os.environ.get("PROOF_API_KEY") or None

    return _do_publish(
        title=title,
        content=content,
        role=role,
        base_url=resolved_base_url,
        api_key=api_key,
    )
