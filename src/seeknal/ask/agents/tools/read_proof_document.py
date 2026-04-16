"""Read the contents of a Proof Editor document by URL or slug.

Uses the canonical agent state endpoint `GET /api/agent/{slug}/state` which
returns the markdown body, baseToken (needed for subsequent edits), and
comment/mark metadata. No approval gate — this is a read-only operation.

See: https://github.com/EveryInc/proof-sdk  (agent-docs)
"""

from __future__ import annotations

import os

import httpx

from seeknal.ask.agents.tools._proof_common import (
    mask_secret,
    parse_proof_url,
    sanitize,
    upgrade_scheme_if_remote,
    validate_base_url,
)
from seeknal.ask.agents.tools.publish_to_proof import resolve_proof_base_url

_TIMEOUT_SECONDS = 20.0
_MAX_MARKDOWN_PREVIEW = 4000
_MAX_ERROR_BODY = 200


def _do_read(
    url_or_slug: str,
    fallback_base: str,
    api_key: str | None,
) -> dict | str:
    """Fetch document state. Returns a dict on success or an error string.

    On success the dict contains: {slug, base, token, markdown, base_token,
    share_url, raw}. The `raw` entry is the full JSON response body for
    downstream edit tools that need additional fields.
    """
    parsed = parse_proof_url(url_or_slug)
    if isinstance(parsed, str):
        return parsed

    base = parsed["base"] or fallback_base
    # Upgrade http → https for hosted servers so the subsequent GET doesn't
    # trigger a redirect (which is fine for GET but wasteful).
    base = upgrade_scheme_if_remote(base)
    slug = parsed["slug"]
    token = parsed["token"]

    base_error = validate_base_url(base)
    if base_error:
        return base_error

    headers = {"Accept": "application/json", "X-Agent-Id": "agent:seeknal"}
    auth_token = token or api_key
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    url = f"{base.rstrip('/')}/api/agent/{slug}/state"

    try:
        with httpx.Client(timeout=_TIMEOUT_SECONDS, follow_redirects=True) as client:
            response = client.get(url, headers=headers)
    except httpx.TimeoutException:
        return (
            f"Error: Proof read request timed out after {_TIMEOUT_SECONDS:.0f}s "
            f"({sanitize(url, max_len=120)})."
        )
    except httpx.RequestError as exc:
        return (
            f"Error: failed to reach Proof at {sanitize(url, max_len=120)}: "
            f"{sanitize(str(exc), max_len=200)}"
        )

    if response.status_code in (401, 403):
        return (
            "Error: Proof rejected the read (auth). The share URL may be "
            "missing its ?token=... parameter, or the document is private."
        )
    if response.status_code == 404:
        return (
            f"Error: Proof document not found: slug "
            f"'{sanitize(slug, max_len=80)}' at {sanitize(base, max_len=120)}."
        )
    if response.status_code >= 400:
        snippet = sanitize(response.text or "", max_len=_MAX_ERROR_BODY)
        return (
            f"Error: Proof returned HTTP {response.status_code} from "
            f"{sanitize(url, max_len=120)}. {snippet}"
        )

    try:
        data = response.json()
    except ValueError:
        return (
            f"Error: Proof returned a non-JSON response "
            f"(HTTP {response.status_code})."
        )

    if not isinstance(data, dict):
        return "Error: Proof state response was not a JSON object."

    markdown = data.get("markdown") or data.get("content") or data.get("text") or ""
    # Proof state response returns the base token under mutationBase.token
    # (the op contract's "preferredPrecondition"). Older aliases are kept as
    # fallbacks so this also works against self-hosted proof-sdk servers.
    base_token = ""
    mutation_base = data.get("mutationBase")
    if isinstance(mutation_base, dict):
        base_token = str(mutation_base.get("token", "") or "")
    if not base_token:
        base_token = str(data.get("baseToken") or data.get("base_token") or "")
    share_url = f"{base.rstrip('/')}/d/{slug}"
    # Revision is required by rewrite.apply on the hosted API.
    revision = data.get("revision")

    return {
        "slug": slug,
        "base": base.rstrip("/"),
        "token": token,
        "markdown": str(markdown),
        "base_token": str(base_token),
        "revision": revision,
        "share_url": share_url,
        "raw": data,
    }


async def read_proof_document(url: str) -> str:
    """Read a Proof Editor document by URL or slug and return its markdown content.

    Accepts:
      - A full share URL: `https://memokami.exe.xyz/d/<slug>?token=<token>`
      - A URL without token (works only for public documents)
      - A bare slug (uses PROOF_BASE_URL or the built-in default)

    The response includes the document body, its `baseToken` (needed if you
    later want to call `edit_proof_document`), and share URL.

    Environment:
        PROOF_BASE_URL: Default base for bare slugs / tokenless URLs.
        PROOF_API_KEY: Fallback bearer token when the URL has no ?token=...

    Args:
        url: Proof share URL (preferred, includes token) or bare slug.
    """
    fallback_base = resolve_proof_base_url(None)
    api_key = os.environ.get("PROOF_API_KEY") or None

    result = _do_read(url, fallback_base, api_key)
    if isinstance(result, str):
        return result

    markdown = result["markdown"]
    preview = markdown
    truncated = False
    if len(preview) > _MAX_MARKDOWN_PREVIEW:
        preview = preview[:_MAX_MARKDOWN_PREVIEW] + "\n…[truncated]"
        truncated = True

    lines = [
        f"Read Proof document: {result['share_url']}",
        f"Slug: {sanitize(result['slug'], max_len=80)}",
    ]
    if result["base_token"]:
        lines.append(
            f"Base token (preview, needed for edits): "
            f"{mask_secret(sanitize(result['base_token'], max_len=120))}"
        )
    lines.append(f"Markdown length: {len(markdown)} chars")
    if truncated:
        lines.append(f"Preview truncated at {_MAX_MARKDOWN_PREVIEW} chars.")
    lines.append("")
    lines.append("---markdown---")
    # Preview is already bounded; sanitize strips any stray control chars
    lines.append(sanitize(preview, max_len=_MAX_MARKDOWN_PREVIEW + 200))
    return "\n".join(lines)
