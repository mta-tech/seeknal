"""Firecrawl-powered web search and scrape tools for seeknal ask.

Uses the Firecrawl REST API directly (no SDK dependency).
Requires FIRECRAWL_API_KEY environment variable.

See: https://docs.firecrawl.dev/api-reference
"""

from __future__ import annotations

import os

import httpx

_FIRECRAWL_BASE = "https://api.firecrawl.dev/v1"
_TIMEOUT = 30.0
_MAX_CONTENT = 8000  # chars to return (avoid blowing up context)


def _get_api_key() -> str | None:
    return os.environ.get("FIRECRAWL_API_KEY")


def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


async def web_search(query: str, limit: int = 5) -> str:
    """Search the web using Firecrawl and return results as text.

    Uses the Firecrawl /search endpoint to find relevant web pages.
    Results include title, URL, and a content snippet for each match.

    Args:
        query: The search query string.
        limit: Maximum number of results to return (default 5, max 10).
    """
    api_key = _get_api_key()
    if not api_key:
        return "Error: FIRECRAWL_API_KEY not set. Set it in your environment."

    limit = min(limit, 10)

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.post(
            f"{_FIRECRAWL_BASE}/search",
            headers=_headers(api_key),
            json={
                "query": query,
                "limit": limit,
                "scrapeOptions": {"formats": ["markdown"]},
            },
        )

    if resp.status_code != 200:
        return f"Error: Firecrawl search failed (HTTP {resp.status_code}): {resp.text[:200]}"

    data = resp.json()
    results = data.get("data", [])
    if not results:
        return f"No results found for: {query}"

    lines = []
    for i, r in enumerate(results, 1):
        title = r.get("metadata", {}).get("title", "Untitled")
        url = r.get("metadata", {}).get("sourceURL", r.get("url", ""))
        snippet = r.get("markdown", "")[:500]
        lines.append(f"{i}. {title}\n   {url}\n   {snippet}\n")

    return "\n".join(lines)


async def web_scrape(url: str) -> str:
    """Scrape a web page and return its content as markdown.

    Uses the Firecrawl /scrape endpoint to extract clean content
    from a URL. Returns the page content in markdown format.

    Args:
        url: The URL of the web page to scrape.
    """
    api_key = _get_api_key()
    if not api_key:
        return "Error: FIRECRAWL_API_KEY not set. Set it in your environment."

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.post(
            f"{_FIRECRAWL_BASE}/scrape",
            headers=_headers(api_key),
            json={
                "url": url,
                "formats": ["markdown"],
            },
        )

    if resp.status_code != 200:
        return f"Error: Firecrawl scrape failed (HTTP {resp.status_code}): {resp.text[:200]}"

    data = resp.json()
    content = data.get("data", {}).get("markdown", "")
    if not content:
        return f"No content extracted from: {url}"

    # Truncate to avoid blowing up agent context
    if len(content) > _MAX_CONTENT:
        content = content[:_MAX_CONTENT] + "\n\n... (truncated)"

    title = data.get("data", {}).get("metadata", {}).get("title", "")
    header = f"Source: {url}"
    if title:
        header = f"{title}\nSource: {url}"

    return f"{header}\n\n{content}"
