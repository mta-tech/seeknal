"""LLM provider factory for Seeknal Ask.

Supports Google Gemini (primary) and Ollama (local).
Returns pydantic-ai model strings for use with Agent() or create_deep_agent().
Configuration via environment variables.
"""

import os
from typing import Optional


def resolve_provider_config(
    provider: Optional[str] = None,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
) -> dict[str, Optional[str]]:
    """Resolve provider/model credentials using Seeknal Ask defaults."""
    resolved_provider = provider or os.environ.get("SEEKNAL_ASK_LLM_PROVIDER", "google")

    if resolved_provider == "google":
        return {
            "provider": resolved_provider,
            "model": model or os.environ.get("SEEKNAL_ASK_MODEL", "gemini-2.5-flash"),
            "api_key": api_key or os.environ.get("GOOGLE_API_KEY"),
            "base_url": None,
        }

    if resolved_provider == "ollama":
        return {
            "provider": resolved_provider,
            "model": model or os.environ.get("SEEKNAL_ASK_MODEL", "llama3.1"),
            "api_key": None,
            "base_url": base_url or os.environ.get("SEEKNAL_ASK_OLLAMA_URL"),
        }

    raise ValueError(
        f"Unsupported LLM provider: '{resolved_provider}'. "
        f"Supported: 'google', 'ollama'."
    )


def get_model_string(
    provider: Optional[str] = None,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
) -> str:
    """Create a pydantic-ai model string from provider configuration.

    Resolution order for each parameter:
    1. Explicit argument
    2. Environment variable (SEEKNAL_ASK_*)
    3. Default value

    Args:
        provider: "google" or "ollama". Default: SEEKNAL_ASK_LLM_PROVIDER or "google".
        model: Model name. Default depends on provider.
        api_key: API key. Default: provider-specific env var.
        base_url: Base URL override (mainly for Ollama).

    Returns:
        A pydantic-ai model string (e.g. "google-gla:gemini-2.5-flash").

    Raises:
        ValueError: If provider is unsupported or required config is missing.
    """
    resolved = resolve_provider_config(
        provider=provider,
        model=model,
        api_key=api_key,
        base_url=base_url,
    )

    if resolved["provider"] == "google":
        return _google_model_string(resolved["model"], resolved["api_key"])
    return _ollama_model_string(resolved["model"], resolved["base_url"])


def _google_model_string(
    model: Optional[str],
    api_key: Optional[str],
) -> str:
    model = model or os.environ.get("SEEKNAL_ASK_MODEL", "gemini-2.5-flash")
    api_key = api_key or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise ValueError(
            "Google API key required. Set GOOGLE_API_KEY environment variable "
            "or pass api_key parameter."
        )
    # pydantic-ai reads GOOGLE_API_KEY from env automatically,
    # but we validate early to give a clear error message.
    return f"google-gla:{model}"


def _ollama_model_string(
    model: Optional[str],
    base_url: Optional[str],
) -> str:
    model = model or os.environ.get("SEEKNAL_ASK_MODEL", "llama3.1")
    # Bridge seeknal's env var to pydantic-ai's expected env var
    base_url = base_url or os.environ.get("SEEKNAL_ASK_OLLAMA_URL")
    if base_url:
        os.environ.setdefault("OLLAMA_BASE_URL", base_url)
    return f"ollama:{model}"
