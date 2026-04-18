"""LLM provider factory for Seeknal Ask.

Supports Google Gemini (primary), Ollama (local), OpenAI (+ any
OpenAI-compatible endpoint via SEEKNAL_ASK_OPENAI_BASE_URL), and
Anthropic (+ any Anthropic-compatible endpoint via
SEEKNAL_ASK_ANTHROPIC_BASE_URL).

Returns pydantic-ai model strings for use with Agent() or
create_deep_agent(). Configuration via environment variables.
"""

import os
from typing import Optional

SUPPORTED_PROVIDERS = ("google", "ollama", "openai", "anthropic")


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

    if resolved_provider == "openai":
        return {
            "provider": resolved_provider,
            "model": model or os.environ.get("SEEKNAL_ASK_MODEL", "gpt-4o"),
            "api_key": api_key or os.environ.get("OPENAI_API_KEY"),
            "base_url": base_url or os.environ.get("SEEKNAL_ASK_OPENAI_BASE_URL"),
        }

    if resolved_provider == "anthropic":
        return {
            "provider": resolved_provider,
            "model": model or os.environ.get("SEEKNAL_ASK_MODEL", "claude-sonnet-4-6"),
            "api_key": api_key or os.environ.get("ANTHROPIC_API_KEY"),
            "base_url": base_url or os.environ.get("SEEKNAL_ASK_ANTHROPIC_BASE_URL"),
        }

    raise ValueError(
        f"Unsupported LLM provider: '{resolved_provider}'. "
        f"Supported: {', '.join(repr(p) for p in SUPPORTED_PROVIDERS)}."
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
        provider: one of "google", "ollama", "openai", "anthropic".
            Default: SEEKNAL_ASK_LLM_PROVIDER or "google".
        model: Model name. Default depends on provider.
        api_key: API key. Default: provider-specific env var.
        base_url: Base URL override. Honoured for Ollama, OpenAI-compatible
            and Anthropic-compatible endpoints.

    Returns:
        A pydantic-ai model string (e.g. "google-gla:gemini-2.5-flash",
        "openai:gpt-4o", "anthropic:claude-sonnet-4-6").

    Raises:
        ValueError: If provider is unsupported or required config is missing.
    """
    resolved = resolve_provider_config(
        provider=provider,
        model=model,
        api_key=api_key,
        base_url=base_url,
    )

    prov = resolved["provider"]
    if prov == "google":
        return _google_model_string(resolved["model"], resolved["api_key"])
    if prov == "ollama":
        return _ollama_model_string(resolved["model"], resolved["base_url"])
    if prov == "openai":
        return _openai_model_string(
            resolved["model"], resolved["api_key"], resolved["base_url"]
        )
    if prov == "anthropic":
        return _anthropic_model_string(
            resolved["model"], resolved["api_key"], resolved["base_url"]
        )
    # resolve_provider_config already raises for unknown providers, but guard anyway.
    raise ValueError(f"Unsupported LLM provider: '{prov}'.")


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


def _openai_model_string(
    model: Optional[str],
    api_key: Optional[str],
    base_url: Optional[str],
) -> str:
    """Build an ``openai:<model>`` string for native OpenAI or any
    OpenAI-compatible endpoint (Azure OpenAI, Together AI, Groq, vLLM,
    LM Studio, self-hosted proxies, …).
    """
    model = model or os.environ.get("SEEKNAL_ASK_MODEL", "gpt-4o")
    api_key = api_key or os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError(
            "OpenAI API key required. Set OPENAI_API_KEY environment variable "
            "or pass api_key parameter."
        )
    # pydantic-ai's OpenAI provider reads OPENAI_API_KEY automatically; we
    # validate early so the error is actionable at config resolution time.
    os.environ.setdefault("OPENAI_API_KEY", api_key)
    # Bridge seeknal's custom-endpoint env var to the one pydantic-ai reads.
    if base_url:
        os.environ.setdefault("OPENAI_BASE_URL", base_url)
    return f"openai:{model}"


def _anthropic_model_string(
    model: Optional[str],
    api_key: Optional[str],
    base_url: Optional[str],
) -> str:
    """Build an ``anthropic:<model>`` string for native Anthropic or any
    Anthropic-compatible endpoint (enterprise proxies, MiniMax, etc.).
    """
    model = model or os.environ.get("SEEKNAL_ASK_MODEL", "claude-sonnet-4-6")
    api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError(
            "Anthropic API key required. Set ANTHROPIC_API_KEY environment "
            "variable or pass api_key parameter."
        )
    os.environ.setdefault("ANTHROPIC_API_KEY", api_key)
    if base_url:
        os.environ.setdefault("ANTHROPIC_BASE_URL", base_url)
    return f"anthropic:{model}"
