"""LLM provider factory for Seeknal Ask.

Supports Google Gemini (primary) and Ollama (local).
Configuration via environment variables or ask_config.yaml.
"""

import os
from pathlib import Path
from typing import Optional

from langchain_core.language_models import BaseChatModel


def get_llm(
    provider: Optional[str] = None,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    temperature: float = 0.0,
) -> BaseChatModel:
    """Create an LLM instance from provider configuration.

    Resolution order for each parameter:
    1. Explicit argument
    2. Environment variable (SEEKNAL_ASK_*)
    3. Default value

    Args:
        provider: "google" or "ollama". Default: SEEKNAL_ASK_LLM_PROVIDER or "google".
        model: Model name. Default depends on provider.
        api_key: API key. Default: provider-specific env var.
        base_url: Base URL override (mainly for Ollama).
        temperature: Sampling temperature. Default: 0.0 (deterministic).

    Returns:
        A LangChain chat model instance.

    Raises:
        ValueError: If provider is unsupported or required config is missing.
    """
    provider = provider or os.environ.get("SEEKNAL_ASK_LLM_PROVIDER", "google")

    if provider == "google":
        return _create_google(model, api_key, temperature)
    elif provider == "ollama":
        return _create_ollama(model, base_url, temperature)
    else:
        raise ValueError(
            f"Unsupported LLM provider: '{provider}'. "
            f"Supported: 'google', 'ollama'."
        )


def _create_google(
    model: Optional[str],
    api_key: Optional[str],
    temperature: float,
) -> BaseChatModel:
    from langchain_google_genai import ChatGoogleGenerativeAI

    model = model or os.environ.get("SEEKNAL_ASK_MODEL", "gemini-2.5-flash")
    api_key = api_key or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise ValueError(
            "Google API key required. Set GOOGLE_API_KEY environment variable "
            "or pass api_key parameter."
        )
    return ChatGoogleGenerativeAI(
        model=model,
        google_api_key=api_key,
        temperature=temperature,
    )


def _create_ollama(
    model: Optional[str],
    base_url: Optional[str],
    temperature: float,
) -> BaseChatModel:
    from langchain_ollama import ChatOllama

    model = model or os.environ.get("SEEKNAL_ASK_MODEL", "llama3.1")
    base_url = base_url or os.environ.get(
        "SEEKNAL_ASK_OLLAMA_URL", "http://localhost:11434"
    )
    return ChatOllama(
        model=model,
        base_url=base_url,
        temperature=temperature,
    )


def load_config(project_path: Optional[Path] = None) -> dict:
    """Load ask configuration from YAML file.

    Search order:
    1. {project_path}/.seeknal/ask_config.yaml
    2. ~/.seeknal/ask_config.yaml

    Returns:
        Configuration dict, or empty dict if no config found.
    """
    import yaml

    search_paths = []
    if project_path:
        search_paths.append(project_path / ".seeknal" / "ask_config.yaml")
    search_paths.append(Path.home() / ".seeknal" / "ask_config.yaml")

    for path in search_paths:
        if path.exists():
            with open(path) as f:
                return yaml.safe_load(f) or {}
    return {}
