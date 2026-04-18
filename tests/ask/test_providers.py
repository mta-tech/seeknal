"""Tests for seeknal.ask.agents.providers — provider factory."""

from __future__ import annotations

import pytest

from seeknal.ask.agents.providers import (
    SUPPORTED_PROVIDERS,
    get_model_string,
    resolve_provider_config,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Strip provider-related env vars so each test starts from a known state."""
    for key in (
        "SEEKNAL_ASK_LLM_PROVIDER", "SEEKNAL_ASK_MODEL",
        "GOOGLE_API_KEY",
        "OPENAI_API_KEY", "OPENAI_BASE_URL", "SEEKNAL_ASK_OPENAI_BASE_URL",
        "ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL", "SEEKNAL_ASK_ANTHROPIC_BASE_URL",
        "SEEKNAL_ASK_OLLAMA_URL", "OLLAMA_BASE_URL",
    ):
        monkeypatch.delenv(key, raising=False)


# ---------------------------------------------------------------------------
# SUPPORTED_PROVIDERS is stable
# ---------------------------------------------------------------------------


def test_supported_providers_includes_all_four():
    assert set(SUPPORTED_PROVIDERS) == {"google", "ollama", "openai", "anthropic"}


# ---------------------------------------------------------------------------
# resolve_provider_config — defaults and env resolution
# ---------------------------------------------------------------------------


def test_google_default_model_and_api_key_from_env(monkeypatch):
    monkeypatch.setenv("GOOGLE_API_KEY", "sk-google-test")
    cfg = resolve_provider_config(provider="google")
    assert cfg["provider"] == "google"
    assert cfg["model"] == "gemini-2.5-flash"
    assert cfg["api_key"] == "sk-google-test"
    assert cfg["base_url"] is None


def test_ollama_default_model_and_optional_base_url(monkeypatch):
    monkeypatch.setenv("SEEKNAL_ASK_OLLAMA_URL", "http://ollama.local:11434")
    cfg = resolve_provider_config(provider="ollama")
    assert cfg["provider"] == "ollama"
    assert cfg["model"] == "llama3.1"
    assert cfg["api_key"] is None
    assert cfg["base_url"] == "http://ollama.local:11434"


def test_openai_default_model_and_env_key(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-openai-test")
    cfg = resolve_provider_config(provider="openai")
    assert cfg["provider"] == "openai"
    assert cfg["model"] == "gpt-4o"
    assert cfg["api_key"] == "sk-openai-test"
    assert cfg["base_url"] is None


def test_openai_honors_base_url_env(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk")
    monkeypatch.setenv("SEEKNAL_ASK_OPENAI_BASE_URL", "https://proxy.example/v1")
    cfg = resolve_provider_config(provider="openai")
    assert cfg["base_url"] == "https://proxy.example/v1"


def test_anthropic_default_model_and_env_key(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
    cfg = resolve_provider_config(provider="anthropic")
    assert cfg["provider"] == "anthropic"
    assert cfg["model"] == "claude-sonnet-4-6"
    assert cfg["api_key"] == "sk-ant-test"
    assert cfg["base_url"] is None


def test_anthropic_honors_base_url_env(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk")
    monkeypatch.setenv("SEEKNAL_ASK_ANTHROPIC_BASE_URL", "https://api.minimax.io/anthropic")
    cfg = resolve_provider_config(provider="anthropic")
    assert cfg["base_url"] == "https://api.minimax.io/anthropic"


def test_explicit_argument_beats_env(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "from-env")
    cfg = resolve_provider_config(
        provider="openai", model="gpt-5", api_key="from-arg",
        base_url="https://custom/v1",
    )
    assert cfg["model"] == "gpt-5"
    assert cfg["api_key"] == "from-arg"
    assert cfg["base_url"] == "https://custom/v1"


def test_provider_from_env_var(monkeypatch):
    monkeypatch.setenv("SEEKNAL_ASK_LLM_PROVIDER", "anthropic")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk")
    cfg = resolve_provider_config()  # no explicit provider arg
    assert cfg["provider"] == "anthropic"


def test_unsupported_provider_raises(monkeypatch):
    with pytest.raises(ValueError, match="Unsupported LLM provider"):
        resolve_provider_config(provider="bedrock")


# ---------------------------------------------------------------------------
# get_model_string — pydantic-ai model strings
# ---------------------------------------------------------------------------


def test_google_model_string(monkeypatch):
    monkeypatch.setenv("GOOGLE_API_KEY", "sk")
    assert get_model_string(provider="google") == "google-gla:gemini-2.5-flash"


def test_ollama_model_string():
    assert get_model_string(provider="ollama") == "ollama:llama3.1"


def test_ollama_model_string_bridges_base_url(monkeypatch):
    get_model_string(provider="ollama", base_url="http://x:1/")
    assert os.environ.get("OLLAMA_BASE_URL") == "http://x:1/"


def test_openai_model_string_requires_api_key():
    with pytest.raises(ValueError, match="OpenAI API key required"):
        get_model_string(provider="openai")


def test_openai_model_string_happy_path(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-openai")
    assert get_model_string(provider="openai") == "openai:gpt-4o"


def test_openai_model_string_honors_custom_model(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk")
    assert get_model_string(provider="openai", model="gpt-4o-mini") == "openai:gpt-4o-mini"


def test_openai_bridges_base_url(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk")
    get_model_string(
        provider="openai",
        base_url="https://azure-openai.example/openai/deployments/x",
    )
    assert os.environ.get("OPENAI_BASE_URL") == (
        "https://azure-openai.example/openai/deployments/x"
    )


def test_anthropic_model_string_requires_api_key():
    with pytest.raises(ValueError, match="Anthropic API key required"):
        get_model_string(provider="anthropic")


def test_anthropic_model_string_happy_path(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant")
    assert get_model_string(provider="anthropic") == "anthropic:claude-sonnet-4-6"


def test_anthropic_bridges_base_url(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk")
    get_model_string(
        provider="anthropic",
        base_url="https://api.minimax.io/anthropic",
    )
    assert os.environ.get("ANTHROPIC_BASE_URL") == "https://api.minimax.io/anthropic"


def test_unsupported_provider_on_get_model_string():
    with pytest.raises(ValueError, match="Unsupported LLM provider"):
        get_model_string(provider="bedrock")


# ``os`` import needed by env-bridging tests.
import os  # noqa: E402
