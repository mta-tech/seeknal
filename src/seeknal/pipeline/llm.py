"""Ask-aligned LLM helpers for Python pipelines."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import httpx
from seeknal.ask.agents.providers import resolve_provider_config


@dataclass
class PipelineLLM:
    """Minimal text/JSON generation wrapper aligned with Seeknal Ask config."""

    provider: str
    model: str
    api_key: str | None = None
    base_url: str | None = None

    @classmethod
    def from_context(cls, ctx: Any) -> "PipelineLLM":
        resolved = resolve_provider_config(
            provider=ctx.get_param("llm_provider"),
            model=ctx.get_param("llm_model"),
            api_key=ctx.get_param("llm_api_key"),
            base_url=ctx.get_param("llm_base_url"),
        )
        return cls(
            provider=str(resolved["provider"]),
            model=str(resolved["model"]),
            api_key=resolved["api_key"],
            base_url=resolved["base_url"],
        )

    def generate_text(self, prompt: str) -> str:
        if self.provider == "google":
            return self._generate_google_text(prompt)
        if self.provider == "ollama":
            return self._generate_ollama_text(prompt)
        raise ValueError(f"Unsupported LLM provider: {self.provider}")

    def generate_json(self, prompt: str, schema: type[Any] | dict[str, Any]) -> dict[str, Any]:
        schema_instructions = self._schema_instructions(schema)
        raw = self.generate_text(
            f"{prompt}\n\nReturn valid JSON only.\n{schema_instructions}".strip()
        )
        parsed = self._parse_json_payload(raw)
        if isinstance(schema, type) and hasattr(schema, "model_validate"):
            return schema.model_validate(parsed).model_dump()
        return parsed

    def _generate_google_text(self, prompt: str) -> str:
        if not self.api_key:
            raise ValueError(
                "Google API key required. Set GOOGLE_API_KEY or provide llm_api_key."
            )
        from google import genai

        client = genai.Client(api_key=self.api_key)
        response = client.models.generate_content(
            model=self.model,
            contents=prompt,
        )
        return (response.text or "").strip()

    def _generate_ollama_text(self, prompt: str) -> str:
        base_url = self.base_url or "http://localhost:11434"
        response = httpx.post(
            f"{base_url.rstrip('/')}/api/generate",
            json={
                "model": self.model,
                "prompt": prompt,
                "stream": False,
            },
            timeout=60.0,
        )
        response.raise_for_status()
        data = response.json()
        return str(data.get("response", "")).strip()

    @staticmethod
    def _schema_instructions(schema: type[Any] | dict[str, Any]) -> str:
        if isinstance(schema, dict):
            return f"JSON schema: {json.dumps(schema, sort_keys=True)}"
        if hasattr(schema, "model_json_schema"):
            return f"JSON schema: {json.dumps(schema.model_json_schema(), sort_keys=True)}"
        return "Return a JSON object."

    @staticmethod
    def _parse_json_payload(raw: str) -> dict[str, Any]:
        text = raw.strip()
        if text.startswith("```"):
            lines = text.splitlines()
            if lines:
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            text = "\n".join(lines).strip()
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            raise ValueError("Expected LLM to return a JSON object.")
        return parsed
