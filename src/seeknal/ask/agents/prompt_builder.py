"""Modular system prompt builder for Seeknal Ask.

Assembles the system prompt from Jinja2 template sections based on the
active tool profile. This replaces the monolithic SYSTEM_PROMPT string
with composable templates that reduce token usage per request.

Profile → Sections loaded:
  analysis: core + skills_catalog + context + safety          (~5KB)
  build:    core + build + skills_catalog + context + safety   (~8KB)
  full:     core + build + report + skills_catalog + context + safety  (~12KB)
"""

from pathlib import Path

from jinja2 import Environment, FileSystemLoader

# Template directory (relative to this file)
_PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

# Sections loaded per profile
PROFILE_SECTIONS = {
    "analysis": ["core", "semantic", "memory", "skills_catalog", "context", "safety"],
    "build": ["core", "build", "semantic", "memory", "skills_catalog", "context", "safety"],
    "full": ["core", "build", "report", "semantic", "memory", "skills_catalog", "context", "safety"],
}


class PromptBuilder:
    """Assembles system prompt from Jinja2 templates based on profile."""

    def __init__(self, template_dir: Path | None = None):
        template_dir = template_dir or _PROMPTS_DIR
        self.env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def build(self, profile: str, context: str = "", **kwargs) -> str:
        """Build system prompt for the given profile.

        Args:
            profile: One of "analysis", "build", "full".
            context: Dynamic project context from ArtifactDiscovery.
            **kwargs: Additional template variables.

        Returns:
            Assembled system prompt string.
        """
        if profile not in PROFILE_SECTIONS:
            profile = "analysis"

        sections = PROFILE_SECTIONS[profile]
        parts = []
        for section_name in sections:
            template = self.env.get_template(f"{section_name}.j2")
            rendered = template.render(context=context, **kwargs)
            if rendered.strip():
                parts.append(rendered.strip())

        return "\n\n".join(parts)
