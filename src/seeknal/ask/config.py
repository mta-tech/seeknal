"""Agent configuration loader for Seeknal Ask.

Reads per-project agent settings from seeknal_agent.yml.
CLI flags override config file values. Falls back to defaults
when no config file exists.

Example seeknal_agent.yml:
    model: gemini-2.5-flash
    temperature: 0.0
    default_profile: analysis
    disabled_tools: []
"""

from pathlib import Path
from typing import Any


def load_agent_config(project_path: Path) -> dict[str, Any]:
    """Load agent configuration from seeknal_agent.yml.

    Args:
        project_path: Path to the seeknal project root.

    Returns:
        Configuration dict. Empty dict if no config file exists.
    """
    config_path = project_path / "seeknal_agent.yml"
    if not config_path.exists():
        return {}

    try:
        import yaml
        data = yaml.safe_load(config_path.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}
