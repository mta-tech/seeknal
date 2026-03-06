"""Artifact discovery service for Seeknal Ask.

Scans seeknal project artifacts (entities, manifests, intermediates)
and builds LLM-ready context for the agent's system prompt.
"""

import json
from pathlib import Path
from typing import Optional


class ArtifactDiscovery:
    """Discovers and describes seeknal project artifacts for the LLM agent."""

    def __init__(self, project_path: Path):
        self.project_path = project_path
        self.target_path = project_path / "target"
        self._entities_cache: Optional[list[dict]] = None
        self._dag_cache: Optional[dict] = None
        self._intermediates_cache: Optional[list[str]] = None

    def get_context_for_prompt(self) -> str:
        """Build a complete context string for the LLM system prompt.

        Includes: entity list with schemas, DAG overview, available tables.

        Returns:
            Markdown-formatted context string.
        """
        sections = []

        entities = self._discover_entities()
        if entities:
            sections.append(self._format_entities(entities))

        dag = self._discover_dag()
        if dag:
            sections.append(self._format_dag(dag))

        intermediates = self._discover_intermediates()
        if intermediates:
            sections.append(self._format_intermediates(intermediates))

        if not sections:
            return (
                "No seeknal artifacts found. The project may not have been "
                "run yet. Try running `seeknal run` first."
            )

        return "\n\n".join(sections)

    def _discover_entities(self) -> list[dict]:
        """Load entity catalogs from target/feature_store/."""
        if self._entities_cache is not None:
            return self._entities_cache

        feature_store = self.target_path / "feature_store"
        if not feature_store.exists():
            self._entities_cache = []
            return self._entities_cache

        entities = []
        for entity_dir in sorted(feature_store.iterdir()):
            if not entity_dir.is_dir():
                continue
            catalog_path = entity_dir / "_entity_catalog.json"
            if catalog_path.exists():
                try:
                    with open(catalog_path) as f:
                        catalog = json.load(f)
                    entities.append(catalog)
                except (json.JSONDecodeError, OSError):
                    continue
        self._entities_cache = entities
        return self._entities_cache

    def _discover_dag(self) -> Optional[dict]:
        """Load DAG manifest from target/manifest.json."""
        if self._dag_cache is not None:
            return self._dag_cache

        manifest_path = self.target_path / "manifest.json"
        if not manifest_path.exists():
            return None
        try:
            with open(manifest_path) as f:
                self._dag_cache = json.load(f)
                return self._dag_cache
        except (json.JSONDecodeError, OSError):
            return None

    def _discover_intermediates(self) -> list[str]:
        """List intermediate parquet files in target/intermediate/."""
        if self._intermediates_cache is not None:
            return self._intermediates_cache

        intermediate_dir = self.target_path / "intermediate"
        if not intermediate_dir.exists():
            self._intermediates_cache = []
            return self._intermediates_cache
        self._intermediates_cache = sorted(
            p.stem for p in intermediate_dir.glob("*.parquet")
        )
        return self._intermediates_cache

    def get_entities_summary(self) -> list[dict]:
        """Get a summary list of all entities for the get_entities tool."""
        entities = self._discover_entities()
        result = []
        for entity in entities:
            result.append({
                "name": entity.get("entity_name", "unknown"),
                "join_keys": entity.get("join_keys", []),
                "feature_group_count": len(entity.get("feature_groups", {})),
            })
        return result

    def get_entity_catalog(self, entity_name: str) -> Optional[dict]:
        """Get the full entity catalog for a specific entity."""
        entities = self._discover_entities()
        for entity in entities:
            if entity.get("entity_name") == entity_name:
                return entity
        return None

    def _format_entities(self, entities: list[dict]) -> str:
        lines = ["## Available Entities\n"]
        for entity in entities:
            name = entity.get("entity_name", "unknown")
            join_keys = entity.get("join_keys", [])
            lines.append(f"### Entity: `{name}`")
            lines.append(f"- **Join keys**: {', '.join(join_keys)}")
            lines.append(f"- **Table**: `{name}` (registered as DuckDB view)")

            feature_groups = entity.get("feature_groups", {})
            if feature_groups:
                lines.append("- **Feature groups**:")
                for fg_name, fg_info in feature_groups.items():
                    features = fg_info.get("features", {})
                    feature_list = [
                        f"`{fname}` ({ftype})"
                        for fname, ftype in features.items()
                    ]
                    lines.append(
                        f"  - `{fg_name}`: {', '.join(feature_list)}"
                    )
            lines.append("")
        return "\n".join(lines)

    def _format_dag(self, dag: dict) -> str:
        lines = ["## DAG Overview\n"]
        nodes = dag.get("nodes", [])
        if nodes:
            lines.append(f"Total nodes: {len(nodes)}\n")
            for node in nodes[:20]:  # Limit to 20 nodes for prompt size
                node_name = node.get("name", "unknown")
                node_type = node.get("type", "unknown")
                lines.append(f"- `{node_name}` ({node_type})")
        return "\n".join(lines)

    def _format_intermediates(self, intermediates: list[str]) -> str:
        lines = ["## Intermediate Tables\n"]
        lines.append(
            "These are intermediate transformation outputs, "
            "registered as DuckDB views:\n"
        )
        for name in intermediates[:30]:  # Limit for prompt size
            lines.append(f"- `{name}`")
        return "\n".join(lines)
