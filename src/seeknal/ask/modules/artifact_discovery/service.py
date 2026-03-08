"""Artifact discovery service for Seeknal Ask.

Scans seeknal project artifacts (entities, manifests, intermediates,
and source pipeline definitions) and builds LLM-ready context for
the agent's system prompt.
"""

import json
from pathlib import Path
from typing import Optional

import yaml

from seeknal.ask.agents.tools._security import BLOCKED_FILES


class ArtifactDiscovery:
    """Discovers and describes seeknal project artifacts for the LLM agent."""

    def __init__(self, project_path: Path):
        self.project_path = project_path
        self.target_path = project_path / "target"
        self.seeknal_dir = project_path / "seeknal"
        self._entities_cache: Optional[list[dict]] = None
        self._dag_cache: Optional[dict] = None
        self._intermediates_cache: Optional[list[str]] = None
        self._pipelines_cache: Optional[list[dict]] = None

    def get_context_for_prompt(self) -> str:
        """Build a complete context string for the LLM system prompt.

        Includes: entity list with schemas, DAG overview, available tables,
        and a summary of source pipeline definitions.

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

        pipelines = self._discover_source_pipelines()
        if pipelines:
            sections.append(self._format_source_pipelines(pipelines))

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
        nodes_raw = dag.get("nodes", {})
        # nodes can be a dict (keyed by node id) or a list
        if isinstance(nodes_raw, dict):
            nodes = list(nodes_raw.values())
        else:
            nodes = nodes_raw
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

    # --- Source pipeline discovery ---

    def _discover_source_pipelines(self) -> list[dict]:
        """Scan seeknal/ dir for YAML and Python pipeline files.

        Returns list of dicts with keys: kind, name, description,
        column_descriptions, file_path.
        Reuses the same glob patterns as DAGBuilder._discover_yaml_files().
        """
        if self._pipelines_cache is not None:
            return self._pipelines_cache

        if not self.seeknal_dir.exists():
            self._pipelines_cache = []
            return self._pipelines_cache

        common_dir = self.seeknal_dir / "common"
        templates_dir = self.seeknal_dir / "templates"
        pipelines: list[dict] = []

        # Discover YAML pipeline files
        for f in sorted(self.seeknal_dir.glob("**/*.yml")):
            if f.is_relative_to(common_dir):
                continue
            try:
                with open(f, encoding="utf-8") as fh:
                    data = yaml.safe_load(fh)
                if isinstance(data, dict):
                    pipelines.append({
                        "kind": data.get("kind", "unknown"),
                        "name": data.get("name", f.stem),
                        "description": data.get("description", ""),
                        "column_descriptions": data.get("column_descriptions", {}),
                        "file_path": str(f.relative_to(self.project_path)),
                    })
            except Exception:
                continue

        # Discover Python pipeline files
        for f in sorted(self.seeknal_dir.glob("**/*.py")):
            if f.is_relative_to(common_dir) or f.is_relative_to(templates_dir):
                continue
            if f.name.startswith("_"):
                continue
            pipelines.append({
                "kind": "python_pipeline",
                "name": f.stem,
                "description": "",
                "file_path": str(f.relative_to(self.project_path)),
            })

        self._pipelines_cache = pipelines
        return self._pipelines_cache

    def get_pipeline_content(self, file_path: str) -> Optional[str]:
        """Read a pipeline definition file securely.

        Args:
            file_path: Path relative to the project root.

        Returns:
            File content as string, or None if the path is invalid/blocked.
        """
        resolved = (self.project_path / file_path).resolve()

        # Security: must stay within project root
        if not resolved.is_relative_to(self.project_path.resolve()):
            return None

        # Block credential and secret files
        if resolved.name.lower() in BLOCKED_FILES:
            return None

        if not resolved.exists() or not resolved.is_file():
            return None

        try:
            return resolved.read_text(encoding="utf-8")
        except Exception:
            return None

    def search_pipeline_content(self, query: str) -> list[dict]:
        """Search pipeline files for matching content (case-insensitive).

        Returns list of dicts with keys: kind, name, file_path, matched_line, context.
        """
        pipelines = self._discover_source_pipelines()
        query_lower = query.lower()
        results = []

        for p in pipelines:
            content = self.get_pipeline_content(p["file_path"])
            if content and query_lower in content.lower():
                for i, line in enumerate(content.splitlines(), 1):
                    if query_lower in line.lower():
                        results.append({
                            **p,
                            "matched_line": i,
                            "context": line.strip(),
                        })
                        break
            if len(results) >= 20:
                break

        return results

    def get_pipelines_summary(self) -> list[dict]:
        """Get a summary list of all pipeline definitions."""
        return self._discover_source_pipelines()

    def _format_source_pipelines(self, pipelines: list[dict]) -> str:
        lines = ["## Pipeline Definitions\n"]
        lines.append(
            "Source pipeline files that define how data is produced. "
            "Use `read_pipeline` to see the full definition.\n"
        )
        by_kind: dict[str, list[dict]] = {}
        for p in pipelines:
            by_kind.setdefault(p["kind"], []).append(p)
        for kind, items in sorted(by_kind.items()):
            lines.append(f"### {kind} ({len(items)} nodes)")
            for item in items[:20]:
                desc = f" — {item['description']}" if item["description"] else ""
                lines.append(f"- `{item['name']}`{desc} (`{item['file_path']}`)")
                col_descs = item.get("column_descriptions", {})
                if col_descs:
                    for col_name, col_desc in col_descs.items():
                        lines.append(f"  - `{col_name}`: {col_desc}")
            lines.append("")
        return "\n".join(lines)
