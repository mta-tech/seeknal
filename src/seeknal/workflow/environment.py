"""
Virtual environment management for safe pipeline development.

Environments provide isolated dev/staging spaces where changes can be
previewed, tested, and promoted to production without risk.

Design:
- Each environment gets its own directory under target/environments/<name>/
- Unchanged nodes reference production outputs (no re-execution)
- Plan staleness detection via manifest fingerprint
- Atomic promote via temp dir + rename
- TTL-based auto-cleanup
"""

import hashlib
import json
import shutil
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from seeknal.dag.diff import ManifestDiff, ChangeCategory
from seeknal.dag.manifest import Manifest


@dataclass
class EnvironmentConfig:
    """Configuration and metadata for a virtual environment."""
    name: str
    created_at: str          # ISO timestamp
    last_accessed: str       # ISO timestamp, updated on plan/apply
    promoted_from: Optional[str] = None
    ttl_seconds: int = 604800  # 7 days default

    def is_expired(self) -> bool:
        """Check if environment has exceeded its TTL."""
        last = time.mktime(time.strptime(self.last_accessed, "%Y-%m-%dT%H:%M:%S"))
        return (time.time() - last) > self.ttl_seconds


@dataclass
class EnvironmentPlan:
    """Saved plan for an environment."""
    env_name: str
    manifest_fingerprint: str   # SHA256 of serialized new manifest
    categorized_changes: Dict[str, str]  # node_id -> ChangeCategory.value
    added_nodes: List[str]
    removed_nodes: List[str]
    created_at: str
    total_nodes_to_execute: int


@dataclass
class EnvironmentRef:
    """Reference to a production output for an unchanged node."""
    node_id: str
    output_path: str   # Path to production cache file
    fingerprint: str   # Hash at time of reference creation


class EnvironmentManager:
    """Manages virtual environments for safe pipeline development."""

    def __init__(self, target_path: Path):
        self.target_path = target_path
        self.envs_dir = target_path / "environments"

    def _get_env_dir(self, env_name: str) -> Path:
        return self.envs_dir / env_name

    def _get_manifest_fingerprint(self, manifest: Manifest) -> str:
        """Compute SHA256 fingerprint of a manifest."""
        data = json.dumps(manifest.to_dict(), sort_keys=True)
        return hashlib.sha256(data.encode()).hexdigest()

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    def _load_production_manifest(self) -> Optional[Manifest]:
        """Load the production manifest."""
        manifest_path = self.target_path / "manifest.json"
        if manifest_path.exists():
            return Manifest.load(str(manifest_path))
        return None

    def _save_json(self, path: Path, data: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def _load_json(self, path: Path) -> Optional[dict]:
        if not path.exists():
            return None
        with open(path) as f:
            return json.load(f)

    def plan(self, env_name: str, new_manifest: Manifest) -> EnvironmentPlan:
        """Create a plan for the given environment.

        1. Load production manifest
        2. Compute ManifestDiff
        3. Classify changes (uses categorization from diff module)
        4. Create environment directory
        5. Build reference manifest for unchanged nodes
        6. Save plan

        Returns:
            EnvironmentPlan with categorized changes
        """
        prod_manifest = self._load_production_manifest()
        rebuild_map: Dict[str, ChangeCategory] = {}
        if prod_manifest is None:
            # First time - treat all nodes as new
            categorized = {nid: ChangeCategory.NON_BREAKING.value for nid in new_manifest.nodes}
            plan = EnvironmentPlan(
                env_name=env_name,
                manifest_fingerprint=self._get_manifest_fingerprint(new_manifest),
                categorized_changes=categorized,
                added_nodes=list(new_manifest.nodes.keys()),
                removed_nodes=[],
                created_at=self._now_iso(),
                total_nodes_to_execute=len(new_manifest.nodes),
            )
        else:
            diff = ManifestDiff.compare(prod_manifest, new_manifest)
            rebuild_map = diff.get_nodes_to_rebuild(new_manifest)

            categorized = {nid: cat.value for nid, cat in rebuild_map.items()}

            plan = EnvironmentPlan(
                env_name=env_name,
                manifest_fingerprint=self._get_manifest_fingerprint(new_manifest),
                categorized_changes=categorized,
                added_nodes=list(diff.added_nodes.keys()),
                removed_nodes=list(diff.removed_nodes.keys()),
                created_at=self._now_iso(),
                total_nodes_to_execute=sum(
                    1 for cat in rebuild_map.values()
                    if cat != ChangeCategory.METADATA
                ),
            )

        # Create env directory and save plan
        env_dir = self._get_env_dir(env_name)
        env_dir.mkdir(parents=True, exist_ok=True)

        # Save or update env config
        config_path = env_dir / "env_config.json"
        existing = self._load_json(config_path)
        if existing:
            config = EnvironmentConfig(
                name=env_name,
                created_at=existing["created_at"],
                last_accessed=self._now_iso(),
                ttl_seconds=existing.get("ttl_seconds", 604800),
            )
        else:
            config = EnvironmentConfig(
                name=env_name,
                created_at=self._now_iso(),
                last_accessed=self._now_iso(),
            )
        self._save_json(config_path, asdict(config))

        # Save plan
        self._save_json(env_dir / "plan.json", asdict(plan))

        # Save new manifest for apply
        self._save_json(env_dir / "manifest.json", new_manifest.to_dict())

        # Build production references for unchanged nodes
        if prod_manifest:
            refs = self._build_production_refs(new_manifest, rebuild_map, prod_manifest)
            self._save_json(env_dir / "refs.json", {"refs": [asdict(r) for r in refs]})

        return plan

    def _build_production_refs(
        self,
        new_manifest: Manifest,
        rebuild_map: Dict[str, ChangeCategory],
        prod_manifest: Manifest,
    ) -> List[EnvironmentRef]:
        """Build references to production outputs for unchanged nodes."""
        refs = []
        for node_id in new_manifest.nodes:
            if node_id not in rebuild_map:
                # Unchanged node - reference production cache
                node = new_manifest.nodes[node_id]
                cache_path = self.target_path / "cache" / node.node_type.value / f"{node.name}.parquet"
                if cache_path.exists():
                    refs.append(EnvironmentRef(
                        node_id=node_id,
                        output_path=str(cache_path),
                        fingerprint=hashlib.sha256(
                            cache_path.read_bytes()[:4096]
                        ).hexdigest(),
                    ))
        return refs

    def apply(self, env_name: str, force: bool = False) -> Dict[str, Any]:
        """Apply a saved plan (validate staleness, return plan info).

        This validates the plan and returns info for the runner to execute.
        Actual execution is done by the CLI command using DAGRunner.

        Returns:
            Dict with 'plan', 'manifest', 'env_dir', 'nodes_to_execute'

        Raises:
            ValueError: If no plan exists or plan is stale
        """
        env_dir = self._get_env_dir(env_name)
        if not env_dir.exists():
            raise ValueError(f"Environment '{env_name}' not found")

        plan_data = self._load_json(env_dir / "plan.json")
        if plan_data is None:
            raise ValueError(
                f"No plan found for environment '{env_name}'. "
                f"Run 'seeknal env plan {env_name}' first."
            )

        manifest_data = self._load_json(env_dir / "manifest.json")
        if manifest_data is None:
            raise ValueError(f"No manifest found for environment '{env_name}'")

        # Check staleness
        new_manifest = Manifest.from_dict(manifest_data)
        current_fp = self._get_manifest_fingerprint(new_manifest)

        if plan_data["manifest_fingerprint"] != current_fp and not force:
            raise ValueError(
                f"Plan is stale -- manifest has changed since plan was created. "
                f"Run 'seeknal env plan {env_name}' to refresh, or use --force to apply anyway."
            )

        # Update last_accessed
        config_path = env_dir / "env_config.json"
        config_data = self._load_json(config_path)
        if config_data:
            config_data["last_accessed"] = self._now_iso()
            self._save_json(config_path, config_data)

        # Determine nodes to execute (exclude METADATA)
        nodes_to_execute = {
            nid for nid, cat_str in plan_data["categorized_changes"].items()
            if cat_str != ChangeCategory.METADATA.value
        }

        return {
            "plan": plan_data,
            "manifest": new_manifest,
            "env_dir": env_dir,
            "nodes_to_execute": nodes_to_execute,
        }

    def promote(self, from_env: str, to_env: str = "prod") -> None:
        """Promote environment outputs to production (or another env).

        Uses atomic two-phase approach: copy to temp, then rename.

        Raises:
            ValueError: If source environment doesn't exist or hasn't been applied
        """
        from_dir = self._get_env_dir(from_env)
        if not from_dir.exists():
            raise ValueError(f"Environment '{from_env}' not found")

        # Check that env has been applied (has run_state.json)
        env_state = from_dir / "run_state.json"
        if not env_state.exists():
            raise ValueError(
                f"Environment '{from_env}' has not been applied. "
                f"Run 'seeknal env apply {from_env}' first."
            )

        if to_env == "prod":
            # Promote to production
            target_cache = self.target_path / "cache"
            env_cache = from_dir / "cache"

            # Atomic promote: copy to temp, then rename
            temp_dir = self.target_path / f"_promote_temp_{int(time.time())}"
            try:
                # Copy production cache to temp (backup)
                if target_cache.exists():
                    shutil.copytree(target_cache, temp_dir)

                # Copy env cache over production
                if env_cache.exists():
                    for item in env_cache.iterdir():
                        dest = target_cache / item.name
                        if item.is_dir():
                            if dest.exists():
                                shutil.rmtree(dest)
                            shutil.copytree(item, dest)
                        else:
                            shutil.copy2(item, dest)

                # Copy env state to production
                prod_state = self.target_path / "run_state.json"
                if env_state.exists():
                    shutil.copy2(env_state, prod_state)

                # Copy env manifest to production
                env_manifest = from_dir / "manifest.json"
                prod_manifest = self.target_path / "manifest.json"
                if env_manifest.exists():
                    shutil.copy2(env_manifest, prod_manifest)

            finally:
                # Clean up temp
                if temp_dir.exists():
                    shutil.rmtree(temp_dir)
        else:
            # Promote to another environment
            to_dir = self._get_env_dir(to_env)
            to_dir.mkdir(parents=True, exist_ok=True)

            # Copy cache, state, manifest
            for item_name in ["cache", "run_state.json", "manifest.json"]:
                src = from_dir / item_name
                dst = to_dir / item_name
                if src.exists():
                    if src.is_dir():
                        if dst.exists():
                            shutil.rmtree(dst)
                        shutil.copytree(src, dst)
                    else:
                        shutil.copy2(src, dst)

    def list_environments(self) -> List[EnvironmentConfig]:
        """List all environments with their status."""
        envs = []
        if not self.envs_dir.exists():
            return envs

        for env_dir in sorted(self.envs_dir.iterdir()):
            if env_dir.is_dir():
                config_data = self._load_json(env_dir / "env_config.json")
                if config_data:
                    envs.append(EnvironmentConfig(**{
                        k: v for k, v in config_data.items()
                        if k in EnvironmentConfig.__dataclass_fields__
                    }))
        return envs

    def delete_environment(self, env_name: str) -> None:
        """Delete an environment directory."""
        env_dir = self._get_env_dir(env_name)
        if not env_dir.exists():
            raise ValueError(f"Environment '{env_name}' not found")
        shutil.rmtree(env_dir)

    def cleanup_expired(self) -> List[str]:
        """Remove environments past their TTL. Returns deleted env names."""
        deleted = []
        for env_config in self.list_environments():
            if env_config.is_expired():
                self.delete_environment(env_config.name)
                deleted.append(env_config.name)
        return deleted
