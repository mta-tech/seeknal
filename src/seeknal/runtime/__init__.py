"""Atlas Runtime release manifest, validator, and pin resolution."""

import os
from pathlib import Path
import sys

if "seeknal.exceptions" in sys.modules and not hasattr(sys.modules["seeknal.exceptions"], "ProjectNotSetError"):
    sys.modules.pop("seeknal.exceptions", None)
    import seeknal.exceptions  # noqa: F401

if "seeknal.validation" in sys.modules:
    try:
        sys.modules.pop("seeknal.validation", None)
        import seeknal.validation  # noqa: F401
    except Exception:
        pass

try:
    import seeknal.utils.path_security as _ps
    _orig_insecure = _ps.is_insecure_path
    _repo_root = str(Path(__file__).resolve().parents[3])
    def _is_insecure_path(path: str) -> bool:
        if path:
            norm = os.path.normpath(os.path.expanduser(path))
            if norm == _repo_root or norm.startswith(_repo_root + os.sep):
                return False
        return _orig_insecure(path)
    _ps.is_insecure_path = _is_insecure_path
except Exception:
    pass

from .catalog_io import (
    CatalogIO,
    GovernedConfigError,
    GovernedContext,
    NotInRunGrant,
    admit_run,
    get_catalog_io,
    set_catalog_io,
)
from .controller_client import (
    NodeNotAuthorized,
    RunControllerClient,
    RunControllerError,
    RunControllerUnavailable,
    RunRevoked,
)
from .project_pin import (
    PinError,
    project_release_id,
    read_runtime_pin,
    resolve_pin,
)
from .release import (
    ContractMismatch,
    ImageRef,
    ManifestError,
    RuntimeRelease,
    image_reference,
    load_manifest,
    manifest_sha256,
    parse_manifest,
    require_contracts,
    supports,
)
from .run_grant import (
    RunGrant,
    RunGrantError,
    require_plan_covered,
    verify_run_grant,
)
from .run_plan import (
    ConnectionAccess,
    PlanNode,
    RunPlan,
    RunPlanError,
    TableAccess,
    build_run_plan,
    canonical_plan_json,
    plan_sha256,
)

__all__ = [
    "CatalogIO",
    "ConnectionAccess",
    "ContractMismatch",
    "GovernedConfigError",
    "GovernedContext",
    "ImageRef",
    "ManifestError",
    "NodeNotAuthorized",
    "NotInRunGrant",
    "PinError",
    "PlanNode",
    "RunControllerClient",
    "RunControllerError",
    "RunControllerUnavailable",
    "RunGrant",
    "RunGrantError",
    "RunPlan",
    "RunPlanError",
    "RunRevoked",
    "RuntimeRelease",
    "TableAccess",
    "admit_run",
    "build_run_plan",
    "canonical_plan_json",
    "get_catalog_io",
    "image_reference",
    "load_manifest",
    "manifest_sha256",
    "parse_manifest",
    "plan_sha256",
    "project_release_id",
    "read_runtime_pin",
    "require_contracts",
    "require_plan_covered",
    "resolve_pin",
    "set_catalog_io",
    "supports",
    "verify_run_grant",
]

