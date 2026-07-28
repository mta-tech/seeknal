"""Declaring and validating online serving for a node.

Two things live here:

**Configuration (1.6).** Online publication is opt-in per target. Rather than
inventing a parallel config block, this extends the existing ``materializations``
list that ``DAGBuilder._normalize_materializations`` already accepts, and reuses
``PostgresMaterializationConfig``'s existing field names. An earlier design
invented ``online_materialization.target`` with a ``mode: upsert`` value that is
not in the real enum (``upsert_by_key``); that duplication is avoided here.

**Terminality (1.7).** ``serve_online: true`` declares *intent*. It does not
establish that a node is terminal -- a caller cannot assert that about itself,
because terminality is a property of the graph. Publishing an intermediate node
would put rows in the online store that nothing serves, and would widen the
refresh-ordering surface for no benefit. Terminality is therefore derived from
the DAG: a node is terminal when nothing downstream of it produces features.

Example
-------

.. code-block:: yaml

    materializations:
      - type: postgresql
        connection: feature_online_pg
        table: feature_online.fg_retail__customer_30d
        mode: upsert_by_key          # existing PostgresMaterializationMode value
        unique_keys: [customer_id]
        serve_online: true           # opt in
        generation_mode: in_place    # staged is declared but not implemented
        read_roles: [data-scientist] # who may read it -- required

**Access policy (2.7).** ``read_roles`` declares which roles may read the served
values. It is required whenever ``serve_online`` is set, and it travels with the
data: the publisher writes it into the store in the same transaction that
activates the values, so a feature group can never be readable before its policy
is. The catalog reads that policy and is fail-closed -- a group with no policy
row is denied to everyone. Requiring it here is what makes the fail-closed
reader safe to deploy: an unlabelled group is unreadable, so silence cannot
leak data.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Mapping, Protocol

logger = logging.getLogger(__name__)

#: Node kinds that produce features. These are the nodes that may serve online,
#: and the presence of one downstream is what makes a node non-terminal.
#:
#: All three were historically unable to reach the multi-target dispatcher --
#: only source/transform/python could -- so every feature-producing node was cut
#: off from PostgreSQL. They are enumerated by value rather than by importing
#: NodeType so this module stays usable without the DAG package loaded.
FEATURE_PRODUCING_KINDS: frozenset[str] = frozenset(
    {"feature_group", "aggregation", "second_order_aggregation"}
)


class GenerationMode(str, Enum):
    """How a publication replaces the served generation."""

    #: Upsert into the live table. Rows are updated in place and never absent.
    IN_PLACE = "in_place"
    #: Build a validated staging table, then activate. Required when the entity
    #: set can shrink, because upsert cannot remove departed entities.
    STAGED = "staged"


class OnlineServingConfigError(ValueError):
    """The online serving declaration is invalid."""


class TerminalityError(OnlineServingConfigError):
    """A node declared ``serve_online`` but is not terminal in the graph."""


class DagLike(Protocol):
    """The slice of DAGBuilder this module needs.

    Declared structurally so terminality can be unit-tested without
    constructing a real DAG.
    """

    def get_all_downstream(self, node_name: str) -> set[str]: ...

    def get_all_upstream(self, node_name: str) -> set[str]: ...

    def get_node(self, qualified_name: str) -> Any: ...


@dataclass(frozen=True)
class OnlineServingTarget:
    """One materialization target that opted into online serving."""

    connection: str
    table: str
    unique_keys: tuple[str, ...]
    mode: str = "upsert_by_key"
    generation_mode: GenerationMode = GenerationMode.STAGED
    #: Age limit for served features, measured from ``source_interval_end``.
    #: ``None`` means no expiry.
    #:
    #: Expiry **retires** rows rather than deleting them: they leave the live
    #: projection but remain queryable for audit, which keeps an intentional
    #: withdrawal distinguishable from a transient absence.
    #:
    #: Deliberately defaults to ``None`` rather than the ``1`` that
    #: ``FeatureGroup``'s docstring long claimed. That value was never
    #: implemented, so adopting it now would silently retire nearly everything
    #: in an existing online store on the first run after upgrade.
    serving_ttl_days: int | None = None
    #: Roles permitted to read this feature group's served values.
    #:
    #: Required, and deliberately not defaulted. A default would have to be
    #: either permissive (every reader role -- which silently publishes
    #: PII-derived features to everyone, the exact failure this exists to
    #: prevent) or empty (which publishes data nobody can read). Neither is a
    #: sane thing to infer, so the declaration is mandatory: a target that asked
    #: to serve online can reasonably be asked who may read it.
    read_roles: tuple[str, ...] = ()
    #: Whether each publication is the complete entity population rather than an
    #: incremental refresh. In-place upsert cannot remove an entity that leaves
    #: the source, so a departed entity is served stale forever. When true, such
    #: entities are retired on publication. Defaults to false: the common case
    #: is incremental, where retiring everything not in the batch would withdraw
    #: entities that were simply not recomputed. Only set it when the pipeline
    #: recomputes the whole population each run.
    full_snapshot: bool = False

    def __post_init__(self) -> None:
        if not self.connection:
            raise OnlineServingConfigError(
                "an online serving target needs a 'connection' naming a "
                "profiles.yml entry"
            )
        if not self.table or "." not in self.table:
            raise OnlineServingConfigError(
                f"table {self.table!r} must be schema-qualified as 'schema.table'"
            )
        if not self.unique_keys:
            raise OnlineServingConfigError(
                f"online target {self.table!r} must declare 'unique_keys'; they "
                "become the primary key and the ON CONFLICT target"
            )
        if not self.read_roles:
            raise OnlineServingConfigError(
                f"online target {self.table!r} must declare 'read_roles': the "
                "roles permitted to read its served values. The catalog is "
                "fail-closed, so a group published without a policy is readable "
                "by nobody. Declare the roles explicitly rather than relying on "
                "a default that would either over-share or serve nothing."
            )
        for role in self.read_roles:
            if not isinstance(role, str) or not role.strip():
                raise OnlineServingConfigError(
                    f"read_roles for {self.table!r} contains a blank entry; each "
                    "role must be a non-empty string"
                )
        if len(set(self.read_roles)) != len(self.read_roles):
            raise OnlineServingConfigError(
                f"read_roles for {self.table!r} contains duplicates: "
                f"{list(self.read_roles)}"
            )
        # Reject the invented value an earlier design used, with a pointer to
        # the real one, rather than silently accepting it.
        if self.mode == "upsert":
            raise OnlineServingConfigError(
                "mode 'upsert' is not a PostgresMaterializationMode value; use "
                "'upsert_by_key'"
            )

    @property
    def schema(self) -> str:
        return self.table.split(".", 1)[0]

    @property
    def table_name(self) -> str:
        return self.table.split(".", 1)[1]


def parse_online_targets(
    materializations: Iterable[Mapping[str, Any]] | None,
) -> list[OnlineServingTarget]:
    """Extract the targets that opted into online serving.

    Absence of ``serve_online`` means offline-only: publication is opt-in, so
    silence never publishes.
    """
    if not materializations:
        return []

    targets: list[OnlineServingTarget] = []
    for entry in materializations:
        if not isinstance(entry, Mapping):
            continue
        if not entry.get("serve_online", False):
            continue

        if entry.get("type") != "postgresql":
            raise OnlineServingConfigError(
                f"serve_online is only supported for type 'postgresql', got "
                f"{entry.get('type')!r} for table {entry.get('table')!r}"
            )

        # Defaults to in_place, not staged. STAGED is declared in the enum and
        # described in the docs, but no code path implements it: the executor
        # never reads generation_mode and always calls the in-place publisher.
        # Defaulting to STAGED would therefore have silently given every target
        # in-place behaviour while its config claimed otherwise -- the same
        # class of phantom setting as the long-documented, never-implemented
        # serving_ttl_days.
        raw_mode = entry.get("generation_mode", GenerationMode.IN_PLACE.value)
        try:
            generation_mode = GenerationMode(raw_mode)
        except ValueError:
            raise OnlineServingConfigError(
                f"unknown generation_mode {raw_mode!r}; expected one of "
                f"{[m.value for m in GenerationMode]}"
            ) from None
        if generation_mode is GenerationMode.STAGED:
            raise OnlineServingConfigError(
                "generation_mode 'staged' is not implemented yet: the executor "
                "always publishes in place, so declaring 'staged' would describe "
                "behaviour the system does not have. Use 'in_place' explicitly, "
                "and note that in-place upsert cannot remove entities that "
                "disappear from the source."
            )

        ttl = entry.get("serving_ttl_days")
        if ttl is not None:
            try:
                ttl = int(ttl)
            except (TypeError, ValueError):
                raise OnlineServingConfigError(
                    f"serving_ttl_days must be an integer number of days, got {ttl!r}"
                ) from None
            if ttl <= 0:
                raise OnlineServingConfigError(
                    f"serving_ttl_days must be positive, got {ttl}; omit it for no expiry"
                )

        raw_roles = entry.get("read_roles", ()) or ()
        if isinstance(raw_roles, str):
            # A bare string would otherwise be iterated character by character,
            # turning "admin" into five one-letter roles that match nothing.
            raise OnlineServingConfigError(
                f"read_roles for {entry.get('table')!r} must be a list of roles, "
                f"not the string {raw_roles!r}"
            )
        read_roles = tuple(str(r).strip() for r in raw_roles)

        targets.append(
            OnlineServingTarget(
                connection=entry.get("connection", ""),
                table=entry.get("table", ""),
                unique_keys=tuple(entry.get("unique_keys", ()) or ()),
                mode=entry.get("mode", "upsert_by_key"),
                generation_mode=generation_mode,
                serving_ttl_days=ttl,
                read_roles=read_roles,
                full_snapshot=bool(entry.get("full_snapshot", False)),
            )
        )
    return targets


def is_terminal(dag: DagLike, node_name: str) -> bool:
    """True when no downstream node produces features.

    Downstream consumers that merely *read* features -- models, exposures,
    metrics -- do not make a node intermediate. Only another feature-producing
    node does, because that means these features are an input to further feature
    computation rather than an end product.
    """
    for downstream_name in dag.get_all_downstream(node_name):
        node = dag.get_node(downstream_name)
        if node is None:
            continue
        kind = getattr(getattr(node, "kind", None), "value", None) or getattr(
            node, "kind", None
        )
        if str(kind) in FEATURE_PRODUCING_KINDS:
            return False
    return True


def assert_terminal(dag: DagLike, node_name: str) -> None:
    """Raise unless *node_name* is terminal.

    Called before publishing so that a ``serve_online: true`` declaration on an
    intermediate node fails loudly instead of quietly filling the online store
    with rows nothing serves.
    """
    if is_terminal(dag, node_name):
        return

    offenders = sorted(
        d
        for d in dag.get_all_downstream(node_name)
        if (node := dag.get_node(d)) is not None
        and str(
            getattr(getattr(node, "kind", None), "value", None)
            or getattr(node, "kind", None)
        )
        in FEATURE_PRODUCING_KINDS
    )
    raise TerminalityError(
        f"node {node_name!r} declared serve_online but is not terminal: it feeds "
        f"feature-producing node(s) {offenders}. Publish the terminal node "
        "instead; intermediates stay offline."
    )


def resolve_upstream_nodes(dag: DagLike, node_name: str) -> list[str]:
    """Transitive upstream closure, for the fail-closed publication gate.

    Uses ``DAGBuilder.get_all_upstream``, which is the authoritative graph.
    ``DependencyRegistry`` is *not* usable here: it is populated dynamically as
    ``ref()``/``source()`` execute and exposes only immediate dependencies, with
    no closure operation.
    """
    return sorted(dag.get_all_upstream(node_name))


__all__ = [
    "FEATURE_PRODUCING_KINDS",
    "DagLike",
    "GenerationMode",
    "OnlineServingConfigError",
    "OnlineServingTarget",
    "TerminalityError",
    "assert_terminal",
    "is_terminal",
    "parse_online_targets",
    "resolve_upstream_nodes",
]
