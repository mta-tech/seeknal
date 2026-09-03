"""Containment for path segments that arrive from outside this process.

Reported as C-1 (CRITICAL) in the IBA v2 security review, 2026-09-01: the
gateway worker took `session_id` and `tenant_id` from the broker and joined them
onto a filesystem path with no check, so one HTTP request to the cloud gateway
could create directories and write files anywhere the worker user can write.

Two shapes matter, and only one is the one people look for.

``../..`` is the obvious one. The other is an ABSOLUTE segment::

    Path("/a/b") / "/tmp/x"   ==   Path("/tmp/x")

pathlib discards the left operand entirely, so the base is silently thrown away
and no traversal sequence appears anywhere in the input. A reviewer grepping for
``..`` would never see it.

Neither is the reason this module resolves the result instead of comparing
strings. That reason is SYMLINKS: a segment like ``escape`` contains no dots and
no separators and passes any character allowlist, and if it names a symlink
pointing out of the base then the join lands outside anyway. Only resolving the
joined path and asking where it actually is will catch that — which is why the
allowlist here is the cheap first pass and the containment check is the actual
guarantee.
"""

from __future__ import annotations

import re
from pathlib import Path

__all__ = ["UnsafePathSegment", "contained_child", "contained_path"]

_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9._-]{1,128}$")

_RESERVED = {".", ".."}
"""Both are built ENTIRELY of characters the allowlist permits.

An alphabet that allows dots therefore accepts the traversal component it was
added to stop, which is a mistake worth naming rather than fixing silently: the
same gap was found in the sibling rule on the IBA side of this boundary.
"""


class UnsafePathSegment(ValueError):
    """A caller-supplied segment that would leave its base directory."""


def contained_child(base: Path, segment: str, *, label: str) -> Path:
    """Join ``segment`` onto ``base`` and PROVE the result is still inside it.

    Raises:
        UnsafePathSegment: if the segment is malformed, or if the joined path
            resolves outside ``base``.

    The check is deliberately in two parts, and the order is not arbitrary. The
    allowlist rejects the malformed cases cheaply and without touching the
    filesystem. The containment check then resolves both sides and compares
    PATH OBJECTS — not string prefixes, which report ``/data/tenant-a-evil`` as
    living inside ``/data/tenant-a``.
    """
    if not isinstance(segment, str) or not segment:
        raise UnsafePathSegment(f"{label} must be a non-empty string")
    if segment in _RESERVED or not _SAFE_SEGMENT.fullmatch(segment):
        raise UnsafePathSegment(
            f"{label} must be 1-128 characters of letters, digits, dot, "
            f"underscore or hyphen, and may not be '.' or '..'")

    # `strict=False` is required: the child usually does NOT exist yet — it is
    # about to be created — so resolution must normalise a path rather than
    # demand one. Existing components, including symlinks, are still followed,
    # which is exactly the case the allowlist above cannot see.
    root = base.resolve()
    return contained_path(root, root / segment, label=label)


def contained_path(base: Path, candidate: Path, *, label: str) -> Path:
    """Resolve ``candidate`` and PROVE it is inside ``base``.

    This is the containment half of :func:`contained_child`, split out for a
    caller that already holds a full path rather than a bare segment to join
    (for example, a path supplied wholesale by a remote caller). It carries
    none of the character-allowlist checks — those only make sense for a
    single path component — so a caller with an untrusted *whole path* still
    needs its own upfront shape checks (absolute, exists, etc.) before this
    is a complete guarantee; this function only proves the result did not
    escape ``base``, resolving both sides and comparing PATH OBJECTS so that
    a symlink inside ``base`` pointing outside it is still caught.

    Raises:
        UnsafePathSegment: if the resolved candidate is outside ``base``.
    """
    root = base.resolve()
    resolved = candidate.resolve()
    if resolved != root and root not in resolved.parents:
        raise UnsafePathSegment(f"{label} resolves outside its base directory")
    return resolved
