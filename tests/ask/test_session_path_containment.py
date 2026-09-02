"""Session and pairing paths must stay inside their base directory.

Reported as C-1 (CRITICAL) in the IBA v2 security review, 2026-09-01. The
premises worker takes `session_id` and `tenant_id` from the broker and joins
them onto a filesystem path with no validation, so an HTTP request to the cloud
gateway could create directories and write files at an attacker-chosen path on
customer infrastructure, with the worker process's privileges.

The chain, across two repositories:

    iba-v2  agent_request.py     conversation_id accepted from the caller
    iba-v2  premises_event_source session_id = conversation_id or work_id
    iba-v2  premises_broker       shipped to the worker in public_dict()
    seeknal cli/gateway.py        session_id = work["session_id"]
    seeknal gateway/server.py     SessionStore(...); store.create(name=session_id)
    seeknal sessions.py           self._base / name   <-- no validation

The IBA half is fixed (iba-v2 4c4eff8) and this is the seeknal half. Both are
needed: seeknal is the component whose property is violated and it cannot rely
on every future broker being careful.

The ABSOLUTE case is the one to read twice. `Path("/a/b") / "/tmp/x"` is
`Path("/tmp/x")` — pathlib discards the left operand — so no `..` appears
anywhere and a reviewer grepping for traversal sees nothing.
"""
from __future__ import annotations

import pytest

from seeknal.ask.sessions import SessionStore
from seeknal.ask.safe_paths import UnsafePathSegment, contained_child

HOSTILE = [
    "../../../../OUTSIDE/pwned",
    "..",
    ".",
    "a/../../b",
    "/etc/passwd",
    "/tmp/absolute-escape",
    "sub/dir",
    "back\\slash",
    "nul\x00byte",
    "",
]


def _store(tmp_path, tenant_id="default"):
    return SessionStore(sessions_dir=tmp_path / "sessions", tenant_id=tenant_id)


# --- the session name -------------------------------------------------------

@pytest.mark.parametrize("name", HOSTILE)
def test_a_hostile_session_name_is_rejected(tmp_path, name) -> None:
    with pytest.raises(UnsafePathSegment):
        _store(tmp_path).create(name=name)


def test_a_rejected_name_creates_nothing_on_disk(tmp_path) -> None:
    """Refusal must come BEFORE `mkdir(parents=True)`. A guard that raises after
    the directory exists has already done the thing it was added to prevent."""
    outside = tmp_path / "OUTSIDE"
    with pytest.raises(UnsafePathSegment):
        _store(tmp_path).create(name="../OUTSIDE/pwned")
    assert not outside.exists(), "the traversal target was created anyway"


# --- the tenant id ----------------------------------------------------------

@pytest.mark.parametrize("tenant_id", ["../../escape", "/tmp/tenant-escape", ".."])
def test_a_hostile_tenant_id_is_rejected(tmp_path, tenant_id) -> None:
    """`self._base = base / tenant_id` is the second sink, and it runs in
    __init__ — so a hostile tenant escapes before any session name is seen."""
    with pytest.raises(UnsafePathSegment):
        SessionStore(sessions_dir=tmp_path / "sessions", tenant_id=tenant_id)


def test_a_rejected_tenant_creates_nothing_on_disk(tmp_path) -> None:
    outside = tmp_path / "OUTSIDE"
    with pytest.raises(UnsafePathSegment):
        SessionStore(sessions_dir=tmp_path / "sessions" / "x",
                     tenant_id="../../OUTSIDE")
    assert not outside.exists()


# --- the case a regex cannot catch -----------------------------------------

def test_a_symlink_that_escapes_the_base_is_rejected(tmp_path) -> None:
    """The reason this resolves and compares instead of matching strings.

    `escape` is a perfectly ordinary name: no dots, no separators, and it passes
    any character allowlist. It is a symlink pointing out of the base, so the
    only thing that catches it is resolving the joined path and checking where
    it actually lands.
    """
    sessions = tmp_path / "sessions"
    sessions.mkdir(parents=True)
    outside = tmp_path / "OUTSIDE"
    outside.mkdir()
    (sessions / "escape").symlink_to(outside, target_is_directory=True)

    with pytest.raises(UnsafePathSegment):
        SessionStore(sessions_dir=sessions).create(name="escape")


def test_a_sibling_whose_name_merely_starts_with_the_base_is_rejected(tmp_path) -> None:
    """Why the check compares PATH OBJECTS and not string prefixes.

    `/data/tenant-a-evil` starts with `/data/tenant-a`, so a `startswith` guard
    reports it as contained. It is not: it is a SIBLING. Reached here through a
    symlink, which is the only way a single-segment join can land beside its own
    base rather than under it.

    This is the mutant that survives every other test in this file, and it is
    the specific failure the fix was asked to avoid.
    """
    base = tmp_path / "tenant-a"
    base.mkdir()
    sibling = tmp_path / "tenant-a-evil"
    sibling.mkdir()
    (base / "s").symlink_to(sibling, target_is_directory=True)

    with pytest.raises(UnsafePathSegment):
        contained_child(base, "s", label="session name")


# --- the over-refusal guards ------------------------------------------------

def test_a_generated_session_name_still_works(tmp_path) -> None:
    """`generate_session_name()` produces 'calm-river-315'. A guard that refused
    seeknal's own names would pass every test above and break the product."""
    store = _store(tmp_path)
    name = store.create()
    assert store.get(name) is not None


def test_the_id_the_iba_gateway_actually_sends_is_accepted(tmp_path) -> None:
    """Taken from the caller rather than invented: Data Formulator sends
    `exploreDataFromNL_<timestamp>`, which IBA forwards as the session id."""
    store = _store(tmp_path)
    store.create(name="exploreDataFromNL_1788237058675")
    assert store.get("exploreDataFromNL_1788237058675") is not None


def test_a_named_tenant_still_gets_its_own_subdirectory(tmp_path) -> None:
    """Multi-tenant layout must keep working; only escaping is refused."""
    store = SessionStore(sessions_dir=tmp_path / "sessions", tenant_id="tenant-a")
    name = store.create()
    assert (tmp_path / "sessions" / "tenant-a" / name).is_dir()


def test_the_default_tenant_keeps_the_legacy_flat_layout(tmp_path) -> None:
    """Pre-multi-tenant deployments put sessions directly under the base. That
    backward-compatibility branch must survive the fix."""
    store = SessionStore(sessions_dir=tmp_path / "sessions", tenant_id="default")
    name = store.create()
    assert (tmp_path / "sessions" / name).is_dir()


# --- the same defect, one module over ---------------------------------------
#
# Found by sweeping the class rather than by a second report. `pairing.py` joins
# the SAME broker-supplied `tenant_id` onto a base and then mkdirs it, in three
# separate stores. A mutant that reverted those three joins survived this file
# until these tests existed — a fix nothing exercises is a fix nobody can keep.


@pytest.mark.parametrize("store_name", ["FilePairingStore", "TelegramLinkStore",
                                        "PublicSessionStore"])
@pytest.mark.parametrize("tenant_id", ["../../escape", "/tmp/pairing-escape", ".."])
def test_a_hostile_tenant_id_is_rejected_by_every_pairing_store(
    tmp_path, store_name, tenant_id
) -> None:
    from seeknal.ask.gateway import pairing

    store = getattr(pairing, store_name)(base_dir=tmp_path / store_name)
    with pytest.raises(UnsafePathSegment):
        store._tenant_dir(tenant_id)


@pytest.mark.parametrize("store_name", ["FilePairingStore", "TelegramLinkStore",
                                        "PublicSessionStore"])
def test_an_ordinary_tenant_still_gets_a_pairing_directory(tmp_path, store_name) -> None:
    """The over-refusal guard for the sweep: multi-tenant pairing must keep
    working, and all three stores are used in production paths."""
    from seeknal.ask.gateway import pairing

    store = getattr(pairing, store_name)(base_dir=tmp_path / store_name)
    assert store._tenant_dir("tenant-a").is_dir()
