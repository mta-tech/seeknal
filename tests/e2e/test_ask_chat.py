"""E2E tests for `seeknal ask chat` — pexpect-based interactive TUI tests.

Spawns the real CLI in a pseudo-TTY, feeds multi-turn input, and verifies
behavioral outcomes (prompts appeared, output produced, session saved, clean
exit).  Tests use the real LLM backend configured in .env.

Requires: GOOGLE_API_KEY or SEEKNAL_ASK_LLM_PROVIDER=ollama
Mark: @pytest.mark.e2e — skipped when LLM credentials are missing.
"""

import json
import os
import random
import string

import pexpect
import pytest

from tests.e2e.conftest import requires_llm, strip_ansi

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SPAWN_CMD = "uv run seeknal ask chat"
INITIAL_TIMEOUT = 60  # seconds — covers uv startup + agent loading
TURN_TIMEOUT = 120  # seconds — real LLM response time
EXIT_TIMEOUT = 15  # seconds — clean shutdown


def _spawn_chat(project_path, extra_args=""):
    """Spawn a seeknal ask chat child process in a PTY."""
    cmd = f"{SPAWN_CMD} --project {project_path} {extra_args}".strip()
    env = os.environ.copy()
    child = pexpect.spawn(cmd, encoding="utf-8", timeout=INITIAL_TIMEOUT, env=env)
    # Log to stdout for debugging on CI failures
    child.logfile_read = None  # set to sys.stdout for verbose debugging
    # Accumulate all output for visual verification
    child._captured_output = []
    return child


def _capture(child):
    """Record child.before into the accumulated output list."""
    if child.before:
        child._captured_output.append(child.before)


def _wait_for_prompt(child, timeout=INITIAL_TIMEOUT):
    """Wait for the 'You: ' input prompt to appear."""
    child.expect("You: ", timeout=timeout)


def _send_and_wait(child, question, timeout=TURN_TIMEOUT):
    """Send a question and wait for the next 'You: ' prompt.

    Returns the response text (ANSI-stripped) between the sent line and the
    next prompt.
    """
    child.sendline(question)
    child.expect("You: ", timeout=timeout)
    raw = child.before or ""
    return strip_ansi(raw).strip()


def _exit_chat(child):
    """Send 'exit' and wait for clean shutdown."""
    child.sendline("exit")
    child.expect(pexpect.EOF, timeout=EXIT_TIMEOUT)


def _unique_session_name():
    """Generate a unique session name for test isolation."""
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"test-e2e-{suffix}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@requires_llm
def test_single_turn(chat_project):
    """R7a: Send one question, verify output appears, send exit.

    Also performs visual verification via pyte: checks that the TUI output
    contains branded styling (teal colors for Seeknal Ask header).
    """
    from tests.e2e.conftest import feed_to_screen

    child = _spawn_chat(chat_project)
    try:
        _wait_for_prompt(child)
        _capture(child)

        response = _send_and_wait(child, "how many rows in the data?")
        _capture(child)
        assert len(response) > 0, (
            f"Expected non-empty response, got empty. "
            f"Raw before: {child.before!r}"
        )

        _exit_chat(child)
        _capture(child)

        # -- Visual verification via pyte --
        full_output = "".join(child._captured_output)
        screen = feed_to_screen(full_output, width=80, height=80)
        display = "\n".join(screen.display)

        # The TUI should have rendered "Seeknal Ask" in the header
        assert "Seeknal" in display or "seeknal" in display.lower(), (
            f"Expected 'Seeknal' in TUI output. Display:\n{display[:500]}"
        )

        # Check that at least some characters have brand teal coloring
        teal_found = False
        for row in range(screen.lines):
            for col in range(screen.columns):
                if screen.buffer[row][col].fg == "00bcb4":
                    teal_found = True
                    break
            if teal_found:
                break
        assert teal_found, "Expected teal (00bcb4) brand color in TUI output"

    except pexpect.TIMEOUT:
        pytest.fail(
            f"Timed out waiting for response. Last output: {child.before!r}"
        )
    finally:
        child.close(force=True)


@pytest.mark.e2e
@requires_llm
def test_multi_turn(chat_project):
    """R7b: Send two questions sequentially, verify both produce output."""
    child = _spawn_chat(chat_project)
    try:
        _wait_for_prompt(child)

        # Turn 1
        r1 = _send_and_wait(child, "how many rows in the data?")
        assert len(r1) > 0, f"Turn 1 empty. Raw: {child.before!r}"

        # Turn 2
        r2 = _send_and_wait(child, "what columns are available?")
        assert len(r2) > 0, f"Turn 2 empty. Raw: {child.before!r}"

        _exit_chat(child)
    except pexpect.TIMEOUT:
        pytest.fail(
            f"Timed out during multi-turn. Last output: {child.before!r}"
        )
    finally:
        child.close(force=True)


@pytest.mark.e2e
@requires_llm
def test_ctrl_c_saves_session(chat_project):
    """R7c: Send Ctrl-C, verify session saved message and metadata file."""
    session_name = _unique_session_name()
    child = _spawn_chat(chat_project, f"--name {session_name}")
    try:
        _wait_for_prompt(child)

        # Send a question so the agent starts working
        child.sendline("describe the data")

        # Give the agent a moment to start responding, then interrupt
        import time
        time.sleep(3)
        child.sendintr()  # SIGINT

        # Wait for the process to exit — expect session saved message or EOF
        try:
            child.expect(pexpect.EOF, timeout=EXIT_TIMEOUT)
        except pexpect.TIMEOUT:
            # Force close if it doesn't exit cleanly
            child.close(force=True)

        # Check output for session saved indication
        full_output = strip_ansi(child.before or "")
        # The CLI prints "Session saved: <name>" on interrupt
        assert (
            "session" in full_output.lower() or session_name in full_output.lower()
        ), f"Expected session save message. Output: {full_output[:500]}"

        # Verify session metadata file was created
        metadata_path = (
            chat_project / ".seeknal" / "sessions" / session_name / "metadata.json"
        )
        assert metadata_path.exists(), (
            f"Session metadata not found at {metadata_path}"
        )
        metadata = json.loads(metadata_path.read_text())
        assert "name" in metadata
        assert metadata["name"] == session_name
    except pexpect.TIMEOUT:
        pytest.fail(
            f"Timed out during Ctrl-C test. Last output: {child.before!r}"
        )
    finally:
        child.close(force=True)


@pytest.mark.e2e
@requires_llm
def test_session_resume(chat_project):
    """R7d: Create named session, exit, resume with --session, verify message."""
    session_name = _unique_session_name()

    # ── First spawn: create session, one turn, exit ──
    child1 = _spawn_chat(chat_project, f"--name {session_name}")
    try:
        _wait_for_prompt(child1)
        _send_and_wait(child1, "how many rows in the data?")
        _exit_chat(child1)
    except pexpect.TIMEOUT:
        pytest.fail(
            f"First session timed out. Last output: {child1.before!r}"
        )
    finally:
        child1.close(force=True)

    # Verify session directory was created
    session_dir = chat_project / ".seeknal" / "sessions" / session_name
    assert session_dir.exists(), f"Session dir not found at {session_dir}"

    # ── Second spawn: resume session ──
    child2 = _spawn_chat(chat_project, f"--session {session_name}")
    try:
        # Should see "Resumed session" message before the prompt
        child2.expect("You: ", timeout=INITIAL_TIMEOUT)
        pre_prompt_output = strip_ansi(child2.before or "")

        assert "resumed" in pre_prompt_output.lower() or "session" in pre_prompt_output.lower(), (
            f"Expected resume message. Output: {pre_prompt_output[:500]}"
        )

        _exit_chat(child2)
    except pexpect.TIMEOUT:
        pytest.fail(
            f"Resume session timed out. Last output: {child2.before!r}"
        )
    finally:
        child2.close(force=True)


@pytest.mark.e2e
@requires_llm
def test_resume_nonexistent_session(chat_project):
    """R7d edge: Resume a session that doesn't exist — expect error and non-zero exit."""
    child = _spawn_chat(chat_project, "--session nonexistent-session-xyz")
    try:
        child.expect(pexpect.EOF, timeout=EXIT_TIMEOUT)
        output = strip_ansi(child.before or "")
        assert "not found" in output.lower(), (
            f"Expected 'not found' error. Output: {output[:500]}"
        )
    except pexpect.TIMEOUT:
        pytest.fail(
            f"Timed out waiting for error. Last output: {child.before!r}"
        )
    finally:
        child.close(force=True)
