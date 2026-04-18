from __future__ import annotations

from typer.testing import CliRunner

from seeknal.cli.session import session_app


runner = CliRunner()


def test_session_pair_generates_code_for_existing_session(tmp_path):
    from seeknal.ask.sessions import SessionStore

    with SessionStore(tmp_path) as store:
        store.create("existing-session")

    result = runner.invoke(
        session_app,
        ["pair", "existing-session", "--project", str(tmp_path)],
    )

    assert result.exit_code == 0
    assert "Pair code:" in result.output
    assert "/pair " in result.output


def test_session_pair_rejects_missing_session(tmp_path):
    result = runner.invoke(
        session_app,
        ["pair", "missing-session", "--project", str(tmp_path)],
    )

    assert result.exit_code == 1
    assert "not found" in result.output.lower()


def test_session_paired_lists_device_ids(tmp_path):
    from seeknal.ask.gateway.pairing import TelegramLinkStore

    link_store = TelegramLinkStore(tmp_path)
    link_store.link_chat("12345", "existing-session")

    result = runner.invoke(
        session_app,
        ["paired", "--project", str(tmp_path)],
    )

    assert result.exit_code == 0
    assert "Device ID" in result.output
    assert "12345" in result.output
    assert "existing-session" in result.output


def test_session_paired_filters_by_session(tmp_path):
    from seeknal.ask.gateway.pairing import TelegramLinkStore

    link_store = TelegramLinkStore(tmp_path)
    link_store.link_chat("12345", "session-a")
    link_store.link_chat("67890", "session-b")

    result = runner.invoke(
        session_app,
        ["paired", "--project", str(tmp_path), "--session", "session-a"],
    )

    assert result.exit_code == 0
    assert "12345" in result.output
    assert "session-a" in result.output
    assert "67890" not in result.output


def test_session_public_set_show_and_clear(tmp_path):
    from seeknal.ask.sessions import SessionStore

    with SessionStore(tmp_path) as store:
        store.create("public-session")

    set_result = runner.invoke(
        session_app,
        ["public", "set", "public-session", "--project", str(tmp_path)],
    )
    assert set_result.exit_code == 0
    assert "public-session" in set_result.output

    show_result = runner.invoke(
        session_app,
        ["public", "show", "--project", str(tmp_path)],
    )
    assert show_result.exit_code == 0
    assert "public-session" in show_result.output

    clear_result = runner.invoke(
        session_app,
        ["public", "clear", "--project", str(tmp_path)],
    )
    assert clear_result.exit_code == 0
    assert "Cleared" in clear_result.output
