"""Regression tests for seeknal.cli.gateway."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from seeknal.cli import gateway as gateway_module


@pytest.mark.asyncio
async def test_http_only_worker_retry_path_does_not_raise_nameerror():
    """The httpx.RequestError retry path must complete without NameError.

    Regression for #60: `_run_http_only_worker` is async-def at module
    scope and calls `await asyncio.sleep(5)` in its retry branch. If
    `asyncio` is not in scope (neither module-level nor function-local),
    the retry path raises NameError on the first transient gateway
    error. This drives one ConnectError → sleep → re-poll cycle and
    asserts no NameError, regardless of whether the import is at module
    or function scope.
    """

    class _StopLoop(Exception):
        pass

    class _FakeClient:
        def __init__(self):
            self.calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                raise httpx.ConnectError("simulated gateway down")
            raise _StopLoop()

        async def post(self, *_args, **_kwargs):  # pragma: no cover - unused
            raise AssertionError("post() must not be called on retry path")

    sleep_calls: list[float] = []

    async def _fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    with (
        patch("asyncio.sleep", _fake_sleep),
        patch("httpx.AsyncClient", return_value=_FakeClient()),
    ):
        with pytest.raises(_StopLoop):
            await gateway_module._run_http_only_worker(
                project_path=Path("/tmp/does-not-matter"),
                gateway_url="http://example.invalid",
                api_token="dummy",
                poll_timeout=1.0,
            )

    # The retry branch must have run exactly once and slept for 5s.
    assert sleep_calls == [5]
