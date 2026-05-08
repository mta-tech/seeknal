"""Regression tests for seeknal.cli.gateway."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from seeknal.cli import gateway as gateway_module


def test_asyncio_imported_at_module_level():
    """`asyncio` must be importable from seeknal.cli.gateway's namespace.

    Regression for #60: `_run_http_only_worker` references
    `asyncio.sleep(5)` from its module globals. If asyncio is only
    imported inside other functions, this raises NameError on the retry
    path.
    """
    assert getattr(gateway_module, "asyncio", None) is asyncio


@pytest.mark.asyncio
async def test_http_only_worker_retry_path_does_not_raise_nameerror():
    """The httpx.RequestError retry path must complete without NameError.

    Regression for #60. Drives _run_http_only_worker through one failed
    poll → sleep → re-poll, then stops the loop with a sentinel exception.
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
        patch.object(gateway_module.asyncio, "sleep", _fake_sleep),
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
