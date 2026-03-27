"""Tests for EventSink protocol and implementations."""

import asyncio

from seeknal.ask.event_sink import CollectorSink, EventSink, NullSink


class TestProtocol:
    def test_collector_sink_is_event_sink(self):
        assert isinstance(CollectorSink(), EventSink)

    def test_null_sink_is_event_sink(self):
        assert isinstance(NullSink(), EventSink)


class TestCollectorSink:
    def test_collects_tokens(self):
        async def _run():
            sink = CollectorSink()
            await sink.on_token("Hello")
            await sink.on_token(" world")
            assert sink.tokens == ["Hello", " world"]

        asyncio.run(_run())

    def test_collects_tool_events(self):
        async def _run():
            sink = CollectorSink()
            await sink.on_tool_start("execute_sql", {"sql": "SELECT 1"})
            await sink.on_tool_end("execute_sql", "| 1 |")
            assert sink.tool_starts == [("execute_sql", {"sql": "SELECT 1"})]
            assert sink.tool_ends == [("execute_sql", "| 1 |")]

        asyncio.run(_run())

    def test_collects_answers(self):
        async def _run():
            sink = CollectorSink()
            await sink.on_answer("The answer is 42")
            assert sink.answers == ["The answer is 42"]

        asyncio.run(_run())

    def test_collects_errors(self):
        async def _run():
            sink = CollectorSink()
            await sink.on_error("Something went wrong")
            assert sink.errors == ["Something went wrong"]

        asyncio.run(_run())

    def test_events_in_order(self):
        async def _run():
            sink = CollectorSink()
            await sink.on_token("Thinking...")
            await sink.on_tool_start("execute_sql", {"sql": "SELECT 1"})
            await sink.on_tool_end("execute_sql", "result")
            await sink.on_answer("Done")

            assert len(sink.tokens) == 1
            assert len(sink.tool_starts) == 1
            assert len(sink.tool_ends) == 1
            assert len(sink.answers) == 1

        asyncio.run(_run())


class TestNullSink:
    def test_discards_all_events(self):
        async def _run():
            sink = NullSink()
            await sink.on_token("test")
            await sink.on_tool_start("test", {})
            await sink.on_tool_end("test", "result")
            await sink.on_answer("answer")
            await sink.on_error("error")
            # No exceptions, no state

        asyncio.run(_run())
