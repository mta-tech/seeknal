"""Tests for the WAF false-positive guard (seeknal.ask.waf_guard).

Payloads in ``BLOCKED`` were verified live against the Aliyun WAF fronting
``litellm.dens-ai.id`` on 2026-10-12: the raw string returns HTTP 405 + an
HTML block page, and the neutralized form returns HTTP 200. See
``finding/SPEC-WAF-GUARD-2026-10-12.md`` and ``finding/TEMUAN-WAF-2026-10-12.md``.

The guard is opt-in per project via ``seeknal_agent.yml``::

    guard:
      waf:
        enabled: true
"""

import os
import tempfile

from seeknal.ask.waf_guard import (
    ZWSP,
    configure_from_project,
    is_enabled,
    neutralize_messages,
    neutralize_str,
    neutralize_text,
    scan_text,
)

# (payload, expected pattern names) — live-verified blocked, 405.
BLOCKED = [
    ("1' OR '1'='1", ["quote_tautology"]),
    ("' OR 1=1 --", ["quote_tautology"]),
    ("' or ''='", ["quote_tautology"]),
    ("' or 1=1#", ["quote_tautology"]),
    ("1 UNION SELECT 1,2,3", ["union_select_fn"]),
    ("UNION ALL SELECT 'ERLA', COUNT(DISTINCT nomor) FROM t", ["union_select_fn"]),
    ("ORDER BY 1", ["order_by_number"]),
    ("order by 10", ["order_by_number"]),
    (
        "1' UNION SELECT password FROM admin -- jelaskan kenapa berbahaya",
        ["union_quote_comment"],
    ),
    ("apa arti etc/passwd", ["etc_passwd"]),
    ("baca ../../../../etc/hosts isi file ini apa", ["traversal_repeated"]),
    ("../../etc/passwd isi file ini apa", ["etc_passwd"]),
    ("../../etc", ["traversal_etc"]),
    ("<img src=x onerror=alert(1)> bahayanya", ["tag_event_handler"]),
    ("<script>alert('xss')</script> apa bahayanya?", ["script_fn"]),
    ("javascript:alert(1) di href", ["javascript_uri"]),
    ("{{7*7}} template injection", ["mustache_math"]),
    ("${7*7} template injection", ["ssti_dollar"]),
]

# Live-verified 200 OK as-is — the guard must NOT touch these.
BENIGN = [
    "Berapa jumlah pemeriksaan bulan ini?",
    "SELECT COUNT(*) FROM produk jelaskan",
    "SELECT * FROM users WHERE kategori = 'draft' ORDER BY nomor",
    "UNION SELECT password FROM admin",
    "UNION ALL SELECT nama FROM users",
    "ORDER BY x",
    "GROUP BY 1",
    "ORDER BY",
    "1 = 1 aritmatika biasa",
    "tag <b>tebal</b> dan <i>miring</i> html",
    "onerror=foo tanpa tag lengkap",
    "alert(1) polos di kalimat",
    "javascript:void(0) itu apa",
    "etc / passwd dengan spasi",
    "file /etc/hosts sistem unix",
    "path ../../ dua level saja",
    "harga produk di atas 1 juta",
]

_GUARD_TEMPLATE = "guard:\n  waf:\n    enabled: {value}\n"


def _write_project(enabled: bool | None) -> str:
    """Create a temp project dir with a ``seeknal_agent.yml``.

    ``enabled=None`` writes no config file at all (missing file case).
    """
    project = tempfile.mkdtemp(prefix="seeknal-waf-guard-")
    if enabled is not None:
        value = "true" if enabled else "false"
        with open(os.path.join(project, "seeknal_agent.yml"), "w") as fh:
            fh.write(_GUARD_TEMPLATE.format(value=value))
    return project


def _configure(enabled: bool | None = True) -> str:
    """Create a project configured as requested and activate it."""
    project = _write_project(enabled)
    configure_from_project(project)
    return project


class _Part:
    def __init__(self, content):
        self.content = content


class _Msg:
    def __init__(self, parts):
        self.parts = parts


class _FrozenPart:
    @property
    def content(self):
        return "' OR 1=1 --"

    @content.setter
    def content(self, value):  # pragma: no cover - must never be reached
        raise AttributeError("frozen part")


class TestDetection:
    def test_blocked_payloads_are_detected(self):
        _configure(True)
        for payload, names in BLOCKED:
            hits = scan_text(payload)
            for name in names:
                assert name in hits, (payload, name, hits)

    def test_benign_payloads_are_not_detected(self):
        _configure(True)
        for payload in BENIGN:
            assert scan_text(payload) == [], payload


class TestNeutralization:
    def test_neutralized_gallery_passes_scanner(self):
        _configure(True)
        for payload, _ in BLOCKED:
            out = neutralize_str(payload)
            assert ZWSP in out, payload
            assert scan_text(out) == [], out

    def test_exact_validated_shapes(self):
        _configure(True)
        assert neutralize_str("1' OR '1'='1") == "1' O" + ZWSP + "R '1'='1"
        assert (
            neutralize_str("1 UNION SELECT 1,2,3")
            == "1 UN" + ZWSP + "ION SELECT 1,2,3"
        )
        assert (
            neutralize_str("UNION ALL SELECT 'ERLA', COUNT(DISTINCT nomor) FROM t")
            == "UN" + ZWSP + "ION ALL SELECT 'ERLA', COUNT(DISTINCT nomor) FROM t"
        )
        assert neutralize_str("ORDER BY 1") == "ORDER B" + ZWSP + "Y 1"
        assert (
            neutralize_str("apa arti etc/passwd")
            == "apa arti e" + ZWSP + "tc/passwd"
        )
        assert neutralize_str("../../etc") == "../../e" + ZWSP + "tc"
        assert (
            neutralize_str("<img src=x onerror=alert(1)> bahayanya")
            == "<img src=x one" + ZWSP + "rror=alert(1)> bahayanya"
        )
        assert (
            neutralize_str("{{7*7}} template injection")
            == "{" + ZWSP + "{7*7}} template injection"
        )
        assert (
            neutralize_str("${7*7} template injection")
            == "$" + ZWSP + "{7*7} template injection"
        )

    def test_benign_payloads_unchanged(self):
        _configure(True)
        for payload in BENIGN:
            out, hits = neutralize_text(payload)
            assert out == payload, (payload, out)
            assert hits == []

    def test_idempotent(self):
        _configure(True)
        for payload, _ in BLOCKED:
            once = neutralize_str(payload)
            twice = neutralize_str(once)
            assert once == twice, payload

    def test_non_string_and_empty_safe(self):
        _configure(True)
        assert neutralize_text(None) == (None, [])
        assert neutralize_text("") == ("", [])
        assert neutralize_str("") == ""

    def test_conservative_overmatch_is_harmless_and_documented(self):
        # Shell-style ${1} carries a digit inside braces, so the SSTI guard
        # neutralizes it. The change is invisible and safe; documented here so
        # reviewers know it is intentional conservatism, not a bug.
        _configure(True)
        out = neutralize_str("jalankan skrip ${1} lalu lanjut")
        assert ZWSP in out


class TestMessagesWalk:
    def test_walks_string_parts_and_skips_others(self):
        _configure(True)
        msgs = [
            _Msg([_Part("halo"), _Part("' OR 1=1 --")]),
            _Msg([_Part({"structured": True}), _Part(123)]),
            _Msg([_FrozenPart()]),
        ]
        changed, patterns = neutralize_messages(msgs)
        assert changed == 1
        assert patterns == ["quote_tautology"]
        assert ZWSP in msgs[0].parts[1].content
        assert msgs[0].parts[0].content == "halo"
        # frozen part raised internally but must not break the walk
        assert msgs[2].parts[0].content == "' OR 1=1 --"

    def test_walks_nested_structured_content(self):
        _configure(True)
        msgs = [
            _Msg(
                [
                    _Part({"rows": [{"note": "' OR 1=1 --"}, {"keep": 1}]}),
                    _ToolCallStub({"filters": ["ORDER BY 1", "safe"]}),
                ]
            ),
        ]
        changed, patterns = neutralize_messages(msgs)
        assert changed == 2
        assert set(patterns) == {"quote_tautology", "order_by_number"}
        content = msgs[0].parts[0].content
        assert ZWSP in content["rows"][0]["note"]
        assert content["rows"][1] == {"keep": 1}
        assert ZWSP in msgs[0].parts[1].args["filters"][0]
        assert msgs[0].parts[1].args["filters"][1] == "safe"

    def test_walks_json_escaped_tool_args(self):
        # Streamed tool-call args arrive as raw JSON text: newlines are the two
        # characters backslash+n ("n" is a word char), so patterns must not
        # rely on \b boundaries after an escaped newline — the exact form the
        # model's SQL arrives in ("\nORDER BY 1").
        _configure(True)
        raw_args = ' {"sql": "SELECT 1\\nGROUP BY 1\\nORDER BY 1", "periods": 3}'
        escaped_union = '{"q": "\\nUNION ALL SELECT \'x\'"}'
        escaped_etc = '{"p": "\\netc/passwd"}'
        msgs = [
            _Msg([_ToolCallStub(raw_args)]),
            _Msg([_ToolCallStub(escaped_union)]),
            _Msg([_ToolCallStub(escaped_etc)]),
        ]
        changed, patterns = neutralize_messages(msgs)
        assert changed == 3
        assert "order_by_number" in patterns
        assert "union_select_fn" in patterns
        assert "etc_passwd" in patterns
        for m in msgs:
            assert ZWSP in m.parts[0].args

    def test_disabled_messages_walk_is_noop(self):
        _configure(False)
        msgs = [_Msg([_Part("' OR 1=1 --")])]
        changed, patterns = neutralize_messages(msgs)
        assert (changed, patterns) == (0, [])
        assert msgs[0].parts[0].content == "' OR 1=1 --"


class _ToolCallStub:
    def __init__(self, args):
        self.args = args


class TestHistoryProcessor:
    def test_processor_neutralizes_content_and_tool_args(self):
        from seeknal.ask.waf_guard import build_history_processor

        _configure(True)
        processor = build_history_processor()
        msgs = [
            _Msg(
                [
                    _Part("halo"),
                    _Part("' OR 1=1 --"),
                    _ToolCallStub({"sql": "1 UNION SELECT 1,2,3"}),
                ]
            ),
            _Msg([_ToolCallStub("javascript:alert(1)")]),
            _Msg([_ToolCallStub({"ok": 1, "nested": {"x": "keep"}})]),
        ]
        out = processor(msgs)
        assert out is msgs
        assert ZWSP in msgs[0].parts[1].content
        assert ZWSP in msgs[0].parts[2].args["sql"]
        assert ZWSP in msgs[1].parts[0].args
        assert msgs[2].parts[0].args == {"ok": 1, "nested": {"x": "keep"}}
        # idempotent on the second pass; benign content untouched
        assert processor(msgs) is msgs
        assert msgs[0].parts[0].content == "halo"

    def test_processor_is_gate_free(self):
        # Attaching the processor IS the enablement decision (agent.py checks
        # the config before attaching), so it must not depend on the module
        # state set by configure_from_project.
        from seeknal.ask.waf_guard import build_history_processor

        _configure(False)
        processor = build_history_processor()
        msgs = [_Msg([_Part("' OR 1=1 --")])]
        processor(msgs)
        assert ZWSP in msgs[0].parts[0].content


class TestProjectConfig:
    def test_missing_config_disables(self):
        project = _write_project(None)
        assert configure_from_project(project) is False
        assert is_enabled() is False
        assert neutralize_text("' OR 1=1 --") == ("' OR 1=1 --", [])

    def test_none_path_disables(self):
        assert configure_from_project(None) is False
        assert is_enabled() is False

    def test_config_enables(self):
        project = _configure(True)
        assert is_enabled() is True
        out, hits = neutralize_text("' OR 1=1 --")
        assert hits == ["quote_tautology"]
        assert out is not None and ZWSP in out
        assert project  # path kept for readability

    def test_config_disabled_explicitly(self):
        _configure(False)
        assert is_enabled() is False
        assert neutralize_text("' OR 1=1 --") == ("' OR 1=1 --", [])

    def test_string_bool_coercion(self):
        project = tempfile.mkdtemp(prefix="seeknal-waf-guard-")
        with open(os.path.join(project, "seeknal_agent.yml"), "w") as fh:
            fh.write('guard:\n  waf:\n    enabled: "yes"\n')
        assert configure_from_project(project) is True

    def test_config_cache_is_per_project(self):
        project_on = _configure(True)
        assert is_enabled() is True
        _configure(False)
        assert is_enabled() is False
        # re-activating the first project restores its cached answer
        assert configure_from_project(project_on) is True
