"""Tests for the prompt builder section registry."""

import pytest

from seeknal.ask.prompt_builder import (
    PROMPT_DYNAMIC_BOUNDARY,
    PromptBuilder,
    PromptSection,
    PromptTier,
    create_default_builder,
    _build_identity,
    _build_asking_questions,
    _build_workflow,
    _build_memory,
    _build_locale,
    _build_agent_profile,
    _build_source_registry,
    _build_environment,
)


class TestPromptBuilder:
    def test_empty_builder_returns_empty(self):
        builder = PromptBuilder()
        assert builder.build() == ""

    def test_static_sections_come_before_dynamic(self):
        builder = PromptBuilder()
        builder.register(PromptSection(
            id="dynamic1", tier=PromptTier.DYNAMIC, priority=10,
            builder=lambda **kw: "dynamic",
        ))
        builder.register(PromptSection(
            id="static1", tier=PromptTier.STATIC, priority=20,
            builder=lambda **kw: "static",
        ))
        result = builder.build()
        static_pos = result.index("static")
        boundary_pos = result.index("PROMPT_DYNAMIC_BOUNDARY")
        dynamic_pos = result.index("dynamic")
        assert static_pos < boundary_pos < dynamic_pos

    def test_sections_ordered_by_priority(self):
        builder = PromptBuilder()
        builder.register(PromptSection(
            id="c", tier=PromptTier.STATIC, priority=30,
            builder=lambda **kw: "third",
        ))
        builder.register(PromptSection(
            id="a", tier=PromptTier.STATIC, priority=10,
            builder=lambda **kw: "first",
        ))
        builder.register(PromptSection(
            id="b", tier=PromptTier.STATIC, priority=20,
            builder=lambda **kw: "second",
        ))
        result = builder.build()
        assert result.index("first") < result.index("second") < result.index("third")

    def test_none_builder_skipped(self):
        builder = PromptBuilder()
        builder.register(PromptSection(
            id="present", tier=PromptTier.STATIC, priority=10,
            builder=lambda **kw: "hello",
        ))
        builder.register(PromptSection(
            id="absent", tier=PromptTier.STATIC, priority=20,
            builder=lambda **kw: None,
        ))
        result = builder.build()
        assert "hello" in result
        assert PROMPT_DYNAMIC_BOUNDARY not in result  # no dynamic sections

    def test_boundary_only_when_dynamic_exists(self):
        builder = PromptBuilder()
        builder.register(PromptSection(
            id="s", tier=PromptTier.STATIC, priority=10,
            builder=lambda **kw: "static only",
        ))
        result = builder.build()
        assert PROMPT_DYNAMIC_BOUNDARY not in result

    def test_override_false_disables_section(self):
        builder = PromptBuilder()
        builder.register(PromptSection(
            id="removable", tier=PromptTier.STATIC, priority=10,
            builder=lambda **kw: "should not appear",
        ))
        result = builder.build(config={"prompt": {"removable": False}})
        assert "should not appear" not in result

    def test_override_string_replaces_section(self):
        builder = PromptBuilder()
        builder.register(PromptSection(
            id="replaceable", tier=PromptTier.STATIC, priority=10,
            builder=lambda **kw: "original",
        ))
        result = builder.build(config={"prompt": {"replaceable": "replaced"}})
        assert "replaced" in result
        assert "original" not in result

    def test_custom_override_appended(self):
        builder = PromptBuilder()
        builder.register(PromptSection(
            id="base", tier=PromptTier.STATIC, priority=10,
            builder=lambda **kw: "base content",
        ))
        result = builder.build(config={"prompt": {"custom": "custom rules"}})
        assert "base content" in result
        assert "custom rules" in result

    def test_environment_kwarg_passed_to_builders(self):
        received = {}

        def capture_env(**kwargs):
            received["env"] = kwargs.get("environment")
            return "ok"

        builder = PromptBuilder()
        builder.register(PromptSection(
            id="test", tier=PromptTier.STATIC, priority=10,
            builder=capture_env,
        ))
        builder.build(environment="telegram")
        assert received["env"] == "telegram"


class TestSectionBuilders:
    def test_identity_returns_content(self):
        result = _build_identity()
        assert "Seeknal Ask" in result
        assert "senior data analyst" in result

    def test_asking_questions_returns_for_interactive(self):
        result = _build_asking_questions(environment="interactive")
        assert result is not None
        assert "ask_user" in result

    def test_asking_questions_none_for_gateway(self):
        assert _build_asking_questions(environment="gateway") is None

    def test_asking_questions_none_for_telegram(self):
        assert _build_asking_questions(environment="telegram") is None

    def test_asking_questions_none_for_exposure(self):
        assert _build_asking_questions(environment="exposure") is None

    def test_workflow_returns_content(self):
        result = _build_workflow()
        # v2: workflow prose moved into builtin_skills/. The remaining
        # workflow section is a thin pointer + discriminator quick reference.
        assert "Workflow" in result
        assert "load_skill" in result
        assert "Generate report now" in result
        assert "Publish to Seeknal Report Server" in result

    def test_workflow_includes_temporal_scope_hygiene(self):
        """A regression test for the Q3 'dan seterusnya' bug.

        Without this rule the agent reused a prior turn's cached number
        ('jumlah NIE tahun 2024 = 50') as the answer for 'jumlah NIE dari
        tahun 2024 dan seterusnya?', missing 2025 and 2026 data. The
        workflow prompt must teach the model to re-query when the follow-up
        question expands the temporal scope.
        """
        result = _build_workflow()
        assert "Temporal-scope hygiene" in result
        # Indonesian scope-expansion triggers
        assert "dan seterusnya" in result
        assert "sampai sekarang" in result
        # English scope-expansion triggers
        assert "onwards" in result
        assert "to date" in result
        # The recovery procedure must be present too — recognizing the
        # signal is useless without the corrective action.
        assert "fresh `execute_sql`" in result or "fresh execute_sql" in result
        assert "MAX(" in result

    def test_workflow_includes_multi_turn_hygiene(self):
        """Regression test for cross-turn answer drift.

        Three weaknesses observed in the 10-scenario reliability test:

        1. Verbatim re-ask burned an extra tool call because the agent
           paraphrased its own SQL (cache miss).
        2. Status filter set drifted across turns (e.g. ERLA `'0099'`
           sometimes included, sometimes not).
        3. Date column choice drifted (`tanggal` vs `tanggal_aju`).

        All three are agent-reasoning issues solvable at the prompt level
        without domain-specific keywords. The rule must reference generic
        concepts (date column, filter values, predicate set) so it applies
        to any project — not just BPOM.
        """
        result = _build_workflow()
        assert "Multi-turn hygiene" in result
        # Generic concepts (no domain-specific tokens like status codes,
        # table names, or project identifiers).
        assert "date column" in result
        assert "filter values" in result
        assert "predicate set" in result
        # The two corrective actions: keep filters stable, restate verbatim
        # re-asks instead of re-querying.
        assert "verbatim" in result
        assert "restate" in result.lower()

    def test_workflow_multi_turn_hygiene_has_no_hardcoded_domain_terms(self):
        """The rule must not reference any project-specific tokens.

        Guard against future edits sneaking in BPOM / Seeknal-specific
        keywords (status codes, table names, business categories) that
        would silently narrow the rule's applicability.
        """
        result = _build_workflow()
        # The multi-turn hygiene rule lives on a single source line; isolate
        # it and check no hardcoded BPOM / domain tokens leaked in.
        line = next(
            (ln for ln in result.splitlines() if "Multi-turn hygiene" in ln),
            "",
        )
        assert line, "Multi-turn hygiene rule must be present"
        forbidden = [
            # BPOM-specific
            "NIE",
            "ERBA",
            "ERLA",
            "0999",
            "0906",
            "9999",
            "0099",
            "kategori_dokumen",
            "jenis_permohonan",
            "tanggal_aju",
            # Seeknal table names
            "t_produk",
            "warehouse.public",
        ]
        for token in forbidden:
            assert token not in line, (
                f"Hardcoded domain term {token!r} leaked into the "
                f"multi-turn hygiene rule. Keep the rule generic so it "
                f"applies to any project."
            )

    def test_memory_returns_content(self):
        result = _build_memory()
        assert "persistent memory" in result

    def test_locale_none_without_config(self):
        assert _build_locale(config=None) is None
        assert _build_locale(config={}) is None

    def test_locale_returns_content_with_config(self):
        config = {"locale": {"currency": "IDR", "currency_symbol": "Rp"}}
        result = _build_locale(config=config)
        assert result is not None
        assert "IDR" in result

    def test_agent_profile_returns_content_with_agent_config(self):
        config = {
            "agent": {
                "name": "CBN Competitive Intelligence",
                "skills": ["competitive_intel"],
                "system_prompt": "Focus on actionable threats.",
            }
        }
        result = _build_agent_profile(config=config)
        assert result is not None
        assert "CBN Competitive Intelligence" in result
        assert "competitive_intel" in result
        assert "Focus on actionable threats." in result

    def test_source_registry_none_without_config(self):
        assert _build_source_registry(config=None) is None
        assert _build_source_registry(config={}) is None

    def test_source_registry_returns_content_with_sources(self):
        config = {
            "sources": {
                "warehouse": {
                    "source_kind": "connected",
                    "source_type": "database",
                    "connector": "postgresql",
                    "namespace": "wh",
                    "access": "read_only",
                    "role": "business_source_of_truth",
                    "description": "Analytics warehouse",
                }
            }
        }
        result = _build_source_registry(config=config)
        assert result is not None
        assert "Data Sources & Mode Policy" in result
        assert "warehouse" in result
        assert "read_only" in result

    def test_environment_interactive(self):
        result = _build_environment(environment="interactive")
        assert "Interactive terminal" in result

    def test_environment_gateway(self):
        result = _build_environment(environment="gateway")
        assert "API gateway" in result
        assert "Do NOT use ask_user" in result

    def test_environment_telegram(self):
        result = _build_environment(environment="telegram")
        assert "Telegram" in result

    def test_environment_exposure(self):
        result = _build_environment(environment="exposure")
        assert "Report re-run" in result
        assert "Do NOT use ask_user" in result

    def test_environment_includes_date(self):
        from datetime import datetime
        result = _build_environment(environment="interactive")
        assert datetime.now().strftime("%Y-%m-%d") in result


class TestDefaultBuilder:
    def test_creates_builder_with_all_sections(self):
        builder = create_default_builder()
        assert len(builder._sections) == 8

    def test_interactive_includes_asking_questions(self):
        builder = create_default_builder()
        result = builder.build(environment="interactive")
        assert "ask_user" in result
        assert "When to ask" in result

    def test_gateway_excludes_asking_questions(self):
        builder = create_default_builder()
        result = builder.build(environment="gateway")
        assert "When to ask" not in result
        assert "API gateway" in result

    def test_contains_boundary_marker(self):
        builder = create_default_builder()
        result = builder.build(environment="interactive")
        assert "PROMPT_DYNAMIC_BOUNDARY" in result

    def test_identity_before_workflow(self):
        builder = create_default_builder()
        result = builder.build()
        # v2: workflow header is now "## Workflow" not "Pipeline Building"
        assert result.index("Seeknal Ask") < result.index("## Workflow")

    def test_memory_section_after_boundary(self):
        builder = create_default_builder()
        result = builder.build()
        boundary_pos = result.index("PROMPT_DYNAMIC_BOUNDARY")
        # The memory section starts with "## Memory" — find the last
        # occurrence which is the dedicated section, not inline mentions.
        memory_section_pos = result.rindex("## Memory")
        assert memory_section_pos > boundary_pos

    def test_agent_profile_section_included_when_configured(self):
        builder = create_default_builder()
        result = builder.build(
            config={
                "agent": {
                    "name": "Custom Analyst",
                    "system_prompt": "Prioritize pricing changes.",
                }
            }
        )
        assert "Custom Analyst" in result
        assert "Prioritize pricing changes." in result
