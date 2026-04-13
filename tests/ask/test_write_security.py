"""Tests for seeknal.ask.agents.tools._write_security.

Covers the draft path validator and the ``.seeknal/drafts/`` scoping rules
introduced to stop the agent dumping ``draft_semantic_model_*.yml`` files
at the project root.
"""


import pytest

from seeknal.ask.agents.tools._write_security import (
    DRAFTS_SUBDIR,
    get_drafts_dir,
    validate_draft_path,
    validate_write_path,
)


class TestGetDraftsDir:
    def test_creates_dir_if_missing(self, tmp_path):
        drafts = get_drafts_dir(tmp_path)
        assert drafts.is_dir()
        assert drafts.relative_to(tmp_path) == DRAFTS_SUBDIR

    def test_idempotent(self, tmp_path):
        d1 = get_drafts_dir(tmp_path)
        d2 = get_drafts_dir(tmp_path)
        assert d1 == d2 and d1.is_dir()


class TestValidateDraftPath:
    """New drafts live under .seeknal/drafts/; root-level paths are legacy."""

    def test_resolves_to_canonical_drafts_dir_for_new_drafts(self, tmp_path):
        drafts = get_drafts_dir(tmp_path)
        target = drafts / "draft_source_customers.yml"
        target.write_text("kind: source\nname: customers\n")

        resolved = validate_draft_path("draft_source_customers.yml", tmp_path)
        assert resolved == target.resolve()

    def test_falls_back_to_project_root_for_legacy_drafts(self, tmp_path):
        legacy = tmp_path / "draft_source_legacy.yml"
        legacy.write_text("kind: source\nname: legacy\n")

        resolved = validate_draft_path("draft_source_legacy.yml", tmp_path)
        assert resolved == legacy.resolve()

    def test_prefers_canonical_when_both_exist(self, tmp_path):
        legacy = tmp_path / "draft_source_dup.yml"
        legacy.write_text("legacy")
        drafts = get_drafts_dir(tmp_path)
        canonical = drafts / "draft_source_dup.yml"
        canonical.write_text("canonical")

        resolved = validate_draft_path("draft_source_dup.yml", tmp_path)
        assert resolved == canonical.resolve()

    def test_rejects_invalid_draft_filename(self, tmp_path):
        with pytest.raises(ValueError, match="Invalid draft filename"):
            validate_draft_path("evil.sh", tmp_path)

    def test_rejects_path_traversal(self, tmp_path):
        with pytest.raises(ValueError):
            validate_draft_path("../../etc/passwd", tmp_path)


class TestWritePathSeekdnalScope:
    """.seeknal/ writes are restricted to .seeknal/drafts/<draft>."""

    def test_allows_write_to_seeknal_drafts(self, tmp_path):
        resolved = validate_write_path(
            ".seeknal/drafts/draft_transform_foo.yml", tmp_path
        )
        assert resolved == (tmp_path / ".seeknal" / "drafts" / "draft_transform_foo.yml").resolve()

    def test_blocks_write_to_seeknal_plans(self, tmp_path):
        with pytest.raises(ValueError, match="drafts/"):
            validate_write_path(".seeknal/plans/foo.md", tmp_path)

    def test_blocks_write_to_seeknal_checkpoints(self, tmp_path):
        with pytest.raises(ValueError, match="drafts/"):
            validate_write_path(".seeknal/checkpoints/abc.json", tmp_path)

    def test_blocks_nondraft_filename_in_seeknal_drafts(self, tmp_path):
        with pytest.raises(ValueError, match="Only draft files"):
            validate_write_path(".seeknal/drafts/evil.sh", tmp_path)


class TestBootstrapSemanticModelWritesToCanonicalDir:
    """Regression: bootstrap used to dump 13+ drafts flat in project root."""

    def test_draft_written_under_seeknal_drafts(self, tmp_path):
        # Simulate what bootstrap_semantic_model does: get the drafts dir
        # and write there. This is the behaviour the production tool now
        # exercises after the fix.
        drafts_dir = get_drafts_dir(tmp_path)
        draft = drafts_dir / "draft_semantic_model_orders.yml"
        draft.write_text("kind: semantic_model\nname: orders\n")

        # The file is NOT at the project root
        assert not (tmp_path / "draft_semantic_model_orders.yml").exists()
        # The file IS under the canonical drafts subdir
        assert draft.exists()
        assert draft.parent.relative_to(tmp_path) == DRAFTS_SUBDIR

        # And the validator routes bare filenames to it
        resolved = validate_draft_path("draft_semantic_model_orders.yml", tmp_path)
        assert resolved == draft.resolve()
