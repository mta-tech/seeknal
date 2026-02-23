"""Tests for NodeFingerprint computation and DAG fingerprinting."""

import pytest
from pathlib import Path

from seeknal.workflow.state import (
    NodeFingerprint,
    compute_node_fingerprint,
    compute_dag_fingerprints,
)


class TestNodeFingerprint:
    """Test NodeFingerprint dataclass."""

    def test_combined_hash_deterministic(self):
        fp = NodeFingerprint(content_hash="a", schema_hash="b", upstream_hash="c", config_hash="d")
        assert fp.combined == fp.combined  # same every time

    def test_different_fingerprints_different_combined(self):
        fp1 = NodeFingerprint(content_hash="a", schema_hash="b", upstream_hash="c", config_hash="d")
        fp2 = NodeFingerprint(content_hash="x", schema_hash="b", upstream_hash="c", config_hash="d")
        assert fp1.combined != fp2.combined

    def test_serialization_roundtrip(self):
        fp = NodeFingerprint(content_hash="abc", schema_hash="def", upstream_hash="ghi", config_hash="jkl")
        d = fp.to_dict()
        restored = NodeFingerprint.from_dict(d)
        assert restored.content_hash == "abc"
        assert restored.schema_hash == "def"
        assert restored.upstream_hash == "ghi"
        assert restored.config_hash == "jkl"

    def test_from_dict_with_none(self):
        fp = NodeFingerprint.from_dict(None)
        assert fp.content_hash == ""

    def test_from_dict_with_empty(self):
        fp = NodeFingerprint.from_dict({})
        assert fp.content_hash == ""


class TestComputeNodeFingerprint:
    """Test compute_node_fingerprint function."""

    def test_basic_transform(self):
        data = {"kind": "transform", "transform": "SELECT * FROM t1"}
        fp = compute_node_fingerprint(data, Path("t.yml"))
        assert len(fp.content_hash) == 64
        assert len(fp.schema_hash) == 64
        assert len(fp.upstream_hash) == 64
        assert len(fp.config_hash) == 64

    def test_same_content_same_fingerprint(self):
        data = {"kind": "transform", "transform": "SELECT 1"}
        fp1 = compute_node_fingerprint(data, Path("t.yml"))
        fp2 = compute_node_fingerprint(data, Path("t.yml"))
        assert fp1.combined == fp2.combined

    def test_different_content_different_fingerprint(self):
        d1 = {"kind": "transform", "transform": "SELECT 1"}
        d2 = {"kind": "transform", "transform": "SELECT 2"}
        fp1 = compute_node_fingerprint(d1, Path("t.yml"))
        fp2 = compute_node_fingerprint(d2, Path("t.yml"))
        assert fp1.content_hash != fp2.content_hash
        assert fp1.combined != fp2.combined

    def test_upstream_changes_propagate(self):
        data = {"kind": "transform", "transform": "SELECT 1"}
        fp_no_up = compute_node_fingerprint(data, Path("t.yml"))
        upstream_fp = NodeFingerprint(content_hash="upstream_changed")
        fp_with_up = compute_node_fingerprint(data, Path("t.yml"), {"s1": upstream_fp})
        assert fp_no_up.upstream_hash != fp_with_up.upstream_hash
        assert fp_no_up.combined != fp_with_up.combined

    def test_schema_hash_from_columns(self):
        d1 = {"kind": "transform", "transform": "SELECT 1", "columns": {"a": "int", "b": "string"}}
        d2 = {"kind": "transform", "transform": "SELECT 1", "columns": {"a": "int", "c": "float"}}
        fp1 = compute_node_fingerprint(d1, Path("t.yml"))
        fp2 = compute_node_fingerprint(d2, Path("t.yml"))
        assert fp1.schema_hash != fp2.schema_hash

    def test_config_hash_from_materialization(self):
        d1 = {"kind": "source", "params": {}, "materialization": {"format": "parquet"}}
        d2 = {"kind": "source", "params": {}, "materialization": {"format": "iceberg"}}
        fp1 = compute_node_fingerprint(d1, Path("s.yml"))
        fp2 = compute_node_fingerprint(d2, Path("s.yml"))
        assert fp1.config_hash != fp2.config_hash


class TestComputeDagFingerprints:
    """Test DAG-wide fingerprint computation."""

    def test_linear_chain(self):
        nodes = {
            "source.s1": {"kind": "source", "config": {"params": {"path": "data.csv"}}, "file_path": "s1.yml"},
            "transform.t1": {"kind": "transform", "config": {"transform": "SELECT * FROM s1"}, "file_path": "t1.yml"},
        }
        upstream = {
            "source.s1": set(),
            "transform.t1": {"source.s1"},
        }
        fps = compute_dag_fingerprints(nodes, upstream)
        assert "source.s1" in fps
        assert "transform.t1" in fps
        # t1's upstream_hash should reflect s1's fingerprint
        assert fps["transform.t1"].upstream_hash != fps["source.s1"].upstream_hash

    def test_diamond_dag(self):
        nodes = {
            "source.s1": {"kind": "source", "config": {}, "file_path": "s1.yml"},
            "transform.t1": {"kind": "transform", "config": {"transform": "A"}, "file_path": "t1.yml"},
            "transform.t2": {"kind": "transform", "config": {"transform": "B"}, "file_path": "t2.yml"},
            "transform.t3": {"kind": "transform", "config": {"transform": "C"}, "file_path": "t3.yml"},
        }
        upstream = {
            "source.s1": set(),
            "transform.t1": {"source.s1"},
            "transform.t2": {"source.s1"},
            "transform.t3": {"transform.t1", "transform.t2"},
        }
        fps = compute_dag_fingerprints(nodes, upstream)
        assert len(fps) == 4
        # t3 depends on both t1 and t2
        assert fps["transform.t3"].upstream_hash != fps["transform.t1"].upstream_hash

    def test_source_change_propagates_to_downstream(self):
        """Changing a source's content changes all downstream fingerprints."""
        def make_nodes(source_sql):
            return {
                "source.s1": {"kind": "source", "config": {"params": {"query": source_sql}}, "file_path": "s1.yml"},
                "transform.t1": {"kind": "transform", "config": {"transform": "SELECT * FROM s1"}, "file_path": "t1.yml"},
            }
        upstream = {"source.s1": set(), "transform.t1": {"source.s1"}}

        fps1 = compute_dag_fingerprints(make_nodes("SELECT 1"), upstream)
        fps2 = compute_dag_fingerprints(make_nodes("SELECT 2"), upstream)

        # Source changed
        assert fps1["source.s1"].content_hash != fps2["source.s1"].content_hash
        # Downstream t1's upstream_hash should differ
        assert fps1["transform.t1"].upstream_hash != fps2["transform.t1"].upstream_hash

    def test_isolated_nodes(self):
        """Nodes with no edges get fingerprints too."""
        nodes = {
            "source.s1": {"kind": "source", "config": {}, "file_path": "s1.yml"},
            "source.s2": {"kind": "source", "config": {}, "file_path": "s2.yml"},
        }
        upstream = {"source.s1": set(), "source.s2": set()}
        fps = compute_dag_fingerprints(nodes, upstream)
        assert len(fps) == 2
