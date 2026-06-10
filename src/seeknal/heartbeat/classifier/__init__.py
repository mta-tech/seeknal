"""Smart classifier for the heartbeat (v0.5).

Phase A.5 of the tick loop: each new file is fingerprinted, matched against
``.seeknal/source_catalog.yml``, and either auto-routed (high-confidence
catalog match) or quarantined for operator review (low confidence).

The classifier is opt-in via ``classifier.enabled: true`` in HEARTBEAT.md.
When disabled, the loop falls back to filename-stem routing (v0.4 behavior).
"""

from seeknal.heartbeat.classifier.models import (
    ClassificationDecision,
    ColumnMapping,
    Fingerprint,
)
from seeknal.heartbeat.classifier.fingerprint import compute_fingerprint, similarity
from seeknal.heartbeat.classifier.catalog import Catalog, CatalogMatch

__all__ = [
    "ClassificationDecision",
    "ColumnMapping",
    "Fingerprint",
    "compute_fingerprint",
    "similarity",
    "Catalog",
    "CatalogMatch",
]
