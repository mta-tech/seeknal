from __future__ import annotations

import base64
import secrets

from sqlalchemy.exc import IntegrityError
from sqlmodel import Session

from seeknal.report_server.models import Publish


class SlugExhaustedError(RuntimeError):
    pass


def generate_unique(session: Session) -> str:
    for _ in range(5):
        raw = secrets.token_bytes(4)
        slug = base64.b32encode(raw).decode().lower().rstrip("=")[:6]
        try:
            # Probe for collision without flushing the real insert
            probe = session.get(Publish, slug)
            if probe is not None:
                continue
            return slug
        except IntegrityError:
            session.rollback()
            continue
    raise SlugExhaustedError("Failed to generate a unique slug after 5 attempts")
