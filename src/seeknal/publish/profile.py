from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class PublishProfile:
    server: str
    api_key: Optional[str] = None
