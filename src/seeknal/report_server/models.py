from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import DateTime, Engine, Integer, MetaData, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

# Isolated metadata — NEVER shared with seeknal.models singleton.
# We deliberately avoid importing seeknal.models or using SQLModel table=True,
# both of which attach to the module-level metadata singleton in seeknal/models.py.
_report_server_metadata = MetaData()


class _Base(DeclarativeBase):
    metadata = _report_server_metadata


class Publish(_Base):
    __tablename__ = "publish"

    slug: Mapped[str] = mapped_column(String, primary_key=True)
    report_name: Mapped[str] = mapped_column(String, nullable=False)
    report_title: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    owner_secret_hash: Mapped[str] = mapped_column(String, nullable=False)
    owner_secret_salt: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    revoked_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    asset_relpath: Mapped[str] = mapped_column(String, nullable=False)
    byte_size: Mapped[int] = mapped_column(Integer, nullable=False)


def get_engine(data_dir) -> Engine:
    from pathlib import Path

    db_path = Path(data_dir) / "metadata.db"
    engine = create_engine(f"sqlite:///{db_path}")
    _report_server_metadata.create_all(engine)
    return engine


def get_session(engine: Engine) -> Session:
    return Session(engine)
