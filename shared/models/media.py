from typing import Optional
from sqlalchemy import Text, ForeignKey, JSON, Integer, BigInteger
from sqlalchemy.orm import mapped_column, Mapped
from shared.db import Base
from shared.models.base import TimestampMixin


class MediaObject(Base, TimestampMixin):
    __tablename__ = "media_objects"

    sha256: Mapped[str] = mapped_column(Text, primary_key=True)
    s3_key: Mapped[str] = mapped_column(Text, nullable=False)
    mime_type: Mapped[Optional[str]] = mapped_column(Text)
    size_bytes: Mapped[Optional[int]] = mapped_column(BigInteger)
    workspace_id: Mapped[str] = mapped_column(Text, ForeignKey("workspaces.id"), nullable=False)


class MediaGroup(Base, TimestampMixin):
    __tablename__ = "media_groups"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(Text, ForeignKey("workspaces.id"), nullable=False)
    grouped_id: Mapped[str] = mapped_column(Text, nullable=False)
    source_id: Mapped[str] = mapped_column(Text, ForeignKey("sources.id"), nullable=False)
    item_count: Mapped[int] = mapped_column(Integer, default=0)
    assembled: Mapped[bool] = mapped_column(default=False)
    vision_summary_s3_key: Mapped[Optional[str]] = mapped_column(Text)
    vision_labels: Mapped[list] = mapped_column(JSON, default=list)
