from typing import Optional
from sqlalchemy import Text, ForeignKey, Integer
from sqlalchemy.orm import mapped_column, Mapped
from shared.db import Base
from shared.models.base import TimestampMixin


class IndexingStatus(Base, TimestampMixin):
    __tablename__ = "indexing_status"

    post_id: Mapped[str] = mapped_column(Text, ForeignKey("posts.id"), primary_key=True)
    embedding_status: Mapped[str] = mapped_column(Text, default="pending")
    # pending | done | dropped | error
    graph_status: Mapped[str] = mapped_column(Text, default="pending")
    vision_status: Mapped[str] = mapped_column(Text, default="pending")
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    qdrant_point_id: Mapped[Optional[str]] = mapped_column(Text)
