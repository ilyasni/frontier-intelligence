from typing import Optional
from sqlalchemy import Text, ForeignKey, Float, JSON
from sqlalchemy.orm import mapped_column, Mapped
from shared.db import Base
from shared.models.base import TimestampMixin


class MissingSignal(Base, TimestampMixin):
    __tablename__ = "missing_signals"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(Text, ForeignKey("workspaces.id"), nullable=False)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    gap_score: Mapped[float] = mapped_column(Float, default=0.0)
    opportunity: Mapped[Optional[str]] = mapped_column(Text)
    searxng_frequency: Mapped[float] = mapped_column(Float, default=0.0)
    frontier_frequency: Mapped[float] = mapped_column(Float, default=0.0)
    evidence_urls: Mapped[list] = mapped_column(JSON, default=list)
    category: Mapped[Optional[str]] = mapped_column(Text)
