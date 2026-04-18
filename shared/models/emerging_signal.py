from typing import Optional
from datetime import datetime

from sqlalchemy import Text, ForeignKey, Float, DateTime, JSON, Integer
from sqlalchemy.orm import mapped_column, Mapped

from shared.db import Base
from shared.models.base import TimestampMixin


class EmergingSignal(Base, TimestampMixin):
    __tablename__ = "emerging_signals"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(Text, ForeignKey("workspaces.id"), nullable=False)
    signal_key: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    signal_stage: Mapped[str] = mapped_column(Text, default="weak")
    signal_score: Mapped[float] = mapped_column(Float, default=0.0)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    velocity_score: Mapped[float] = mapped_column(Float, default=0.0)
    acceleration_score: Mapped[float] = mapped_column(Float, default=0.0)
    baseline_rate: Mapped[float] = mapped_column(Float, default=0.0)
    current_rate: Mapped[float] = mapped_column(Float, default=0.0)
    change_point_count: Mapped[int] = mapped_column(Integer, default=0)
    change_point_strength: Mapped[float] = mapped_column(Float, default=0.0)
    has_recent_change_point: Mapped[bool] = mapped_column(default=False)
    supporting_semantic_cluster_ids: Mapped[list] = mapped_column(JSON, default=list)
    doc_ids: Mapped[list] = mapped_column(JSON, default=list)
    source_ids: Mapped[list] = mapped_column(JSON, default=list)
    source_count: Mapped[int] = mapped_column(Integer, default=0)
    keywords: Mapped[list] = mapped_column(JSON, default=list)
    evidence: Mapped[list] = mapped_column(JSON, default=list)
    explainability: Mapped[dict] = mapped_column(JSON, default=dict)
    recommended_watch_action: Mapped[Optional[str]] = mapped_column(Text)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    first_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
