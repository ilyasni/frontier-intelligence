from typing import Optional
from datetime import datetime

from sqlalchemy import Text, ForeignKey, Float, DateTime, JSON, Integer, Boolean
from sqlalchemy.orm import mapped_column, Mapped

from shared.db import Base
from shared.models.base import TimestampMixin


class WeakSignalSnapshot(Base, TimestampMixin):
    """Снимок weak-кандидата на момент прогона кластеризации.

    Субстрат ретроспективной петли: по этим строкам крон через 30/60/90 дней
    проверяет, оправдался ли (вырос ли burst_score) когда-то слабый сигнал, и
    подавленные weak-гейтом кандидаты (``suppressed = True``) — это поздние
    false-negative самого детектора.
    """

    __tablename__ = "weak_signal_snapshots"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    run_id: Mapped[str] = mapped_column(Text, nullable=False)
    workspace_id: Mapped[str] = mapped_column(Text, ForeignKey("workspaces.id"), nullable=False)
    signal_key: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[Optional[str]] = mapped_column(Text)
    stage: Mapped[str] = mapped_column(Text, nullable=False)
    suppressed: Mapped[bool] = mapped_column(Boolean, default=False)
    suppression_reason: Mapped[Optional[str]] = mapped_column(Text)
    signal_score: Mapped[float] = mapped_column(Float, default=0.0)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    burst_score: Mapped[float] = mapped_column(Float, default=0.0)
    source_diversity: Mapped[float] = mapped_column(Float, default=0.0)
    source_count: Mapped[int] = mapped_column(Integer, default=0)
    doc_count: Mapped[int] = mapped_column(Integer, default=0)
    velocity_score: Mapped[float] = mapped_column(Float, default=0.0)
    acceleration_score: Mapped[float] = mapped_column(Float, default=0.0)
    keywords: Mapped[list] = mapped_column(JSON, default=list)
    thresholds: Mapped[dict] = mapped_column(JSON, default=dict)
    threshold_version: Mapped[str] = mapped_column(Text, nullable=False)
    judge_verdict: Mapped[Optional[dict]] = mapped_column(JSON)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
