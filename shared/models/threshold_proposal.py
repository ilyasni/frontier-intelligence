from typing import Optional
from datetime import datetime

from sqlalchemy import Text, ForeignKey, Float, DateTime, JSON, Integer
from sqlalchemy.orm import mapped_column, Mapped

from shared.db import Base
from shared.models.base import TimestampMixin


class ThresholdProposal(Base, TimestampMixin):
    """Предложение ретро-петли по изменению порога weak-signal детектора (Контур A).

    Петля наблюдает, оправдались ли когда-то слабые сигналы (см. [[weak_signal_snapshot]]),
    и предлагает сдвиг порога, который лучше отделил бы оправдавшихся от угасших. Человек
    утверждает через MCP; approve применяет значение в workspaces.extra->cluster_analysis.
    """

    __tablename__ = "threshold_proposals"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(Text, ForeignKey("workspaces.id"), nullable=False)
    threshold_key: Mapped[str] = mapped_column(Text, nullable=False)
    current_value: Mapped[float] = mapped_column(Float, nullable=False)
    proposed_value: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(Text, default="pending")
    cohort_days: Mapped[int] = mapped_column(Integer, default=0)
    direction: Mapped[Optional[str]] = mapped_column(Text)
    rationale: Mapped[Optional[str]] = mapped_column(Text)
    evidence: Mapped[dict] = mapped_column(JSON, default=dict)
    threshold_version: Mapped[Optional[str]] = mapped_column(Text)
    run_id: Mapped[Optional[str]] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(Text, default="retrospective_loop")
    reviewed_by: Mapped[Optional[str]] = mapped_column(Text)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    applied_value: Mapped[Optional[float]] = mapped_column(Float)
