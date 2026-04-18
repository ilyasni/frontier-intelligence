from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, JSON, Text
from sqlalchemy.orm import Mapped, mapped_column

from shared.db import Base
from shared.models.base import TimestampMixin


class SignalTimeSeries(Base, TimestampMixin):
    __tablename__ = "signal_time_series"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(Text, nullable=False)
    entity_kind: Mapped[str] = mapped_column(Text, nullable=False)
    entity_id: Mapped[str] = mapped_column(Text, nullable=False)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    doc_count: Mapped[int] = mapped_column(Integer, default=0)
    source_count: Mapped[int] = mapped_column(Integer, default=0)
    avg_relevance: Mapped[float] = mapped_column(Float, default=0.0)
    avg_source_score: Mapped[float] = mapped_column(Float, default=0.0)
    freshness_score: Mapped[float] = mapped_column(Float, default=0.0)
    window_rate: Mapped[float] = mapped_column(Float, default=0.0)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
