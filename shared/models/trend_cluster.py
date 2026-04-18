"""
Тренд-кластеры (семантическая ось по эмбеддингам/времени) ортогональны posts.category
(релевантность к фиксированным slug'ам workspace). Поле category здесь — опциональная
производная метка (например мода категорий постов), не вход в матчинг кластера.
"""
from typing import Optional
from datetime import datetime
from sqlalchemy import Text, ForeignKey, Float, DateTime, JSON, Integer
from sqlalchemy.orm import mapped_column, Mapped
from shared.db import Base
from shared.models.base import TimestampMixin


class TrendCluster(Base, TimestampMixin):
    __tablename__ = "trend_clusters"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(Text, ForeignKey("workspaces.id"), nullable=False)
    cluster_key: Mapped[str] = mapped_column(Text, nullable=False)
    pipeline: Mapped[str] = mapped_column(Text, nullable=False)  # reactive | stable
    title: Mapped[str] = mapped_column(Text, nullable=False)
    insight: Mapped[Optional[str]] = mapped_column(Text)
    opportunity: Mapped[Optional[str]] = mapped_column(Text)
    time_horizon: Mapped[Optional[str]] = mapped_column(Text)
    burst_score: Mapped[float] = mapped_column(Float, default=0.0)
    coherence: Mapped[float] = mapped_column(Float, default=0.0)
    novelty: Mapped[float] = mapped_column(Float, default=0.0)
    source_diversity_score: Mapped[float] = mapped_column(Float, default=0.0)
    freshness_score: Mapped[float] = mapped_column(Float, default=0.0)
    evidence_strength_score: Mapped[float] = mapped_column(Float, default=0.0)
    velocity_score: Mapped[float] = mapped_column(Float, default=0.0)
    acceleration_score: Mapped[float] = mapped_column(Float, default=0.0)
    baseline_rate: Mapped[float] = mapped_column(Float, default=0.0)
    current_rate: Mapped[float] = mapped_column(Float, default=0.0)
    change_point_count: Mapped[int] = mapped_column(Integer, default=0)
    change_point_strength: Mapped[float] = mapped_column(Float, default=0.0)
    has_recent_change_point: Mapped[bool] = mapped_column(default=False)
    signal_score: Mapped[float] = mapped_column(Float, default=0.0)
    signal_stage: Mapped[str] = mapped_column(Text, default="stable")
    doc_count: Mapped[int] = mapped_column(Integer, default=0)
    source_count: Mapped[int] = mapped_column(Integer, default=0)
    doc_ids: Mapped[list] = mapped_column(JSON, default=list)
    semantic_cluster_ids: Mapped[list] = mapped_column(JSON, default=list)
    keywords: Mapped[list] = mapped_column(JSON, default=list)
    explainability: Mapped[dict] = mapped_column(JSON, default=dict)
    category: Mapped[Optional[str]] = mapped_column(Text)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
