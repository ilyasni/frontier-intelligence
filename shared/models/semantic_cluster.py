from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, ForeignKey, Integer, JSON, Text
from sqlalchemy.orm import Mapped, mapped_column

from shared.db import Base
from shared.models.base import TimestampMixin


class SemanticCluster(Base, TimestampMixin):
    __tablename__ = "semantic_clusters"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(Text, ForeignKey("workspaces.id"), nullable=False)
    cluster_key: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    representative_post_id: Mapped[Optional[str]] = mapped_column(Text)
    post_count: Mapped[int] = mapped_column(Integer, default=0)
    source_count: Mapped[int] = mapped_column(Integer, default=0)
    doc_ids: Mapped[list] = mapped_column(JSON, default=list)
    source_ids: Mapped[list] = mapped_column(JSON, default=list)
    top_concepts: Mapped[list] = mapped_column(JSON, default=list)
    evidence: Mapped[list] = mapped_column(JSON, default=list)
    representative_evidence: Mapped[dict] = mapped_column(JSON, default=dict)
    related_cluster_ids: Mapped[list] = mapped_column(JSON, default=list)
    lifecycle_state: Mapped[str] = mapped_column(Text, default="new")
    avg_relevance: Mapped[float] = mapped_column(Float, default=0.0)
    avg_source_score: Mapped[float] = mapped_column(Float, default=0.0)
    freshness_score: Mapped[float] = mapped_column(Float, default=0.0)
    coherence_score: Mapped[float] = mapped_column(Float, default=0.0)
    explainability: Mapped[dict] = mapped_column(JSON, default=dict)
    time_window: Mapped[str] = mapped_column(Text, default="7d")
    embedding_version: Mapped[str] = mapped_column(Text, default="EmbeddingsGigaR")
    first_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
