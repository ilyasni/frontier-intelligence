from typing import Optional
from datetime import datetime

from sqlalchemy import Text, ForeignKey, Float, DateTime, Integer
from sqlalchemy.orm import mapped_column, Mapped

from shared.db import Base
from shared.models.base import TimestampMixin


class EntityMergeProposal(Base, TimestampMixin):
    """Предложение семантического слияния двух концептов Neo4j (Контур D+).

    Пара (акроним ↔ расшифровка), подтверждённая LLM-судьёй другой семьи как одна сущность.
    Человек утверждает через MCP; approve сливает узлы графа (canonical + alias).
    """

    __tablename__ = "entity_merge_proposals"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(Text, ForeignKey("workspaces.id"), nullable=False)
    norm_a: Mapped[str] = mapped_column(Text, nullable=False)
    name_a: Mapped[str] = mapped_column(Text, nullable=False)
    norm_b: Mapped[str] = mapped_column(Text, nullable=False)
    name_b: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_norm: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_name: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    reasoning: Mapped[Optional[str]] = mapped_column(Text)
    signal: Mapped[str] = mapped_column(Text, default="acronym")
    cooccurrence: Mapped[int] = mapped_column(Integer, default=0)
    judge_provider: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="pending")
    reviewed_by: Mapped[Optional[str]] = mapped_column(Text)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    run_id: Mapped[Optional[str]] = mapped_column(Text)
