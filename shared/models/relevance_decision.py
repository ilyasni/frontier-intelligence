from typing import Optional
from datetime import datetime

from sqlalchemy import Text, ForeignKey, Float, DateTime, Boolean
from sqlalchemy.orm import mapped_column, Mapped

from shared.db import Base
from shared.models.base import TimestampMixin


class RelevanceDecision(Base, TimestampMixin):
    """Лог решения Relevance Filter (пишется на reject'ах).

    Сейчас отклонённый пост получает ``embedding_status = dropped`` и тихо выпадает
    из пайплайна. Здесь сохраняется обоснование (score, reasoning, escalated, model),
    чтобы периодический human spot-audit мог найти тихие false-negative. ``audit_status``
    проставляется человеком (Контур C) и сохраняется при повторной обработке поста.
    """

    __tablename__ = "relevance_decisions"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    post_id: Mapped[str] = mapped_column(Text, nullable=False)
    workspace_id: Mapped[str] = mapped_column(Text, ForeignKey("workspaces.id"), nullable=False)
    relevant: Mapped[bool] = mapped_column(Boolean, nullable=False)
    score: Mapped[float] = mapped_column(Float, default=0.0)
    threshold: Mapped[float] = mapped_column(Float, default=0.0)
    category: Mapped[Optional[str]] = mapped_column(Text)
    reasoning: Mapped[Optional[str]] = mapped_column(Text)
    escalated: Mapped[bool] = mapped_column(Boolean, default=False)
    model: Mapped[Optional[str]] = mapped_column(Text)
    source_id: Mapped[Optional[str]] = mapped_column(Text)
    content_excerpt: Mapped[Optional[str]] = mapped_column(Text)
    audit_status: Mapped[Optional[str]] = mapped_column(Text)
    audit_note: Mapped[Optional[str]] = mapped_column(Text)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
