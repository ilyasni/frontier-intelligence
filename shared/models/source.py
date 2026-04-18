from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, JSON, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from shared.db import Base
from shared.models.base import TimestampMixin

if TYPE_CHECKING:
    from shared.models.workspace import Workspace


class Source(Base, TimestampMixin):
    __tablename__ = "sources"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[str] = mapped_column(Text, ForeignKey("workspaces.id"), nullable=False)
    source_type: Mapped[str] = mapped_column(Text, nullable=False)  # telegram|rss|web|api|email
    name: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[Optional[str]] = mapped_column(Text)
    tg_channel: Mapped[Optional[str]] = mapped_column(Text)
    tg_account_idx: Mapped[int] = mapped_column(Integer, default=0)
    schedule_cron: Mapped[str] = mapped_column(Text, default="*/5 * * * *")
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    proxy_config: Mapped[Optional[dict]] = mapped_column(JSON)
    extra: Mapped[dict] = mapped_column(JSON, default=dict)
    source_authority: Mapped[float] = mapped_column(Float, default=0.5)
    source_score: Mapped[Optional[float]] = mapped_column(Float)
    source_score_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    workspace: Mapped["Workspace"] = relationship("Workspace", back_populates="sources")
