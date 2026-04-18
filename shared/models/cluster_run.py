from typing import Optional
from datetime import datetime

from sqlalchemy import Text, DateTime, JSON
from sqlalchemy.orm import mapped_column, Mapped

from shared.db import Base
from shared.models.base import TimestampMixin


class ClusterRun(Base, TimestampMixin):
    __tablename__ = "cluster_runs"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    workspace_id: Mapped[Optional[str]] = mapped_column(Text)
    stage: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    thresholds: Mapped[dict] = mapped_column(JSON, default=dict)
    summary: Mapped[dict] = mapped_column(JSON, default=dict)
    metrics: Mapped[dict] = mapped_column(JSON, default=dict)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
