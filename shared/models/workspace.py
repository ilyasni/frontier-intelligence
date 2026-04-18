from typing import Optional
from sqlalchemy import Text, JSON
from sqlalchemy.orm import mapped_column, Mapped, relationship
from shared.db import Base
from shared.models.base import TimestampMixin


class Workspace(Base, TimestampMixin):
    __tablename__ = "workspaces"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    categories: Mapped[list] = mapped_column(JSON, default=list)
    relevance_weights: Mapped[dict] = mapped_column(JSON, default=dict)
    design_lenses: Mapped[list] = mapped_column(JSON, default=list)
    cross_workspace_bridges: Mapped[list] = mapped_column(JSON, default=list)
    extra: Mapped[dict] = mapped_column(JSON, default=dict)
    is_active: Mapped[bool] = mapped_column(default=True)

    sources: Mapped[list] = relationship("Source", back_populates="workspace", lazy="select")
