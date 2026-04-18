from typing import Optional
from sqlalchemy import Text, ForeignKey, JSON
from sqlalchemy.orm import mapped_column, Mapped
from shared.db import Base
from shared.models.base import TimestampMixin


class PostEnrichment(Base, TimestampMixin):
    __tablename__ = "post_enrichments"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    post_id: Mapped[str] = mapped_column(Text, ForeignKey("posts.id"), nullable=False)
    kind: Mapped[str] = mapped_column(Text, nullable=False)  # concepts | vision | tags | crawl | valence
    data: Mapped[dict] = mapped_column(JSON, nullable=False)
    s3_key: Mapped[Optional[str]] = mapped_column(Text)
