from pydantic import BaseModel, Field
import uuid


class AlbumAssembledEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    album_id: str
    workspace_id: str
    grouped_id: str
    vision_summary_s3_key: str
    vision_labels: list[str] = Field(default_factory=list)
    post_ids: list[str] = Field(default_factory=list)
