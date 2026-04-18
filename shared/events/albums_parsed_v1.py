from pydantic import BaseModel, Field
import uuid


class AlbumParsedEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    album_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    workspace_id: str
    source_id: str
    grouped_id: str
    post_ids: list[str] = Field(default_factory=list)
