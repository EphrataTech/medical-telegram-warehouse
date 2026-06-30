"""api/schemas.py — Pydantic models (expanded in Task 3)."""
from __future__ import annotations
from datetime import datetime
from typing import Any, Optional
from pydantic import BaseModel


class MediaInfo(BaseModel):
    type: str
    has_photo: bool
    photo_id: Optional[int] = None


class TelegramMessage(BaseModel):
    message_id: int
    channel: str
    date: datetime
    text: str
    views: Optional[int]
    forwards: Optional[int]
    media: Optional[MediaInfo]
    reply_to_msg_id: Optional[int]
    edit_date: Optional[datetime]
    post_author: Optional[str]
    grouped_id: Optional[int]
    out: Optional[bool]
    mentioned: Optional[bool]
    pinned: Optional[bool]
    scraped_at: datetime
    local_image_path: Optional[str] = None
