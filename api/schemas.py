"""
api/schemas.py
──────────────
Pydantic v2 request/response models for every API endpoint.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict


# ──────────────────────────────────────────────────────────────────────────────
# Shared / raw models
# ──────────────────────────────────────────────────────────────────────────────
class MediaInfo(BaseModel):
    type: str
    has_photo: bool
    photo_id: Optional[int] = None


class TelegramMessage(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    message_id: int
    channel: str
    date: datetime
    text: str
    views: Optional[int] = None
    forwards: Optional[int] = None
    media: Optional[MediaInfo] = None
    reply_to_msg_id: Optional[int] = None
    edit_date: Optional[datetime] = None
    post_author: Optional[str] = None
    grouped_id: Optional[int] = None
    out: Optional[bool] = None
    mentioned: Optional[bool] = None
    pinned: Optional[bool] = None
    scraped_at: datetime
    local_image_path: Optional[str] = None


# ──────────────────────────────────────────────────────────────────────────────
# Endpoint 1 — Top Products
# GET /api/reports/top-products
# ──────────────────────────────────────────────────────────────────────────────
class TopProductItem(BaseModel):
    """A single term/keyword and the number of messages it appears in."""
    term: str = Field(description="Extracted keyword or product term.")
    mention_count: int = Field(description="Number of messages containing this term.")
    channels: list[str] = Field(description="Channels where this term was found.")


class TopProductsResponse(BaseModel):
    total_terms: int = Field(description="Number of distinct terms returned.")
    items: list[TopProductItem]


# ──────────────────────────────────────────────────────────────────────────────
# Endpoint 2 — Channel Activity
# GET /api/channels/{channel_name}/activity
# ──────────────────────────────────────────────────────────────────────────────
class DailyActivity(BaseModel):
    """Message count and engagement for a single day."""
    date: str = Field(description="Date in YYYY-MM-DD format.")
    message_count: int
    total_views: int
    total_forwards: int
    images_posted: int


class ChannelActivityResponse(BaseModel):
    channel: str
    channel_type: str
    total_posts: int
    avg_views: float
    avg_forwards: float
    first_post_date: Optional[datetime]
    last_post_date: Optional[datetime]
    daily_activity: list[DailyActivity] = Field(
        description="Per-day breakdown of activity (most recent 30 days)."
    )


# ──────────────────────────────────────────────────────────────────────────────
# Endpoint 3 — Message Search
# GET /api/search/messages
# ──────────────────────────────────────────────────────────────────────────────
class MessageSearchResult(BaseModel):
    """A single message matching the search query."""
    message_id: int
    channel: str
    message_text: str
    posted_at: datetime
    view_count: int
    forward_count: int
    has_image: bool


class MessageSearchResponse(BaseModel):
    query: str
    total_results: int
    items: list[MessageSearchResult]


# ──────────────────────────────────────────────────────────────────────────────
# Endpoint 4 — Visual Content Stats
# GET /api/reports/visual-content
# ──────────────────────────────────────────────────────────────────────────────
class ChannelVisualStats(BaseModel):
    """Image usage statistics for a single channel."""
    channel: str
    total_messages: int
    messages_with_images: int
    pct_with_images: float = Field(description="Percentage of messages containing images.")
    avg_views_with_image: float
    avg_views_without_image: float
    view_lift_pct: float = Field(
        description="Percentage view increase for posts with images vs without."
    )


class ImageCategoryBreakdown(BaseModel):
    """YOLO-detected image category counts across all channels."""
    image_category: str
    count: int
    avg_views: float
    avg_forwards: float


class VisualContentResponse(BaseModel):
    channel_stats: list[ChannelVisualStats]
    category_breakdown: list[ImageCategoryBreakdown]


# ──────────────────────────────────────────────────────────────────────────────
# Health check
# ──────────────────────────────────────────────────────────────────────────────
class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
