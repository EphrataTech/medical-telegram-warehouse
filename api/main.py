"""
api/main.py
───────────
FastAPI application exposing the Medical Telegram Warehouse as a REST API.

Endpoints
---------
GET  /health
GET  /api/reports/top-products
GET  /api/channels/{channel_name}/activity
GET  /api/search/messages
GET  /api/reports/visual-content

Run locally:
    uvicorn api.main:app --reload --port 8000
Then open: http://localhost:8000/docs
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.database import get_db
from api.schemas import (
    ChannelActivityResponse,
    ChannelVisualStats,
    DailyActivity,
    HealthResponse,
    ImageCategoryBreakdown,
    MessageSearchResponse,
    MessageSearchResult,
    TopProductItem,
    TopProductsResponse,
    VisualContentResponse,
)

# ──────────────────────────────────────────────────────────────────────────────
# App setup
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Medical Telegram Warehouse API",
    description=(
        "Analytical REST API over the Ethiopian Medical Businesses Telegram "
        "data warehouse. Built with FastAPI + SQLAlchemy + PostgreSQL."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

DbDep = Annotated[AsyncSession, Depends(get_db)]


# ──────────────────────────────────────────────────────────────────────────────
# Health
# ──────────────────────────────────────────────────────────────────────────────
@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    tags=["System"],
)
async def health() -> HealthResponse:
    """Returns service status and current UTC timestamp."""
    return HealthResponse(status="ok", timestamp=datetime.now(timezone.utc))


# ──────────────────────────────────────────────────────────────────────────────
# Endpoint 1 — Top Products
# ──────────────────────────────────────────────────────────────────────────────
@app.get(
    "/api/reports/top-products",
    response_model=TopProductsResponse,
    summary="Most frequently mentioned product terms across all channels",
    tags=["Reports"],
)
async def top_products(
    db: DbDep,
    limit: Annotated[
        int,
        Query(ge=1, le=100, description="Maximum number of terms to return."),
    ] = 10,
) -> TopProductsResponse:
    """
    Tokenises message text from **fct_messages** and returns the most
    frequently occurring terms (excluding common stop-words).

    Terms are normalised to lower-case and must be at least 4 characters long.
    """
    sql = text("""
        WITH words AS (
            SELECT
                lower(word)                  AS term,
                channel
            FROM marts.fct_messages,
                 regexp_split_to_table(message_text, E'\\\\s+') AS word
            WHERE message_length > 0
              AND lower(word) NOT IN (
                  'and','the','for','this','that','with','from',
                  'have','your','will','they','been','more','were',
                  'also','into','when','than','can','not','all',
                  'its','our','are','but','was','had','has','his',
                  'her','she','him','we','you','it','is','in',
                  'of','to','a','an','on','at','be','by','or',
                  'so','if','do','no','up','as','us','my'
              )
              AND length(word) >= 4
        ),
        ranked AS (
            SELECT
                term,
                count(*)                     AS mention_count,
                array_agg(DISTINCT channel)  AS channels
            FROM words
            WHERE term ~ '^[a-z][a-z0-9]+$'
            GROUP BY term
            ORDER BY mention_count DESC
            LIMIT :limit
        )
        SELECT term, mention_count, channels FROM ranked;
    """)

    result = await db.execute(sql, {"limit": limit})
    rows = result.fetchall()

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No message data found. Run the scraper and dbt first.",
        )

    items = [
        TopProductItem(
            term=row.term,
            mention_count=row.mention_count,
            channels=list(row.channels),
        )
        for row in rows
    ]
    return TopProductsResponse(total_terms=len(items), items=items)


# ──────────────────────────────────────────────────────────────────────────────
# Endpoint 2 — Channel Activity
# ──────────────────────────────────────────────────────────────────────────────
@app.get(
    "/api/channels/{channel_name}/activity",
    response_model=ChannelActivityResponse,
    summary="Posting activity and engagement trends for a channel",
    tags=["Channels"],
)
async def channel_activity(
    channel_name: str,
    db: DbDep,
    days: Annotated[
        int,
        Query(ge=1, le=365, description="Number of most-recent days to include in daily breakdown."),
    ] = 30,
) -> ChannelActivityResponse:
    """
    Returns overall statistics from **dim_channels** plus a per-day
    activity breakdown (last *days* days) from **fct_messages**.
    """
    # Channel metadata
    meta_sql = text("""
        SELECT
            channel_name,
            channel_type,
            total_posts,
            avg_views,
            avg_forwards,
            first_post_date,
            last_post_date
        FROM marts.dim_channels
        WHERE lower(channel_name) = lower(:channel)
        LIMIT 1;
    """)
    meta_result = await db.execute(meta_sql, {"channel": channel_name})
    meta = meta_result.fetchone()

    if meta is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Channel '{channel_name}' not found in the warehouse.",
        )

    # Daily breakdown
    daily_sql = text("""
        SELECT
            to_char(posted_at::date, 'YYYY-MM-DD')      AS date,
            count(*)                                     AS message_count,
            coalesce(sum(view_count), 0)                 AS total_views,
            coalesce(sum(forward_count), 0)              AS total_forwards,
            sum(case when has_image then 1 else 0 end)   AS images_posted
        FROM marts.fct_messages
        WHERE lower(channel) = lower(:channel)
          AND posted_at >= now() - (:days || ' days')::interval
        GROUP BY posted_at::date
        ORDER BY posted_at::date DESC;
    """)
    daily_result = await db.execute(daily_sql, {"channel": channel_name, "days": days})
    daily_rows = daily_result.fetchall()

    return ChannelActivityResponse(
        channel=meta.channel_name,
        channel_type=meta.channel_type,
        total_posts=meta.total_posts,
        avg_views=float(meta.avg_views or 0),
        avg_forwards=float(meta.avg_forwards or 0),
        first_post_date=meta.first_post_date,
        last_post_date=meta.last_post_date,
        daily_activity=[
            DailyActivity(
                date=row.date,
                message_count=row.message_count,
                total_views=row.total_views,
                total_forwards=row.total_forwards,
                images_posted=row.images_posted,
            )
            for row in daily_rows
        ],
    )


# ──────────────────────────────────────────────────────────────────────────────
# Endpoint 3 — Message Search
# ──────────────────────────────────────────────────────────────────────────────
@app.get(
    "/api/search/messages",
    response_model=MessageSearchResponse,
    summary="Full-text search across all message content",
    tags=["Search"],
)
async def search_messages(
    db: DbDep,
    query: Annotated[
        str,
        Query(min_length=2, max_length=200, description="Keyword or phrase to search for."),
    ],
    limit: Annotated[
        int,
        Query(ge=1, le=100, description="Maximum results to return."),
    ] = 20,
    channel: Annotated[
        str | None,
        Query(description="Restrict search to a specific channel."),
    ] = None,
) -> MessageSearchResponse:
    """
    Performs a case-insensitive substring search on **fct_messages.message_text**.
    Optionally scoped to a single channel.
    Results are ordered by view_count descending so the most-seen messages
    surface first.
    """
    channel_filter = "AND lower(channel) = lower(:channel)" if channel else ""

    sql = text(f"""
        SELECT
            message_id,
            channel,
            message_text,
            posted_at,
            view_count,
            forward_count,
            has_image
        FROM marts.fct_messages
        WHERE message_text ILIKE :pattern
          {channel_filter}
        ORDER BY view_count DESC, posted_at DESC
        LIMIT :limit;
    """)

    params: dict = {"pattern": f"%{query}%", "limit": limit}
    if channel:
        params["channel"] = channel

    result = await db.execute(sql, params)
    rows = result.fetchall()

    # Count without the LIMIT for the total
    count_sql = text(f"""
        SELECT count(*) AS n
        FROM marts.fct_messages
        WHERE message_text ILIKE :pattern
          {channel_filter};
    """)
    count_result = await db.execute(count_sql, params)
    total = count_result.scalar() or 0

    return MessageSearchResponse(
        query=query,
        total_results=total,
        items=[
            MessageSearchResult(
                message_id=row.message_id,
                channel=row.channel,
                message_text=row.message_text,
                posted_at=row.posted_at,
                view_count=row.view_count,
                forward_count=row.forward_count,
                has_image=row.has_image,
            )
            for row in rows
        ],
    )


# ──────────────────────────────────────────────────────────────────────────────
# Endpoint 4 — Visual Content Stats
# ──────────────────────────────────────────────────────────────────────────────
@app.get(
    "/api/reports/visual-content",
    response_model=VisualContentResponse,
    summary="Image usage statistics and YOLO category breakdown",
    tags=["Reports"],
)
async def visual_content(db: DbDep) -> VisualContentResponse:
    """
    Returns two sections:

    - **channel_stats**: per-channel image usage rate and the view-count lift
      for messages that include an image versus those that don't.
    - **category_breakdown**: aggregate counts and engagement metrics grouped
      by the YOLO-derived image category (promotional, product_display,
      lifestyle, other).
    """
    # Per-channel image stats from fct_messages
    channel_sql = text("""
        SELECT
            channel,
            count(*)                                            AS total_messages,
            sum(case when has_image then 1 else 0 end)         AS messages_with_images,
            round(
                100.0 * sum(case when has_image then 1 else 0 end)
                / nullif(count(*), 0), 2
            )                                                   AS pct_with_images,
            round(avg(case when has_image
                          then view_count end)::numeric, 2)     AS avg_views_with_image,
            round(avg(case when not has_image
                          then view_count end)::numeric, 2)     AS avg_views_without_image
        FROM marts.fct_messages
        GROUP BY channel
        ORDER BY pct_with_images DESC;
    """)
    ch_result = await db.execute(channel_sql)
    ch_rows = ch_result.fetchall()

    channel_stats = []
    for row in ch_rows:
        with_img    = float(row.avg_views_with_image    or 0)
        without_img = float(row.avg_views_without_image or 0)
        lift = (
            round((with_img - without_img) / without_img * 100, 2)
            if without_img > 0 else 0.0
        )
        channel_stats.append(
            ChannelVisualStats(
                channel=row.channel,
                total_messages=row.total_messages,
                messages_with_images=row.messages_with_images,
                pct_with_images=float(row.pct_with_images or 0),
                avg_views_with_image=with_img,
                avg_views_without_image=without_img,
                view_lift_pct=lift,
            )
        )

    # YOLO category breakdown from fct_image_detections
    category_sql = text("""
        SELECT
            image_category,
            count(DISTINCT message_id)              AS count,
            round(avg(view_count)::numeric, 2)      AS avg_views,
            round(avg(forward_count)::numeric, 2)   AS avg_forwards
        FROM marts.fct_image_detections
        GROUP BY image_category
        ORDER BY count DESC;
    """)
    try:
        cat_result = await db.execute(category_sql)
        cat_rows = cat_result.fetchall()
    except Exception:
        # fct_image_detections may not exist yet if YOLO hasn't been run
        cat_rows = []

    category_breakdown = [
        ImageCategoryBreakdown(
            image_category=row.image_category,
            count=row.count,
            avg_views=float(row.avg_views or 0),
            avg_forwards=float(row.avg_forwards or 0),
        )
        for row in cat_rows
    ]

    return VisualContentResponse(
        channel_stats=channel_stats,
        category_breakdown=category_breakdown,
    )
