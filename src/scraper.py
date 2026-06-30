"""
src/scraper.py
──────────────
Telegram channel scraper for the Ethiopian Medical Businesses data pipeline.

Extracts messages (id, date, text, views, forwards, media info) and downloads
photos from public Telegram channels, then persists them to a partitioned raw
data lake under data/raw/.

Usage
-----
    python src/scraper.py                        # scrape all default channels
    python src/scraper.py --channels CheMed123   # scrape specific channel(s)
    python src/scraper.py --limit 500            # cap messages per channel
    python src/scraper.py --start-date 2024-01-01 --end-date 2024-06-30
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.tl.types import Message, MessageMediaPhoto

# ──────────────────────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = ROOT_DIR / "data" / "raw"
MESSAGES_DIR = DATA_RAW / "telegram_messages"
IMAGES_DIR = DATA_RAW / "images"
LOGS_DIR = ROOT_DIR / "logs"
SESSION_DIR = ROOT_DIR / "data" / "sessions"

for _dir in (MESSAGES_DIR, IMAGES_DIR, LOGS_DIR, SESSION_DIR):
    _dir.mkdir(parents=True, exist_ok=True)

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
def setup_logging() -> logging.Logger:
    """Configure file + console logging with timestamps."""
    log_file = LOGS_DIR / f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logger = logging.getLogger("scraper")
    logger.info("Logging initialised → %s", log_file)
    return logger


logger = setup_logging()

# ──────────────────────────────────────────────────────────────────────────────
# Default channels (public usernames or invite links)
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_CHANNELS: list[str] = [
    "CheMed123",          # CheMed Telegram Channel
    "lobelia4cosmetics",  # Lobelia Cosmetics
    "tikvahpharma",       # Tikvah Pharma
    # Add more channels discovered from et.tgstat.com/medicine below:
    # "DoctorsETBot",
    # "ethiopian_pharmaceuticals",
]


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _sanitise_channel_name(channel: str) -> str:
    """Return a filesystem-safe version of the channel identifier."""
    return channel.strip("@/ ").replace("/", "_").replace("\\", "_")


def _message_date(msg: Message) -> date:
    """Return the UTC date of a Telethon message."""
    return msg.date.astimezone(timezone.utc).date()


def _partition_path(channel_name: str, msg_date: date) -> Path:
    """
    Return the full path for the JSON partition file.
    Structure: data/raw/telegram_messages/YYYY-MM-DD/<channel_name>.json
    """
    date_str = msg_date.isoformat()          # e.g. "2024-06-15"
    partition_dir = MESSAGES_DIR / date_str
    partition_dir.mkdir(parents=True, exist_ok=True)
    return partition_dir / f"{channel_name}.json"


def _extract_message_fields(msg: Message) -> dict[str, Any]:
    """
    Extract and return a flat dict of the required message fields, preserving
    the original API structure as much as possible.
    """
    has_photo = isinstance(msg.media, MessageMediaPhoto)

    media_info: dict[str, Any] | None = None
    if msg.media is not None:
        media_info = {
            "type": type(msg.media).__name__,
            "has_photo": has_photo,
        }
        if has_photo:
            media_info["photo_id"] = msg.media.photo.id

    return {
        # Required fields
        "message_id": msg.id,
        "date": msg.date.isoformat(),
        "text": msg.message or "",
        "views": msg.views,
        "forwards": msg.forwards,
        "media": media_info,
        # Bonus metadata preserved from the raw API response
        "reply_to_msg_id": msg.reply_to.reply_to_msg_id if msg.reply_to else None,
        "edit_date": msg.edit_date.isoformat() if msg.edit_date else None,
        "post_author": msg.post_author,
        "grouped_id": msg.grouped_id,
        "out": msg.out,
        "mentioned": msg.mentioned,
        "pinned": msg.pinned,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def _append_to_partition(partition_path: Path, record: dict[str, Any]) -> None:
    """
    Append a single message record to a JSON-lines file (one JSON object per
    line), creating the file if it does not yet exist.
    """
    with partition_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


# ──────────────────────────────────────────────────────────────────────────────
# Image downloader
# ──────────────────────────────────────────────────────────────────────────────
async def download_image(
    client: TelegramClient,
    msg: Message,
    channel_name: str,
) -> Path | None:
    """
    Download the photo attached to *msg* and save it to:
        data/raw/images/<channel_name>/<message_id>.jpg

    Returns the saved path, or None if the download failed.
    """
    image_dir = IMAGES_DIR / channel_name
    image_dir.mkdir(parents=True, exist_ok=True)
    image_path = image_dir / f"{msg.id}.jpg"

    if image_path.exists():
        logger.debug("Image already exists, skipping: %s", image_path)
        return image_path

    try:
        await client.download_media(msg.media, file=str(image_path))
        logger.debug("Downloaded image → %s", image_path)
        return image_path
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to download image for msg %d: %s", msg.id, exc)
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Per-channel scraper
# ──────────────────────────────────────────────────────────────────────────────
async def scrape_channel(
    client: TelegramClient,
    channel: str,
    limit: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    """
    Scrape messages from a single Telegram channel.

    Parameters
    ----------
    client     : authenticated TelegramClient
    channel    : channel username or invite link
    limit      : maximum number of messages to fetch (None = all)
    start_date : only include messages on or after this UTC date
    end_date   : only include messages on or before this UTC date

    Returns
    -------
    A summary dict with counts for messages scraped and images downloaded.
    """
    channel_name = _sanitise_channel_name(channel)
    summary: dict[str, Any] = {
        "channel": channel,
        "channel_name": channel_name,
        "messages_scraped": 0,
        "images_downloaded": 0,
        "errors": [],
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
    }

    logger.info("── Starting channel: @%s", channel_name)

    try:
        entity = await client.get_entity(channel)
    except (UsernameInvalidError, UsernameNotOccupiedError) as exc:
        msg = f"Channel not found: {channel} — {exc}"
        logger.error(msg)
        summary["errors"].append(msg)
        summary["finished_at"] = datetime.now(timezone.utc).isoformat()
        return summary
    except ChannelPrivateError as exc:
        msg = f"Channel is private, cannot access: {channel} — {exc}"
        logger.error(msg)
        summary["errors"].append(msg)
        summary["finished_at"] = datetime.now(timezone.utc).isoformat()
        return summary

    # iter_messages yields newest-first by default; we iterate the full history
    async for msg in client.iter_messages(entity, limit=limit):
        # Type guard — skip non-message objects (e.g. MessageService)
        if not isinstance(msg, Message):
            continue

        msg_date = _message_date(msg)

        # Date range filtering
        if end_date and msg_date > end_date:
            continue
        if start_date and msg_date < start_date:
            # Messages are in reverse-chronological order; once we're before
            # start_date we can stop entirely.
            break

        record = _extract_message_fields(msg)
        record["channel"] = channel_name

        # Persist to the partitioned data lake
        partition_path = _partition_path(channel_name, msg_date)
        _append_to_partition(partition_path, record)
        summary["messages_scraped"] += 1

        # Download photo if present
        if isinstance(msg.media, MessageMediaPhoto):
            image_path = await download_image(client, msg, channel_name)
            if image_path:
                summary["images_downloaded"] += 1
                record["local_image_path"] = str(image_path)

        # Telethon auto-handles rate limiting internally but we log it anyway
        await asyncio.sleep(0.05)  # small courtesy delay

    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    logger.info(
        "── Finished @%s | messages=%d, images=%d",
        channel_name,
        summary["messages_scraped"],
        summary["images_downloaded"],
    )
    return summary


# ──────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────────────────────────────────────────
async def run_scraper(
    channels: list[str],
    limit: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> None:
    """
    Authenticate once and scrape all requested channels sequentially.
    A scrape_summary.json is written to logs/ on completion.
    """
    load_dotenv()

    api_id_raw = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    phone = os.getenv("TELEGRAM_PHONE")

    if not api_id_raw or not api_hash:
        logger.critical(
            "TELEGRAM_API_ID and TELEGRAM_API_HASH must be set in .env"
        )
        sys.exit(1)

    api_id = int(api_id_raw)
    session_file = str(SESSION_DIR / "telegram_session")

    logger.info("Connecting to Telegram …")
    async with TelegramClient(session_file, api_id, api_hash) as client:
        if not await client.is_user_authorized():
            logger.info("Not authorised — sending code to %s", phone)
            await client.send_code_request(phone)
            code = input("Enter the Telegram verification code: ").strip()
            await client.sign_in(phone, code)
            logger.info("Signed in successfully.")

        summaries: list[dict[str, Any]] = []
        for channel in channels:
            try:
                summary = await scrape_channel(
                    client,
                    channel,
                    limit=limit,
                    start_date=start_date,
                    end_date=end_date,
                )
            except FloodWaitError as exc:
                wait_seconds = exc.seconds
                logger.warning(
                    "FloodWaitError for @%s — sleeping %ds", channel, wait_seconds
                )
                await asyncio.sleep(wait_seconds)
                # Retry once after the wait
                summary = await scrape_channel(
                    client,
                    channel,
                    limit=limit,
                    start_date=start_date,
                    end_date=end_date,
                )
            summaries.append(summary)

        # Write run summary to logs/
        summary_path = LOGS_DIR / f"scrape_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with summary_path.open("w", encoding="utf-8") as fh:
            json.dump(
                {
                    "run_at": datetime.now(timezone.utc).isoformat(),
                    "channels_requested": channels,
                    "limit_per_channel": limit,
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None,
                    "summaries": summaries,
                },
                fh,
                indent=2,
                ensure_ascii=False,
            )
        logger.info("Run summary saved → %s", summary_path)

        total_messages = sum(s["messages_scraped"] for s in summaries)
        total_images = sum(s["images_downloaded"] for s in summaries)
        logger.info(
            "All done. Total messages=%d, Total images=%d across %d channel(s).",
            total_messages,
            total_images,
            len(channels),
        )


# ──────────────────────────────────────────────────────────────────────────────
# CLI entry-point
# ──────────────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Ethiopian medical Telegram channels into a raw data lake."
    )
    parser.add_argument(
        "--channels",
        nargs="+",
        default=DEFAULT_CHANNELS,
        metavar="CHANNEL",
        help="One or more channel usernames (without @). Defaults to the built-in list.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Maximum messages to fetch per channel (default: unlimited).",
    )
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help="Only include messages on or after this date (UTC).",
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help="Only include messages on or before this date (UTC).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(
        run_scraper(
            channels=args.channels,
            limit=args.limit,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    )
