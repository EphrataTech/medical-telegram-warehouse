"""
scripts/load_raw.py
───────────────────
Reads every JSON-lines file from the partitioned data lake
(data/raw/telegram_messages/YYYY-MM-DD/<channel>.json) and upserts
all records into the raw.telegram_messages table in PostgreSQL.

Usage
-----
    python scripts/load_raw.py                 # load everything
    python scripts/load_raw.py --date 2024-06-15   # single partition
    python scripts/load_raw.py --channel CheMed123  # single channel
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
MESSAGES_DIR = ROOT_DIR / "data" / "raw" / "telegram_messages"
LOGS_DIR = ROOT_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "load_raw.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("load_raw")

# ──────────────────────────────────────────────────────────────────────────────
# DDL — raw schema + table
# ──────────────────────────────────────────────────────────────────────────────
CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS raw;"

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS raw.telegram_messages (
    message_id        BIGINT,
    channel           TEXT,
    date              TIMESTAMPTZ,
    text              TEXT,
    views             INTEGER,
    forwards          INTEGER,
    has_photo         BOOLEAN,
    photo_id          BIGINT,
    media_type        TEXT,
    reply_to_msg_id   BIGINT,
    edit_date         TIMESTAMPTZ,
    post_author       TEXT,
    grouped_id        BIGINT,
    out               BOOLEAN,
    mentioned         BOOLEAN,
    pinned            BOOLEAN,
    scraped_at        TIMESTAMPTZ,
    -- composite PK prevents duplicate loads
    PRIMARY KEY (channel, message_id)
);
"""

UPSERT_SQL = """
INSERT INTO raw.telegram_messages (
    message_id, channel, date, text, views, forwards,
    has_photo, photo_id, media_type,
    reply_to_msg_id, edit_date, post_author, grouped_id,
    out, mentioned, pinned, scraped_at
) VALUES %s
ON CONFLICT (channel, message_id) DO UPDATE SET
    text            = EXCLUDED.text,
    views           = EXCLUDED.views,
    forwards        = EXCLUDED.forwards,
    has_photo       = EXCLUDED.has_photo,
    photo_id        = EXCLUDED.photo_id,
    media_type      = EXCLUDED.media_type,
    edit_date       = EXCLUDED.edit_date,
    post_author     = EXCLUDED.post_author,
    grouped_id      = EXCLUDED.grouped_id,
    out             = EXCLUDED.out,
    mentioned       = EXCLUDED.mentioned,
    pinned          = EXCLUDED.pinned,
    scraped_at      = EXCLUDED.scraped_at;
"""


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _get_connection() -> psycopg2.extensions.connection:
    load_dotenv()
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _parse_record(raw: dict) -> tuple:
    """Convert a raw JSON record dict into an ordered tuple for psycopg2."""
    media = raw.get("media") or {}
    return (
        raw.get("message_id"),
        raw.get("channel"),
        raw.get("date"),
        raw.get("text") or "",
        raw.get("views"),
        raw.get("forwards"),
        media.get("has_photo", False),
        media.get("photo_id"),
        media.get("type"),
        raw.get("reply_to_msg_id"),
        raw.get("edit_date"),
        raw.get("post_author"),
        raw.get("grouped_id"),
        raw.get("out"),
        raw.get("mentioned"),
        raw.get("pinned"),
        raw.get("scraped_at"),
    )


def _collect_files(
    filter_date: date | None = None,
    filter_channel: str | None = None,
) -> list[Path]:
    """Return all matching .json partition files from the data lake."""
    files: list[Path] = []
    for date_dir in sorted(MESSAGES_DIR.iterdir()):
        if not date_dir.is_dir():
            continue
        if filter_date and date_dir.name != filter_date.isoformat():
            continue
        for json_file in sorted(date_dir.glob("*.json")):
            if filter_channel and json_file.stem.lower() != filter_channel.lower():
                continue
            files.append(json_file)
    return files


# ──────────────────────────────────────────────────────────────────────────────
# Core loader
# ──────────────────────────────────────────────────────────────────────────────
def load_raw(
    filter_date: date | None = None,
    filter_channel: str | None = None,
    batch_size: int = 500,
) -> None:
    files = _collect_files(filter_date, filter_channel)
    if not files:
        log.warning("No JSON files found matching the given filters.")
        return

    log.info("Found %d partition file(s) to load.", len(files))

    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_SCHEMA)
            cur.execute(CREATE_TABLE)
        conn.commit()
        log.info("Schema and table ensured.")

        total_inserted = 0

        for json_file in files:
            records: list[tuple] = []
            skipped = 0

            with json_file.open(encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw = json.loads(line)
                        records.append(_parse_record(raw))
                    except (json.JSONDecodeError, KeyError) as exc:
                        log.warning("Skipping malformed record in %s: %s", json_file, exc)
                        skipped += 1

            # Batch upsert
            inserted = 0
            with conn.cursor() as cur:
                for i in range(0, len(records), batch_size):
                    batch = records[i : i + batch_size]
                    psycopg2.extras.execute_values(cur, UPSERT_SQL, batch)
                    inserted += len(batch)
            conn.commit()

            total_inserted += inserted
            log.info(
                "  %-45s  loaded=%d  skipped=%d",
                str(json_file.relative_to(ROOT_DIR)),
                inserted,
                skipped,
            )

        log.info("Done. Total records upserted: %d", total_inserted)

    except Exception:
        conn.rollback()
        log.exception("Load failed — transaction rolled back.")
        sys.exit(1)
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load raw Telegram JSON files into raw.telegram_messages."
    )
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help="Only load a specific date partition.",
    )
    parser.add_argument(
        "--channel",
        type=str,
        default=None,
        metavar="CHANNEL_NAME",
        help="Only load a specific channel.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        metavar="N",
        help="Number of rows per INSERT batch (default: 500).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    load_raw(
        filter_date=args.date,
        filter_channel=args.channel,
        batch_size=args.batch_size,
    )
