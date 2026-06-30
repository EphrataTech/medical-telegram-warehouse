"""
tests/test_scraper.py
─────────────────────
Unit tests for the pure helper functions in src/scraper.py.
No live Telegram connection is required.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Make sure src/ is importable when running from the project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from scraper import (
    _append_to_partition,
    _extract_message_fields,
    _message_date,
    _partition_path,
    _sanitise_channel_name,
)


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────
def _make_message(
    msg_id: int = 42,
    text: str = "Test message",
    views: int = 100,
    forwards: int = 5,
    media=None,
    date_: datetime | None = None,
) -> MagicMock:
    """Return a minimal mock that mimics a Telethon Message object."""
    msg = MagicMock()
    msg.id = msg_id
    msg.message = text
    msg.views = views
    msg.forwards = forwards
    msg.media = media
    msg.date = date_ or datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
    msg.reply_to = None
    msg.edit_date = None
    msg.post_author = None
    msg.grouped_id = None
    msg.out = False
    msg.mentioned = False
    msg.pinned = False
    return msg


# ──────────────────────────────────────────────────────────────────────────────
# _sanitise_channel_name
# ──────────────────────────────────────────────────────────────────────────────
class TestSanitiseChannelName:
    def test_strips_at_symbol(self):
        assert _sanitise_channel_name("@CheMed123") == "CheMed123"

    def test_strips_leading_slash(self):
        assert _sanitise_channel_name("/lobelia4cosmetics") == "lobelia4cosmetics"

    def test_replaces_forward_slash(self):
        assert _sanitise_channel_name("a/b") == "a_b"

    def test_plain_name_unchanged(self):
        assert _sanitise_channel_name("tikvahpharma") == "tikvahpharma"

    def test_strips_spaces(self):
        assert _sanitise_channel_name("  CheMed123  ") == "CheMed123"


# ──────────────────────────────────────────────────────────────────────────────
# _message_date
# ──────────────────────────────────────────────────────────────────────────────
class TestMessageDate:
    def test_returns_utc_date(self):
        msg = _make_message(date_=datetime(2024, 3, 21, 23, 59, tzinfo=timezone.utc))
        assert _message_date(msg) == date(2024, 3, 21)

    def test_converts_to_utc(self):
        from datetime import timedelta

        tz_plus3 = timezone(timedelta(hours=3))
        # 2024-06-01 01:00 +03:00  →  2024-05-31 22:00 UTC
        aware_dt = datetime(2024, 6, 1, 1, 0, 0, tzinfo=tz_plus3)
        msg = _make_message(date_=aware_dt)
        assert _message_date(msg) == date(2024, 5, 31)


# ──────────────────────────────────────────────────────────────────────────────
# _partition_path
# ──────────────────────────────────────────────────────────────────────────────
class TestPartitionPath:
    def test_correct_structure(self, tmp_path, monkeypatch):
        # Redirect MESSAGES_DIR to tmp_path so nothing is written to disk
        import scraper as scraper_module

        monkeypatch.setattr(scraper_module, "MESSAGES_DIR", tmp_path)
        result = _partition_path.__wrapped__(
            "tikvahpharma", date(2024, 6, 15)
        ) if hasattr(_partition_path, "__wrapped__") else None

        # Call directly using the module-level MESSAGES_DIR override
        with patch.object(scraper_module, "MESSAGES_DIR", tmp_path):
            from scraper import _partition_path as pp

            path = pp("tikvahpharma", date(2024, 6, 15))

        assert path == tmp_path / "2024-06-15" / "tikvahpharma.json"

    def test_creates_directory(self, tmp_path, monkeypatch):
        import scraper as scraper_module

        with patch.object(scraper_module, "MESSAGES_DIR", tmp_path):
            from scraper import _partition_path as pp

            path = pp("CheMed123", date(2024, 1, 1))

        assert path.parent.exists()


# ──────────────────────────────────────────────────────────────────────────────
# _extract_message_fields
# ──────────────────────────────────────────────────────────────────────────────
class TestExtractMessageFields:
    def test_required_keys_present(self):
        msg = _make_message()
        record = _extract_message_fields(msg)
        for key in ("message_id", "date", "text", "views", "forwards", "media"):
            assert key in record, f"Missing key: {key}"

    def test_no_media(self):
        msg = _make_message(media=None)
        record = _extract_message_fields(msg)
        assert record["media"] is None

    def test_photo_media(self):
        from telethon.tl.types import MessageMediaPhoto, Photo

        photo = MagicMock(spec=Photo)
        photo.id = 99999
        media = MagicMock(spec=MessageMediaPhoto)
        media.photo = photo

        msg = _make_message(media=media)
        record = _extract_message_fields(msg)

        assert record["media"] is not None
        assert record["media"]["has_photo"] is True
        assert record["media"]["photo_id"] == 99999

    def test_text_content(self):
        msg = _make_message(text="Buy medicine now")
        record = _extract_message_fields(msg)
        assert record["text"] == "Buy medicine now"

    def test_empty_text_becomes_empty_string(self):
        msg = _make_message(text=None)
        msg.message = None
        record = _extract_message_fields(msg)
        assert record["text"] == ""

    def test_scraped_at_is_present(self):
        msg = _make_message()
        record = _extract_message_fields(msg)
        assert "scraped_at" in record
        # Should be a valid ISO timestamp
        datetime.fromisoformat(record["scraped_at"])


# ──────────────────────────────────────────────────────────────────────────────
# _append_to_partition
# ──────────────────────────────────────────────────────────────────────────────
class TestAppendToPartition:
    def test_creates_file_and_writes_json(self, tmp_path):
        partition = tmp_path / "2024-06-15" / "test_channel.json"
        partition.parent.mkdir(parents=True)

        record = {"message_id": 1, "text": "hello"}
        _append_to_partition(partition, record)

        lines = partition.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 1
        assert json.loads(lines[0]) == record

    def test_appends_multiple_records(self, tmp_path):
        partition = tmp_path / "test.json"
        records = [{"message_id": i, "text": f"msg {i}"} for i in range(5)]

        for r in records:
            _append_to_partition(partition, r)

        lines = partition.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 5
        for i, line in enumerate(lines):
            assert json.loads(line)["message_id"] == i

    def test_unicode_preserved(self, tmp_path):
        partition = tmp_path / "unicode.json"
        record = {"text": "የጤና ምርት"}  # Amharic text
        _append_to_partition(partition, record)

        content = json.loads(partition.read_text(encoding="utf-8").strip())
        assert content["text"] == "የጤና ምርት"
