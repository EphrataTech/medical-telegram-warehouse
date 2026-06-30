"""
src/yolo_detect.py
──────────────────
YOLOv8 object detection pipeline for Telegram channel images.

For every image downloaded by the scraper (data/raw/images/**/*.jpg) this
script:
  1. Runs YOLOv8n inference and records every detected object + confidence.
  2. Classifies the image into one of four categories:
       promotional     — person AND product-like object detected
       product_display — product-like object, no person
       lifestyle       — person only, no product-like object
       other           — neither detected
  3. Saves a flat CSV of per-detection rows to data/yolo_detections.csv.
  4. Loads the CSV into PostgreSQL table raw.yolo_detections (idempotent).

Usage
-----
    python src/yolo_detect.py                        # process all images
    python src/yolo_detect.py --channel CheMed123    # single channel
    python src/yolo_detect.py --no-db                # skip DB load
    python src/yolo_detect.py --conf 0.30            # confidence threshold
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
IMAGES_DIR = ROOT_DIR / "data" / "raw" / "images"
DATA_DIR = ROOT_DIR / "data"
CSV_PATH = DATA_DIR / "yolo_detections.csv"
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
        logging.FileHandler(LOGS_DIR / "yolo_detect.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("yolo_detect")

# ──────────────────────────────────────────────────────────────────────────────
# YOLO product-like object labels (COCO classes that map to medical/retail items)
# ──────────────────────────────────────────────────────────────────────────────
PRODUCT_LABELS: frozenset[str] = frozenset({
    "bottle", "cup", "bowl", "vase",
    "book", "scissors", "toothbrush",
    "handbag", "backpack", "suitcase",
    "box", "package",                   # not in COCO but added for clarity
})

PERSON_LABEL = "person"

CSV_FIELDNAMES = [
    "image_path",
    "channel",
    "message_id",
    "detected_class",
    "confidence",
    "image_category",
    "bbox_x1", "bbox_y1", "bbox_x2", "bbox_y2",
    "detected_at",
]

# ──────────────────────────────────────────────────────────────────────────────
# Classification
# ──────────────────────────────────────────────────────────────────────────────
def classify_image(labels: list[str]) -> str:
    """
    Map the set of detected labels for one image to a business category.

    promotional     — person + product both present
    product_display — product present, no person
    lifestyle       — person present, no product
    other           — neither
    """
    label_set = {lbl.lower() for lbl in labels}
    has_person  = PERSON_LABEL in label_set
    has_product = bool(label_set & PRODUCT_LABELS)

    if has_person and has_product:
        return "promotional"
    if has_product:
        return "product_display"
    if has_person:
        return "lifestyle"
    return "other"


# ──────────────────────────────────────────────────────────────────────────────
# Image collection
# ──────────────────────────────────────────────────────────────────────────────
def collect_images(channel_filter: str | None = None) -> list[Path]:
    """Return all .jpg files under data/raw/images/, optionally filtered."""
    images: list[Path] = []
    if not IMAGES_DIR.exists():
        log.warning("Images directory does not exist: %s", IMAGES_DIR)
        return images

    for channel_dir in sorted(IMAGES_DIR.iterdir()):
        if not channel_dir.is_dir():
            continue
        if channel_filter and channel_dir.name.lower() != channel_filter.lower():
            continue
        images.extend(sorted(channel_dir.glob("*.jpg")))

    log.info("Found %d image(s) to process.", len(images))
    return images


# ──────────────────────────────────────────────────────────────────────────────
# Core detection loop
# ──────────────────────────────────────────────────────────────────────────────
def run_detection(
    images: list[Path],
    conf_threshold: float = 0.25,
) -> list[dict]:
    """
    Run YOLOv8n on each image and return a flat list of detection dicts.
    One dict per detected object (multiple dicts per image when >1 object found).
    If an image has no detections above the threshold a single summary row is
    still written so the image appears in the output with image_category='other'.
    """
    # Lazy-import so the module is importable even without ultralytics installed
    try:
        from ultralytics import YOLO  # type: ignore
    except ImportError:
        log.critical(
            "ultralytics is not installed. Run: pip install ultralytics"
        )
        sys.exit(1)

    model = YOLO("yolov8n.pt")   # downloads ~6 MB on first run
    log.info("YOLOv8n model loaded.")

    rows: list[dict] = []
    detected_at = datetime.now(timezone.utc).isoformat()

    for idx, img_path in enumerate(images, 1):
        channel   = img_path.parent.name
        # message_id is the stem of the filename (e.g. "12345" from "12345.jpg")
        try:
            message_id = int(img_path.stem)
        except ValueError:
            message_id = None

        try:
            results = model(str(img_path), conf=conf_threshold, verbose=False)
        except Exception as exc:          # noqa: BLE001
            log.warning("Inference failed for %s: %s", img_path, exc)
            continue

        result     = results[0]
        names      = model.names          # {class_id: label}
        boxes      = result.boxes

        if boxes is None or len(boxes) == 0:
            # No detections — write one placeholder row
            rows.append({
                "image_path":     str(img_path.relative_to(ROOT_DIR)),
                "channel":        channel,
                "message_id":     message_id,
                "detected_class": None,
                "confidence":     None,
                "image_category": "other",
                "bbox_x1": None, "bbox_y1": None,
                "bbox_x2": None, "bbox_y2": None,
                "detected_at":    detected_at,
            })
            continue

        # Build per-detection rows first, then assign category from all labels
        all_labels: list[str] = []
        detections: list[dict] = []

        for box in boxes:
            cls_id     = int(box.cls[0].item())
            label      = names.get(cls_id, str(cls_id))
            confidence = round(float(box.conf[0].item()), 4)
            x1, y1, x2, y2 = (round(float(v), 2) for v in box.xyxy[0].tolist())

            all_labels.append(label)
            detections.append({
                "image_path":     str(img_path.relative_to(ROOT_DIR)),
                "channel":        channel,
                "message_id":     message_id,
                "detected_class": label,
                "confidence":     confidence,
                "image_category": None,           # filled below
                "bbox_x1": x1, "bbox_y1": y1,
                "bbox_x2": x2, "bbox_y2": y2,
                "detected_at":    detected_at,
            })

        category = classify_image(all_labels)
        for det in detections:
            det["image_category"] = category

        rows.extend(detections)

        if idx % 50 == 0:
            log.info("  Processed %d / %d images …", idx, len(images))

    log.info("Detection complete. Total detection rows: %d", len(rows))
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# CSV writer
# ──────────────────────────────────────────────────────────────────────────────
def save_csv(rows: list[dict], path: Path = CSV_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    log.info("Results saved → %s  (%d rows)", path, len(rows))


# ──────────────────────────────────────────────────────────────────────────────
# PostgreSQL loader
# ──────────────────────────────────────────────────────────────────────────────
CREATE_YOLO_TABLE = """
CREATE TABLE IF NOT EXISTS raw.yolo_detections (
    id              SERIAL PRIMARY KEY,
    image_path      TEXT        NOT NULL,
    channel         TEXT        NOT NULL,
    message_id      BIGINT,
    detected_class  TEXT,
    confidence      NUMERIC(6,4),
    image_category  TEXT        NOT NULL DEFAULT 'other',
    bbox_x1         NUMERIC(10,2),
    bbox_y1         NUMERIC(10,2),
    bbox_x2         NUMERIC(10,2),
    bbox_y2         NUMERIC(10,2),
    detected_at     TIMESTAMPTZ NOT NULL,
    UNIQUE (image_path, detected_class, bbox_x1, bbox_y1)
);
"""

UPSERT_YOLO = """
INSERT INTO raw.yolo_detections
    (image_path, channel, message_id, detected_class, confidence,
     image_category, bbox_x1, bbox_y1, bbox_x2, bbox_y2, detected_at)
VALUES %s
ON CONFLICT (image_path, detected_class, bbox_x1, bbox_y1) DO UPDATE SET
    confidence     = EXCLUDED.confidence,
    image_category = EXCLUDED.image_category,
    detected_at    = EXCLUDED.detected_at;
"""


def load_to_postgres(rows: list[dict]) -> None:
    load_dotenv()
    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            cur.execute(CREATE_YOLO_TABLE)
        conn.commit()

        tuples = [
            (
                r["image_path"], r["channel"], r["message_id"],
                r["detected_class"], r["confidence"], r["image_category"],
                r["bbox_x1"], r["bbox_y1"], r["bbox_x2"], r["bbox_y2"],
                r["detected_at"],
            )
            for r in rows
        ]

        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, UPSERT_YOLO, tuples, page_size=500)
        conn.commit()
        log.info("Upserted %d detection rows into raw.yolo_detections.", len(tuples))
    except Exception:
        conn.rollback()
        log.exception("DB load failed — rolled back.")
        raise
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run YOLOv8 object detection on scraped Telegram images."
    )
    parser.add_argument(
        "--channel",
        type=str, default=None, metavar="CHANNEL_NAME",
        help="Process only a specific channel's images.",
    )
    parser.add_argument(
        "--conf",
        type=float, default=0.25, metavar="THRESHOLD",
        help="Minimum confidence score for detections (default: 0.25).",
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Skip loading results into PostgreSQL.",
    )
    parser.add_argument(
        "--csv-path",
        type=Path, default=CSV_PATH, metavar="PATH",
        help=f"Output CSV path (default: {CSV_PATH}).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    load_dotenv()

    images = collect_images(channel_filter=args.channel)
    if not images:
        log.warning("No images found. Run the scraper first.")
        sys.exit(0)

    rows = run_detection(images, conf_threshold=args.conf)
    save_csv(rows, path=args.csv_path)

    if not args.no_db:
        load_to_postgres(rows)
