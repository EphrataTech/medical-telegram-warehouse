"""
pipeline/assets.py
──────────────────
Four Dagster software-defined assets that represent each stage of the
Medical Telegram Warehouse pipeline:

  1. telegram_raw_data         — scrape channels → JSON data lake
  2. postgres_raw_messages     — load JSON → raw.telegram_messages
  3. dbt_warehouse_models      — run dbt staging + mart models
  4. yolo_image_detections     — run YOLO on downloaded images
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetMaterialization,
    MetadataValue,
    Output,
    asset,
)

from pipeline.resources import DbtCliResource, PipelineConfig, PostgresResource

ROOT_DIR = Path(__file__).resolve().parent.parent
MESSAGES_DIR = ROOT_DIR / "data" / "raw" / "telegram_messages"
IMAGES_DIR = ROOT_DIR / "data" / "raw" / "images"


# ──────────────────────────────────────────────────────────────────────────────
# Asset 1 — Scrape Telegram channels
# ──────────────────────────────────────────────────────────────────────────────
@asset(
    name="telegram_raw_data",
    group_name="ingestion",
    description=(
        "Scrapes messages and images from configured Telegram channels. "
        "Writes JSON-lines to data/raw/telegram_messages/YYYY-MM-DD/<channel>.json "
        "and photos to data/raw/images/<channel>/<message_id>.jpg."
    ),
    metadata={
        "owner": "data-engineering",
        "tier": "bronze",
    },
)
def telegram_raw_data(
    context: AssetExecutionContext,
    config: PipelineConfig,
) -> Output[dict]:
    """Run src/scraper.py as a subprocess and report partition counts."""

    cmd = [sys.executable, str(ROOT_DIR / "src" / "scraper.py")]

    if config.channels:
        cmd += ["--channels"] + config.channels
    if config.scrape_limit and config.scrape_limit > 0:
        cmd += ["--limit", str(config.scrape_limit)]
    if config.scrape_start_date:
        cmd += ["--start-date", config.scrape_start_date]
    if config.scrape_end_date:
        cmd += ["--end-date", config.scrape_end_date]

    context.log.info("Running scraper: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=True, text=True, cwd=str(ROOT_DIR))

    # Count partition files written
    partition_files = list(MESSAGES_DIR.rglob("*.json"))
    image_files = list(IMAGES_DIR.rglob("*.jpg"))

    context.log.info(
        "Scrape complete. Partition files: %d | Images: %d",
        len(partition_files),
        len(image_files),
    )

    return Output(
        value={
            "partition_files": len(partition_files),
            "image_files": len(image_files),
            "channels": config.channels,
        },
        metadata={
            "partition_files": MetadataValue.int(len(partition_files)),
            "image_files": MetadataValue.int(len(image_files)),
            "channels_scraped": MetadataValue.text(", ".join(config.channels)),
            "data_lake_path": MetadataValue.path(str(MESSAGES_DIR)),
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# Asset 2 — Load raw data into PostgreSQL
# ──────────────────────────────────────────────────────────────────────────────
@asset(
    name="postgres_raw_messages",
    group_name="ingestion",
    deps=[telegram_raw_data],
    description=(
        "Loads every JSON-lines partition from the data lake into "
        "raw.telegram_messages in PostgreSQL. Uses ON CONFLICT upsert — "
        "safe to re-run."
    ),
    metadata={
        "owner": "data-engineering",
        "tier": "bronze",
        "target_table": "raw.telegram_messages",
    },
)
def postgres_raw_messages(
    context: AssetExecutionContext,
    config: PipelineConfig,
    postgres: PostgresResource,
) -> Output[dict]:
    """Call scripts/load_raw.py to bulk-upsert all partitions."""

    cmd = [
        sys.executable,
        str(ROOT_DIR / "scripts" / "load_raw.py"),
        "--batch-size", str(config.loader_batch_size),
    ]

    context.log.info("Running raw loader: %s", " ".join(cmd))
    subprocess.run(cmd, check=True, text=True, cwd=str(ROOT_DIR))

    # Query the count of loaded rows from PG for metadata
    row_count: int | None = None
    try:
        conn = postgres.get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw.telegram_messages;")
            row_count = cur.fetchone()[0]
        conn.close()
        context.log.info("raw.telegram_messages row count: %d", row_count)
    except Exception as exc:
        context.log.warning("Could not query row count: %s", exc)

    return Output(
        value={"row_count": row_count},
        metadata={
            "raw_row_count": MetadataValue.int(row_count or 0),
            "target_table": MetadataValue.text("raw.telegram_messages"),
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# Asset 3 — Run dbt transformations
# ──────────────────────────────────────────────────────────────────────────────
@asset(
    name="dbt_warehouse_models",
    group_name="transformation",
    deps=[postgres_raw_messages],
    description=(
        "Executes all dbt models in dependency order: "
        "stg_telegram_messages → dim_channels → dim_dates → fct_messages → "
        "fct_image_detections. Then runs dbt test to validate the output."
    ),
    metadata={
        "owner": "analytics-engineering",
        "tier": "silver/gold",
        "dbt_project": "medical_warehouse",
    },
)
def dbt_warehouse_models(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    postgres: PostgresResource,
) -> Output[dict]:
    """Run dbt run then dbt test; surface model and test counts as metadata."""

    context.log.info("Running: dbt run")
    dbt.run("run")
    context.log.info("dbt run complete.")

    context.log.info("Running: dbt test")
    try:
        dbt.run("test")
        tests_passed = True
        context.log.info("dbt test passed.")
    except subprocess.CalledProcessError as exc:
        context.log.error("dbt test failures detected: %s", exc)
        tests_passed = False
        # Surface failure but don't abort — let the job log show detail
        raise

    # Count mart rows for metadata
    mart_counts: dict[str, int] = {}
    tables = ["fct_messages", "dim_channels", "dim_dates"]
    try:
        conn = postgres.get_connection()
        with conn.cursor() as cur:
            for tbl in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM marts.{tbl};")
                    mart_counts[tbl] = cur.fetchone()[0]
                except Exception:
                    mart_counts[tbl] = -1
        conn.close()
    except Exception as exc:
        context.log.warning("Could not query mart counts: %s", exc)

    context.log.info("Mart row counts: %s", mart_counts)

    return Output(
        value={"mart_counts": mart_counts, "tests_passed": tests_passed},
        metadata={
            **{f"rows_{k}": MetadataValue.int(v) for k, v in mart_counts.items()},
            "dbt_tests_passed": MetadataValue.bool(tests_passed),
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# Asset 4 — Run YOLO object detection
# ──────────────────────────────────────────────────────────────────────────────
@asset(
    name="yolo_image_detections",
    group_name="enrichment",
    deps=[dbt_warehouse_models],
    description=(
        "Runs YOLOv8n object detection on all images in data/raw/images/. "
        "Classifies images as: promotional | product_display | lifestyle | other. "
        "Writes results to data/yolo_detections.csv and loads into "
        "raw.yolo_detections, then rebuilds fct_image_detections via dbt."
    ),
    metadata={
        "owner": "data-science",
        "tier": "gold",
        "model": "yolov8n",
        "target_table": "raw.yolo_detections",
    },
)
def yolo_image_detections(
    context: AssetExecutionContext,
    config: PipelineConfig,
    dbt: DbtCliResource,
    postgres: PostgresResource,
) -> Output[dict]:
    """Run YOLO detection, load results, rebuild fct_image_detections."""

    image_files = list(IMAGES_DIR.rglob("*.jpg"))
    if not image_files:
        context.log.warning(
            "No images found in %s — skipping YOLO. "
            "Run the scraper first to download images.",
            IMAGES_DIR,
        )
        return Output(
            value={"detection_rows": 0, "images_processed": 0},
            metadata={
                "images_processed": MetadataValue.int(0),
                "detection_rows": MetadataValue.int(0),
                "skipped": MetadataValue.bool(True),
            },
        )

    context.log.info("Found %d images. Starting YOLO inference …", len(image_files))

    cmd = [
        sys.executable,
        str(ROOT_DIR / "src" / "yolo_detect.py"),
        "--conf", str(config.yolo_conf_threshold),
    ]

    subprocess.run(cmd, check=True, text=True, cwd=str(ROOT_DIR))

    # Query detection row count
    detection_count: int = 0
    try:
        conn = postgres.get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw.yolo_detections;")
            detection_count = cur.fetchone()[0]
        conn.close()
        context.log.info("raw.yolo_detections row count: %d", detection_count)
    except Exception as exc:
        context.log.warning("Could not query detection count: %s", exc)

    # Rebuild the fct_image_detections mart model
    context.log.info("Rebuilding fct_image_detections …")
    dbt.run("run", "--select", "fct_image_detections")

    return Output(
        value={
            "images_processed": len(image_files),
            "detection_rows": detection_count,
        },
        metadata={
            "images_processed": MetadataValue.int(len(image_files)),
            "detection_rows": MetadataValue.int(detection_count),
            "csv_path": MetadataValue.path(
                str(ROOT_DIR / "data" / "yolo_detections.csv")
            ),
            "conf_threshold": MetadataValue.float(config.yolo_conf_threshold),
        },
    )
