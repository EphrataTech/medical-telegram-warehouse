"""
pipeline/definitions.py
───────────────────────
Top-level Dagster Definitions object.
Wires together all assets, resources, jobs, schedules, and sensors.
"""

from __future__ import annotations

import os

from dagster import Definitions, EnvVar

from pipeline.assets import (
    dbt_warehouse_models,
    postgres_raw_messages,
    telegram_raw_data,
    yolo_image_detections,
)
from pipeline.jobs import (
    daily_schedule,
    enrichment_job,
    ingestion_job,
    medical_pipeline_job,
    pipeline_failure_sensor,
    transformation_job,
)
from pipeline.resources import DbtCliResource, PipelineConfig, PostgresResource


defs = Definitions(
    # ── Assets ───────────────────────────────────────────────────────────────
    assets=[
        telegram_raw_data,
        postgres_raw_messages,
        dbt_warehouse_models,
        yolo_image_detections,
    ],

    # ── Jobs ─────────────────────────────────────────────────────────────────
    jobs=[
        medical_pipeline_job,
        ingestion_job,
        transformation_job,
        enrichment_job,
    ],

    # ── Schedules & Sensors ──────────────────────────────────────────────────
    schedules=[daily_schedule],
    sensors=[pipeline_failure_sensor],

    # ── Resources ────────────────────────────────────────────────────────────
    resources={
        "postgres": PostgresResource(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "medical_warehouse"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
        ),
        "dbt": DbtCliResource(
            target=os.getenv("DBT_TARGET", "dev"),
        ),
        "config": PipelineConfig(),
    },
)
