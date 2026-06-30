"""
pipeline/jobs.py
────────────────
Defines:
  medical_pipeline_job  — the full four-stage job
  daily_schedule        — runs at 02:00 UTC every day
  failure_sensor        — logs (and optionally alerts) on any run failure
"""

from __future__ import annotations

from dagster import (
    DefaultScheduleStatus,
    RunFailureSensorContext,
    RunRequest,
    ScheduleDefinition,
    define_asset_job,
    run_failure_sensor,
)

from pipeline.assets import (
    dbt_warehouse_models,
    postgres_raw_messages,
    telegram_raw_data,
    yolo_image_detections,
)


# ──────────────────────────────────────────────────────────────────────────────
# Job — full pipeline
# ──────────────────────────────────────────────────────────────────────────────
medical_pipeline_job = define_asset_job(
    name="medical_pipeline_job",
    selection=[
        telegram_raw_data,
        postgres_raw_messages,
        dbt_warehouse_models,
        yolo_image_detections,
    ],
    description=(
        "End-to-end Medical Telegram Warehouse pipeline: "
        "scrape → load → transform (dbt) → enrich (YOLO)."
    ),
    tags={"team": "data-engineering", "pipeline": "medical_warehouse"},
)


# ──────────────────────────────────────────────────────────────────────────────
# Partial jobs — useful for ad-hoc backfills / debugging
# ──────────────────────────────────────────────────────────────────────────────
ingestion_job = define_asset_job(
    name="ingestion_job",
    selection=[telegram_raw_data, postgres_raw_messages],
    description="Scrape Telegram channels and load raw data only (no dbt / YOLO).",
)

transformation_job = define_asset_job(
    name="transformation_job",
    selection=[dbt_warehouse_models],
    description="Run dbt models only (assumes raw data is already loaded).",
)

enrichment_job = define_asset_job(
    name="enrichment_job",
    selection=[yolo_image_detections],
    description="Run YOLO detection only (assumes dbt models are already built).",
)


# ──────────────────────────────────────────────────────────────────────────────
# Schedule — daily at 02:00 UTC
# ──────────────────────────────────────────────────────────────────────────────
daily_schedule = ScheduleDefinition(
    name="daily_medical_pipeline",
    job=medical_pipeline_job,
    cron_schedule="0 2 * * *",        # 02:00 UTC every day
    default_status=DefaultScheduleStatus.STOPPED,   # start manually in UI
    description="Runs the full Medical Telegram pipeline daily at 02:00 UTC.",
    tags={"schedule": "daily"},
)


# ──────────────────────────────────────────────────────────────────────────────
# Failure sensor
# ──────────────────────────────────────────────────────────────────────────────
@run_failure_sensor(
    monitored_jobs=[medical_pipeline_job, ingestion_job, transformation_job, enrichment_job],
    name="pipeline_failure_sensor",
    description=(
        "Fires whenever any monitored job run fails. "
        "Logs the failure details; extend with Slack/email notifications as needed."
    ),
)
def pipeline_failure_sensor(context: RunFailureSensorContext) -> None:
    """
    Handle pipeline run failures.

    This sensor is intentionally minimal — it logs structured failure
    information so the Dagster UI captures it. To add external alerts:

        import requests
        requests.post(
            os.environ["SLACK_WEBHOOK_URL"],
            json={"text": f":red_circle: Pipeline failed\\n{message}"},
        )
    """
    run_id   = context.dagster_run.run_id
    job_name = context.dagster_run.job_name
    error    = context.failure_event.message if context.failure_event else "unknown error"

    message = (
        f"Pipeline run FAILED\n"
        f"  Job     : {job_name}\n"
        f"  Run ID  : {run_id}\n"
        f"  Error   : {error}\n"
        f"  UI link : http://localhost:3000/runs/{run_id}"
    )

    context.log.error(message)
    # ── Extend here to send a Slack / email / PagerDuty alert ──────────────
    # Example (Slack):
    #   import os, requests
    #   webhook = os.getenv("SLACK_WEBHOOK_URL")
    #   if webhook:
    #       requests.post(webhook, json={"text": message}, timeout=5)
