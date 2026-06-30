"""
pipeline.py
───────────
Root entry-point for the Dagster development server.

Usage
─────
    # Start the Dagster UI (http://localhost:3000)
    dagster dev -f pipeline.py

    # Run the full pipeline once from the CLI (no UI)
    dagster job execute -f pipeline.py -j medical_pipeline_job

    # Run a specific partial job
    dagster job execute -f pipeline.py -j ingestion_job
    dagster job execute -f pipeline.py -j transformation_job
    dagster job execute -f pipeline.py -j enrichment_job

Environment
───────────
All credentials are read from .env via the Definitions resource config.
Make sure your .env file is populated before running:
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD,
    TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE
"""

from dotenv import load_dotenv

load_dotenv()

# Re-export the Definitions object — Dagster discovers it automatically
from pipeline.definitions import defs  # noqa: E402, F401
