"""
pipeline/resources.py
─────────────────────
Dagster resources shared across all assets:

  postgres_resource   — psycopg2 connection factory
  dbt_resource        — thin wrapper around the dbt CLI
  pipeline_config     — typed config (channels, limits, paths)
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import psycopg2
from dagster import ConfigurableResource, EnvVar, InitResourceContext, resource
from pydantic import Field


ROOT_DIR = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = ROOT_DIR / "medical_warehouse"


# ──────────────────────────────────────────────────────────────────────────────
# PostgreSQL resource
# ──────────────────────────────────────────────────────────────────────────────
class PostgresResource(ConfigurableResource):
    """Provides a psycopg2 connection to the data warehouse."""

    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    dbname: str = Field(default="medical_warehouse")
    user: str = Field(default="postgres")
    password: str = Field(default="")

    def get_connection(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )


# ──────────────────────────────────────────────────────────────────────────────
# dbt CLI resource
# ──────────────────────────────────────────────────────────────────────────────
class DbtCliResource(ConfigurableResource):
    """
    Thin wrapper that runs dbt commands as subprocesses.
    The project directory and profiles directory are resolved from the repo root.
    """

    project_dir: str = str(DBT_PROJECT_DIR)
    profiles_dir: str = str(DBT_PROJECT_DIR)
    target: str = "dev"

    def run(self, *args: str) -> subprocess.CompletedProcess:
        """
        Execute a dbt command, e.g.:
            dbt_resource.run("run", "--select", "staging")
        Raises CalledProcessError on non-zero exit.
        """
        cmd = [
            "dbt",
            *args,
            "--project-dir", self.project_dir,
            "--profiles-dir", self.profiles_dir,
            "--target", self.target,
        ]
        return subprocess.run(
            cmd,
            check=True,
            capture_output=False,   # stream to Dagster's log handler
            text=True,
            cwd=str(ROOT_DIR),
        )


# ──────────────────────────────────────────────────────────────────────────────
# Pipeline config resource
# ──────────────────────────────────────────────────────────────────────────────
class PipelineConfig(ConfigurableResource):
    """
    Central configuration for the full pipeline run.
    All fields can be overridden per-run from the Dagster Launchpad UI.
    """

    # Scraper settings
    channels: list[str] = Field(
        default=["CheMed123", "lobelia4cosmetics", "tikvahpharma"],
        description="Telegram channel usernames to scrape.",
    )
    scrape_limit: int = Field(
        default=0,
        description="Max messages per channel (0 = unlimited).",
    )
    scrape_start_date: str = Field(
        default="",
        description="ISO date (YYYY-MM-DD) — only scrape messages on/after this date.",
    )
    scrape_end_date: str = Field(
        default="",
        description="ISO date (YYYY-MM-DD) — only scrape messages on/before this date.",
    )

    # YOLO settings
    yolo_conf_threshold: float = Field(
        default=0.25,
        description="YOLO minimum confidence threshold (0–1).",
    )

    # Loader settings
    loader_batch_size: int = Field(
        default=500,
        description="Rows per INSERT batch for the raw loader.",
    )
