"""Global configuration. Resolve auth once; never thread creds through calls.

Auth is Application Default Credentials only (ADC + workload identity). Locally
that is `gcloud auth application-default login`; in CI/cloud it is the attached
service account. We never accept key files as call-site arguments.
"""

from __future__ import annotations

import dataclasses
import os
from typing import Literal


@dataclasses.dataclass(slots=True)
class Settings:
    # --- GCP ---
    project: str | None = None
    location: str = "US"  # BigQuery location (a multi-region like "US" or "EU")

    # --- cache ---
    cache_dir: str = "~/.mooncompute/cache"
    cache_enabled: bool = True

    # --- llm ---
    # Vertex needs a region (e.g. "us-central1"), distinct from the BigQuery
    # `location` multi-region, so they are separate fields.
    llm_location: str = "us-central1"
    llm_default_model: str = "gemini-2.5-flash"
    llm_embed_model: str = "gemini-embedding-001"
    llm_concurrency: int = 16
    llm_max_retries: int = 6
    # Inert in v0.4 (documented seams):
    llm_spend_cap_usd: float | None = None
    llm_use_batch_api: bool = False

    # --- safety ---
    bq_dry_run_default: bool = True
    bq_max_bytes_billed: int | None = None

    engine: Literal["polars", "duckdb"] = "polars"


settings = Settings(
    project=os.getenv("MOONCOMPUTE_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT"),
    cache_dir=os.getenv("MOONCOMPUTE_CACHE_DIR", "~/.mooncompute/cache"),
)


def configure(**kwargs) -> Settings:
    """Mutate the process-global settings. Call once at startup."""
    for key, value in kwargs.items():
        if not hasattr(settings, key):
            raise AttributeError(f"unknown setting: {key!r}")
        setattr(settings, key, value)
    return settings
