"""mooncompute - thin Arrow-glue for BigQuery / GCS / Polars / DuckDB + LLMs.

import mooncompute as mc

df  = mc.read("bq://proj.dataset.table", columns=["user_id", "ts"])
df  = mc.read("gs://bucket/events/*.parquet")
out = mc.sql("select user_id, count(*) from df group by 1")
df  = df.with_columns(mc.llm.map("review", prompt="Classify: {review}").alias("y"))
mc.write(df, "bq://proj.dataset.out", mode="overwrite")
"""

from __future__ import annotations

from importlib.metadata import version

from . import llm
from .cache import cache, clear_cache
from .config import Settings, configure, settings
from .io import dry_run, read, write
from .sql import sql

__version__ = version("mooncompute")

__all__ = [
    "read",
    "write",
    "dry_run",
    "sql",
    "cache",
    "clear_cache",
    "configure",
    "settings",
    "Settings",
    "llm",
    "__version__",
]
