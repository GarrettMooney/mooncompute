"""BigQuery <-> polars helpers."""

from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl

PROJECT_DEV = "gcp-dsw-data-lake-dev"
PROJECT_PROD = "gcp-dsw-data-lake-prod"


def read_sql(path: str | Path, **subs: str) -> str:
    """Read a .sql file; optionally substitute {placeholder} tokens via str.format.

    Note: when subs are passed, literal `{`/`}` in the SQL must be escaped as
    `{{`/`}}` (str.format rules). With no subs, the text is returned verbatim.
    """
    text = Path(path).read_text()
    return text.format(**subs) if subs else text


def _sql_hash(sql: str) -> str:
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def _decimals_to_float(df: pl.DataFrame) -> pl.DataFrame:
    decimal_cols = [c for c in df.columns if str(df[c].dtype).startswith("Decimal")]
    if not decimal_cols:
        return df
    return df.with_columns([pl.col(c).cast(pl.Float64) for c in decimal_cols])
