"""BigQuery <-> polars helpers."""

from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl
from google.cloud import bigquery

from ._creds import materialize_gcp_creds

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


def _client(project: str) -> bigquery.Client:
    materialize_gcp_creds()
    return bigquery.Client(project=project)


def bq2pl(
    sql: str,
    *,
    project: str = PROJECT_PROD,
    client: bigquery.Client | None = None,
    decimals_to_float: bool = True,
) -> pl.DataFrame:
    """Run SQL, return a polars DataFrame.

    Uses the BigQuery Storage API fast path. When decimals_to_float (default),
    casts every Decimal column to Float64. Pass a project string (a client is
    built internally) or your own client.
    """
    client = client or _client(project)
    arrow = client.query(sql).to_arrow(create_bqstorage_client=True)
    df = pl.from_arrow(arrow)
    if decimals_to_float:
        df = _decimals_to_float(df)
    return df
