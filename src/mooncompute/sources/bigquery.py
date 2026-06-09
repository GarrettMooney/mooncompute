"""BigQuery source. Storage Read API (Arrow) fast path, never REST paging."""

from __future__ import annotations

import io
import logging
import os
import re
from typing import Any, cast

import polars as pl
from google.cloud import bigquery

from .._creds import materialize_gcp_creds
from ..config import settings

PROJECT_ENV = "GOOGLE_CLOUD_PROJECT"
log = logging.getLogger(__name__)

_BQ_RE = re.compile(r"^bq://(?P<project>[^.]+)\.(?P<dataset>[^.]+)\.(?P<table>.+)$")
# On-demand price: $6.25 / TiB scanned.
_USD_PER_BYTE = 6.25 / 2**40


def _resolve_project(project: str | None) -> str:
    project = project or settings.project or os.environ.get(PROJECT_ENV)
    if not project:
        raise ValueError(f"no GCP project: pass project= or set ${PROJECT_ENV}")
    return project


def _client(project: str | None = None) -> bigquery.Client:
    materialize_gcp_creds()
    return bigquery.Client(project=_resolve_project(project))


def read_sql(path, **subs: str) -> str:
    """Read a .sql file; optionally substitute {placeholder} tokens via format."""
    from pathlib import Path

    text = Path(path).read_text()
    return text.format(**subs) if subs else text


def _decimals_to_float(df: pl.DataFrame) -> pl.DataFrame:
    decimal_cols = [c for c in df.columns if str(df[c].dtype).startswith("Decimal")]
    if not decimal_cols:
        return df
    return df.with_columns([pl.col(c).cast(pl.Float64) for c in decimal_cols])


def _parse_bq_uri(uri: str) -> tuple[str, str, str]:
    m = _BQ_RE.match(uri)
    if not m:
        raise ValueError(f"bad bq uri: {uri!r} (want bq://project.dataset.table)")
    return m["project"], m["dataset"], m["table"]


def bq2pl(sql, *, project=None, client=None, decimals_to_float=True) -> pl.DataFrame:
    client = client or _client(project)
    arrow = client.query(sql).to_arrow(create_bqstorage_client=True)
    df = cast(pl.DataFrame, pl.from_arrow(arrow))
    return _decimals_to_float(df) if decimals_to_float else df


def dry_run(sql, *, params=None, client=None, project=None) -> dict[str, Any]:
    """Cost estimate without execution."""
    client = client or _client(project)
    cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    if params:
        cfg.query_parameters = _query_params(params)
    job = client.query(sql, job_config=cfg)
    n = int(job.total_bytes_processed or 0)
    return {"bytes_processed": n, "estimated_usd": n * _USD_PER_BYTE}


def _query_params(params: dict) -> list:
    out = []
    for k, v in params.items():
        t = "STRING"
        if isinstance(v, bool):
            t = "BOOL"
        elif isinstance(v, int):
            t = "INT64"
        elif isinstance(v, float):
            t = "FLOAT64"
        out.append(bigquery.ScalarQueryParameter(k, t, v))
    return out


def read_query(
    sql,
    *,
    params,
    lazy,
    engine,
    client=None,
    project=None,
    dry_run_default=None,
    max_bytes_billed=None,
) -> Any:
    """Parameterized query (@name binding) -> Arrow -> frame, with cost guardrail."""
    client = client or _client(project)
    do_dry = settings.bq_dry_run_default if dry_run_default is None else dry_run_default
    cap = settings.bq_max_bytes_billed if max_bytes_billed is None else max_bytes_billed
    if do_dry or cap:
        est = dry_run(sql, params=params, client=client)
        if cap and est["bytes_processed"] > cap:
            raise RuntimeError(
                f"query would scan {est['bytes_processed']:,} bytes "
                f"(${est['estimated_usd']:.2f}), over cap of {cap:,}"
            )
    cfg = bigquery.QueryJobConfig(query_parameters=_query_params(params or {}))
    arrow = client.query(sql, job_config=cfg).to_arrow(create_bqstorage_client=True)
    df = _decimals_to_float(cast(pl.DataFrame, pl.from_arrow(arrow)))
    return df.lazy() if lazy else df


def read_table(uri, *, lazy, columns, engine, client=None, project=None) -> Any:
    """bq://p.d.t -> frame. Projection is pushed server-side via SELECT cols."""
    project_, dataset, table = _parse_bq_uri(uri)
    cols = ", ".join(f"`{c}`" for c in columns) if columns else "*"
    sql = f"SELECT {cols} FROM `{project_}.{dataset}.{table}`"
    return read_query(
        sql,
        params={},
        lazy=lazy,
        engine=engine,
        client=client,
        project=project or project_,
        dry_run_default=False,
    )


def table_modified(uri, *, client=None, project=None) -> str:
    """Freshness token: the table's last-modified timestamp (isoformat)."""
    project_, dataset, table = _parse_bq_uri(uri)
    client = client or _client(project or project_)
    ref = f"{project_}.{dataset}.{table}"
    t = client.get_table(ref)
    return t.modified.isoformat() if t.modified else ""


def write_table(
    df: pl.DataFrame, uri, *, mode, client=None, project=None, job_id=None
) -> None:
    """Load a polars frame into bq://p.d.t via a Parquet load job (list inference
    on). `job_id` is an idempotency key: a retried load with the same id is a
    safe no-op, not a double-load."""
    project_, dataset, table = _parse_bq_uri(uri)
    client = client or _client(project or project_)
    destination = f"{project_}.{dataset}.{table}"
    disp = {
        "overwrite": bigquery.WriteDisposition.WRITE_TRUNCATE,
        "append": bigquery.WriteDisposition.WRITE_APPEND,
        "error": bigquery.WriteDisposition.WRITE_EMPTY,
    }[mode]
    job_config = bigquery.LoadJobConfig(write_disposition=disp)
    job_config.source_format = bigquery.SourceFormat.PARQUET
    opts = bigquery.ParquetOptions()
    opts.enable_list_inference = True
    job_config.parquet_options = opts
    with io.BytesIO() as stream:
        df.write_parquet(stream)
        stream.seek(0)
        client.load_table_from_file(
            stream, destination, project=project_, job_config=job_config, job_id=job_id
        ).result()
