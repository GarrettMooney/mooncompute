"""BigQuery <-> polars helpers."""

from __future__ import annotations

import hashlib
import io
import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import polars as pl
from google.cloud import bigquery

from ._creds import materialize_gcp_creds

PROJECT_ENV = "GOOGLE_CLOUD_PROJECT"


def _resolve_project(project: str | None) -> str:
    """Return an explicit project, else ${GOOGLE_CLOUD_PROJECT}, else raise."""
    project = project or os.environ.get(PROJECT_ENV)
    if not project:
        raise ValueError(f"no GCP project: pass project= or set ${PROJECT_ENV}")
    return project


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


def _client(project: str | None = None) -> bigquery.Client:
    materialize_gcp_creds()
    return bigquery.Client(project=_resolve_project(project))


def bq2pl(
    sql: str,
    *,
    project: str | None = None,
    client: bigquery.Client | None = None,
    decimals_to_float: bool = True,
) -> pl.DataFrame:
    """Run SQL, return a polars DataFrame.

    Uses the BigQuery Storage API fast path. When decimals_to_float (default),
    casts every Decimal column to Float64. Pass a project (or set
    $GOOGLE_CLOUD_PROJECT) so a client is built internally, or pass your own
    client.
    """
    client = client or _client(project)
    arrow = client.query(sql).to_arrow(create_bqstorage_client=True)
    df = cast(pl.DataFrame, pl.from_arrow(arrow))
    if decimals_to_float:
        df = _decimals_to_float(df)
    return df


def _manifest_path(cache: Path) -> Path:
    return cache.with_suffix(cache.suffix + ".manifest.json")


@dataclass(frozen=True)
class Manifest:
    sql_sha256: str
    project: str
    rows: int
    size_bytes: int
    written_at: str

    @staticmethod
    def build(cache: Path, sql: str, project: str, df: pl.DataFrame) -> Manifest:
        return Manifest(
            sql_sha256=_sql_hash(sql),
            project=project,
            rows=df.height,
            size_bytes=cache.stat().st_size,
            written_at=datetime.now(UTC).isoformat(timespec="seconds"),
        )

    @staticmethod
    def load(path: Path) -> Manifest:
        return Manifest(**json.loads(path.read_text()))

    def write(self, path: Path) -> None:
        path.write_text(json.dumps(asdict(self), indent=2) + "\n")


def extract_cached(
    sql: str,
    cache: str | Path,
    *,
    project: str | None = None,
    name: str = "",
) -> pl.DataFrame:
    """Return parquet at `cache`, querying BQ on cache-miss or SQL change.

    Cache is valid iff `<cache>.manifest.json` exists and its sql_sha256
    matches the current SQL. A parquet without a manifest is adopted on first
    read (trusted, manifest written from current SQL).

    Note: the cache is a local file. In ephemeral environments (containers,
    Cloud Functions) it does not persist across runs; prefer `gcs.*` for
    durable artifacts there.
    """
    cache = Path(cache)
    project = _resolve_project(project)
    label = name or cache.name
    manifest = _manifest_path(cache)

    if cache.exists():
        if manifest.exists():
            if Manifest.load(manifest).sql_sha256 == _sql_hash(sql):
                df = pl.read_parquet(cache)
                print(f"  [{label}] cached: {cache.name}  rows={df.height:,}")
                return df
            print(f"  [{label}] SQL changed since cache; re-extracting -> {cache.name}")
        else:
            df = pl.read_parquet(cache)
            print(f"  [{label}] adopting existing cache (no manifest): {cache.name}")
            Manifest.build(cache, sql, project, df).write(manifest)
            return df

    client = _client(project)
    print(f"  [{label}] querying BQ -> {cache.name}")
    arrow = client.query(sql).to_arrow(create_bqstorage_client=True)
    df = _decimals_to_float(cast(pl.DataFrame, pl.from_arrow(arrow)))
    df.write_parquet(cache)
    Manifest.build(cache, sql, client.project, df).write(manifest)
    print(
        f"  [{label}] wrote {df.height:,} rows  ({cache.stat().st_size / 1e9:.2f} GB)"
    )
    return df


def pl2bq(
    df: pl.DataFrame,
    *,
    dataset: str,
    table: str,
    project: str | None = None,
    client: bigquery.Client | None = None,
) -> None:
    """Load a polars DataFrame into a BQ table via a Parquet load job.

    Always sets enable_list_inference=True so ARRAY columns load correctly.
    Pass a project (or set $GOOGLE_CLOUD_PROJECT), or pass your own client.
    """
    project = _resolve_project(project)
    client = client or _client(project)
    destination = f"{project}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.PARQUET
    parquet_options = bigquery.ParquetOptions()
    parquet_options.enable_list_inference = True
    job_config.parquet_options = parquet_options
    with io.BytesIO() as stream:
        df.write_parquet(stream)
        stream.seek(0)
        job = client.load_table_from_file(
            stream,
            destination,
            project=project,
            job_config=job_config,
        )
    job.result()
