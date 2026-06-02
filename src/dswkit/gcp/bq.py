"""BigQuery <-> polars helpers."""

from __future__ import annotations

import hashlib
import io
import json
from datetime import datetime, timezone
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


def _manifest_path(cache: Path) -> Path:
    return cache.with_suffix(cache.suffix + ".manifest.json")


def _write_manifest(cache: Path, sql: str, project: str, df: pl.DataFrame) -> None:
    manifest = {
        "sql_sha256": _sql_hash(sql),
        "project": project,
        "rows": df.height,
        "size_bytes": cache.stat().st_size,
        "written_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    _manifest_path(cache).write_text(json.dumps(manifest, indent=2) + "\n")


def extract_cached(
    sql: str,
    cache: str | Path,
    *,
    project: str = PROJECT_PROD,
    name: str = "",
) -> pl.DataFrame:
    """Return parquet at `cache`, querying BQ on cache-miss or SQL change.

    Cache is valid iff `<cache>.manifest.json` exists and its sql_sha256
    matches the current SQL. A parquet without a manifest is adopted on first
    read (trusted, manifest written from current SQL). Generalized from
    predictive-clv/clv/cache.py:read_or_extract.
    """
    cache = Path(cache)
    label = name or cache.name
    manifest = _manifest_path(cache)

    if cache.exists():
        if manifest.exists():
            stored = json.loads(manifest.read_text()).get("sql_sha256")
            if stored == _sql_hash(sql):
                df = pl.read_parquet(cache)
                print(f"  [{label}] cached: {cache.name}  rows={df.height:,}")
                return df
            print(f"  [{label}] SQL changed since cache; re-extracting -> {cache.name}")
        else:
            df = pl.read_parquet(cache)
            print(f"  [{label}] adopting existing cache (no manifest): {cache.name}")
            _write_manifest(cache, sql, project, df)
            return df

    client = _client(project)
    print(f"  [{label}] querying BQ -> {cache.name}")
    arrow = client.query(sql).to_arrow(create_bqstorage_client=True)
    df = _decimals_to_float(pl.from_arrow(arrow))
    df.write_parquet(cache)
    _write_manifest(cache, sql, client.project, df)
    print(
        f"  [{label}] wrote {df.height:,} rows  ({cache.stat().st_size / 1e9:.2f} GB)"
    )
    return df


def pl2bq(
    df: pl.DataFrame,
    *,
    project: str,
    dataset: str,
    table: str,
) -> None:
    """Load a polars DataFrame into a BQ table via a Parquet load job.

    Always sets enable_list_inference=True so ARRAY columns load correctly.
    """
    materialize_gcp_creds()
    client = bigquery.Client(project=project)
    destination = f"{project}.{dataset}.{table}"
    with io.BytesIO() as stream:
        df.write_parquet(stream)
        stream.seek(0)
        parquet_options = bigquery.ParquetOptions()
        parquet_options.enable_list_inference = True
        job = client.load_table_from_file(
            stream,
            destination=destination,
            project=project,
            source_format=bigquery.SourceFormat.PARQUET,
            parquet_options=parquet_options,
        )
    job.result()
