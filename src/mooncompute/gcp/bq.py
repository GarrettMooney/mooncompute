"""BigQuery <-> polars helpers."""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import polars as pl
from google.cloud import bigquery

from ._creds import materialize_gcp_creds

PROJECT_ENV = "GOOGLE_CLOUD_PROJECT"

log = logging.getLogger(__name__)


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


def _tmp_sibling(path: Path) -> Path:
    """A same-directory temp path, so os.replace stays on one filesystem."""
    return path.with_suffix(path.suffix + f".{os.getpid()}.tmp")


def _atomic_write_text(path: Path, text: str) -> None:
    tmp = _tmp_sibling(path)
    tmp.write_text(text)
    os.replace(tmp, path)


def _atomic_write_parquet(df: pl.DataFrame, path: Path) -> None:
    tmp = _tmp_sibling(path)
    df.write_parquet(tmp)
    os.replace(tmp, path)


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
        _atomic_write_text(path, json.dumps(asdict(self), indent=2) + "\n")


def _fmt_age(td: timedelta) -> str:
    s = int(td.total_seconds())
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m"
    if s < 86400:
        return f"{s // 3600}h"
    return f"{s // 86400}d"


def _try_read_parquet(cache: Path, label: str) -> pl.DataFrame | None:
    """Read the cache, or return None on a corrupt/unreadable file.

    A cache must never harden a failure: if the local parquet cannot be read,
    we fall back to the live source rather than raising.
    """
    try:
        return pl.read_parquet(cache)
    except Exception as exc:  # noqa: BLE001 - any read failure should fail open
        log.warning(
            "[%s] cache unreadable (%s); re-extracting -> %s",
            label,
            exc,
            cache.name,
        )
        return None


def extract_cached(
    sql: str,
    cache: str | Path,
    *,
    project: str | None = None,
    name: str = "",
    max_age: timedelta | None = None,
    content_only: bool = False,
) -> pl.DataFrame:
    """Return parquet at `cache`, querying BQ on cache-miss or SQL change.

    Cache is valid iff `<cache>.manifest.json` exists and its sql_sha256
    matches the current SQL. A parquet without a manifest is adopted on first
    read (trusted, manifest written from current SQL).

    **You must pick an invalidation mode.** The cache is keyed on the SQL text,
    so identical SQL always hits the same cache. Whether that is correct depends
    on the query, so this is not allowed to default silently:

    - `content_only=True` — invalidate on SQL change only. Correct *only* for a
      deterministic query (e.g. pinned to a fixed `snapshot_date`), where the
      same SQL must return the same rows. For a non-deterministic query (one
      using `CURRENT_DATE()`, a relative window, or a live table) this serves
      stale results indefinitely while the data drifts.
    - `max_age=<timedelta>` — also re-query once the cache is older than the
      TTL. Use for live/relative queries. (For an always-live read, use
      `bq2pl`.)

    Passing neither raises `ValueError`; passing both is contradictory and also
    raises. The mode must be a deliberate choice at the call site.

    **Failure model.** The cache is a derived artifact, never a source of
    truth: a corrupt or unreadable parquet fails open (logs a warning and
    re-queries BQ) rather than raising. Writes are atomic (temp file + rename),
    so a crash mid-write leaves the previous cache intact, not a truncated one.

    Note: the cache is a local file. In ephemeral environments (containers,
    Cloud Functions) it does not persist across runs; prefer `gcs.*` for
    durable artifacts there.
    """
    if content_only and max_age is not None:
        raise ValueError(
            "content_only=True and max_age are contradictory: content_only "
            "means SQL-change-only invalidation (no TTL). Pass one, not both."
        )
    if not content_only and max_age is None:
        raise ValueError(
            "extract_cached needs an invalidation mode: pass content_only=True "
            "for a deterministic/pinned query, or max_age=<timedelta> for a "
            "live/relative one (or use bq2pl for an always-live read)."
        )
    cache = Path(cache)
    project = _resolve_project(project)
    label = name or cache.name
    manifest = _manifest_path(cache)

    if cache.exists():
        if manifest.exists():
            m = Manifest.load(manifest)
            if m.sql_sha256 == _sql_hash(sql):
                age = datetime.now(UTC) - datetime.fromisoformat(m.written_at)
                if max_age is not None and age > max_age:
                    log.info(
                        "[%s] cache expired (age %s > %s); re-extracting -> %s",
                        label,
                        _fmt_age(age),
                        _fmt_age(max_age),
                        cache.name,
                    )
                else:
                    df = _try_read_parquet(cache, label)
                    if df is not None:
                        log.info(
                            "[%s] cached (age %s): %s  rows=%s",
                            label,
                            _fmt_age(age),
                            cache.name,
                            f"{df.height:,}",
                        )
                        return df
            else:
                log.info("[%s] SQL changed; re-extracting -> %s", label, cache.name)
        else:
            df = _try_read_parquet(cache, label)
            if df is not None:
                log.info(
                    "[%s] adopting existing cache (no manifest): %s", label, cache.name
                )
                Manifest.build(cache, sql, project, df).write(manifest)
                return df

    client = _client(project)
    log.info("[%s] querying BQ -> %s", label, cache.name)
    arrow = client.query(sql).to_arrow(create_bqstorage_client=True)
    df = _decimals_to_float(cast(pl.DataFrame, pl.from_arrow(arrow)))
    _atomic_write_parquet(df, cache)
    Manifest.build(cache, sql, client.project, df).write(manifest)
    log.info(
        "[%s] wrote %s rows  (%.2f GB)",
        label,
        f"{df.height:,}",
        cache.stat().st_size / 1e9,
    )
    return df


def pl2bq(
    df: pl.DataFrame,
    *,
    dataset: str,
    table: str,
    project: str | None = None,
    client: bigquery.Client | None = None,
    job_id: str | None = None,
) -> None:
    """Load a polars DataFrame into a BQ table via a Parquet load job.

    Always sets enable_list_inference=True so ARRAY columns load correctly.
    Pass a project (or set $GOOGLE_CLOUD_PROJECT), or pass your own client.

    Pass `job_id` as an idempotency key: BQ rejects a duplicate job id, so a
    retried call with the same id is a safe no-op instead of a double-load. Use
    a stable id derived from the load's identity (e.g. the source SQL hash plus
    the destination), not a fresh value per attempt.
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
            job_id=job_id,
        )
    job.result()
