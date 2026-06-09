"""The single I/O surface. read/write dispatch on URI scheme; lazy by default."""

from __future__ import annotations

from typing import Any, Literal
from urllib.parse import urlparse

import polars as pl

from .config import settings

Frame = Any


def read(
    source: str,
    *,
    lazy: bool = True,
    cache: str | None = None,
    columns: list[str] | None = None,
    engine: str | None = None,
    **params: Any,
) -> Frame:
    """Read any supported source into a (lazy) frame.

    cache: None (off) | "6h"/"30m"/"7d" (TTL mode) | "pinned" (content mode).
    """
    engine = engine or settings.engine
    if cache is not None:
        from .cache import read_cached  # ty: ignore[unresolved-import]

        return read_cached(
            source, cache=cache, lazy=lazy, columns=columns, engine=engine, **params
        )
    scheme = _scheme(source)
    if scheme == "bq":
        from .sources import bigquery

        return bigquery.read_table(source, lazy=lazy, columns=columns, engine=engine)
    if scheme == "gs":
        from .sources import gcs

        lf = gcs.scan_parquet(source, columns=columns)
        return lf if lazy else lf.collect()
    if scheme == "sql":
        from .sources import bigquery

        return bigquery.read_query(source, params=params, lazy=lazy, engine=engine)
    raise ValueError(f"unrecognized source: {source!r}")


def write(
    df: Frame,
    dest: str,
    *,
    mode: Literal["overwrite", "append", "error"] = "error",
    partition_by: list[str] | None = None,
) -> None:
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    scheme = _scheme(dest)
    if scheme == "bq":
        from .sources import bigquery

        return bigquery.write_table(df, dest, mode=mode)
    if scheme == "gs":
        from .sources import gcs

        return gcs.write_parquet(df, dest)
    raise ValueError(f"cannot write to: {dest!r}")


def dry_run(source: str, **params: Any) -> dict[str, Any]:
    from .sources import bigquery

    return bigquery.dry_run(source, params=params)


def _scheme(source: str) -> str:
    if source.startswith("bq://"):
        return "bq"
    if source.startswith("gs://"):
        return "gs"
    if urlparse(source).scheme in ("", None) and " " in source.strip():
        return "sql"
    raise ValueError(f"cannot infer source type from {source!r}")
