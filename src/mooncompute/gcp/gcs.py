"""GCS read/write helpers for Parquet, JSON, and bytes over gs:// URIs."""

from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING, Any

from google.cloud import storage

from ._creds import materialize_gcp_creds

if TYPE_CHECKING:
    import polars as pl


def _polars():
    """Import polars lazily so the JSON/bytes path stays free of it.

    polars (+ pyarrow) is tens of MB and a real Cloud Function cold-start cost;
    only the parquet helpers need it. Installed via the `bq` extra.
    """
    try:
        import polars as pl
    except ImportError as exc:  # pragma: no cover - exercised via install shape
        raise ImportError(
            "parquet I/O needs polars. Install it with "
            "`pip install 'mooncompute[bq]'` (or add polars directly)."
        ) from exc
    return pl


def _client() -> storage.Client:
    materialize_gcp_creds()
    return storage.Client()


def _parse_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"expected a gs:// URI, got: {uri!r}")
    bucket, _, key = uri[5:].partition("/")
    return bucket, key


def read_parquet(uri: str) -> pl.DataFrame:
    pl = _polars()
    bucket, key = _parse_uri(uri)
    blob = _client().bucket(bucket).blob(key)
    buf = io.BytesIO()
    blob.download_to_file(buf)
    buf.seek(0)
    return pl.read_parquet(buf)


def read_parquet_glob(prefix_uri: str) -> pl.DataFrame:
    """Read and concat all *.parquet shards under a gs:// prefix.

    Use for BigQuery `EXPORT DATA` output, which writes many shards.
    """
    pl = _polars()
    bucket, prefix = _parse_uri(prefix_uri.rstrip("/") + "/")
    shards = [
        b
        for b in _client().bucket(bucket).list_blobs(prefix=prefix)
        if b.name.endswith(".parquet")
    ]
    if not shards:
        raise FileNotFoundError(f"no .parquet shards under {prefix_uri}")
    frames = []
    for blob in shards:
        buf = io.BytesIO()
        blob.download_to_file(buf)
        buf.seek(0)
        frames.append(pl.read_parquet(buf))
    return pl.concat(frames, how="vertical_relaxed")


def write_parquet(df: pl.DataFrame, uri: str) -> None:
    bucket, key = _parse_uri(uri)
    buf = io.BytesIO()
    df.write_parquet(buf)
    _client().bucket(bucket).blob(key).upload_from_string(
        buf.getvalue(), content_type="application/octet-stream"
    )


def read_json(uri: str) -> Any:
    bucket, key = _parse_uri(uri)
    return json.loads(_client().bucket(bucket).blob(key).download_as_text())


def write_json(uri: str, obj: Any) -> None:
    bucket, key = _parse_uri(uri)
    _client().bucket(bucket).blob(key).upload_from_string(
        json.dumps(obj, default=str), content_type="application/json"
    )


def read_bytes(uri: str) -> bytes:
    bucket, key = _parse_uri(uri)
    return _client().bucket(bucket).blob(key).download_as_bytes()


def write_bytes(
    uri: str, data: bytes, content_type: str = "application/octet-stream"
) -> None:
    bucket, key = _parse_uri(uri)
    _client().bucket(bucket).blob(key).upload_from_string(
        data, content_type=content_type
    )
