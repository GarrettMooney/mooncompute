"""Content-addressed caching backed by local Parquet. The iteration unlock.

No silent default: the caller picks an invalidation mode. TTL mode re-runs past
a deadline (live queries); content mode invalidates only on source/body/freshness
change (deterministic queries). For bq:// sources a `table.modified` freshness
token is recorded in the manifest and compared on read, so a reloaded table busts
the entry even in content mode. The shared-GCS tier is a future seam; v0.4 ships
the local tier only.
"""

from __future__ import annotations

import functools
import hashlib
import inspect
import json
import logging
import os
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import polars as pl

from .config import settings

log = logging.getLogger(__name__)

_TTL_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def _ttl_seconds(ttl: str | None) -> int | None:
    if ttl is None or ttl == "pinned":
        return None
    if not ttl or ttl[-1] not in _TTL_UNITS or not ttl[:-1].isdigit():
        raise ValueError(
            f"bad ttl {ttl!r}; use forms like '30m', '6h', '7d', or 'pinned'"
        )
    return int(ttl[:-1]) * _TTL_UNITS[ttl[-1]]


def _function_fingerprint(func: Callable) -> str:
    try:
        return inspect.getsource(func)
    except OSError:
        return func.__qualname__  # ty: ignore[unresolved-attribute]  # REPL / notebook cell


def _key(salt: str, args: tuple, kwargs: dict, key_extra: Any) -> str:
    h = hashlib.blake2b(digest_size=20)
    h.update(salt.encode())
    h.update(repr(args).encode())
    h.update(repr(sorted(kwargs.items())).encode())
    h.update(repr(key_extra).encode())
    return h.hexdigest()


def source_fingerprint(source: str) -> str:
    """Freshness token. bq://table -> table.modified; otherwise "" (TTL-only)."""
    if source.startswith("bq://"):
        try:
            from .sources import bigquery

            return bigquery.table_modified(source)
        except Exception as exc:  # noqa: BLE001 - freshness is best-effort
            log.warning("freshness token unavailable for %s: %s", source, exc)
    return ""


@dataclass(frozen=True)
class Manifest:
    key: str
    freshness: str
    rows: int
    written_at: float


class CacheStore:
    """Local Parquet artifact + JSON manifest sidecar. Seam for a future GCS tier."""

    def __init__(self, cache_dir: str):
        self.dir = Path(cache_dir).expanduser()

    @classmethod
    def default(cls) -> CacheStore:
        return cls(settings.cache_dir)

    def _paths(self, key: str) -> tuple[Path, Path]:
        return self.dir / f"{key}.parquet", self.dir / f"{key}.manifest.json"

    def get(
        self, key: str, *, ttl_seconds: int | None, freshness: str
    ) -> pl.DataFrame | None:
        data, manifest = self._paths(key)
        if not (data.exists() and manifest.exists()):
            return None
        try:
            m = Manifest(**json.loads(manifest.read_text()))
        except Exception:  # noqa: BLE001
            return None
        if freshness and m.freshness and freshness != m.freshness:
            return None  # upstream changed
        if ttl_seconds is not None and (time.time() - m.written_at) > ttl_seconds:
            return None  # expired
        try:
            return pl.read_parquet(data)
        except Exception as exc:  # noqa: BLE001 - fail open, never harden a failure
            log.warning("cache unreadable (%s); re-running", exc)
            return None

    def put(self, key: str, df: pl.DataFrame, *, freshness: str) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        # Data is written before the manifest, and get() requires BOTH to exist,
        # so a torn write reads as a miss. The pair is not a single atomic unit:
        # on overwrite a reader can briefly see new data + old manifest, which at
        # worst forces a safe re-run (rows is informational; freshness re-checks).
        data, manifest = self._paths(key)
        self._atomic_parquet(df, data)
        m = Manifest(
            key=key, freshness=freshness, rows=df.height, written_at=time.time()
        )
        self._atomic_text(manifest, json.dumps(asdict(m)))

    @staticmethod
    def _tmp(path: Path) -> Path:
        return path.with_suffix(path.suffix + f".{os.getpid()}.tmp")

    def _atomic_parquet(self, df: pl.DataFrame, path: Path) -> None:
        tmp = self._tmp(path)
        df.write_parquet(tmp)
        os.replace(tmp, path)

    def _atomic_text(self, path: Path, text: str) -> None:
        tmp = self._tmp(path)
        tmp.write_text(text)
        os.replace(tmp, path)

    def clear(self, prefix: str | None = None) -> int:
        if not self.dir.exists():
            return 0
        n = 0
        for p in self.dir.glob("*"):
            if prefix is None or p.name.startswith(prefix):
                p.unlink()
                n += 1
        return n


def _materialize(result: Any) -> pl.DataFrame:
    if isinstance(result, pl.LazyFrame):
        return result.collect()
    if isinstance(result, pl.DataFrame):
        return result
    raise TypeError(f"cannot cache a {type(result).__name__}; expected a Polars frame")


def cache(fn: Callable | None = None, *, ttl: str | None = None, pinned: bool = False):
    """Memoize a frame-returning function to the local store.

    Pick a mode: @cache(ttl="6h") for live data, @cache(pinned=True) for a
    deterministic/pinned computation. Bare @cache raises - the choice is yours.

    Two limitations to know:
    - The key hashes args via repr(), which is sound for repr-stable scalars
      (dates, strings, ints). Do NOT pass large arrays / DataFrames as args: their
      repr is truncated and can collide, serving a stale frame for new input.
    - Only the decorated function's own source is fingerprinted, not helpers it
      calls. If you edit a callee, pass `key_extra=` (e.g. a version string) to
      bust the key; otherwise a "pinned" result can go stale.
    """
    if ttl is None and not pinned:
        raise ValueError(
            "cache needs a mode: @cache(ttl='6h') for live data, or "
            "@cache(pinned=True) for a deterministic computation."
        )

    def decorate(func: Callable) -> Callable:
        src = _function_fingerprint(func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not settings.cache_enabled:
                return func(*args, **kwargs)
            key = _key(src, args, kwargs, None)
            store = CacheStore.default()
            hit = store.get(key, ttl_seconds=_ttl_seconds(ttl), freshness="")
            if hit is not None:
                return hit
            result = _materialize(func(*args, **kwargs))
            store.put(key, result, freshness="")
            return result

        wrapper.cache_key = lambda *a, **k: _key(src, a, k, None)  # ty: ignore[unresolved-attribute]
        return wrapper

    return decorate if fn is None else decorate(fn)


def read_cached(source: str, *, cache: str, **read_kwargs) -> Any:
    """Query-level cache used by io.read(..., cache=...).

    cache="pinned" -> content mode; cache="6h"/etc -> TTL mode. Freshness token
    comes from the source, so a reloaded bq:// table busts even a pinned entry.

    Freshness is best-effort: if the bq:// table.modified lookup transiently fails
    it returns "" (logged), and a pinned entry is then served regardless of upstream
    changes. Pinned is not a hard freshness guarantee under BQ flakiness.
    """
    if not settings.cache_enabled:
        from .io import read

        return read(source, cache=None, **read_kwargs)
    freshness = source_fingerprint(source)
    # `lazy` only changes the return wrapper, not the cached bytes; keep it out
    # of the key so lazy and eager reads share one artifact.
    key_kwargs = {k: v for k, v in read_kwargs.items() if k != "lazy"}
    key = _key("read", (source,), key_kwargs, None)
    store = CacheStore.default()
    hit = store.get(key, ttl_seconds=_ttl_seconds(cache), freshness=freshness)
    if hit is not None:
        return hit.lazy() if read_kwargs.get("lazy", True) else hit
    from .io import read

    result = _materialize(read(source, cache=None, **read_kwargs))
    store.put(key, result, freshness=freshness)
    return result.lazy() if read_kwargs.get("lazy", True) else result


def clear_cache(prefix: str | None = None) -> int:
    return CacheStore.default().clear(prefix)
