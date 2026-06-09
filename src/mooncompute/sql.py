"""Polars <-> DuckDB interop. Drop into SQL, get Polars back (zero-copy Arrow)."""

from __future__ import annotations

import inspect
from typing import Any

import duckdb
import polars as pl


def sql(query: str, *, lazy: bool = False, **frames: pl.DataFrame) -> Any:
    """Run DuckDB SQL against Polars frames, returning a Polars frame.

    Pass frames by name, or rely on caller-scope capture so a local `df` is
    queryable as `df`. Use for window functions, ASOF joins, or VSS similarity
    search over an embedding column.
    """
    if not frames:
        frame_obj = inspect.currentframe()
        caller = frame_obj.f_back.f_locals if frame_obj and frame_obj.f_back else {}
        frames = {k: v for k, v in caller.items() if isinstance(v, pl.DataFrame)}
    # DuckDB autoloads known extensions (httpfs, vss) on first reference, so a
    # query using them works without an explicit INSTALL/LOAD here.
    con = duckdb.connect()
    try:
        for name, frame in frames.items():
            con.register(name, frame.to_arrow())
        rel = con.sql(query)
        out = rel.pl()
    finally:
        con.close()
    return out.lazy() if lazy else out
