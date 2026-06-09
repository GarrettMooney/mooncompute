"""map / embed as Polars expressions via map_batches.

map_batches hands the whole column to our function as one Series, so async
fan-out with bounded concurrency runs inside it. Native with_columns ergonomics
are preserved; the tradeoff is the column materializes at .collect() (it cannot
stay lazy past the LLM call).

We declare return_dtype on every map_batches call. Without it Polars probes the
UDF with an empty Series to infer the output type, which fires a (wasted) batch
and, for the embed path, yields an invalid zero-width Array. Declaring the dtype
skips the probe entirely."""

from __future__ import annotations

import asyncio
from typing import Any

import polars as pl

from ..config import settings

# pydantic primitive annotation -> polars dtype, for deriving a struct schema.
_PRIM = {str: pl.Utf8, int: pl.Int64, float: pl.Float64, bool: pl.Boolean}


def _struct_dtype(schema: Any) -> pl.Struct | None:
    """Best-effort polars Struct from a pydantic model; None if not derivable
    (e.g. a bare dict), in which case we let Polars infer the output."""
    fields = getattr(schema, "model_fields", None)
    if not fields:
        return None
    return pl.Struct(
        {name: _PRIM.get(info.annotation, pl.Utf8) for name, info in fields.items()}
    )


def map(  # noqa: A001
    column: str,
    *,
    prompt: str,
    schema: Any = None,
    model: str | None = None,
    concurrency: int | None = None,
    system: str | None = None,
    max_tokens: int = 1024,
    temperature: float = 0.0,
) -> pl.Expr:
    """Map an LLM over a column. With a Pydantic `schema`, returns a Struct
    column via structured output; otherwise a Utf8 column."""
    model = model or settings.llm_default_model
    concurrency = concurrency or settings.llm_concurrency
    struct_dtype = _struct_dtype(schema) if schema is not None else None
    return_dtype = pl.Utf8 if schema is None else struct_dtype

    def _run(s: pl.Series) -> pl.Series:
        from . import _gemini

        rows = s.to_list()
        prompts = [
            prompt.format(**{column: v}) if v is not None else None for v in rows
        ]
        results = asyncio.run(
            _gemini.complete_batch(
                prompts,
                model=model,
                schema=schema,
                system=system,
                max_tokens=max_tokens,
                temperature=temperature,
                concurrency=concurrency,
                max_retries=settings.llm_max_retries,
            )
        )
        if schema is None:
            return pl.Series(results, dtype=pl.Utf8)
        if struct_dtype is not None:
            return pl.Series(results, dtype=struct_dtype)
        return pl.Series(results)  # non-pydantic schema: let Polars infer

    return pl.col(column).map_batches(_run, return_dtype=return_dtype)


def embed(
    column: str,
    *,
    model: str | None = None,
    concurrency: int | None = None,
    dims: int | None = None,
) -> pl.Expr:
    """Embed a text column into a List(Float32) column.

    Cast to a fixed-width array for DuckDB VSS, e.g.
    `df.with_columns(mc.llm.embed("t").cast(pl.Array(pl.Float32, 768)).alias("v"))`.
    Pass `dims` to request a specific output dimensionality from the model.
    """
    model = model or settings.llm_embed_model
    concurrency = concurrency or settings.llm_concurrency

    def _run(s: pl.Series) -> pl.Series:
        from . import _gemini

        vecs = asyncio.run(
            _gemini.embed_batch(
                s.to_list(),
                model=model,
                concurrency=concurrency,
                dims=dims,
                max_retries=settings.llm_max_retries,
            )
        )
        return pl.Series(vecs, dtype=pl.List(pl.Float32))

    return pl.col(column).map_batches(_run, return_dtype=pl.List(pl.Float32))
