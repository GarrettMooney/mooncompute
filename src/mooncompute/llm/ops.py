"""map / embed as Polars expressions via map_batches.

map_batches hands the whole column to our function as one Series, so async
fan-out with bounded concurrency runs inside it. Native with_columns ergonomics
are preserved; the tradeoff is the column materializes at .collect() (it cannot
stay lazy past the LLM call)."""

from __future__ import annotations

import asyncio
from typing import Any

import polars as pl

from ..config import settings


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
        if schema is not None:
            return pl.Series(results)
        return pl.Series(results, dtype=pl.Utf8)

    return pl.col(column).map_batches(_run)


def embed(
    column: str,
    *,
    model: str | None = None,
    concurrency: int | None = None,
    dims: int | None = None,
) -> pl.Expr:
    """Embed a text column into a fixed-width Array(Float32) column."""
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
        width = dims or (len(next(v for v in vecs if v)) if any(vecs) else 0)
        return pl.Series(vecs, dtype=pl.Array(pl.Float32, width))

    return pl.col(column).map_batches(_run)
