"""Vertex Gemini async core: bounded concurrency, retry, structured output, and
a SQLite per-row cache. The completion key folds in model, system, prompt, the
schema's JSON shape, and (temperature, max_tokens); the embed key folds in model,
text, and output dimensionality."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import sqlite3
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from ..config import settings

log = logging.getLogger(__name__)

_DB_PATH = Path("~/.mooncompute/llm-cache.db").expanduser()


class RetryableError(Exception):
    """Raised for 429/503-class responses; triggers backoff."""


# ---- per-row cache (sqlite) ------------------------------------------------


def _db() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(_DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)")
    return con


def _row_key(
    prompt: str, *, model: str, system: str | None, schema: Any, extra: Any = None
) -> str:
    h = hashlib.blake2b(digest_size=20)
    if schema is None:
        schema_repr = ""
    else:
        try:
            schema_repr = json.dumps(schema.model_json_schema(), sort_keys=True)
        except Exception:  # noqa: BLE001 - fall back to a stable-ish name
            schema_repr = getattr(schema, "__name__", repr(schema))
    for part in (model, system or "", schema_repr, prompt, repr(extra)):
        h.update(part.encode())
    return h.hexdigest()


def _cache_get(con: sqlite3.Connection, key: str) -> Any | None:
    row = con.execute("SELECT v FROM kv WHERE k = ?", (key,)).fetchone()
    return json.loads(row[0]) if row else None


def _cache_put(con: sqlite3.Connection, key: str, value: Any) -> None:
    con.execute(
        "INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)", (key, json.dumps(value))
    )
    con.commit()


# ---- orchestration ---------------------------------------------------------


async def _with_retry(
    coro_factory: Callable[[], Awaitable[Any]], *, max_retries: int
) -> Any:
    for attempt in range(max_retries + 1):
        try:
            return await coro_factory()
        except RetryableError:
            if attempt == max_retries:
                raise
            await asyncio.sleep(min(2**attempt, 30) + random.random())


async def complete_batch(
    prompts: list[Any],
    *,
    model: str,
    schema: Any,
    system: str | None,
    max_tokens: int,
    temperature: float,
    concurrency: int,
    max_retries: int,
    call: Any = None,
) -> list[Any]:
    """Fan out over prompts with a semaphore + per-row cache. `call` is the
    single-prompt coroutine (defaults to the live Gemini call)."""
    call = call or _make_call(max_tokens=max_tokens, temperature=temperature)
    con = _db()
    sem = asyncio.Semaphore(concurrency)

    async def one(prompt: Any) -> Any:
        if prompt is None:
            return None
        key = _row_key(
            prompt,
            model=model,
            system=system,
            schema=schema,
            extra=(temperature, max_tokens),
        )
        cached = _cache_get(con, key)
        if cached is not None:
            return cached
        async with sem:
            try:
                result = await _with_retry(
                    lambda: call(prompt, model=model, schema=schema, system=system),
                    max_retries=max_retries,
                )
            except Exception as exc:  # noqa: BLE001 - isolate a row failure
                log.warning("llm row failed (%s); -> null", exc)
                return None
        _cache_put(con, key, result)
        return result

    try:
        return list(await asyncio.gather(*(one(p) for p in prompts)))
    finally:
        con.close()


async def embed_batch(
    texts: list[Any],
    *,
    model: str,
    concurrency: int,
    dims: int | None,
    max_retries: int,
    call: Any = None,
) -> list[Any]:
    call = call or _make_embed_call(dims=dims)
    con = _db()
    sem = asyncio.Semaphore(concurrency)

    async def one(text: Any) -> Any:
        if text is None:
            return None
        key = _row_key(text, model=model, system=None, schema=None, extra=dims)
        cached = _cache_get(con, key)
        if cached is not None:
            return cached
        async with sem:
            vec = await _with_retry(
                lambda: call(text, model=model), max_retries=max_retries
            )
        _cache_put(con, key, vec)
        return vec

    try:
        return list(await asyncio.gather(*(one(t) for t in texts)))
    finally:
        con.close()


# ---- live Gemini calls (Vertex via google-genai) ---------------------------


def _vertex_client() -> Any:
    from google import genai

    return genai.Client(
        vertexai=True, project=settings.project, location=settings.location
    )


def _make_call(*, max_tokens: int, temperature: float) -> Callable[..., Awaitable[Any]]:
    client = _vertex_client()

    async def call(prompt, *, model, schema, system) -> Any:
        from google.genai import errors as genai_errors
        from google.genai import types

        cfg = types.GenerateContentConfig(
            system_instruction=system,
            temperature=temperature,
            max_output_tokens=max_tokens,
        )
        if schema is not None:
            cfg.response_mime_type = "application/json"
            cfg.response_schema = schema
        try:
            resp = await client.aio.models.generate_content(
                model=model, contents=prompt, config=cfg
            )
        except genai_errors.APIError as exc:
            if getattr(exc, "code", None) in (429, 503):
                raise RetryableError(str(exc)) from exc
            raise
        if schema is not None:
            return json.loads(resp.text) if resp.text else None
        return resp.text

    return call


def _make_embed_call(*, dims: int | None) -> Callable[..., Awaitable[Any]]:
    client = _vertex_client()

    async def call(text, *, model) -> list[float]:
        from google.genai import errors as genai_errors
        from google.genai import types

        cfg = types.EmbedContentConfig(output_dimensionality=dims) if dims else None
        try:
            resp = await client.aio.models.embed_content(
                model=model, contents=text, config=cfg
            )
        except genai_errors.APIError as exc:
            if getattr(exc, "code", None) in (429, 503):
                raise RetryableError(str(exc)) from exc
            raise
        return list(resp.embeddings[0].values)

    return call
