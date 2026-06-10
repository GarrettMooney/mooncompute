import asyncio

from mooncompute.llm import _gemini


def test_per_row_cache_only_calls_misses(tmp_path, monkeypatch):
    monkeypatch.setattr(_gemini, "_DB_PATH", tmp_path / "llm.db")
    calls = []

    async def fake_call(prompt, **kw):
        calls.append(prompt)
        return prompt.upper()

    prompts = ["a", "b", "a"]  # "a" repeats -> one cached
    # Note: dedup here relies on cooperative scheduling (fake_call has no await,
    # so the first "a" caches before the second starts). Under real awaiting I/O
    # both "a" rows could miss before either writes; INSERT OR REPLACE keeps that
    # correct, just not call-deduplicated. In-flight coalescing is out of scope.
    out = asyncio.run(
        _gemini.complete_batch(
            prompts,
            model="m",
            schema=None,
            system=None,
            max_tokens=8,
            temperature=0.0,
            concurrency=4,
            max_retries=2,
            call=fake_call,
        )
    )
    assert out == ["A", "B", "A"]
    assert sorted(calls) == ["a", "b"]  # "a" called once, reused for row 3


def test_none_rows_pass_through(tmp_path, monkeypatch):
    monkeypatch.setattr(_gemini, "_DB_PATH", tmp_path / "llm.db")

    async def fake_call(prompt, **kw):
        return "x"

    out = asyncio.run(
        _gemini.complete_batch(
            [None, "y"],
            model="m",
            schema=None,
            system=None,
            max_tokens=8,
            temperature=0.0,
            concurrency=4,
            max_retries=2,
            call=fake_call,
        )
    )
    assert out == [None, "x"]


def test_retry_then_succeed(tmp_path, monkeypatch):
    monkeypatch.setattr(_gemini, "_DB_PATH", tmp_path / "llm.db")

    async def fast_sleep(*_):
        pass

    monkeypatch.setattr(_gemini.asyncio, "sleep", fast_sleep)
    attempts = {"n": 0}

    async def flaky(prompt, **kw):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise _gemini.RetryableError("429")
        return "ok"

    out = asyncio.run(
        _gemini.complete_batch(
            ["p"],
            model="m",
            schema=None,
            system=None,
            max_tokens=8,
            temperature=0.0,
            concurrency=1,
            max_retries=5,
            call=flaky,
        )
    )
    assert out == ["ok"]
    assert attempts["n"] == 3


def test_row_failure_isolated_to_null(tmp_path, monkeypatch):
    monkeypatch.setattr(_gemini, "_DB_PATH", tmp_path / "llm.db")

    async def fail_b(prompt, **kw):
        if prompt == "b":
            raise ValueError("boom")
        return prompt.upper()

    out = asyncio.run(
        _gemini.complete_batch(
            ["a", "b", "c"],
            model="m",
            schema=None,
            system=None,
            max_tokens=8,
            temperature=0.0,
            concurrency=4,
            max_retries=0,
            call=fail_b,
        )
    )
    assert out == ["A", None, "C"]  # b failed -> null, others fine


def test_embed_batch_caches_misses(tmp_path, monkeypatch):
    monkeypatch.setattr(_gemini, "_DB_PATH", tmp_path / "llm.db")
    seen = []

    async def fake_embed(text, *, model):
        seen.append(text)
        return [0.1, 0.2]

    out = asyncio.run(
        _gemini.embed_batch(
            ["x", "y", "x"],
            model="m",
            concurrency=4,
            dims=2,
            max_retries=1,
            call=fake_embed,
        )
    )
    assert out == [[0.1, 0.2], [0.1, 0.2], [0.1, 0.2]]
    assert seen.count("x") == 1  # second "x" served from cache


def test_embed_batch_none_passthrough(tmp_path, monkeypatch):
    monkeypatch.setattr(_gemini, "_DB_PATH", tmp_path / "llm.db")

    async def fake_embed(text, *, model):
        return [0.0, 0.0]

    out = asyncio.run(
        _gemini.embed_batch(
            [None, "y"],
            model="m",
            concurrency=2,
            dims=2,
            max_retries=0,
            call=fake_embed,
        )
    )
    assert out == [None, [0.0, 0.0]]


def test_embed_batch_dims_busts_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(_gemini, "_DB_PATH", tmp_path / "llm.db")
    seen = []

    async def fake_embed(text, *, model):
        seen.append(text)
        return [0.0, 0.0]

    asyncio.run(
        _gemini.embed_batch(
            ["x"], model="m", concurrency=1, dims=2, max_retries=0, call=fake_embed
        )
    )
    asyncio.run(
        _gemini.embed_batch(
            ["x"], model="m", concurrency=1, dims=4, max_retries=0, call=fake_embed
        )
    )
    assert seen == ["x", "x"]  # different dims is a different key, not a hit


def test_vertex_client_uses_llm_location(monkeypatch):
    from google import genai

    captured = {}
    monkeypatch.setattr(_gemini, "materialize_gcp_creds", lambda: None)
    monkeypatch.setattr(genai, "Client", lambda **kw: captured.update(kw) or "client")
    # Patch the settings object _gemini actually holds (other tests reload the
    # config module, which would otherwise desync a fresh config.settings).
    monkeypatch.setattr(_gemini.settings, "project", "p")
    monkeypatch.setattr(_gemini.settings, "location", "US")  # BQ loc must NOT be used
    monkeypatch.setattr(_gemini.settings, "llm_location", "europe-west4")

    _gemini._vertex_client()
    assert captured["location"] == "europe-west4"
    assert captured["project"] == "p"
