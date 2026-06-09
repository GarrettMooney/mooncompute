import asyncio

from mooncompute.llm import _gemini


def test_per_row_cache_only_calls_misses(tmp_path, monkeypatch):
    monkeypatch.setattr(_gemini, "_DB_PATH", tmp_path / "llm.db")
    calls = []

    async def fake_call(prompt, **kw):
        calls.append(prompt)
        return prompt.upper()

    prompts = ["a", "b", "a"]  # "a" repeats -> one cached
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
