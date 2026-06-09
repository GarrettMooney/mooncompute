import polars as pl
import pytest

from mooncompute import llm


def test_map_utf8(monkeypatch):
    from mooncompute.llm import _gemini

    async def fake_batch(prompts, **kw):
        return [None if p is None else p.split()[-1].upper() for p in prompts]

    monkeypatch.setattr(_gemini, "complete_batch", fake_batch)
    df = pl.DataFrame({"review": ["love it", "hate it"]})
    out = df.with_columns(llm.map("review", prompt="Sentiment: {review}").alias("s"))
    assert out["s"].to_list() == ["IT", "IT"]


def test_map_struct_schema(monkeypatch):
    from mooncompute.llm import _gemini

    async def fake_batch(prompts, **kw):
        return [{"label": "pos", "score": 0.9} for _ in prompts]

    monkeypatch.setattr(_gemini, "complete_batch", fake_batch)
    df = pl.DataFrame({"review": ["a", "b"]})
    out = df.with_columns(llm.map("review", prompt="{review}", schema=dict).alias("r"))
    assert out["r"].struct.field("label").to_list() == ["pos", "pos"]


def test_embed(monkeypatch):
    from mooncompute.llm import _gemini

    async def fake_embed(texts, **kw):
        return [[0.1, 0.2, 0.3] for _ in texts]

    monkeypatch.setattr(_gemini, "embed_batch", fake_embed)
    df = pl.DataFrame({"t": ["x", "y"]})
    out = df.with_columns(llm.embed("t").alias("v"))
    # stored as Array(Float32), so compare approximately (float32 rounds 0.1)
    assert out["v"].to_list()[0] == pytest.approx([0.1, 0.2, 0.3], rel=1e-6)
