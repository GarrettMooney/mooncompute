import polars as pl
import pytest

from mooncompute import io as mio


def test_scheme_detection():
    assert mio._scheme("bq://p.d.t") == "bq"
    assert mio._scheme("gs://b/x.parquet") == "gs"
    assert mio._scheme("select * from t") == "sql"
    with pytest.raises(ValueError):
        mio._scheme("relative/path")


def test_read_dispatches_gs(monkeypatch):
    from mooncompute.sources import gcs

    monkeypatch.setattr(
        gcs, "scan_parquet", lambda uri, columns=None: pl.LazyFrame({"a": [1]})
    )
    out = mio.read("gs://b/x.parquet")
    assert isinstance(out, pl.LazyFrame)


def test_read_dispatches_bq(monkeypatch):
    from mooncompute.sources import bigquery

    seen = {}
    monkeypatch.setattr(
        bigquery,
        "read_table",
        lambda uri, **kw: seen.setdefault("uri", uri) or pl.DataFrame({"a": [1]}),
    )
    mio.read("bq://p.d.t", lazy=False)
    assert seen["uri"] == "bq://p.d.t"


def test_read_cache_pinned_routes_to_read_cached(monkeypatch):
    import importlib

    cache = importlib.import_module("mooncompute.cache")

    called = {}
    monkeypatch.setattr(
        cache,
        "read_cached",
        lambda *a, **k: called.setdefault("hit", True) or pl.DataFrame({"a": [1]}),
    )
    mio.read("bq://p.d.t", cache="pinned")
    assert called["hit"]
