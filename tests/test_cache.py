import polars as pl
import pytest

from mooncompute import cache as mc
from mooncompute.config import settings


@pytest.fixture(autouse=True)
def tmp_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "cache_dir", str(tmp_path))
    monkeypatch.setattr(settings, "cache_enabled", True)


def test_ttl_seconds():
    assert mc._ttl_seconds("30m") == 1800
    assert mc._ttl_seconds("6h") == 21600
    assert mc._ttl_seconds("7d") == 604800
    with pytest.raises(ValueError):
        mc._ttl_seconds("6x")


def test_bare_cache_requires_mode():
    with pytest.raises(ValueError):

        @mc.cache
        def f():
            return pl.DataFrame({"a": [1]})


def test_cache_decorator_memoizes():
    calls = {"n": 0}

    @mc.cache(pinned=True)
    def features(date):
        calls["n"] += 1
        return pl.DataFrame({"a": [1], "d": [date]})

    a = features("2024-01-01")
    b = features("2024-01-01")
    assert calls["n"] == 1  # second call is a cache hit
    assert a.equals(b)
    features("2024-02-01")  # different arg -> miss
    assert calls["n"] == 2


def test_cache_fails_open_on_corrupt(tmp_path):
    @mc.cache(pinned=True)
    def f():
        return pl.DataFrame({"a": [1]})

    f()  # populate
    for p in tmp_path.glob("*.parquet"):
        p.write_bytes(b"not parquet")
    out = f()  # must re-run, not raise
    assert out["a"][0] == 1


def test_key_changes_with_function_source():
    def v1():
        return pl.DataFrame({"a": [1]})

    def v2():
        return pl.DataFrame({"a": [2]})  # different body

    k1 = mc._key(mc._function_fingerprint(v1), (), {}, None)
    k2 = mc._key(mc._function_fingerprint(v2), (), {}, None)
    assert k1 != k2


def test_ttl_expiry_triggers_rerun(monkeypatch):
    calls = {"n": 0}

    @mc.cache(ttl="1h")
    def f():
        calls["n"] += 1
        return pl.DataFrame({"a": [calls["n"]]})

    f()  # n == 1, written "now"
    real = mc.time.time
    monkeypatch.setattr(mc.time, "time", lambda: real() + 7200)  # +2h > 1h ttl
    out = f()
    assert calls["n"] == 2
    assert out["a"][0] == 2


def test_read_cached_hit_then_freshness_busts(monkeypatch):
    import mooncompute.io as mio

    fresh = {"v": "t1"}
    monkeypatch.setattr(mc, "source_fingerprint", lambda s: fresh["v"])
    runs = {"n": 0}

    def fake_read(source, cache=None, **kw):
        runs["n"] += 1
        return pl.DataFrame({"a": [runs["n"]]})

    monkeypatch.setattr(mio, "read", fake_read)

    mc.read_cached("bq://p.d.t", cache="pinned", lazy=False)
    assert runs["n"] == 1
    mc.read_cached("bq://p.d.t", cache="pinned", lazy=False)
    assert runs["n"] == 1  # cache hit, same freshness
    fresh["v"] = "t2"  # table reloaded
    mc.read_cached("bq://p.d.t", cache="pinned", lazy=False)
    assert runs["n"] == 2  # freshness mismatch busts the entry


def test_read_cached_lazy_and_eager_share_artifact(monkeypatch):
    import mooncompute.io as mio

    monkeypatch.setattr(mc, "source_fingerprint", lambda s: "")
    runs = {"n": 0}

    def fake_read(source, cache=None, **kw):
        runs["n"] += 1
        return pl.DataFrame({"a": [1]})

    monkeypatch.setattr(mio, "read", fake_read)

    mc.read_cached("bq://p.d.t", cache="pinned", lazy=False)
    out = mc.read_cached("bq://p.d.t", cache="pinned", lazy=True)
    assert runs["n"] == 1  # lazy read hit the artifact written by the eager read
    assert isinstance(out, pl.LazyFrame)


def test_clear_cache(monkeypatch):
    @mc.cache(pinned=True)
    def f():
        return pl.DataFrame({"a": [1]})

    f()
    assert mc.clear_cache() >= 2  # parquet + manifest removed
    assert mc.clear_cache() == 0


def test_cache_disabled_bypasses(monkeypatch):
    monkeypatch.setattr(settings, "cache_enabled", False)
    calls = {"n": 0}

    @mc.cache(pinned=True)
    def f():
        calls["n"] += 1
        return pl.DataFrame({"a": [1]})

    f()
    f()
    assert calls["n"] == 2  # no caching when disabled
