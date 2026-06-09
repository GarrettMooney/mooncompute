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
