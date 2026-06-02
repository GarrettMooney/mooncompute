import json
from decimal import Decimal

import polars as pl
import pyarrow as pa

from mooncompute.gcp import bq
from tests.fakes import FakeBQClient


def _table() -> pa.Table:
    return pa.table({"amt": pa.array([Decimal("1.50")], type=pa.decimal128(10, 2))})


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(bq.bigquery, "Client", lambda project=None: client)


def test_cache_miss_queries_and_writes(monkeypatch, tmp_path):
    client = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, client)
    cache = tmp_path / "x.parquet"
    df = bq.extract_cached("SELECT 1", cache, project="p")
    assert df["amt"].dtype == pl.Float64
    assert cache.exists()
    manifest = json.loads((tmp_path / "x.parquet.manifest.json").read_text())
    assert manifest["sql_sha256"] == bq._sql_hash("SELECT 1")
    assert client.queries == ["SELECT 1"]


def test_cache_hit_does_not_query(monkeypatch, tmp_path):
    cache = tmp_path / "x.parquet"
    first = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, first)
    bq.extract_cached("SELECT 1", cache, project="p")

    second = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, second)
    bq.extract_cached("SELECT 1", cache, project="p")
    assert second.queries == []


def test_sql_change_requeries(monkeypatch, tmp_path):
    cache = tmp_path / "x.parquet"
    c1 = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, c1)
    bq.extract_cached("SELECT 1", cache, project="p")

    c2 = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, c2)
    bq.extract_cached("SELECT 2", cache, project="p")
    assert c2.queries == ["SELECT 2"]
    manifest = json.loads((tmp_path / "x.parquet.manifest.json").read_text())
    assert manifest["sql_sha256"] == bq._sql_hash("SELECT 2")


def test_manifestless_parquet_is_adopted(monkeypatch, tmp_path):
    cache = tmp_path / "x.parquet"
    pl.DataFrame({"amt": [9.0]}).write_parquet(cache)

    client = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, client)
    df = bq.extract_cached("SELECT 1", cache, project="p")
    assert client.queries == []
    assert df["amt"].to_list() == [9.0]
    assert (tmp_path / "x.parquet.manifest.json").exists()
