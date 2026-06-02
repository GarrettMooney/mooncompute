import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import polars as pl
import pyarrow as pa
import pytest

from mooncompute.gcp import bq
from tests.fakes import FakeBQClient


def _table() -> pa.Table:
    return pa.table({"amt": pa.array([Decimal("1.50")], type=pa.decimal128(10, 2))})


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(bq.bigquery, "Client", lambda **_: client)


def test_requires_an_invalidation_mode(monkeypatch, tmp_path):
    client = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, client)
    cache = tmp_path / "x.parquet"
    with pytest.raises(ValueError, match="invalidation mode"):
        bq.extract_cached("SELECT 1", cache, project="p")
    assert client.queries == []  # raised before touching BQ


def test_content_only_and_max_age_are_mutually_exclusive(monkeypatch, tmp_path):
    client = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, client)
    cache = tmp_path / "x.parquet"
    with pytest.raises(ValueError, match="contradictory"):
        bq.extract_cached(
            "SELECT 1",
            cache,
            project="p",
            content_only=True,
            max_age=timedelta(hours=1),
        )


def test_cache_miss_queries_and_writes(monkeypatch, tmp_path):
    client = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, client)
    cache = tmp_path / "x.parquet"
    df = bq.extract_cached("SELECT 1", cache, project="p", content_only=True)
    assert df["amt"].dtype == pl.Float64
    assert cache.exists()
    manifest = json.loads((tmp_path / "x.parquet.manifest.json").read_text())
    assert manifest["sql_sha256"] == bq._sql_hash("SELECT 1")
    assert client.queries == ["SELECT 1"]


def test_cache_hit_does_not_query(monkeypatch, tmp_path):
    cache = tmp_path / "x.parquet"
    first = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, first)
    bq.extract_cached("SELECT 1", cache, project="p", content_only=True)

    second = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, second)
    bq.extract_cached("SELECT 1", cache, project="p", content_only=True)
    assert second.queries == []


def test_sql_change_requeries(monkeypatch, tmp_path):
    cache = tmp_path / "x.parquet"
    c1 = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, c1)
    bq.extract_cached("SELECT 1", cache, project="p", content_only=True)

    c2 = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, c2)
    bq.extract_cached("SELECT 2", cache, project="p", content_only=True)
    assert c2.queries == ["SELECT 2"]
    manifest = json.loads((tmp_path / "x.parquet.manifest.json").read_text())
    assert manifest["sql_sha256"] == bq._sql_hash("SELECT 2")


def test_manifestless_parquet_is_adopted(monkeypatch, tmp_path):
    cache = tmp_path / "x.parquet"
    pl.DataFrame({"amt": [9.0]}).write_parquet(cache)

    client = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, client)
    df = bq.extract_cached("SELECT 1", cache, project="p", content_only=True)
    assert client.queries == []
    assert df["amt"].to_list() == [9.0]
    assert (tmp_path / "x.parquet.manifest.json").exists()


def _age_manifest(tmp_path, delta: timedelta) -> None:
    mpath = tmp_path / "x.parquet.manifest.json"
    data = json.loads(mpath.read_text())
    data["written_at"] = (datetime.now(UTC) - delta).isoformat(timespec="seconds")
    mpath.write_text(json.dumps(data))


def test_max_age_expired_requeries(monkeypatch, tmp_path):
    cache = tmp_path / "x.parquet"
    first = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, first)
    bq.extract_cached("SELECT 1", cache, project="p", content_only=True)

    _age_manifest(tmp_path, timedelta(hours=2))

    second = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, second)
    bq.extract_cached("SELECT 1", cache, project="p", max_age=timedelta(hours=1))
    assert second.queries == ["SELECT 1"]  # expired -> re-queried


def test_max_age_fresh_served_from_cache(monkeypatch, tmp_path):
    cache = tmp_path / "x.parquet"
    first = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, first)
    bq.extract_cached("SELECT 1", cache, project="p", content_only=True)

    second = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, second)
    bq.extract_cached("SELECT 1", cache, project="p", max_age=timedelta(hours=1))
    assert second.queries == []  # within max_age -> served from cache


def test_corrupt_cache_fails_open_and_requeries(monkeypatch, tmp_path):
    cache = tmp_path / "x.parquet"
    first = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, first)
    # writes valid cache + manifest
    bq.extract_cached("SELECT 1", cache, project="p", content_only=True)

    cache.write_bytes(b"not a parquet file")  # corrupt the cache, keep the manifest

    second = FakeBQClient(project="p", table=_table())
    _patch_client(monkeypatch, second)
    df = bq.extract_cached("SELECT 1", cache, project="p", content_only=True)
    assert second.queries == ["SELECT 1"]  # unreadable cache -> re-queried, not raised
    assert df["amt"].dtype == pl.Float64
