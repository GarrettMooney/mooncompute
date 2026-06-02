import polars as pl
import pytest

from dswkit.gcp import gcs
from tests.fakes import FakeStorageClient


def _use_fake(monkeypatch) -> dict:
    store: dict = {}
    monkeypatch.setattr(gcs.storage, "Client", lambda *a, **k: FakeStorageClient(store))
    return store


def test_parse_uri_ok():
    assert gcs._parse_uri("gs://my-bucket/path/to/obj.parquet") == (
        "my-bucket",
        "path/to/obj.parquet",
    )


def test_parse_uri_rejects_non_gs():
    with pytest.raises(ValueError):
        gcs._parse_uri("s3://nope/x")


def test_parquet_roundtrip(monkeypatch):
    _use_fake(monkeypatch)
    df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    gcs.write_parquet(df, "gs://bkt/data.parquet")
    got = gcs.read_parquet("gs://bkt/data.parquet")
    assert got.equals(df)


def test_json_roundtrip(monkeypatch):
    _use_fake(monkeypatch)
    gcs.write_json("gs://bkt/x.json", {"k": 1, "vals": [1, 2, 3]})
    assert gcs.read_json("gs://bkt/x.json") == {"k": 1, "vals": [1, 2, 3]}


def test_bytes_roundtrip(monkeypatch):
    _use_fake(monkeypatch)
    gcs.write_bytes("gs://bkt/blob.bin", b"\x00\x01\x02")
    assert gcs.read_bytes("gs://bkt/blob.bin") == b"\x00\x01\x02"


def test_read_parquet_glob_concats_shards(monkeypatch):
    _use_fake(monkeypatch)
    gcs.write_parquet(pl.DataFrame({"a": [1]}), "gs://bkt/out/part-0.parquet")
    gcs.write_parquet(pl.DataFrame({"a": [2]}), "gs://bkt/out/part-1.parquet")
    got = gcs.read_parquet_glob("gs://bkt/out").sort("a")
    assert got["a"].to_list() == [1, 2]


def test_read_parquet_glob_raises_when_empty(monkeypatch):
    _use_fake(monkeypatch)
    with pytest.raises(FileNotFoundError):
        gcs.read_parquet_glob("gs://bkt/missing")
