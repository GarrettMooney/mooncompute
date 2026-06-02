from decimal import Decimal
from typing import cast

import polars as pl
import pyarrow as pa
from google.cloud import bigquery

from mooncompute.gcp import bq
from tests.fakes import FakeBQClient


def _table_with_decimal() -> pa.Table:
    return pa.table(
        {
            "amt": pa.array(
                [Decimal("1.50"), Decimal("2.25")], type=pa.decimal128(10, 2)
            ),
            "n": pa.array([1, 2], type=pa.int64()),
        }
    )


def test_bq2pl_with_injected_client_casts_decimals():
    client = FakeBQClient(table=_table_with_decimal())
    df = bq.bq2pl("SELECT 1", client=cast(bigquery.Client, client))
    assert df["amt"].dtype == pl.Float64
    assert df["amt"].to_list() == [1.5, 2.25]
    assert client.queries == ["SELECT 1"]


def test_bq2pl_can_disable_decimal_cast():
    client = FakeBQClient(table=_table_with_decimal())
    df = bq.bq2pl(
        "SELECT 1", client=cast(bigquery.Client, client), decimals_to_float=False
    )
    assert str(df["amt"].dtype).startswith("Decimal")


def test_bq2pl_builds_client_from_project(monkeypatch):
    captured = {}

    def fake_ctor(project: str | None = None):
        captured["project"] = project
        return FakeBQClient(project=project or "p", table=_table_with_decimal())

    monkeypatch.setattr(bq.bigquery, "Client", fake_ctor)
    bq.bq2pl("SELECT 1", project="my-proj")
    assert captured["project"] == "my-proj"
