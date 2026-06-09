import datetime
from decimal import Decimal

import polars as pl
import pyarrow as pa
import pytest

from mooncompute.sources import bigquery as bq
from tests.fakes import FakeBQClient


def _arrow():
    vals = [Decimal("1.5"), Decimal("2.5")]
    return pa.table({"id": [1, 2], "amt": pa.array(vals, pa.decimal128(5, 2))})


def test_bq2pl_recasts_decimals():
    client = FakeBQClient(table=_arrow())
    df = bq.bq2pl("select 1", client=client)
    assert df["amt"].dtype == pl.Float64


def test_read_query_dry_run_guardrail():
    client = FakeBQClient(table=_arrow())
    client.dry_run_bytes = 10**12
    try:
        bq.read_query(
            "select 1",
            params={},
            lazy=False,
            engine="polars",
            client=client,
            max_bytes_billed=10**6,
        )
    except RuntimeError as e:
        assert "over cap" in str(e)
    else:
        raise AssertionError("expected RuntimeError")


def test_table_modified_returns_isoformat():
    client = FakeBQClient(table=_arrow())
    client._modified = datetime.datetime(2024, 5, 1, tzinfo=datetime.UTC)
    token = bq.table_modified("bq://proj.ds.t", client=client)
    assert token.startswith("2024-05-01")


def test_read_table_lazy_returns_lazyframe():
    client = FakeBQClient(table=_arrow())
    lf = bq.read_table(
        "bq://proj.ds.t", lazy=True, columns=["id"], engine="polars", client=client
    )
    assert isinstance(lf, pl.LazyFrame)
    q = client.queries[0]
    assert "`id`" in q and "*" not in q  # server-side projection, not select *


def test_query_params_types_and_bool_before_int():
    ps = bq._query_params({"flag": True, "n": 3, "x": 1.5, "s": "a"})
    types = {p.name: p.type_ for p in ps}
    assert types == {"flag": "BOOL", "n": "INT64", "x": "FLOAT64", "s": "STRING"}


def test_query_params_rejects_unknown_type():
    with pytest.raises(TypeError):
        bq._query_params({"d": object()})


def test_write_table_disposition_and_job_id():
    client = FakeBQClient()
    bq.write_table(
        pl.DataFrame({"a": [1, 2]}),
        "bq://proj.ds.t",
        mode="append",
        client=client,
        job_id="job-1",
    )
    call = client.load_calls[0]
    assert call["job_config"].write_disposition == "WRITE_APPEND"
    assert call["job_id"] == "job-1"


def test_write_table_rejects_bad_mode():
    client = FakeBQClient()
    with pytest.raises(ValueError):
        bq.write_table(
            pl.DataFrame({"a": [1]}), "bq://p.d.t", mode="nope", client=client
        )
