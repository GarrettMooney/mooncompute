import polars as pl

from dswkit.gcp import bq
from tests.fakes import FakeBQClient


def test_pl2bq_enables_list_inference_and_targets_table(monkeypatch):
    client = FakeBQClient(project="p")
    monkeypatch.setattr(bq.bigquery, "Client", lambda project=None: client)

    bq.pl2bq(
        pl.DataFrame({"a": [1, 2]}),
        project="my-proj",
        dataset="ds",
        table="tbl",
    )

    assert len(client.load_calls) == 1
    call = client.load_calls[0]
    assert call["destination"] == "my-proj.ds.tbl"
    assert call["parquet_options"].enable_list_inference is True
    assert call["source_format"] == bq.bigquery.SourceFormat.PARQUET
