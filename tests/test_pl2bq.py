from typing import cast

import polars as pl
from google.cloud import bigquery

from mooncompute.gcp import bq
from tests.fakes import FakeBQClient


def test_pl2bq_enables_list_inference_and_targets_table():
    client = FakeBQClient(project="p")
    bq.pl2bq(
        pl.DataFrame({"a": [1, 2]}),
        project="my-proj",
        dataset="ds",
        table="tbl",
        client=cast(bigquery.Client, client),
    )

    assert len(client.load_calls) == 1
    call = client.load_calls[0]
    assert call["destination"] == "my-proj.ds.tbl"
    job_config = call["job_config"]
    assert job_config.parquet_options.enable_list_inference is True
    assert job_config.source_format == bq.bigquery.SourceFormat.PARQUET


def test_pl2bq_passes_job_id_as_idempotency_key():
    client = FakeBQClient(project="p")
    bq.pl2bq(
        pl.DataFrame({"a": [1, 2]}),
        project="my-proj",
        dataset="ds",
        table="tbl",
        client=cast(bigquery.Client, client),
        job_id="load-abc123",
    )
    assert client.load_calls[0]["job_id"] == "load-abc123"
