# dswkit

Shared GCP data-access helpers for Designer Brands marketing data science.
Harvested from the best existing implementations across `~/work` to stop
re-writing BigQuery↔polars, GCS, and credential boilerplate.

## Install

Pin a tag (works locally, in CI, Modal, and Vertex with a PAT):

    uv add "git+https://dev.azure.com/designerbrands/IT/_git/dswkit@v0.1.0"

## Usage

```python
from dswkit.gcp import bq, gcs

# query -> polars (Decimal columns cast to Float64 by default)
df = bq.bq2pl("SELECT * FROM `proj.ds.tbl` LIMIT 10", project=bq.PROJECT_PROD)

# SQL-hash-keyed parquet cache: re-queries only when the SQL changes
from pathlib import Path
df = bq.extract_cached(bq.read_sql("features.sql", snapshot="2024-10-01"),
                       Path("data/features.parquet"), project=bq.PROJECT_PROD)

# polars -> BQ (ARRAY columns handled via enable_list_inference)
bq.pl2bq(df, project=bq.PROJECT_DEV, dataset="garrett_scratch", table="out")

# GCS round-trips over gs:// URIs
gcs.write_parquet(df, "gs://gcp-dsw-data-lake-dev-garrett/out.parquet")
df = gcs.read_parquet("gs://gcp-dsw-data-lake-dev-garrett/out.parquet")
```

## Scope

v1 ships `dswkit.gcp` only. Modal, LLM/eval, and run-utils tiers may follow
once this proves adoption. See the design spec in the work repo:
`docs/superpowers/specs/2026-06-02-dswkit-design.md`.

## Develop

    uv run pytest
