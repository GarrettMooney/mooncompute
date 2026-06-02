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
`docs/superpowers/specs/2026-06-02-dswkit-design.md`. Planned changes are in
`docs/ROADMAP.md`.

## Deploying on GCP

dswkit is happiest baked into a container. ADC comes from the workload's
service account, so `materialize_gcp_creds` no-ops and the BQ/GCS clients just
work.

Recommended path (Cloud Run service or job, Vertex custom job, KFP container
component): install the package at image-build time, where the Azure PAT is
available, then push to Artifact Registry / GCR.

```dockerfile
# syntax=docker/dockerfile:1
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
# PAT supplied as a build secret, never baked into a layer
RUN --mount=type=secret,id=azpat \
    pip install "git+https://$(cat /run/secrets/azpat)@dev.azure.com/designerbrands/IT/_git/dswkit@v0.1.0"
```

Build with `--platform linux/amd64` and `--secret id=azpat,src=...`.

Caveats by target:

- **Cloud Run** (service or job): best fit. Container path above, no install-auth
  or cold-start concerns.
- **KFP lightweight components** (`packages_to_install=[...]`): the install
  happens in-pod at runtime and needs the Azure PAT injected there. Prefer a
  container component instead.
- **Cloud Functions**: weakest fit. The deploy buildpack has no Azure PAT (so the
  private git install fails), polars + pyarrow add real cold-start cost, and
  `extract_cached`'s parquet cache only lives in ephemeral `/tmp`. For GCP-native
  installs without a PAT, mirror dswkit to an Artifact Registry Python repo (see
  ROADMAP).

## Develop

    just            # fmt + lint + typecheck + test
    just ci         # non-mutating gate (check formatting, lint, typecheck, test)
