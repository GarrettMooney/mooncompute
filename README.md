# mooncompute

Ergonomic GCP data-access helpers, so you stop re-writing the same
BigQuery <-> polars, GCS, and credential boilerplate in every project.

`mooncompute.gcp` gives you:

- `bq2pl(sql)` - run SQL, get a polars DataFrame (Decimal columns cast to
  Float64 by default).
- `extract_cached(sql, cache, ...)` - the same, wrapped in a local parquet cache
  keyed by a hash of the SQL. You must pick an invalidation mode:
  `content_only=True` (re-query on SQL change only) or `max_age=<timedelta>`
  (also re-query past a TTL); a bare call raises.
- `pl2bq(df, dataset=, table=)` - load a polars DataFrame into BigQuery, with
  `enable_list_inference` set so ARRAY columns load correctly.
- `gcs.*` - read/write parquet, JSON, and bytes over `gs://` URIs.

## Install

Dependencies are split into extras so you install only what you use. The base
package is dependency-free (credential helper + `gs://` URI parsing); pull an
extra for actual I/O:

```sh
uv add "mooncompute[bq]"        # BigQuery <-> polars (polars, pyarrow, bigquery, db-dtypes)
uv add "mooncompute[gcs]"       # GCS JSON/bytes I/O (google-cloud-storage only)
uv add "mooncompute[bq,gcs]"    # both; also what you need for parquet on GCS
uv add "mooncompute[all]"       # alias for [bq,gcs]
```

GCS parquet helpers (`gcs.read_parquet` / `write_parquet`) lazily require polars,
so install `[bq,gcs]` for them; the JSON/bytes path stays polars-free under
`[gcs]` alone, keeping Cloud Function cold-starts light.

## Configuration

The common path: set `GOOGLE_CLOUD_PROJECT` once and omit `project=` everywhere.
The gcloud SDK already sets it, so on a configured machine there is nothing to
do; otherwise:

```sh
export GOOGLE_CLOUD_PROJECT=my-project
```

Pass `project=` on any call to override it (e.g. multi-project work). A call
that is given neither an explicit `project=` nor the env var raises.

Credentials use Application Default Credentials; in a Modal container, set a
`GOOGLE_APPLICATION_CREDENTIALS_JSON` secret and it is materialized to ADC
automatically.

## Usage

```python
from pathlib import Path

from mooncompute.gcp import bq, gcs

# With GOOGLE_CLOUD_PROJECT set, calls need no project= (pass project= to override):
df = bq.bq2pl("SELECT * FROM `proj.ds.tbl` LIMIT 10")

# SQL-hash-keyed parquet cache. You must pick an invalidation mode; a bare
# call raises. For a deterministic query (e.g. pinned to a snapshot date) pass
# content_only=True to invalidate on SQL change only:
df = bq.extract_cached(
    bq.read_sql("features.sql", snapshot="2024-10-01"),
    Path("data/features.parquet"),
    content_only=True,
)

# For a live/relative query, pass max_age to also re-query past a TTL (or use
# bq2pl for an always-live read). content_only and max_age are mutually exclusive.
from datetime import timedelta

df = bq.extract_cached(
    bq.read_sql("daily_active.sql"),
    Path("data/dau.parquet"),
    max_age=timedelta(hours=12),
)

# polars -> BQ (ARRAY columns handled via enable_list_inference)
bq.pl2bq(df, dataset="scratch", table="out")

# GCS round-trips over gs:// URIs
gcs.write_parquet(df, "gs://my-bucket/out.parquet")
df = gcs.read_parquet("gs://my-bucket/out.parquet")
```

## Deploying on GCP

mooncompute is happiest baked into a container. ADC comes from the workload's
service account, so credential materialization no-ops and the BQ/GCS clients
just work.

Recommended path (Cloud Run service or job, Vertex custom job, Kubeflow
container component): install at image-build time, then push to your registry.

```dockerfile
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
RUN pip install "mooncompute[bq,gcs]"
```

Caveats by target:

- **Cloud Run** (service or job): best fit. No install-auth or cold-start
  concerns once baked into the image.
- **Kubeflow lightweight components** (`packages_to_install=[...]`): the install
  runs in-pod at runtime; prefer a container component for a public/private
  install you control.
- **Cloud Functions**: weakest fit, but install `mooncompute[gcs]` to keep the
  JSON/bytes path free of polars + pyarrow and their cold-start cost. Note
  `extract_cached`'s parquet cache only lives in ephemeral `/tmp`; prefer `gcs.*`
  for durable artifacts there.

## Scope

v1 ships `mooncompute.gcp` only. Modal, LLM/eval, and run-utils tiers may
follow; see `docs/ROADMAP.md`.

## Develop

```sh
just            # fmt + lint + typecheck + test
just ci         # non-mutating gate (check formatting, lint, typecheck, test)
```
