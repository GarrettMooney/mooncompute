# mooncompute

[![PyPI](https://img.shields.io/pypi/v/mooncompute.svg)](https://pypi.org/project/mooncompute/)
[![Python versions](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://pypi.org/project/mooncompute/)
[![CI](https://github.com/GarrettMooney/mooncompute/actions/workflows/test.yml/badge.svg)](https://github.com/GarrettMooney/mooncompute/actions/workflows/test.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Thin Arrow-glue for data-science iteration. One `read`/`write`/`sql` surface over
BigQuery, GCS, Polars, and DuckDB, plus LLM calls as a column operation.

Arrow is already the common substrate: BigQuery's Storage API, Polars, DuckDB,
and Parquet all speak it. mooncompute does not reimplement those connectors. It
adds the two things they leave to you, which is where the iteration-speed and
cost wins live: **content-addressed caching** and **LLM-as-a-column-op** with
per-row caching and bounded concurrency.

```python
import mooncompute as mc
from pydantic import BaseModel

df  = mc.read("bq://proj.dataset.events", columns=["user_id", "review"])
out = mc.sql("select user_id, count(*) n from df group by 1 order by n desc")

class Sentiment(BaseModel):
    label: str
    score: float

scored = df.with_columns(
    mc.llm.map("review", prompt="Classify the sentiment: {review}",
               schema=Sentiment).alias("sentiment")
)
mc.write(scored, "bq://proj.dataset.scored", mode="overwrite")
```

## Install

Core pulls the Arrow substrate (`polars`, `pyarrow`, `duckdb`), so `mc.read("gs://...")`
and `mc.sql(...)` work on a bare install. GCP clients and the LLM provider are
opt-in extras:

```sh
uv add "mooncompute[gcp]"   # BigQuery + GCS (google-cloud-bigquery, storage, db-dtypes)
uv add "mooncompute[llm]"   # Vertex Gemini column ops (google-genai)
uv add "mooncompute[all]"   # both
```

## Configuration

Auth is Application Default Credentials only. Locally that is
`gcloud auth application-default login`; in CI or on GCP it is the attached
service account. In a Modal container, set a `GOOGLE_APPLICATION_CREDENTIALS_JSON`
secret and it is materialized to ADC automatically.

Set the project once via the environment, or configure at startup:

```sh
export MOONCOMPUTE_PROJECT=my-project   # GOOGLE_CLOUD_PROJECT also works
```

```python
mc.configure(project="my-project", location="US")
```

## Reading and writing

`read` dispatches on the URI scheme and is lazy by default (returns a Polars
`LazyFrame`, so column and predicate selection push down; compute fires on
`.collect()`):

```python
mc.read("bq://proj.ds.table", columns=["a", "b"])      # Storage API, server-side projection
mc.read("gs://bucket/events/*.parquet")                # scan_parquet, pushdown preserved
mc.read("select * from `proj.ds.t` where dt = @dt", dt="2024-01-01")  # parameterized BQ
mc.read("bq://proj.ds.t", lazy=False)                  # eager DataFrame

mc.dry_run("select ...")        # {'bytes_processed': int, 'estimated_usd': float}
mc.write(df, "gs://bucket/out.parquet")
mc.write(df, "bq://proj.ds.out", mode="append")
```

A BigQuery dry-run guardrail runs before execution; set
`mc.configure(bq_max_bytes_billed=...)` to reject queries that would scan more
than a cap, and the same cap is enforced server-side on the real job.

## Caching

Re-running an expensive scan or a long feature build should become a metadata
check. The `@cache` decorator memoizes any frame-returning function to a local
Parquet store; the key folds in the function's source, so editing the body
invalidates automatically.

There is no silent default: you pick an invalidation mode, because whether
identical inputs may be served from cache depends on the query.

```python
@mc.cache(ttl="6h")        # live/relative query: re-run past the TTL
def daily_active(date): ...

@mc.cache(pinned=True)      # deterministic/pinned query: invalidate on source change only
def dim_users(): ...
```

The same modes apply to `read` via the `cache` argument:

```python
mc.read(sql, cache="6h")        # TTL mode
mc.read("bq://proj.ds.dim", cache="pinned")   # content mode
```

For `bq://` sources a freshness token (the table's `modified` timestamp) is
folded into the staleness check, so a reloaded table busts even a pinned entry.
Writes are atomic and reads fail open: a corrupt cache re-runs the source rather
than raising. The cache is local in v0.4; a shared GCS tier is planned.

## LLM column operations

`llm.map` and `llm.embed` return Polars expressions, so they compose with
`with_columns`. Under the hood each hands the whole column to an async
bounded-concurrency fan-out against Vertex Gemini (ADC auth), with retry and a
SQLite per-row cache keyed on `(model, system, prompt, schema, params)`. A re-run
after editing three rows costs three calls, not N.

```python
df = df.with_columns(
    mc.llm.map("review", prompt="Classify: {review}", schema=Sentiment).alias("label")
)
emb = df.with_columns(mc.llm.embed("text").alias("vec"))   # List(Float32) column
```

With a Pydantic `schema`, `map` uses structured output and returns a Struct
column; otherwise a Utf8 column. `temperature` defaults to `0.0` so the cache is
meaningful. Tradeoff: a mapped column materializes at `.collect()` (it cannot
stay lazy past the LLM call). `embed` returns a `List(Float32)` column; cast it
to `pl.Array(pl.Float32, k)` to pair with `sql()` and DuckDB's VSS extension for
similarity search on the same in-memory data.

## DuckDB interop

`sql()` runs DuckDB SQL over in-scope Polars frames (zero-copy via Arrow) and
returns a Polars frame. Reach for it when SQL is terser: window functions, ASOF
joins, or VSS search over an embedding column.

```python
out = mc.sql("select user_id, count(*) n from df group by 1")   # df captured from scope
```

## Migration from v0.3

v0.4 is a clean break. The `mooncompute.gcp.*` namespace is gone in favor of the
top-level surface.

| v0.3 | v0.4 |
| --- | --- |
| `from mooncompute.gcp import bq2pl; bq2pl(sql)` | `mc.read(sql, lazy=False)` |
| `gcp.extract_cached(sql, path, max_age=...)` | `mc.read(sql, cache="6h")` |
| `gcp.extract_cached(sql, path, content_only=True)` | `mc.read(sql, cache="pinned")` |
| `gcp.pl2bq(df, dataset=, table=)` | `mc.write(df, "bq://proj.ds.table", mode="append")` |
| `gcp.gcs.read_parquet(uri)` | `mc.read(uri, lazy=False)` |

## Deferred

Planned, not in v0.4: a shared GCS cache tier, the LLM Batch API (~50% cheaper),
and USD spend-cap accounting.

## Develop

```sh
just            # fmt + lint + typecheck + test
just ci         # non-mutating gate (check formatting, lint, typecheck, test)
```
