# mooncompute v0.4 — design

- Date: 2026-06-09
- Status: approved, pre-implementation
- Source of vision: `docs/claude-sessions/arrow-based-python-package-for-data-science-workflows/`

## Summary

v0.4 rebuilds mooncompute around the "flow" vision from the Claude session: an
opinionated, thin orchestration layer over Arrow for BigQuery / GCS / Polars /
DuckDB plus LLM column operations. We do not reimplement connectors; Polars,
DuckDB, the BigQuery Storage API, and Parquet all speak Arrow, and this package
is glue over that substrate. The differentiated value lives in exactly three
places:

1. one I/O surface (`read` / `write`), lazy by default, URI-dispatched;
2. content-addressed caching (the iteration unlock);
3. LLM-as-a-column-operation with per-row caching and bounded concurrency.

The v0.3 package (`mooncompute.gcp.*`) is replaced. Its hard-won implementation
details are ported forward, but the public namespace changes.

## Decisions

These were settled during brainstorming and are not open for re-litigation
during implementation:

- **Architecture follows the session stubs.** The stub module layout
  (`io` / `cache` / `sql` / `llm` / `sources`) is the target shape.
- **Port v0.3's hard-won details forward**, not its structure: decimal→float
  recasting, atomic temp-file+rename writes, the BigQuery Storage API fast path,
  the "you must pick an invalidation mode" discipline, idempotent load-job
  `job_id`, and ADC/Modal credential materialization.
- **Top-level surface only, clean break.** `import mooncompute as mc` exposes
  `read / write / dry_run / sql / llm / cache / clear_cache / configure /
  settings / Settings`. The old `mooncompute.gcp.*` namespace is removed; that
  code becomes private `mooncompute.sources.*`. This is a breaking change and
  the version bump (0.4.0) signals it.
- **Cache scope (Goedecke option 1):** generalize `@cache` to memoize any
  frame-returning function, add a BigQuery `table.modified` freshness token, and
  keep the boring local parquet+manifest store. The shared GCS tier is a
  documented seam to fill in a later version, not built now.
- **LLM provider: Vertex AI / Gemini** for both `map` and `embed`, authenticated
  via ADC. No multi-provider abstraction.

## Public API

```python
import mooncompute as mc
from pydantic import BaseModel

# --- I/O surface (lazy by default) ---
df  = mc.read("bq://proj.dataset.table", columns=["user_id", "ts"])
gcs = mc.read("gs://bucket/events/*.parquet")
q   = mc.read("select * from `proj.ds.t` where dt = @dt", dt="2024-01-01",
              cache="6h")
mc.write(df, "bq://proj.dataset.out", mode="overwrite")
est = mc.dry_run("select ... ")          # {'bytes_processed', 'estimated_usd'}

# --- DuckDB <-> Polars interop ---
out = mc.sql("select user_id, count(*) n from df group by 1 order by n desc")

# --- LLM as a column op ---
class Sentiment(BaseModel):
    label: str
    score: float

df = df.with_columns(
    mc.llm.map("review", prompt="Classify: {review}", schema=Sentiment)
       .alias("label")
)
emb = df.with_columns(mc.llm.embed("text").alias("vec"))

# --- caching ---
@mc.cache(ttl="6h")          # live query: TTL invalidation
def features(date): ...

@mc.cache(pinned=True)       # deterministic/pinned query: source-hash only
def dim_table(): ...

# --- configure once at startup ---
mc.configure(project="my-proj", location="US")
```

## Package layout

```
src/mooncompute/
  __init__.py     # public exports + __version__
  config.py       # Settings dataclass, configure(), settings singleton
  io.py           # read / write / dry_run — URI-scheme dispatch
  cache.py        # @cache, read_cached, CacheStore (local), source_fingerprint
  sql.py          # DuckDB <-> Polars zero-copy interop
  llm/
    __init__.py   # re-export map, embed
    ops.py        # map / embed as pl.Expr via map_batches
    _gemini.py    # Vertex Gemini client: structured output, embeddings,
                  # concurrency, retry, per-row cache
  sources/
    __init__.py
    bigquery.py   # read_table / read_query / write_table / dry_run
    gcs.py        # scan / read / write parquet, json, bytes
  _creds.py       # materialize_gcp_creds (ported as-is from v0.3)
  py.typed
```

v0.3 → v0.4 source mapping:

- `gcp/bq.py` → `sources/bigquery.py` (bq2pl, pl2bq, extract_cached logic,
  read_sql, `_decimals_to_float`, `Manifest`, atomic writes) plus new
  Storage-API table reads and `dry_run`.
- `gcp/gcs.py` → `sources/gcs.py` (parquet/json/bytes helpers, glob read).
- `gcp/_creds.py` → `_creds.py` (unchanged).

## Configuration

`Settings` dataclass, process-global `settings` singleton, `configure(**kwargs)`
mutator. Auth is ADC-only; key files are never threaded through call sites.

Environment variables are renamed from the stub's `FLOW_*` to `MOONCOMPUTE_*`:

- `MOONCOMPUTE_PROJECT` (falls back to `GOOGLE_CLOUD_PROJECT`)
- `MOONCOMPUTE_CACHE_DIR` (default `~/.mooncompute/cache`)

Fields: `project`, `location` (default `US`), `cache_dir`, `cache_enabled`,
`llm_default_model` (default `gemini-2.5-flash`), `llm_embed_model` (default
`gemini-embedding-001`), `llm_concurrency` (default 16), `llm_max_retries`
(default 6), `bq_dry_run_default` (True), `bq_max_bytes_billed` (None),
`engine` ("polars"). Spend-cap and batch-API fields are documented as inert in
v0.4 (see Deferred).

Exact Vertex model IDs are confirmed at build time; the planned defaults are
`gemini-2.5-flash` (map) and `gemini-embedding-001` (embed).

## I/O surface (`io.py`)

`read(source, *, lazy=True, cache=None, columns=None, engine=None, **params)`
dispatches on URI scheme:

- `bq://project.dataset.table` → Storage Read API → Arrow → Polars, with
  server-side projection when `columns` is given.
- `gs://bucket/path/*.parquet` → `pl.scan_parquet` with ADC storage_options
  (keeps projection/predicate pushdown).
- bare SQL string (heuristic: no scheme + whitespace) → parameterized BigQuery
  query (`@name` binding from `**params`, never string-formatted) → Arrow.

Lazy by default: returns a Polars `LazyFrame` so filters/projections push down;
compute fires on `.collect()`. `write(df, dest, *, mode, partition_by)` collects
lazy frames first, then dispatches `bq://` (load job / Storage Write) or `gs://`
(parquet, optional Hive partitioning). `dry_run(source, **params)` returns
`{'bytes_processed', 'estimated_usd'}` without executing.

Decimal columns are recast to Float64 on the way out of BigQuery (ported from
v0.3), since downstream Polars/Arrow consumers trip over Decimal.

## Caching (`cache.py`)

### Invalidation discipline (preserved from v0.3)

There is no silent default. The caller always chooses an invalidation mode,
because whether identical SQL may be served from cache depends on the query:

- **TTL mode** — re-run once the artifact is older than the TTL. Correct for
  live / relative queries (`CURRENT_DATE()`, rolling windows, live tables).
- **Content mode** — invalidate only when the function source / SQL / source
  freshness changes. Correct only for deterministic / pinned queries.

The `cache=` argument on `read` *is* the mode selector, reconciling the stub's
ergonomic string form with the discipline:

- `cache="6h" | "30m" | "7d"` → TTL mode.
- `cache="pinned"` → content mode.
- `cache=None` (default) → no caching.

On the decorator: `@cache(ttl="6h")` (TTL) or `@cache(pinned=True)` (content).
Bare `@cache` raises and instructs the caller to choose, mirroring
`extract_cached`'s refusal to default.

### Freshness token (`source_fingerprint`)

The key folds in a source-freshness term so a reloaded upstream busts the entry:

- `bq://table` → `table.modified` (one cheap metadata call). **Implemented in
  v0.4.** This closes the v0.3 footgun where the same SQL against a reloaded
  table serves stale data.
- bare SQL referencing tables → best-effort; when it can't be computed (e.g.
  `CURRENT_DATE()`), the token is empty and the caller is correctly on TTL.
- `gs://` glob → object etags (folded in when listed).

### `@cache` decorator (generalized)

Memoizes any frame-returning function, not just a single SQL extraction. The key
is `blake2(function_source + args + kwargs + freshness_token)`, so editing the
function body invalidates automatically. Documented gotcha: closures over
module-level constants are not captured in the source hash; pass such values as
arguments.

### CacheStore (local, GCS seam)

Local parquet artifact + `.manifest.json` sidecar under `cache_dir`. Manifest
records sql/source hash, freshness token, rows, size, written_at. Behaviors
ported from v0.3:

- atomic writes (temp sibling + `os.replace`) so a crash never leaves a torn
  artifact a later read trusts;
- fail open: a corrupt/unreadable parquet logs a warning and re-runs the source
  rather than raising — a cache must never harden a failure;
- manifest adoption: a parquet without a manifest is adopted on first read.

`CacheStore` is the seam for a future shared GCS tier; v0.4 ships only the local
tier. `clear_cache(prefix=None)` deletes artifacts and returns the count.

## DuckDB <-> Polars interop (`sql.py`)

`sql(query, *, lazy=False, **frames)` runs DuckDB SQL over Polars frames and
returns a Polars frame. Frames are resolved by name; with none passed
explicitly, Polars frames are captured from the caller's local scope. DuckDB
reads the frame's Arrow buffers zero-copy. Extensions referenced in the query
(httpfs, vss) are auto-loaded on demand. Use it for window functions, ASOF
joins, or VSS similarity search over an embedding column produced by
`llm.embed`.

## LLM column ops (`llm/`)

### The `map_batches` move

`map` / `embed` return `pl.col(column).map_batches(f)`. Polars hands `f` the
entire column as one Series, so async fan-out with bounded concurrency runs
*inside* `f`. Native `with_columns` ergonomics are preserved; the tradeoff is
that the column materializes at `.collect()` (it cannot stay lazy past the LLM
call). This is documented loudly.

### `map`

`map(column, *, prompt, schema=None, model=None, concurrency=None, system=None,
max_tokens=1024, temperature=0.0)`. Per row, fills `{column}` / `{field}` into
`prompt`. With a Pydantic `schema`, uses Gemini structured output (response
schema) and returns a Struct column; otherwise a Utf8 column. `temperature=0.0`
default so caching is meaningful.

### `embed`

`embed(column, *, model=None, concurrency=None, dims=None)` → `pl.Array(Float32,
width)` column. Pairs with `sql()` + DuckDB VSS for similarity search on the same
in-memory data.

### Async core (`_gemini.py`)

- Bounded concurrency via `asyncio.Semaphore(concurrency)`.
- Exponential backoff with jitter on 429 / 503, honoring `llm_max_retries`.
- Structured output: Pydantic schema → Gemini response schema; a parse failure
  surfaces as `null` plus a logged warning, and never kills the batch.
- A single row's failure → `null`, isolated from the rest of the batch.
- Per-row cache: SQLite at `~/.mooncompute/llm-cache.db`, keyed
  `blake2(model, system, prompt, schema, params)`. Only misses become live API
  calls, so a re-run after editing three rows costs three calls, not N. SQLite
  is chosen over a directory of JSON files because it is the boring,
  well-tested, stdlib primitive that survives restarts.
- Vertex client built via google-genai with `vertexai=True`, project/location
  from `settings`, ADC auth.

## Error handling

- BigQuery dry-run guardrail rejects queries over `bq_max_bytes_billed` before
  spending, when `bq_dry_run_default` is set.
- Cache reads fail open (corrupt → warn + re-run).
- Atomic writes everywhere (temp + rename).
- LLM: per-row failure and structured-parse failure both yield `null` + a logged
  warning; the batch always completes.
- `pl2bq` / BigQuery writes accept an idempotent `job_id` so a retried load is a
  safe no-op, not a double-load (ported from v0.3).

## Dependencies

Core install pulls the Arrow substrate so `mc.read("gs://…")` and `mc.sql()`
work on base:

- core: `polars>=1.0`, `pyarrow>=15`, `duckdb>=1.0`
- `[gcp]`: `google-cloud-bigquery>=3.0`, `google-cloud-storage>=2.0`,
  `db-dtypes>=1.0`
- `[llm]`: `google-genai`
- `[all]`: `mooncompute[gcp,llm]`

This drops v0.3's dependency-free base, which is incompatible with a unified
top-level surface. Installs stay scoped via the extras.

## Testing

Port v0.3's fake-client approach — no live GCP in unit tests:

- URI-scheme dispatch (`bq://`, `gs://`, bare SQL).
- TTL parsing; both invalidation modes; bare `@cache` raises.
- decimal→float recast; manifest adoption; cache fail-open on corrupt parquet.
- freshness-token busting (a changed `table.modified` invalidates).
- LLM against a fake Gemini client: fan-out shape, per-row cache hit/miss,
  structured parse, retry/backoff, per-row failure isolation.
- `sql()` against real in-process DuckDB (cheap): Polars round-trip,
  caller-scope frame capture.

## Deferred (documented, not built in v0.4)

- Shared GCS cache tier (`CacheStore` is the seam).
- LLM Batch API (~50% discount) — config field stays inert.
- USD spend-cap accounting — config field stays inert; a simple in-process
  request counter can enforce a hard ceiling if needed.
- `gcs://`-glob etag freshness in the frame cache (folded in only when listed).

## Migration note

v0.4 is a breaking change. Code importing `mooncompute.gcp.bq2pl` /
`extract_cached` / `pl2bq` / `gcs.*` must move to the top-level surface
(`mc.read` / `mc.write` / `@mc.cache`). The README ships a short migration table.
