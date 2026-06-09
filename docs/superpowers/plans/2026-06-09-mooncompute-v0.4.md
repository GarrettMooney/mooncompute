# mooncompute v0.4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild mooncompute as a thin Arrow-glue layer with a top-level `read / write / sql / llm / cache` surface over BigQuery, GCS, Polars, DuckDB, and Vertex Gemini.

**Architecture:** Top-level public surface (`mc.read`, `mc.sql`, `mc.llm.map`, `@mc.cache`) dispatched by URI scheme. Connectors are not reimplemented; we delegate to Polars / DuckDB / the BigQuery Storage API. v0.3's `gcp.*` namespace is removed and its hard-won internals (decimal recast, atomic writes, manifest cache, invalidation discipline, creds materialization) are ported into private `sources/`. Caching is local parquet+manifest with a BigQuery `table.modified` freshness token; the GCS tier is a seam. LLM column ops use Vertex Gemini with bounded-concurrency async, structured output, and a SQLite per-row cache.

**Tech Stack:** Python ≥3.11, polars, pyarrow, duckdb, google-cloud-bigquery, google-cloud-storage, db-dtypes, google-genai, pytest.

**Reference:** `docs/superpowers/specs/2026-06-09-mooncompute-v0.4-design.md`. The v0.3 source being ported lives at `src/mooncompute/gcp/{bq,gcs,_creds}.py` (read it for the originals; this plan reproduces the code to port).

---

## File Structure

```
src/mooncompute/
  __init__.py     # public exports + __version__            (Task 10)
  config.py       # Settings, configure(), settings          (Task 1)
  _creds.py       # materialize_gcp_creds (ported)            (Task 1)
  sources/
    __init__.py
    gcs.py        # parquet/json/bytes + scan_parquet         (Task 2)
    bigquery.py   # read_table/read_query/write_table/dry_run/table_modified  (Task 3)
  io.py           # read / write / dry_run dispatch           (Task 4)
  cache.py        # @cache, read_cached, CacheStore, fingerprint  (Task 5)
  sql.py          # DuckDB <-> Polars interop                 (Task 6)
  llm/
    __init__.py   # re-export map, embed                      (Task 8)
    _gemini.py    # async core, sqlite per-row cache          (Task 7)
    ops.py        # map/embed as pl.Expr                      (Task 8)
  py.typed
tests/
  fakes.py        # extend existing GCS/BQ fakes              (Task 3)
  test_config.py / test_gcs.py / test_bigquery.py / test_io.py /
  test_cache.py / test_sql.py / test_gemini.py / test_llm_ops.py /
  test_public_api.py
```

Build order is bottom-up (no-deps config first, public `__init__` last) so each task's tests run against already-built layers.

---

## Task 0: Reset the package & dependencies

**Files:**
- Modify: `pyproject.toml`
- Delete: `src/mooncompute/gcp/` (whole dir), old `tests/test_*.py` for the gcp surface
- Create: `src/mooncompute/sources/__init__.py`, `src/mooncompute/llm/__init__.py` (empty placeholders, filled later)

- [ ] **Step 1: Rewrite `pyproject.toml`** project + deps sections (leave `[tool.*]` blocks intact):

```toml
[project]
name = "mooncompute"
version = "0.4.0"
description = "Thin Arrow-glue for data science: one read/write/sql surface over BigQuery, GCS, Polars, DuckDB, plus LLM column ops."
readme = "README.md"
requires-python = ">=3.11"
license = "MIT"
authors = [{ name = "Garrett Mooney" }]
keywords = ["bigquery", "polars", "duckdb", "gcs", "arrow", "llm", "gemini"]
classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Topic :: Database",
    "Topic :: Software Development :: Libraries",
]
# Core pulls the Arrow substrate so read("gs://...") and sql() work on a bare
# install. GCP clients and the LLM provider are opt-in extras.
dependencies = [
    "polars>=1.0",
    "pyarrow>=15",
    "duckdb>=1.0",
]

[project.optional-dependencies]
gcp = [
    "google-cloud-bigquery>=3.0",
    "google-cloud-storage>=2.0",
    "db-dtypes>=1.0",
]
llm = [
    "google-genai>=0.3",
]
all = ["mooncompute[gcp,llm]"]

[project.urls]
Homepage = "https://github.com/garrettmooney/mooncompute"
Repository = "https://github.com/garrettmooney/mooncompute"

[dependency-groups]
dev = [
    "mooncompute[gcp,llm]",
    "pytest>=8",
    "ruff>=0.15.14",
    "ty>=0.0.39",
]
```

Keep the existing `[build-system]`, `[tool.hatch.*]`, `[tool.pytest.ini_options]`, `[tool.ruff*]`, `[tool.ty*]` blocks unchanged.

- [ ] **Step 2: Delete the old surface**

```bash
git rm -r src/mooncompute/gcp
git rm tests/test_bq2pl.py tests/test_pl2bq.py tests/test_bq_helpers.py \
       tests/test_extract_cached.py tests/test_creds.py tests/test_public_api.py
mkdir -p src/mooncompute/sources src/mooncompute/llm
touch src/mooncompute/sources/__init__.py src/mooncompute/llm/__init__.py
```
(Keep `tests/test_gcs.py` and `tests/fakes.py` — reused/extended later. Keep `tests/__init__.py`.)

- [ ] **Step 3: Sync and confirm the env resolves**

Run: `uv sync`
Expected: resolves and installs polars, duckdb, pyarrow, google-genai, google-cloud-*.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "Reset package for v0.4: drop gcp.* surface, restructure deps"
```

---

## Task 1: Config & credentials

**Files:**
- Create: `src/mooncompute/_creds.py`, `src/mooncompute/config.py`, `tests/test_config.py`

- [ ] **Step 1: Write failing test** `tests/test_config.py`:

```python
import importlib

import mooncompute.config as cfg


def test_defaults():
    s = cfg.Settings()
    assert s.location == "US"
    assert s.llm_default_model == "gemini-2.5-flash"
    assert s.llm_embed_model == "gemini-embedding-001"
    assert s.llm_concurrency == 16
    assert s.bq_dry_run_default is True


def test_configure_mutates_singleton():
    cfg.configure(project="p1", location="EU")
    assert cfg.settings.project == "p1"
    assert cfg.settings.location == "EU"


def test_configure_rejects_unknown_key():
    try:
        cfg.configure(nope=1)
    except AttributeError as e:
        assert "nope" in str(e)
    else:
        raise AssertionError("expected AttributeError")


def test_project_env_fallback(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "envproj")
    monkeypatch.delenv("MOONCOMPUTE_PROJECT", raising=False)
    reloaded = importlib.reload(cfg)
    assert reloaded.settings.project == "envproj"
```

- [ ] **Step 2: Run to verify fail**

Run: `uv run pytest tests/test_config.py -v`
Expected: FAIL (`ModuleNotFoundError: mooncompute.config`).

- [ ] **Step 3: Port `_creds.py` verbatim** from v0.3:

```python
"""Materialize service-account JSON (e.g. a Modal secret) into ADC.

Modal secrets expose the key as GOOGLE_APPLICATION_CREDENTIALS_JSON. Write it
to a file and point GOOGLE_APPLICATION_CREDENTIALS at it. Idempotent; a no-op
locally where ADC already points at a file or the JSON env var is absent.
"""

from __future__ import annotations

import os
from pathlib import Path

_CREDS_PATH = "/tmp/gcp-creds.json"


def materialize_gcp_creds() -> None:
    existing = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if existing and Path(existing).exists():
        return
    blob = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not blob:
        return
    Path(_CREDS_PATH).write_text(blob)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _CREDS_PATH
```

- [ ] **Step 4: Write `config.py`**:

```python
"""Global configuration. Resolve auth once; never thread creds through calls.

Auth is Application Default Credentials only (ADC + workload identity). Locally
that is `gcloud auth application-default login`; in CI/cloud it is the attached
service account. We never accept key files as call-site arguments.
"""

from __future__ import annotations

import dataclasses
import os
from typing import Literal


@dataclasses.dataclass(slots=True)
class Settings:
    # --- GCP ---
    project: str | None = None
    location: str = "US"

    # --- cache ---
    cache_dir: str = "~/.mooncompute/cache"
    cache_enabled: bool = True

    # --- llm ---
    llm_default_model: str = "gemini-2.5-flash"
    llm_embed_model: str = "gemini-embedding-001"
    llm_concurrency: int = 16
    llm_max_retries: int = 6
    # Inert in v0.4 (documented seams):
    llm_spend_cap_usd: float | None = None
    llm_use_batch_api: bool = False

    # --- safety ---
    bq_dry_run_default: bool = True
    bq_max_bytes_billed: int | None = None

    engine: Literal["polars", "duckdb"] = "polars"


settings = Settings(
    project=os.getenv("MOONCOMPUTE_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT"),
    cache_dir=os.getenv("MOONCOMPUTE_CACHE_DIR", "~/.mooncompute/cache"),
)


def configure(**kwargs) -> Settings:
    """Mutate the process-global settings. Call once at startup."""
    for key, value in kwargs.items():
        if not hasattr(settings, key):
            raise AttributeError(f"unknown setting: {key!r}")
        setattr(settings, key, value)
    return settings
```

- [ ] **Step 5: Run to verify pass**

Run: `uv run pytest tests/test_config.py -v`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add src/mooncompute/_creds.py src/mooncompute/config.py tests/test_config.py
git commit -m "Add config settings and ADC creds materialization"
```

---

## Task 2: GCS source

**Files:**
- Create: `src/mooncompute/sources/gcs.py`
- Modify: `tests/test_gcs.py` (update import path to `mooncompute.sources.gcs`)

- [ ] **Step 1: Repoint the existing GCS test** — change its import from `from mooncompute.gcp import gcs` to `from mooncompute.sources import gcs`, and any `mooncompute.gcp.gcs` patch targets to `mooncompute.sources.gcs`. Add one new test for the lazy scan path:

```python
def test_scan_parquet_returns_lazyframe(monkeypatch):
    import polars as pl

    from mooncompute.sources import gcs

    captured = {}

    def fake_scan(uri, **kw):
        captured["uri"] = uri
        return pl.LazyFrame({"a": [1, 2]})

    monkeypatch.setattr(pl, "scan_parquet", fake_scan)
    lf = gcs.scan_parquet("gs://b/p/*.parquet", columns=["a"])
    assert isinstance(lf, pl.LazyFrame)
    assert captured["uri"] == "gs://b/p/*.parquet"
    assert lf.collect().columns == ["a"]
```

- [ ] **Step 2: Run to verify fail**

Run: `uv run pytest tests/test_gcs.py -v`
Expected: FAIL (`ModuleNotFoundError: mooncompute.sources.gcs`).

- [ ] **Step 3: Port `sources/gcs.py`** from v0.3 `gcp/gcs.py`, adding `scan_parquet` for the lazy `read("gs://")` path. Full module:

```python
"""GCS read/write helpers for Parquet, JSON, and bytes over gs:// URIs."""

from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING, Any

from google.cloud import storage

from .._creds import materialize_gcp_creds

if TYPE_CHECKING:
    import polars as pl


def _polars():
    try:
        import polars as pl
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "parquet I/O needs polars. Install `pip install 'mooncompute[gcp]'`."
        ) from exc
    return pl


def _client() -> storage.Client:
    materialize_gcp_creds()
    return storage.Client()


def _parse_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"expected a gs:// URI, got: {uri!r}")
    bucket, _, key = uri[5:].partition("/")
    return bucket, key


def scan_parquet(uri: str, *, columns: list[str] | None = None) -> "pl.LazyFrame":
    """Lazy scan of gs://...parquet (glob ok). Keeps projection/predicate
    pushdown. Auth via ADC; storage_options are inferred from the environment.
    """
    pl = _polars()
    lf = pl.scan_parquet(uri)
    if columns:
        lf = lf.select(columns)
    return lf


def read_parquet(uri: str) -> "pl.DataFrame":
    pl = _polars()
    bucket, key = _parse_uri(uri)
    blob = _client().bucket(bucket).blob(key)
    buf = io.BytesIO()
    blob.download_to_file(buf)
    buf.seek(0)
    return pl.read_parquet(buf)


def read_parquet_glob(prefix_uri: str) -> "pl.DataFrame":
    """Read and concat all *.parquet shards under a gs:// prefix (EXPORT DATA)."""
    pl = _polars()
    bucket, prefix = _parse_uri(prefix_uri.rstrip("/") + "/")
    shards = [
        b
        for b in _client().bucket(bucket).list_blobs(prefix=prefix)
        if b.name.endswith(".parquet")
    ]
    if not shards:
        raise FileNotFoundError(f"no .parquet shards under {prefix_uri}")
    frames = []
    for blob in shards:
        buf = io.BytesIO()
        blob.download_to_file(buf)
        buf.seek(0)
        frames.append(pl.read_parquet(buf))
    return pl.concat(frames, how="vertical_relaxed")


def write_parquet(df: "pl.DataFrame", uri: str) -> None:
    bucket, key = _parse_uri(uri)
    buf = io.BytesIO()
    df.write_parquet(buf)
    _client().bucket(bucket).blob(key).upload_from_string(
        buf.getvalue(), content_type="application/octet-stream"
    )


def read_json(uri: str) -> Any:
    bucket, key = _parse_uri(uri)
    return json.loads(_client().bucket(bucket).blob(key).download_as_text())


def write_json(uri: str, obj: Any) -> None:
    bucket, key = _parse_uri(uri)
    _client().bucket(bucket).blob(key).upload_from_string(
        json.dumps(obj, default=str), content_type="application/json"
    )


def read_bytes(uri: str) -> bytes:
    bucket, key = _parse_uri(uri)
    return _client().bucket(bucket).blob(key).download_as_bytes()


def write_bytes(uri: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    bucket, key = _parse_uri(uri)
    _client().bucket(bucket).blob(key).upload_from_string(data, content_type=content_type)
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/test_gcs.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/mooncompute/sources/gcs.py tests/test_gcs.py
git commit -m "Port GCS source with lazy scan_parquet path"
```

---

## Task 3: BigQuery source

**Files:**
- Create: `src/mooncompute/sources/bigquery.py`, `tests/test_bigquery.py`
- Modify: `tests/fakes.py` (add table-metadata + dry-run support to `FakeBQClient`)

- [ ] **Step 1: Extend `tests/fakes.py`** `FakeBQClient` — add `get_table`, dry-run jobs, and a `modified` timestamp. Append these methods/classes:

```python
import datetime as _dt


class FakeTable:
    def __init__(self, modified=None, full_id="proj.ds.t"):
        self.modified = modified or _dt.datetime(2024, 1, 1, tzinfo=_dt.timezone.utc)
        self.full_table_id = full_id


class FakeDryRunJob:
    def __init__(self, total_bytes):
        self.total_bytes_processed = total_bytes
```

Then add to `FakeBQClient.__init__`: `self._modified = None` and `self.dry_run_bytes = 0`. Add methods:

```python
    def get_table(self, ref):
        return FakeTable(modified=self._modified, full_id=str(ref))

    def query(self, sql, job_config=None):
        self.queries.append(sql)
        if job_config is not None and getattr(job_config, "dry_run", False):
            return FakeDryRunJob(self.dry_run_bytes)
        return FakeQueryJob(self._table)
```
(Replace the existing one-arg `query`; keep `load_table_from_file` and the other fakes.)

- [ ] **Step 2: Write failing test** `tests/test_bigquery.py`:

```python
import datetime

import polars as pl
import pyarrow as pa

from mooncompute.sources import bigquery as bq
from tests.fakes import FakeBQClient


def _arrow():
    return pa.table({"id": [1, 2], "amt": pa.array([1.5, 2.5], pa.decimal128(5, 2))})


def test_bq2pl_recasts_decimals():
    client = FakeBQClient(table=_arrow())
    df = bq.bq2pl("select 1", client=client)
    assert df["amt"].dtype == pl.Float64


def test_read_query_dry_run_guardrail():
    client = FakeBQClient(table=_arrow())
    client.dry_run_bytes = 10**12
    try:
        bq.read_query("select 1", params={}, lazy=False, engine="polars",
                      client=client, max_bytes_billed=10**6)
    except RuntimeError as e:
        assert "over cap" in str(e)
    else:
        raise AssertionError("expected RuntimeError")


def test_table_modified_returns_isoformat():
    client = FakeBQClient(table=_arrow())
    client._modified = datetime.datetime(2024, 5, 1, tzinfo=datetime.timezone.utc)
    token = bq.table_modified("bq://proj.ds.t", client=client)
    assert token.startswith("2024-05-01")


def test_read_table_lazy_returns_lazyframe():
    client = FakeBQClient(table=_arrow())
    lf = bq.read_table("bq://proj.ds.t", lazy=True, columns=["id"], engine="polars",
                       client=client)
    assert isinstance(lf, pl.LazyFrame)
    assert "select" in client.queries[0].lower()
```

- [ ] **Step 3: Run to verify fail**

Run: `uv run pytest tests/test_bigquery.py -v`
Expected: FAIL (`ModuleNotFoundError: mooncompute.sources.bigquery`).

- [ ] **Step 4: Write `sources/bigquery.py`.** Port `bq2pl`, `pl2bq`, `read_sql`, `_decimals_to_float`, `_resolve_project`, `Manifest`, atomic-write helpers from v0.3 `gcp/bq.py`, and add `read_table`, `read_query`, `write_table`, `dry_run`, `table_modified`. Full module:

```python
"""BigQuery source. Storage Read API (Arrow) fast path, never REST paging."""

from __future__ import annotations

import io
import logging
import os
import re
from typing import Any, Literal, cast

import polars as pl
from google.cloud import bigquery

from .._creds import materialize_gcp_creds
from ..config import settings

PROJECT_ENV = "GOOGLE_CLOUD_PROJECT"
log = logging.getLogger(__name__)

_BQ_RE = re.compile(r"^bq://(?P<project>[^.]+)\.(?P<dataset>[^.]+)\.(?P<table>.+)$")
# On-demand price: $6.25 / TiB scanned.
_USD_PER_BYTE = 6.25 / 2**40


def _resolve_project(project: str | None) -> str:
    project = project or settings.project or os.environ.get(PROJECT_ENV)
    if not project:
        raise ValueError(f"no GCP project: pass project= or set ${PROJECT_ENV}")
    return project


def _client(project: str | None = None) -> bigquery.Client:
    materialize_gcp_creds()
    return bigquery.Client(project=_resolve_project(project))


def read_sql(path, **subs: str) -> str:
    """Read a .sql file; optionally substitute {placeholder} tokens via format."""
    from pathlib import Path

    text = Path(path).read_text()
    return text.format(**subs) if subs else text


def _decimals_to_float(df: pl.DataFrame) -> pl.DataFrame:
    decimal_cols = [c for c in df.columns if str(df[c].dtype).startswith("Decimal")]
    if not decimal_cols:
        return df
    return df.with_columns([pl.col(c).cast(pl.Float64) for c in decimal_cols])


def _parse_bq_uri(uri: str) -> tuple[str, str, str]:
    m = _BQ_RE.match(uri)
    if not m:
        raise ValueError(f"bad bq uri: {uri!r} (want bq://project.dataset.table)")
    return m["project"], m["dataset"], m["table"]


def bq2pl(sql, *, project=None, client=None, decimals_to_float=True) -> pl.DataFrame:
    client = client or _client(project)
    arrow = client.query(sql).to_arrow(create_bqstorage_client=True)
    df = cast(pl.DataFrame, pl.from_arrow(arrow))
    return _decimals_to_float(df) if decimals_to_float else df


def dry_run(sql, *, params=None, client=None, project=None) -> dict[str, Any]:
    """Cost estimate without execution."""
    client = client or _client(project)
    cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    if params:
        cfg.query_parameters = _query_params(params)
    job = client.query(sql, job_config=cfg)
    n = int(job.total_bytes_processed or 0)
    return {"bytes_processed": n, "estimated_usd": n * _USD_PER_BYTE}


def _query_params(params: dict) -> list:
    out = []
    for k, v in params.items():
        t = "STRING"
        if isinstance(v, bool):
            t = "BOOL"
        elif isinstance(v, int):
            t = "INT64"
        elif isinstance(v, float):
            t = "FLOAT64"
        out.append(bigquery.ScalarQueryParameter(k, t, v))
    return out


def read_query(sql, *, params, lazy, engine, client=None, project=None,
               dry_run_default=None, max_bytes_billed=None) -> Any:
    """Parameterized query (@name binding) -> Arrow -> frame, with cost guardrail."""
    client = client or _client(project)
    do_dry = settings.bq_dry_run_default if dry_run_default is None else dry_run_default
    cap = settings.bq_max_bytes_billed if max_bytes_billed is None else max_bytes_billed
    if do_dry or cap:
        est = dry_run(sql, params=params, client=client)
        if cap and est["bytes_processed"] > cap:
            raise RuntimeError(
                f"query would scan {est['bytes_processed']:,} bytes "
                f"(${est['estimated_usd']:.2f}), over cap of {cap:,}"
            )
    cfg = bigquery.QueryJobConfig(query_parameters=_query_params(params or {}))
    arrow = client.query(sql, job_config=cfg).to_arrow(create_bqstorage_client=True)
    df = _decimals_to_float(cast(pl.DataFrame, pl.from_arrow(arrow)))
    return df.lazy() if lazy else df


def read_table(uri, *, lazy, columns, engine, client=None, project=None) -> Any:
    """bq://p.d.t -> frame. Projection is pushed server-side via SELECT cols."""
    project_, dataset, table = _parse_bq_uri(uri)
    cols = ", ".join(f"`{c}`" for c in columns) if columns else "*"
    sql = f"SELECT {cols} FROM `{project_}.{dataset}.{table}`"
    return read_query(sql, params={}, lazy=lazy, engine=engine, client=client,
                      project=project or project_, dry_run_default=False)


def table_modified(uri, *, client=None, project=None) -> str:
    """Freshness token: the table's last-modified timestamp (isoformat)."""
    project_, dataset, table = _parse_bq_uri(uri)
    client = client or _client(project or project_)
    ref = f"{project_}.{dataset}.{table}"
    t = client.get_table(ref)
    return t.modified.isoformat() if t.modified else ""


def write_table(df: pl.DataFrame, uri, *, mode, client=None, project=None,
                job_id=None) -> None:
    """Load a polars frame into bq://p.d.t via a Parquet load job (list inference
    on). `job_id` is an idempotency key: a retried load with the same id is a
    safe no-op, not a double-load."""
    project_, dataset, table = _parse_bq_uri(uri)
    client = client or _client(project or project_)
    destination = f"{project_}.{dataset}.{table}"
    disp = {
        "overwrite": bigquery.WriteDisposition.WRITE_TRUNCATE,
        "append": bigquery.WriteDisposition.WRITE_APPEND,
        "error": bigquery.WriteDisposition.WRITE_EMPTY,
    }[mode]
    job_config = bigquery.LoadJobConfig(write_disposition=disp)
    job_config.source_format = bigquery.SourceFormat.PARQUET
    opts = bigquery.ParquetOptions()
    opts.enable_list_inference = True
    job_config.parquet_options = opts
    with io.BytesIO() as stream:
        df.write_parquet(stream)
        stream.seek(0)
        client.load_table_from_file(
            stream, destination, project=project_, job_config=job_config, job_id=job_id
        ).result()
```

- [ ] **Step 5: Run to verify pass**

Run: `uv run pytest tests/test_bigquery.py -v`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add src/mooncompute/sources/bigquery.py tests/test_bigquery.py tests/fakes.py
git commit -m "Port BigQuery source: storage-API reads, dry-run guardrail, freshness token"
```

---

## Task 4: I/O dispatch surface

**Files:**
- Create: `src/mooncompute/io.py`, `tests/test_io.py`

- [ ] **Step 1: Write failing test** `tests/test_io.py`:

```python
import polars as pl
import pytest

from mooncompute import io as mio


def test_scheme_detection():
    assert mio._scheme("bq://p.d.t") == "bq"
    assert mio._scheme("gs://b/x.parquet") == "gs"
    assert mio._scheme("select * from t") == "sql"
    with pytest.raises(ValueError):
        mio._scheme("relative/path")


def test_read_dispatches_gs(monkeypatch):
    from mooncompute.sources import gcs

    monkeypatch.setattr(gcs, "scan_parquet",
                        lambda uri, columns=None: pl.LazyFrame({"a": [1]}))
    out = mio.read("gs://b/x.parquet")
    assert isinstance(out, pl.LazyFrame)


def test_read_dispatches_bq(monkeypatch):
    from mooncompute.sources import bigquery

    seen = {}
    monkeypatch.setattr(bigquery, "read_table",
                        lambda uri, **kw: seen.setdefault("uri", uri) or pl.DataFrame({"a": [1]}))
    mio.read("bq://p.d.t", lazy=False)
    assert seen["uri"] == "bq://p.d.t"


def test_read_cache_pinned_routes_to_read_cached(monkeypatch):
    from mooncompute import cache

    called = {}
    monkeypatch.setattr(cache, "read_cached",
                        lambda *a, **k: called.setdefault("hit", True) or pl.DataFrame({"a": [1]}))
    mio.read("bq://p.d.t", cache="pinned")
    assert called["hit"]
```

- [ ] **Step 2: Run to verify fail**

Run: `uv run pytest tests/test_io.py -v`
Expected: FAIL (`ModuleNotFoundError: mooncompute.io`).

- [ ] **Step 3: Write `io.py`**:

```python
"""The single I/O surface. read/write dispatch on URI scheme; lazy by default."""

from __future__ import annotations

from typing import Any, Literal
from urllib.parse import urlparse

import polars as pl

from .config import settings

Frame = Any


def read(source: str, *, lazy: bool = True, cache: str | None = None,
         columns: list[str] | None = None, engine: str | None = None,
         **params: Any) -> Frame:
    """Read any supported source into a (lazy) frame.

    cache: None (off) | "6h"/"30m"/"7d" (TTL mode) | "pinned" (content mode).
    """
    engine = engine or settings.engine
    if cache is not None:
        from .cache import read_cached

        return read_cached(source, cache=cache, lazy=lazy, columns=columns,
                           engine=engine, **params)
    scheme = _scheme(source)
    if scheme == "bq":
        from .sources import bigquery

        return bigquery.read_table(source, lazy=lazy, columns=columns, engine=engine)
    if scheme == "gs":
        from .sources import gcs

        lf = gcs.scan_parquet(source, columns=columns)
        return lf if lazy else lf.collect()
    if scheme == "sql":
        from .sources import bigquery

        return bigquery.read_query(source, params=params, lazy=lazy, engine=engine)
    raise ValueError(f"unrecognized source: {source!r}")


def write(df: Frame, dest: str, *, mode: Literal["overwrite", "append", "error"] = "error",
          partition_by: list[str] | None = None) -> None:
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    scheme = _scheme(dest)
    if scheme == "bq":
        from .sources import bigquery

        return bigquery.write_table(df, dest, mode=mode)
    if scheme == "gs":
        from .sources import gcs

        return gcs.write_parquet(df, dest)
    raise ValueError(f"cannot write to: {dest!r}")


def dry_run(source: str, **params: Any) -> dict[str, Any]:
    from .sources import bigquery

    return bigquery.dry_run(source, params=params)


def _scheme(source: str) -> str:
    if source.startswith("bq://"):
        return "bq"
    if source.startswith("gs://"):
        return "gs"
    if urlparse(source).scheme in ("", None) and " " in source.strip():
        return "sql"
    raise ValueError(f"cannot infer source type from {source!r}")
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/test_io.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/mooncompute/io.py tests/test_io.py
git commit -m "Add read/write/dry_run URI-dispatch surface"
```

---

## Task 5: Caching

**Files:**
- Create: `src/mooncompute/cache.py`, `tests/test_cache.py`

- [ ] **Step 1: Write failing test** `tests/test_cache.py`:

```python
import polars as pl
import pytest

from mooncompute import cache as mc
from mooncompute.config import settings


@pytest.fixture(autouse=True)
def tmp_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "cache_dir", str(tmp_path))
    monkeypatch.setattr(settings, "cache_enabled", True)


def test_ttl_seconds():
    assert mc._ttl_seconds("30m") == 1800
    assert mc._ttl_seconds("6h") == 21600
    assert mc._ttl_seconds("7d") == 604800
    with pytest.raises(ValueError):
        mc._ttl_seconds("6x")


def test_bare_cache_requires_mode():
    with pytest.raises(ValueError):
        @mc.cache
        def f():
            return pl.DataFrame({"a": [1]})


def test_cache_decorator_memoizes(monkeypatch):
    calls = {"n": 0}

    @mc.cache(pinned=True)
    def features(date):
        calls["n"] += 1
        return pl.DataFrame({"a": [1], "d": [date]})

    a = features("2024-01-01")
    b = features("2024-01-01")
    assert calls["n"] == 1            # second call is a cache hit
    assert a.equals(b)
    c = features("2024-02-01")        # different arg -> miss
    assert calls["n"] == 2


def test_cache_fails_open_on_corrupt(monkeypatch, tmp_path):
    @mc.cache(pinned=True)
    def f():
        return pl.DataFrame({"a": [1]})

    f()  # populate
    # corrupt every parquet in the cache dir
    for p in tmp_path.glob("*.parquet"):
        p.write_bytes(b"not parquet")
    out = f()  # must re-run, not raise
    assert out["a"][0] == 1


def test_key_changes_with_function_source():
    def v1():
        return pl.DataFrame({"a": [1]})

    def v2():
        return pl.DataFrame({"a": [2]})  # different body

    k1 = mc._key(mc._function_fingerprint(v1), (), {}, None)
    k2 = mc._key(mc._function_fingerprint(v2), (), {}, None)
    assert k1 != k2
```

- [ ] **Step 2: Run to verify fail**

Run: `uv run pytest tests/test_cache.py -v`
Expected: FAIL (`ModuleNotFoundError: mooncompute.cache`).

- [ ] **Step 3: Write `cache.py`** (local parquet+manifest, atomic, fail-open, freshness token, invalidation discipline):

```python
"""Content-addressed caching backed by local Parquet. The iteration unlock.

No silent default: the caller picks an invalidation mode. TTL mode re-runs past
a deadline (live queries); content mode invalidates only on source/body/freshness
change (deterministic queries). For bq:// sources a `table.modified` freshness
token is folded into the key so a reloaded table busts the entry even in content
mode. The shared-GCS tier is a future seam; v0.4 ships the local tier only.
"""

from __future__ import annotations

import functools
import hashlib
import inspect
import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

import polars as pl

from .config import settings

log = logging.getLogger(__name__)

_TTL_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def _ttl_seconds(ttl: str | None) -> int | None:
    if ttl is None or ttl == "pinned":
        return None
    unit = ttl[-1]
    if unit not in _TTL_UNITS:
        raise ValueError(f"bad ttl {ttl!r}; use forms like '30m', '6h', '7d', or 'pinned'")
    return int(ttl[:-1]) * _TTL_UNITS[unit]


def _function_fingerprint(func: Callable) -> str:
    try:
        return inspect.getsource(func)
    except OSError:
        return func.__qualname__  # REPL / notebook cell


def _key(salt: str, args: tuple, kwargs: dict, key_extra: Any) -> str:
    h = hashlib.blake2b(digest_size=20)
    h.update(salt.encode())
    h.update(repr(args).encode())
    h.update(repr(sorted(kwargs.items())).encode())
    h.update(repr(key_extra).encode())
    return h.hexdigest()


def source_fingerprint(source: str) -> str:
    """Freshness token. bq://table -> table.modified; otherwise "" (TTL-only)."""
    if source.startswith("bq://"):
        try:
            from .sources import bigquery

            return bigquery.table_modified(source)
        except Exception as exc:  # noqa: BLE001 - freshness is best-effort
            log.warning("freshness token unavailable for %s: %s", source, exc)
    return ""


@dataclass(frozen=True)
class Manifest:
    key: str
    freshness: str
    rows: int
    written_at: float


class CacheStore:
    """Local Parquet artifact + JSON manifest sidecar. Seam for a future GCS tier."""

    def __init__(self, cache_dir: str):
        self.dir = Path(cache_dir).expanduser()

    @classmethod
    def default(cls) -> "CacheStore":
        return cls(settings.cache_dir)

    def _paths(self, key: str) -> tuple[Path, Path]:
        return self.dir / f"{key}.parquet", self.dir / f"{key}.manifest.json"

    def get(self, key: str, *, ttl_seconds: int | None, freshness: str) -> pl.DataFrame | None:
        data, manifest = self._paths(key)
        if not (data.exists() and manifest.exists()):
            return None
        try:
            m = Manifest(**json.loads(manifest.read_text()))
        except Exception:  # noqa: BLE001
            return None
        if freshness and m.freshness and freshness != m.freshness:
            return None  # upstream changed
        if ttl_seconds is not None and (time.time() - m.written_at) > ttl_seconds:
            return None  # expired
        try:
            return pl.read_parquet(data)
        except Exception as exc:  # noqa: BLE001 - fail open, never harden a failure
            log.warning("cache unreadable (%s); re-running", exc)
            return None

    def put(self, key: str, df: pl.DataFrame, *, freshness: str) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        data, manifest = self._paths(key)
        self._atomic_parquet(df, data)
        m = Manifest(key=key, freshness=freshness, rows=df.height, written_at=time.time())
        self._atomic_text(manifest, json.dumps(asdict(m)))

    @staticmethod
    def _tmp(path: Path) -> Path:
        return path.with_suffix(path.suffix + f".{os.getpid()}.tmp")

    def _atomic_parquet(self, df: pl.DataFrame, path: Path) -> None:
        tmp = self._tmp(path)
        df.write_parquet(tmp)
        os.replace(tmp, path)

    def _atomic_text(self, path: Path, text: str) -> None:
        tmp = self._tmp(path)
        tmp.write_text(text)
        os.replace(tmp, path)

    def clear(self, prefix: str | None = None) -> int:
        if not self.dir.exists():
            return 0
        n = 0
        for p in self.dir.glob("*"):
            if prefix is None or p.name.startswith(prefix):
                p.unlink()
                n += 1
        return n


def _materialize(result: Any) -> pl.DataFrame:
    if isinstance(result, pl.LazyFrame):
        return result.collect()
    if isinstance(result, pl.DataFrame):
        return result
    raise TypeError(f"cannot cache a {type(result).__name__}; expected a Polars frame")


def cache(fn: Callable | None = None, *, ttl: str | None = None, pinned: bool = False):
    """Memoize a frame-returning function to the local store.

    Pick a mode: @cache(ttl="6h") for live data, @cache(pinned=True) for a
    deterministic/pinned computation. Bare @cache raises — the choice is yours.
    """
    if ttl is None and not pinned:
        raise ValueError(
            "cache needs a mode: @cache(ttl='6h') for live data, or "
            "@cache(pinned=True) for a deterministic computation."
        )

    def decorate(func: Callable) -> Callable:
        src = _function_fingerprint(func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not settings.cache_enabled:
                return func(*args, **kwargs)
            key = _key(src, args, kwargs, None)
            store = CacheStore.default()
            hit = store.get(key, ttl_seconds=_ttl_seconds(ttl), freshness="")
            if hit is not None:
                return hit
            result = _materialize(func(*args, **kwargs))
            store.put(key, result, freshness="")
            return result

        wrapper.cache_key = lambda *a, **k: _key(src, a, k, None)
        return wrapper

    return decorate if fn is None else decorate(fn)


def read_cached(source: str, *, cache: str, **read_kwargs) -> Any:
    """Query-level cache used by io.read(..., cache=...).

    cache="pinned" -> content mode; cache="6h"/etc -> TTL mode. Freshness token
    comes from the source, so a reloaded bq:// table busts even a pinned entry.
    """
    if not settings.cache_enabled:
        from .io import read

        return read(source, cache=None, **read_kwargs)
    freshness = source_fingerprint(source)
    key = _key("read", (source,), read_kwargs, key_extra="pinned")
    store = CacheStore.default()
    hit = store.get(key, ttl_seconds=_ttl_seconds(cache), freshness=freshness)
    if hit is not None:
        return hit.lazy() if read_kwargs.get("lazy", True) else hit
    from .io import read

    result = _materialize(read(source, cache=None, **read_kwargs))
    store.put(key, result, freshness=freshness)
    return result.lazy() if read_kwargs.get("lazy", True) else result


def clear_cache(prefix: str | None = None) -> int:
    return CacheStore.default().clear(prefix)
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/test_cache.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/mooncompute/cache.py tests/test_cache.py
git commit -m "Add local content-addressed cache with BQ freshness token"
```

---

## Task 6: DuckDB <-> Polars interop

**Files:**
- Create: `src/mooncompute/sql.py`, `tests/test_sql.py`

- [ ] **Step 1: Write failing test** `tests/test_sql.py`:

```python
import polars as pl

from mooncompute.sql import sql


def test_sql_explicit_frame():
    df = pl.DataFrame({"user_id": [1, 1, 2], "x": [10, 20, 30]})
    out = sql("select user_id, count(*) n from df group by 1 order by user_id", df=df)
    assert out.sort("user_id")["n"].to_list() == [2, 1]


def test_sql_captures_caller_scope():
    df = pl.DataFrame({"a": [1, 2, 3]})  # noqa: F841 - referenced via SQL by name
    out = sql("select sum(a) s from df")
    assert out["s"][0] == 6


def test_sql_lazy_returns_lazyframe():
    df = pl.DataFrame({"a": [1, 2]})
    out = sql("select a from df", lazy=True, df=df)
    assert isinstance(out, pl.LazyFrame)
```

- [ ] **Step 2: Run to verify fail**

Run: `uv run pytest tests/test_sql.py -v`
Expected: FAIL (`ModuleNotFoundError: mooncompute.sql`).

- [ ] **Step 3: Write `sql.py`**:

```python
"""Polars <-> DuckDB interop. Drop into SQL, get Polars back (zero-copy Arrow)."""

from __future__ import annotations

import inspect
from typing import Any

import duckdb
import polars as pl


def sql(query: str, *, lazy: bool = False, **frames: pl.DataFrame) -> Any:
    """Run DuckDB SQL against Polars frames, returning a Polars frame.

    Pass frames by name, or rely on caller-scope capture so a local `df` is
    queryable as `df`. Use for window functions, ASOF joins, or VSS similarity
    search over an embedding column.
    """
    if not frames:
        caller = inspect.currentframe().f_back.f_locals
        frames = {k: v for k, v in caller.items() if isinstance(v, pl.DataFrame)}
    con = duckdb.connect()
    try:
        for name, frame in frames.items():
            con.register(name, frame.to_arrow())
        rel = con.sql(query)
        out = rel.pl()
    finally:
        con.close()
    return out.lazy() if lazy else out
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/test_sql.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/mooncompute/sql.py tests/test_sql.py
git commit -m "Add DuckDB <-> Polars sql() interop"
```

---

## Task 7: LLM async core (Gemini + per-row cache)

**Files:**
- Create: `src/mooncompute/llm/_gemini.py`, `tests/test_gemini.py`

The single-call function `_call_one` is the only thing that touches the network; tests inject a fake via `complete_batch(..., call=fake)` so no API key is needed.

- [ ] **Step 1: Write failing test** `tests/test_gemini.py`:

```python
import asyncio

from mooncompute.llm import _gemini


def test_per_row_cache_only_calls_misses(tmp_path, monkeypatch):
    monkeypatch.setattr(_gemini, "_DB_PATH", tmp_path / "llm.db")
    calls = []

    async def fake_call(prompt, **kw):
        calls.append(prompt)
        return prompt.upper()

    prompts = ["a", "b", "a"]  # "a" repeats -> one cached
    out = asyncio.run(_gemini.complete_batch(
        prompts, model="m", schema=None, system=None, max_tokens=8,
        temperature=0.0, concurrency=4, max_retries=2, call=fake_call))
    assert out == ["A", "B", "A"]
    assert sorted(calls) == ["a", "b"]  # "a" called once, reused for row 3


def test_none_rows_pass_through(tmp_path, monkeypatch):
    monkeypatch.setattr(_gemini, "_DB_PATH", tmp_path / "llm.db")

    async def fake_call(prompt, **kw):
        return "x"

    out = asyncio.run(_gemini.complete_batch(
        [None, "y"], model="m", schema=None, system=None, max_tokens=8,
        temperature=0.0, concurrency=4, max_retries=2, call=fake_call))
    assert out == [None, "x"]


def test_retry_then_succeed(tmp_path, monkeypatch):
    monkeypatch.setattr(_gemini, "_DB_PATH", tmp_path / "llm.db")
    monkeypatch.setattr(_gemini.asyncio, "sleep", lambda *_: asyncio.sleep(0))
    attempts = {"n": 0}

    async def flaky(prompt, **kw):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise _gemini.RetryableError("429")
        return "ok"

    out = asyncio.run(_gemini.complete_batch(
        ["p"], model="m", schema=None, system=None, max_tokens=8,
        temperature=0.0, concurrency=1, max_retries=5, call=flaky))
    assert out == ["ok"]
    assert attempts["n"] == 3
```

- [ ] **Step 2: Run to verify fail**

Run: `uv run pytest tests/test_gemini.py -v`
Expected: FAIL (`ModuleNotFoundError: mooncompute.llm._gemini`).

- [ ] **Step 3: Write `llm/_gemini.py`**:

```python
"""Vertex Gemini async core: bounded concurrency, retry, structured output, and
a SQLite per-row cache keyed on (model, system, prompt, schema, params)."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import sqlite3
from pathlib import Path
from typing import Any, Awaitable, Callable

from ..config import settings

log = logging.getLogger(__name__)

_DB_PATH = Path("~/.mooncompute/llm-cache.db").expanduser()


class RetryableError(Exception):
    """Raised for 429/503-class responses; triggers backoff."""


# ---- per-row cache (sqlite) ------------------------------------------------

def _db() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(_DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)")
    return con


def _row_key(prompt: str, *, model: str, system: str | None, schema: Any) -> str:
    h = hashlib.blake2b(digest_size=20)
    schema_name = getattr(schema, "__name__", "")
    for part in (model, system or "", schema_name, prompt):
        h.update(part.encode())
    return h.hexdigest()


def _cache_get(con, key: str) -> Any | None:
    row = con.execute("SELECT v FROM kv WHERE k = ?", (key,)).fetchone()
    return json.loads(row[0]) if row else None


def _cache_put(con, key: str, value: Any) -> None:
    con.execute("INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)",
                (key, json.dumps(value)))
    con.commit()


# ---- orchestration ---------------------------------------------------------

async def _with_retry(coro_factory: Callable[[], Awaitable], *, max_retries: int) -> Any:
    for attempt in range(max_retries + 1):
        try:
            return await coro_factory()
        except RetryableError:
            if attempt == max_retries:
                raise
            await asyncio.sleep(min(2 ** attempt, 30) + random.random())


async def complete_batch(prompts, *, model, schema, system, max_tokens, temperature,
                         concurrency, max_retries, call=None) -> list:
    """Fan out over prompts with a semaphore + per-row cache. `call` is the
    single-prompt coroutine (defaults to the live Gemini call)."""
    call = call or _make_call(max_tokens=max_tokens, temperature=temperature)
    con = _db()
    sem = asyncio.Semaphore(concurrency)

    async def one(prompt):
        if prompt is None:
            return None
        key = _row_key(prompt, model=model, system=system, schema=schema)
        cached = _cache_get(con, key)
        if cached is not None:
            return cached
        async with sem:
            try:
                result = await _with_retry(
                    lambda: call(prompt, model=model, schema=schema, system=system),
                    max_retries=max_retries,
                )
            except Exception as exc:  # noqa: BLE001 - isolate a row failure
                log.warning("llm row failed (%s); -> null", exc)
                return None
        _cache_put(con, key, result)
        return result

    try:
        return await asyncio.gather(*(one(p) for p in prompts))
    finally:
        con.close()


async def embed_batch(texts, *, model, concurrency, dims, max_retries, call=None) -> list:
    call = call or _make_embed_call(dims=dims)
    con = _db()
    sem = asyncio.Semaphore(concurrency)

    async def one(text):
        if text is None:
            return None
        key = _row_key(text, model=model, system=None, schema=None)
        cached = _cache_get(con, key)
        if cached is not None:
            return cached
        async with sem:
            vec = await _with_retry(lambda: call(text, model=model), max_retries=max_retries)
        _cache_put(con, key, vec)
        return vec

    try:
        return await asyncio.gather(*(one(t) for t in texts))
    finally:
        con.close()


# ---- live Gemini calls (Vertex via google-genai) ---------------------------

def _vertex_client():
    from google import genai

    return genai.Client(vertexai=True, project=settings.project, location=settings.location)


def _make_call(*, max_tokens: int, temperature: float):
    async def call(prompt, *, model, schema, system) -> Any:
        from google.genai import types
        from google.genai import errors as genai_errors

        client = _vertex_client()
        cfg = types.GenerateContentConfig(
            system_instruction=system,
            temperature=temperature,
            max_output_tokens=max_tokens,
        )
        if schema is not None:
            cfg.response_mime_type = "application/json"
            cfg.response_schema = schema
        try:
            resp = await client.aio.models.generate_content(
                model=model, contents=prompt, config=cfg)
        except genai_errors.APIError as exc:
            if getattr(exc, "code", None) in (429, 503):
                raise RetryableError(str(exc)) from exc
            raise
        if schema is not None:
            return json.loads(resp.text) if resp.text else None
        return resp.text

    return call


def _make_embed_call(*, dims):
    async def call(text, *, model) -> list[float]:
        from google.genai import types
        from google.genai import errors as genai_errors

        client = _vertex_client()
        cfg = types.EmbedContentConfig(output_dimensionality=dims) if dims else None
        try:
            resp = await client.aio.models.embed_content(
                model=model, contents=text, config=cfg)
        except genai_errors.APIError as exc:
            if getattr(exc, "code", None) in (429, 503):
                raise RetryableError(str(exc)) from exc
            raise
        return list(resp.embeddings[0].values)

    return call
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/test_gemini.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/mooncompute/llm/_gemini.py tests/test_gemini.py
git commit -m "Add Gemini async core: concurrency, retry, sqlite per-row cache"
```

---

## Task 8: LLM column ops (`map` / `embed`)

**Files:**
- Create: `src/mooncompute/llm/ops.py`
- Modify: `src/mooncompute/llm/__init__.py`
- Create: `tests/test_llm_ops.py`

- [ ] **Step 1: Write failing test** `tests/test_llm_ops.py`:

```python
import polars as pl

from mooncompute import llm


def test_map_utf8(monkeypatch):
    from mooncompute.llm import _gemini

    async def fake_batch(prompts, **kw):
        return [None if p is None else p.split()[-1].upper() for p in prompts]

    monkeypatch.setattr(_gemini, "complete_batch", fake_batch)
    df = pl.DataFrame({"review": ["love it", "hate it"]})
    out = df.with_columns(llm.map("review", prompt="Sentiment: {review}").alias("s"))
    assert out["s"].to_list() == ["IT", "IT"]


def test_map_struct_schema(monkeypatch):
    from mooncompute.llm import _gemini

    async def fake_batch(prompts, **kw):
        return [{"label": "pos", "score": 0.9} for _ in prompts]

    monkeypatch.setattr(_gemini, "complete_batch", fake_batch)
    df = pl.DataFrame({"review": ["a", "b"]})
    out = df.with_columns(
        llm.map("review", prompt="{review}", schema=dict).alias("r"))
    assert out["r"].struct.field("label").to_list() == ["pos", "pos"]


def test_embed(monkeypatch):
    from mooncompute.llm import _gemini

    async def fake_embed(texts, **kw):
        return [[0.1, 0.2, 0.3] for _ in texts]

    monkeypatch.setattr(_gemini, "embed_batch", fake_embed)
    df = pl.DataFrame({"t": ["x", "y"]})
    out = df.with_columns(llm.embed("t").alias("v"))
    assert out["v"].to_list()[0] == [pl.Series([0.1, 0.2, 0.3]).to_list()][0]
```

- [ ] **Step 2: Run to verify fail**

Run: `uv run pytest tests/test_llm_ops.py -v`
Expected: FAIL (`ImportError: cannot import name 'map' from 'mooncompute.llm'`).

- [ ] **Step 3: Write `llm/ops.py`**:

```python
"""map / embed as Polars expressions via map_batches.

map_batches hands the whole column to our function as one Series, so async
fan-out with bounded concurrency runs inside it. Native with_columns ergonomics
are preserved; the tradeoff is the column materializes at .collect() (it cannot
stay lazy past the LLM call)."""

from __future__ import annotations

import asyncio
from typing import Any

import polars as pl

from ..config import settings


def map(column: str, *, prompt: str, schema: Any = None, model: str | None = None,
        concurrency: int | None = None, system: str | None = None,
        max_tokens: int = 1024, temperature: float = 0.0) -> pl.Expr:
    """Map an LLM over a column. With a Pydantic `schema`, returns a Struct
    column via structured output; otherwise a Utf8 column."""
    model = model or settings.llm_default_model
    concurrency = concurrency or settings.llm_concurrency

    def _run(s: pl.Series) -> pl.Series:
        from . import _gemini

        rows = s.to_list()
        prompts = [prompt.format(**{column: v}) if v is not None else None for v in rows]
        results = asyncio.run(_gemini.complete_batch(
            prompts, model=model, schema=schema, system=system, max_tokens=max_tokens,
            temperature=temperature, concurrency=concurrency,
            max_retries=settings.llm_max_retries))
        if schema is not None:
            return pl.Series(results, dtype=pl.Struct)
        return pl.Series(results, dtype=pl.Utf8)

    return pl.col(column).map_batches(_run)


def embed(column: str, *, model: str | None = None, concurrency: int | None = None,
          dims: int | None = None) -> pl.Expr:
    """Embed a text column into a fixed-width Array(Float32) column."""
    model = model or settings.llm_embed_model
    concurrency = concurrency or settings.llm_concurrency

    def _run(s: pl.Series) -> pl.Series:
        from . import _gemini

        vecs = asyncio.run(_gemini.embed_batch(
            s.to_list(), model=model, concurrency=concurrency, dims=dims,
            max_retries=settings.llm_max_retries))
        width = dims or (len(next(v for v in vecs if v)) if any(vecs) else 0)
        return pl.Series(vecs, dtype=pl.Array(pl.Float32, width))

    return pl.col(column).map_batches(_run)
```

- [ ] **Step 4: Write `llm/__init__.py`**:

```python
"""LLM column operations (Vertex Gemini)."""

from __future__ import annotations

from .ops import embed, map

__all__ = ["map", "embed"]
```

- [ ] **Step 5: Run to verify pass**

Run: `uv run pytest tests/test_llm_ops.py -v`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add src/mooncompute/llm/ops.py src/mooncompute/llm/__init__.py tests/test_llm_ops.py
git commit -m "Add llm.map / llm.embed Polars column ops"
```

---

## Task 9: Public surface wiring

**Files:**
- Modify: `src/mooncompute/__init__.py`
- Create: `tests/test_public_api.py`

- [ ] **Step 1: Write failing test** `tests/test_public_api.py`:

```python
import mooncompute as mc


def test_public_exports():
    for name in ["read", "write", "dry_run", "sql", "cache", "clear_cache",
                 "configure", "settings", "Settings", "llm", "__version__"]:
        assert hasattr(mc, name), name


def test_llm_namespace():
    assert hasattr(mc.llm, "map")
    assert hasattr(mc.llm, "embed")


def test_version_matches_metadata():
    from importlib.metadata import version

    assert mc.__version__ == version("mooncompute")
```

- [ ] **Step 2: Run to verify fail**

Run: `uv run pytest tests/test_public_api.py -v`
Expected: FAIL (missing exports).

- [ ] **Step 3: Write `__init__.py`**:

```python
"""mooncompute — thin Arrow-glue for BigQuery / GCS / Polars / DuckDB + LLMs.

    import mooncompute as mc

    df  = mc.read("bq://proj.dataset.table", columns=["user_id", "ts"])
    df  = mc.read("gs://bucket/events/*.parquet")
    out = mc.sql("select user_id, count(*) from df group by 1")
    df  = df.with_columns(mc.llm.map("review", prompt="Classify: {review}").alias("y"))
    mc.write(df, "bq://proj.dataset.out", mode="overwrite")
"""

from __future__ import annotations

from importlib.metadata import version

from . import llm
from .cache import cache, clear_cache
from .config import Settings, configure, settings
from .io import dry_run, read, write
from .sql import sql

__version__ = version("mooncompute")

__all__ = [
    "read", "write", "dry_run", "sql", "cache", "clear_cache",
    "configure", "settings", "Settings", "llm", "__version__",
]
```

- [ ] **Step 4: Run to verify pass; then the full suite**

Run: `uv run pytest tests/test_public_api.py -v`
Expected: PASS (3 tests).
Run: `uv run pytest -q`
Expected: all tests pass.

- [ ] **Step 5: Lint, format, type-check**

Run: `uv run ruff check --fix . && uv run ruff format . && uv run ty check`
Expected: clean (fix any findings before committing).

- [ ] **Step 6: Commit**

```bash
git add src/mooncompute/__init__.py tests/test_public_api.py
git commit -m "Wire public top-level surface"
```

---

## Task 10: README & migration note

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Rewrite `README.md`** to document the v0.4 surface. Required sections, with real content (not placeholders):
  - one-paragraph pitch (thin Arrow glue; caching + LLM are the differentiators);
  - install (`pip install 'mooncompute[all]'`, or `[gcp]` / `[llm]`);
  - quickstart mirroring the `__init__` docstring (`read` / `sql` / `llm.map` / `write` / `@cache`);
  - caching section explaining the two invalidation modes (`cache="6h"` TTL vs `cache="pinned"` content) and the BQ freshness token;
  - LLM section noting Vertex/ADC auth, structured output, per-row SQLite cache, and the `.collect()`-materialization tradeoff;
  - a **Migration from v0.3** table:

    | v0.3 | v0.4 |
    | --- | --- |
    | `from mooncompute.gcp import bq2pl; bq2pl(sql)` | `mc.read(sql, lazy=False)` |
    | `gcp.extract_cached(sql, path, max_age=...)` | `mc.read(sql, cache="6h")` |
    | `gcp.extract_cached(sql, path, content_only=True)` | `mc.read(sql, cache="pinned")` |
    | `gcp.pl2bq(df, dataset=..., table=...)` | `mc.write(df, "bq://proj.ds.table", mode="append")` |
    | `gcp.gcs.read_parquet(uri)` | `mc.read(uri, lazy=False)` |

  - a short "Deferred" note: shared GCS cache tier, LLM Batch API, and USD spend cap are planned, not in v0.4.

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "Rewrite README for v0.4 surface with v0.3 migration table"
```

---

## Self-Review

**Spec coverage:** read/write/dry_run (Task 4) ✓; URI dispatch (Task 4) ✓; decimal recast, Storage-API reads, idempotent job_id (Task 3) ✓; invalidation discipline + freshness token + generalized @cache + atomic/fail-open (Task 5) ✓; sql() interop (Task 6) ✓; map/embed via map_batches + concurrency + retry + structured + per-row sqlite cache (Tasks 7–8) ✓; config/ADC/env rename (Task 1) ✓; creds materialization (Task 1) ✓; deps/extras (Task 0) ✓; top-level exports + clean break (Tasks 0, 9) ✓; README migration (Task 10) ✓. Deferred items (GCS tier, Batch API, spend cap) are intentionally inert and documented.

**Type/name consistency:** `complete_batch` / `embed_batch` / `RetryableError` / `_DB_PATH` used identically across Tasks 7–8; `CacheStore.get(...,freshness=)` / `put(...,freshness=)` consistent within Task 5; `read_cached(source, cache=...)` signature matches the call in `io.read` (Task 4) and the definition (Task 5); `table_modified` defined (Task 3) and called by `source_fingerprint` (Task 5).

**Note on the struct test (Task 8):** `schema=dict` is a stand-in so the fake returns plain dicts and Polars infers the Struct; real usage passes a Pydantic model. The fake-injection seam (`call=` / monkeypatched `complete_batch`) keeps every LLM test offline.
