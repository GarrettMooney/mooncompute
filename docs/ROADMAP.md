# mooncompute roadmap

v1 (v0.1.0): `mooncompute.gcp` only. bq2pl, extract_cached, pl2bq, read_sql,
gcs read/write, creds materialization. Project resolved from `project=` or
`$GOOGLE_CLOUD_PROJECT`.

## Unreleased (on main, since v0.1.0)

- `extract_cached` gained an opt-in `max_age` TTL: re-query when the cached
  manifest's `written_at` is older than the given `timedelta`. Default stays
  content-only (no TTL), correct for pinned-snapshot queries. The hit log now
  shows cache age, and the docstring states the determinism contract (the
  cache is for deterministic SQL; live/relative queries need `max_age` or
  `bq2pl`). Deliberately did NOT add a source-table `last_modified_time`
  freshness check: boring TTL over the impressive-but-fragile option.

- **`extract_cached` now requires an explicit invalidation mode (breaking).**
  A bare call raises `ValueError`; the caller must pass `content_only=True`
  (deterministic/pinned query, SQL-change-only invalidation) or
  `max_age=<timedelta>` (live/relative query, TTL). The two are mutually
  exclusive. This makes the silent-stale-cache footgun a deliberate opt-in
  rather than the default — fail-closed on ambiguous intent.

- Hardened the stateful edges (good-system-design review):
  - **Cache fails open.** A corrupt/unreadable parquet logs a warning and
    re-queries BQ instead of raising. The cache is a derived artifact, never a
    source of truth, so it must not be able to harden a failure.
  - **Atomic cache writes.** Parquet and manifest are written to a temp
    sibling then `os.replace`d, so a crash mid-write leaves the previous cache
    intact rather than a truncated file.
  - **`pl2bq` accepts a `job_id` idempotency key.** A retried load with the
    same id is a no-op (BQ rejects duplicate job ids) instead of a double-load.
  - **Logging, not `print`.** `bq` emits via a module logger so consumers
    control verbosity; unhappy paths (cache miss/expiry/SQL change/corruption)
    are logged, not just the happy path.

## v0.3: GCP-deploy ergonomics

Driven by the Cloud Function / Kubeflow usability review. None of these change
the public API.

1. **Optional-dependency extras to cut weight. (DONE, v0.3.0)** Heavy deps are
   split so a consumer that only needs JSON/bytes GCS I/O does not drag in
   polars + pyarrow (tens of MB, real Cloud Function cold-start cost).
   - `mooncompute[bq]`  -> polars, pyarrow, google-cloud-bigquery, db-dtypes
   - `mooncompute[gcs]` -> google-cloud-storage (parquet additionally needs
     polars from `[bq]`, lazily imported)
   - `mooncompute[all]` -> `[bq,gcs]`
   - base install -> credential + URI helpers only (no third-party deps)
   polars is **lazy-imported inside the parquet functions** in `gcs.py`, and the
   `gcp` package imports submodules lazily (PEP 562) so `import mooncompute.gcp`
   pulls neither the BigQuery stack nor polars until a BigQuery helper is touched.
   A `[gcs]`-only install can `from mooncompute.gcp import gcs` for JSON/bytes
   with no polars present.

2. **Publish to PyPI. (v0.3.0)** Releases publish via **Trusted Publishing
   (OIDC)** from `.github/workflows/ci.yml` on a `v*` tag -- no API token stored
   anywhere (methodology per microsoft/durabletask-python#139; test/CI/deploy
   workflow split per the lmmx house style). After the one-time PyPI
   pending-publisher registration (see `docs/RELEASING.md`), installs are
   `uv add "mooncompute[bq]"` instead of the git URL. For private GCP-native
   installs without a token, an Artifact Registry Python repo mirror is an
   option (workload-identity keyring auth).

3. **Doc: extract_cached is a laptop/VM helper. (DONE)** Its local parquet cache
   is ephemeral in containers and Cloud Functions. In those environments prefer
   `gcs.*` for durable artifacts. Noted in the README and the docstring.

## Later tiers (the "compute" in mooncompute, not yet scoped)

- `mooncompute.modal` — image preset, BQ -> volume staging, fan-out, .nc checkpoint.
- `mooncompute.llm` — client factory, async retry + semaphore, verdict/JSON parsers.
- `mooncompute.run` — timestamped run dirs, JSONL resume, markdown tables, logging.
