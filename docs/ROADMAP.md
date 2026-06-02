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

## v0.2 (planned): GCP-deploy ergonomics

Driven by the Cloud Function / Kubeflow usability review. None of these change
the public API.

1. **Optional-dependency extras to cut weight.** Split the heavy deps so a
   consumer that only needs JSON/bytes GCS I/O does not drag in polars +
   pyarrow (tens of MB, real Cloud Function cold-start cost).
   - `mooncompute[bq]`  -> polars, pyarrow, google-cloud-bigquery, db-dtypes
   - `mooncompute[gcs]` -> google-cloud-storage (+ polars only for parquet)
   - base install -> credential + URI helpers only
   Pair this with **lazy-importing polars inside the parquet functions** in
   `gcs.py` (currently imported at module top, so importing `mooncompute.gcp.gcs`
   pulls polars even for `read_json`). The JSON/bytes path should stay light.

2. **Publish to PyPI.** Once the extras split lands, `uv publish` so installs
   are `uv add mooncompute` instead of the git URL. For private GCP-native
   installs without a token, an Artifact Registry Python repo mirror is an
   option (workload-identity keyring auth).

3. **Doc: extract_cached is a laptop/VM helper.** Its local parquet cache is
   ephemeral in containers and Cloud Functions. In those environments prefer
   `gcs.*` for durable artifacts. Noted in the README and the docstring.

## Later tiers (the "compute" in mooncompute, not yet scoped)

- `mooncompute.modal` — image preset, BQ -> volume staging, fan-out, .nc checkpoint.
- `mooncompute.llm` — client factory, async retry + semaphore, verdict/JSON parsers.
- `mooncompute.run` — timestamped run dirs, JSONL resume, markdown tables, logging.
