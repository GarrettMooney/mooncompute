# dswkit roadmap

v1 (shipped, v0.1.0): `dswkit.gcp` only. bq2pl, extract_cached, pl2bq, read_sql,
gcs read/write, creds materialization. git+https distribution, new-code-only.

## v0.2 (planned): GCP-deploy ergonomics

Driven by the Cloud Function / Kubeflow usability review. None of these change
the public API.

1. **Artifact Registry Python repo mirror.** Publish dswkit to a GCP Artifact
   Registry Python repo in addition to Azure git. Lets Cloud Functions and KFP
   lightweight components install via workload-identity keyring instead of an
   Azure PAT, which the GCP deploy/runtime environments do not have. This is the
   single biggest unlock for GCP-native consumers and is the GCP-side version of
   the "graduate to a feed" step in the design spec.

2. **Optional-dependency extras to cut weight.** Split the heavy deps so a
   consumer that only needs JSON/bytes GCS I/O does not drag in polars +
   pyarrow (tens of MB, real Cloud Function cold-start cost).
   - `dswkit[bq]`  -> polars, pyarrow, google-cloud-bigquery, db-dtypes
   - `dswkit[gcs]` -> google-cloud-storage (+ polars only for parquet)
   - base install -> credential + URI helpers only
   Pair this with **lazy-importing polars inside the parquet functions** in
   `gcs.py` (currently imported at module top, so importing `dswkit.gcp.gcs`
   pulls polars even for `read_json`). The JSON/bytes path should stay light.

3. **Doc: extract_cached is a laptop/VM helper.** Its local parquet cache is
   ephemeral in containers and Cloud Functions. In those environments prefer
   `gcs.*` for durable artifacts. Already noted in the README deploy section;
   fold into the function docstring.

## Later tiers (from the design spec, not yet scoped)

- `dswkit.modal` — image preset, BQ -> volume staging, fan-out, .nc checkpoint.
- `dswkit.llm` — client factory, async retry + semaphore, verdict/JSON parsers.
- `dswkit.run` — timestamped run dirs, JSONL resume, markdown tables, logging.
