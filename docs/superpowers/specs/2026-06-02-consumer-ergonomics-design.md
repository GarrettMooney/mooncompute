# Consumer ergonomics: design

Date: 2026-06-02
Status: approved (brainstorm), pending implementation plan

## Goal

Make `mooncompute` more ergonomic for the people who *import* it, without
changing any public API signature or runtime behavior. Three changes, all
low-risk and non-breaking.

## Non-goals

Tracked on the v0.2 roadmap, explicitly out of scope here:

- PyPI publish (install stays the git URL for now).
- Optional-dependency extras split (`mooncompute[bq]` / `[gcs]`).
- Lazy polars import inside `gcs.py`.

No public function signature, return type, or behavior changes.

## Changes

### 1. Ship type information (`py.typed`)

The library is fully type-annotated, but ships no [PEP 561][pep561] marker, so a
consumer's type checker (ty, pyright, mypy) treats `mooncompute` as untyped and
ignores its annotations.

- Add an empty marker file at `src/mooncompute/py.typed`.
- Ensure it ships in the wheel. The wheel target already sets
  `packages = ["src/mooncompute"]`; add an explicit hatchling include for the
  marker so it is packaged regardless of default data-file behavior (e.g.
  `[tool.hatch.build.targets.wheel.force-include]` mapping
  `"src/mooncompute/py.typed"` to `"mooncompute/py.typed"`, or the equivalent
  `artifacts`/`include` entry — implementer picks whichever hatchling honors,
  verified by inspecting the built wheel).

**Acceptance:** `uv build` produces a wheel that contains `mooncompute/py.typed`
(verify with `unzip -l dist/*.whl | grep py.typed`).

### 2. Expose the `gcp` tier at the top level

Today `import mooncompute` exposes only `__version__`; the helpers live one level
down at `mooncompute.gcp`. Expose the *tier* (not the individual `bq`/`gcs`
modules) at the top level so it is discoverable, while keeping
`mooncompute.gcp.bq` / `mooncompute.gcp.gcs` as the canonical paths.

- In `src/mooncompute/__init__.py`: `from . import gcp`, and add `"gcp"` to a
  module `__all__` (alongside `__version__`).
- Result: `from mooncompute import gcp` and `import mooncompute as mc;
  mc.gcp.bq2pl(...)` both work; `dir(mooncompute)` reveals `gcp`.

**Why the tier, not `bq`/`gcs` directly:** re-exporting `bq`/`gcs` to the top
level would collapse the tier namespace that later tiers (`modal`, `llm`, `run`)
are designed to share, and would couple `import mooncompute` to the same eager
heavy imports for every future tier. Exposing only `gcp` keeps tiers parallel.

**Accepted tradeoff:** `import mooncompute` now eagerly imports `mooncompute.gcp`,
which pulls polars + the google clients. This already happens the moment a
consumer touches `gcp`. When the v0.2 extras/lazy split lands, this top-level
re-export becomes a lazy `PEP 562` `__getattr__` so the base import stays light.
Noted here so the future change is expected, not a surprise.

**Acceptance:** `from mooncompute import gcp; gcp.bq2pl` resolves; `"gcp"` is in
`dir(mooncompute)`; existing `from mooncompute.gcp import bq, gcs` still works.

### 3. Make `$GOOGLE_CLOUD_PROJECT` the blessed path in docs

`project=` is accepted on every client-building call but does not need to be
threaded through when `$GOOGLE_CLOUD_PROJECT` is set (the gcloud SDK already sets
it). Documentation should lead with the env var so the common path is the
no-`project=` one; `project=` is the per-call override.

- README Configuration section: lead with `export GOOGLE_CLOUD_PROJECT=...`.
- README usage block: show the no-`project=` call style as the default, and
  present `project=` as the override for multi-project work.
- No code change.

**Acceptance:** README's primary usage examples omit `project=` and a one-line
env-var export precedes them; `project=` is shown once as the override.

## Verification

- `just check` (fmt, lint, typecheck, test) passes.
- `uv build` wheel contains `mooncompute/py.typed`.
- A scratch check: `python -c "from mooncompute import gcp; print(gcp.bq2pl)"`.

[pep561]: https://peps.python.org/pep-0561/
