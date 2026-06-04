# Consumer Ergonomics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `mooncompute` more ergonomic to import (ship type info, expose the `gcp` tier, blessed env-var docs) with no public API or behavior changes.

**Architecture:** Three independent, non-breaking changes: (1) add a PEP 561 `py.typed` marker and ensure it ships in the wheel; (2) re-export the `gcp` tier from the top-level package; (3) lead the README with `$GOOGLE_CLOUD_PROJECT` so the common call path omits `project=`.

**Tech Stack:** Python 3.11, hatchling build backend, polars, google-cloud-bigquery/storage, pytest, ruff, ty, uv, just.

Spec: `docs/superpowers/specs/2026-06-02-consumer-ergonomics-design.md`

---

## File Structure

- `src/mooncompute/py.typed` — **create.** Empty PEP 561 marker. Signals the package ships inline types.
- `pyproject.toml` — **modify.** Add a hatchling wheel include so the marker is packaged.
- `src/mooncompute/__init__.py` — **modify.** Re-export the `gcp` subpackage and declare `__all__`.
- `tests/test_public_api.py` — **modify.** Assert the marker exists and the top-level `gcp` re-export resolves.
- `README.md` — **modify.** Lead Configuration with the env var; show no-`project=` usage as default, `project=` as override.

Tasks are independent and can be done in any order; they are sequenced for clean commits.

---

### Task 1: Top-level `gcp` re-export

**Files:**
- Modify: `src/mooncompute/__init__.py`
- Test: `tests/test_public_api.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_public_api.py`:

```python
def test_top_level_gcp_reexport():
    import mooncompute

    assert hasattr(mooncompute, "gcp")
    assert "gcp" in dir(mooncompute)
    # the tier's helpers resolve through the top-level name
    assert hasattr(mooncompute.gcp, "bq2pl")
    # `from mooncompute import gcp` is the documented entry point
    from mooncompute import gcp

    assert gcp is mooncompute.gcp


def test_version_still_exposed():
    import mooncompute

    assert isinstance(mooncompute.__version__, str)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_public_api.py::test_top_level_gcp_reexport -v`
Expected: FAIL with `AttributeError: module 'mooncompute' has no attribute 'gcp'`.

- [ ] **Step 3: Write minimal implementation**

Replace the contents of `src/mooncompute/__init__.py` with:

```python
from importlib.metadata import version

from . import gcp

__version__ = version("mooncompute")

__all__ = ["gcp", "__version__"]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_public_api.py -v`
Expected: PASS (including the pre-existing `test_public_surface`).

- [ ] **Step 5: Commit**

```bash
git add src/mooncompute/__init__.py tests/test_public_api.py
git commit -m "Expose gcp tier from top-level mooncompute package"
```

---

### Task 2: Ship type information (`py.typed`)

**Files:**
- Create: `src/mooncompute/py.typed`
- Modify: `pyproject.toml`
- Test: `tests/test_public_api.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_public_api.py`:

```python
def test_py_typed_marker_present():
    import mooncompute
    from pathlib import Path

    pkg_dir = Path(mooncompute.__file__).parent
    assert (pkg_dir / "py.typed").is_file()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_public_api.py::test_py_typed_marker_present -v`
Expected: FAIL on the `assert ... .is_file()` (marker does not exist yet).

- [ ] **Step 3: Create the marker file**

Create `src/mooncompute/py.typed` as an empty file:

```bash
touch src/mooncompute/py.typed
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_public_api.py::test_py_typed_marker_present -v`
Expected: PASS.

- [ ] **Step 5: Ensure the marker ships in the wheel**

In `pyproject.toml`, the wheel target currently reads:

```toml
[tool.hatch.build.targets.wheel]
packages = ["src/mooncompute"]
```

Add a force-include directly below it so the marker is packaged regardless of default data-file handling:

```toml
[tool.hatch.build.targets.wheel]
packages = ["src/mooncompute"]

[tool.hatch.build.targets.wheel.force-include]
"src/mooncompute/py.typed" = "mooncompute/py.typed"
```

- [ ] **Step 6: Build the wheel and verify the marker is inside**

Run:
```bash
uv build && unzip -l dist/*.whl | grep py.typed
```
Expected: a line listing `mooncompute/py.typed`. (`uv build` writes to `dist/`; that directory is already git-ignored — confirm with `git status` that no `dist/` artifacts are staged.)

- [ ] **Step 7: Commit**

```bash
git add src/mooncompute/py.typed pyproject.toml tests/test_public_api.py
git commit -m "Ship py.typed marker so consumers get inline types"
```

---

### Task 3: Blessed `$GOOGLE_CLOUD_PROJECT` in README

**Files:**
- Modify: `README.md`

No automated test (docs only); verification is by reading the rendered section.

- [ ] **Step 1: Rewrite the Configuration section**

Replace the current Configuration paragraph in `README.md` with one that leads with the env var:

```markdown
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
```

- [ ] **Step 2: Update the Usage block to the no-`project=` default**

In the `## Usage` code block, drop `project=...` from the example calls so the
default style is the env-var one, and add a single comment showing the override.
The block becomes:

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

- [ ] **Step 3: Verify the rendered section**

Read the edited `README.md` and confirm: the env-var export precedes the usage
examples, the primary examples omit `project=`, and `project=` is shown exactly
once as the override. Confirm no other code fences in the README still pass
`project=...` as the default style.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "Lead README with GOOGLE_CLOUD_PROJECT as the blessed project path"
```

---

### Task 4: Full check + roadmap note

**Files:**
- Modify: `docs/ROADMAP.md`

- [ ] **Step 1: Run the full check suite**

Run: `just check`
Expected: fmt, lint, typecheck, and test all pass with no diffs left by the formatter.

- [ ] **Step 2: Record the shipped work in the roadmap**

Under the `## Unreleased (on main, since v0.1.0)` section of `docs/ROADMAP.md`,
add a bullet:

```markdown
- **Consumer ergonomics (no API change).** Ship a `py.typed` marker so
  consumers' type checkers see inline types; re-export the `gcp` tier from the
  top-level package (`from mooncompute import gcp`); README now leads with
  `$GOOGLE_CLOUD_PROJECT` so the common call path omits `project=`. The
  top-level `gcp` re-export is eager for now; it becomes a lazy `__getattr__`
  when the v0.2 extras/lazy-import split lands.
```

- [ ] **Step 3: Commit**

```bash
git add docs/ROADMAP.md
git commit -m "Note consumer-ergonomics changes in roadmap"
```

---

## Self-Review

**Spec coverage:**
- Spec change 1 (py.typed + wheel) → Task 2. ✓
- Spec change 2 (top-level `gcp` tier, not `bq`/`gcs`) → Task 1. ✓
- Spec change 3 (README env-var blessed path) → Task 3. ✓
- Spec verification (`just check`, wheel contains marker, scratch import) → Task 2 step 6, Task 4 step 1, Task 1 test. ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete content. ✓

**Type consistency:** `gcp` name used consistently across `__init__.py`, tests, README, roadmap. Marker filename `py.typed` consistent across file, pyproject include, and test. ✓
