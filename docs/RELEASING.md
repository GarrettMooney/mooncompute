# Releasing

mooncompute publishes to PyPI via **Trusted Publishing (OIDC)**, not an API
token. PyPI verifies that an upload originates from this repository's GitHub
Actions workflow, so the package users install is provably the artifact CI built
(and is accompanied by a build-provenance attestation). This eliminates a
long-lived credential that could be stolen and used to republish a malicious
package. Methodology per
[microsoft/durabletask-python#139](https://github.com/microsoft/durabletask-python/issues/139),
structured after the [lmmx](https://github.com/lmmx/polars-expr-hopper) house
style.

## The three workflows

- **`test.yml`** ("Test") -- lint (ruff + ty) and pytest across the Python
  versions derived from `requires-python`. Runs on pushes to `main` and PRs.
- **`ci.yml`** ("CI") -- builds with `uv build` and publishes via
  `pypa/gh-action-pypi-publish` (OIDC). The release job runs on a `v*` tag push
  or after "Deploy Release" completes (`workflow_run`).
- **`deploy.yml`** ("Deploy Release") -- the manual release button. The
  github-actions bot runs `just release <bump>` (bump + commit + tag + push).

Why `ci.yml` also has a `workflow_run` trigger: a tag pushed by the Deploy
Release job uses the default `GITHUB_TOKEN`, and pushes made by `GITHUB_TOKEN`
do not trigger further workflows. The `workflow_run` hook fires CI after Deploy
Release finishes so the release still happens.

## One-time setup (PyPI side, maintainer only)

The project does not exist on PyPI yet, so register a **pending publisher**
before the first release:

1. Go to <https://pypi.org/manage/account/publishing/>.
2. Under "Add a new pending publisher", enter:
   - **PyPI Project Name:** `mooncompute`
   - **Owner:** `GarrettMooney`
   - **Repository name:** `mooncompute`
   - **Workflow name:** `ci.yml`
   - **Environment name:** `pypi`
3. Save. (Optional but recommended: create a `pypi` environment in the repo's
   GitHub settings with required reviewers, so a human approves each publish.
   GitHub auto-creates the environment on first run if you skip this.)

After the first successful publish, PyPI converts the pending publisher into a
normal trusted publisher automatically.

## Cutting a release

Two equivalent paths, both end in a `chore(release)` commit + `v*` tag that the
CI release job picks up:

- **From GitHub (recommended):** Actions tab -> "Deploy Release" -> Run workflow
  -> pick the bump level. The bot does the rest; CI publishes.
- **Locally:** `just release <patch|minor|major>` bumps, commits, tags, and
  pushes (the tag push triggers CI directly, since it is a user push, not a
  `GITHUB_TOKEN` push).

## Verifying

Watch the **CI** workflow's Release job in the Actions tab; on success the
version appears at <https://pypi.org/project/mooncompute/>. Install with
`uv add "mooncompute[bq]"`.
