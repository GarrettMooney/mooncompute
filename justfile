default: check

# Format, lint (with fixes), type-check, and test
check: fmt lint typecheck test

fmt:
    uv run ruff format .

lint:
    uv run ruff check --fix .

typecheck:
    uv run ty check

test:
    uv run pytest -q

# CI-style: verify formatting and lint without mutating, then typecheck + test
ci:
    uv run ruff format --check .
    uv run ruff check .
    uv run ty check
    uv run pytest -q

# Run all pre-commit hooks over the tree (prek = fast pre-commit, no install)
pc:
    uvx prek run --all-files

# Install the git pre-commit hook so it runs on every commit
pc-install:
    uvx prek install
