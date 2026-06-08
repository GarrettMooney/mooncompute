# Cut a release: bump the version, commit, tag, and push.
# Adapted from github.com/lmmx/polars-expr-hopper (.just/ship.just), minus the
# echo-comment dependency and its temp-commit/soft-reset dance (uv version
# --short already reads the bumped value). Tags keep the `v` prefix to match the
# existing v0.1.0 tag and the CI workflow's `tags: ["v*"]` trigger. Pushing the
# tag triggers .github/workflows/ci.yml, which publishes to PyPI via Trusted
# Publishing (OIDC) -- no token. See docs/RELEASING.md for the release flow and
# the one-time PyPI pending-publisher registration it depends on.

# Bump (patch|minor|major), commit, tag vX.Y.Z, and push branch + tag
release bump_level="patch":
    #!/usr/bin/env bash
    set -euo pipefail

    if [ "{{bump_level}}" = "--help" ]; then uv version --help; exit 0; fi

    uv version --bump {{bump_level}}
    new_version="v$(uv version --short)"
    echo "releasing $new_version"

    git add --all
    git commit -m "chore(release): bump -> $new_version"
    git tag -a "$new_version" -m "Release $new_version"

    branch=$(git rev-parse --abbrev-ref HEAD)
    git push origin "$branch"
    git push origin "$new_version"
