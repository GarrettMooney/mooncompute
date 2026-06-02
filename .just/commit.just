# Stash-aware commit: run the autofixer over the staged tree, then commit.
# Adapted from github.com/lmmx/polars-expr-hopper (.just/commit.just), minus the
# echo-comment dependency. Unstaged/untracked changes are stashed first (except
# this justfile) so the fixer's edits don't sweep in unrelated work, then the
# stash is restored on exit.

# git pathspec we never stash (so `just` itself stays put during a commit)
NO_STASH := "justfile"

# name of the Justfile task that formats/lints/fixes
FIXER := "pc"

# Autofix, then commit the staged changes with MSG (extra args pass to git commit)
commit msg *args:
    #!/usr/bin/env bash
    set -euo pipefail

    unstaged=$(git diff --name-only -- . ":(exclude){{NO_STASH}}" | wc -l | tr -d ' ')
    untracked=$(git ls-files --others --exclude-standard -- ':/*' ":(exclude){{NO_STASH}}" | wc -l | tr -d ' ')
    stash_count=$((unstaged + untracked))

    if [ "$stash_count" -ne 0 ]; then
      echo "stashing $stash_count unstaged/untracked file(s) before fixing"
      git stash push --keep-index --include-untracked -m "commit-temp" -- ':/*' ":(exclude){{NO_STASH}}"
      trap 'echo "restoring stashed changes"; git stash pop --quiet' EXIT
    fi

    # don't abort if the fixer auto-fixed (prek exits non-zero on modification)
    just {{FIXER}} || true

    if [ -n "$(git diff --name-only -- . ":(exclude){{NO_STASH}}")" ]; then
      echo "fixer modified files, staging them"
      git add --all
    fi

    git commit -m "{{msg}}" {{args}}
