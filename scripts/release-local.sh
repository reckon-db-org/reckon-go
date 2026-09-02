#!/usr/bin/env bash
# One-command manual release: cross-compile + publish to GitHub Releases.
# The normal path is the tag-triggered .github/workflows/release.yml; use
# this when a release must be cut from a workstation.
#
# Usage:  scripts/release-local.sh [version]
#   version defaults to the exact tag at HEAD, else `git describe`.
#   Needs the GitHub CLI (`gh`) authenticated with repo write scope.
set -euo pipefail

cd "$(dirname "$0")/.."

VERSION="${1:-$(git describe --tags --exact-match 2>/dev/null || git describe --tags)}"
command -v gh >/dev/null || { echo "gh (GitHub CLI) is required" >&2; exit 64; }

echo "==> releasing $VERSION"
scripts/build-release.sh "$VERSION"
gh release create "$VERSION" dist/reckon_* dist/SHA256SUMS \
    --repo reckon-db-org/reckon-go --title "$VERSION" --generate-notes
echo "done: https://github.com/reckon-db-org/reckon-go/releases/tag/$VERSION"
