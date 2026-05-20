#!/usr/bin/env bash
# One-command manual release: cross-compile + publish to Codeberg releases.
# Use this for releases while the GitHub Actions path is unavailable (the
# Codeberg->GitHub push-mirror / org workflow policy — see DESIGN §7a).
#
# Usage:  CODEBERG_TOKEN=<token> scripts/release-local.sh [version]
#   version defaults to the exact tag at HEAD, else `git describe`.
#   CODEBERG_TOKEN needs repo write scope; the repo's Releases unit must be
#   enabled (Settings -> Units, or has_releases=true via the API).
set -euo pipefail

cd "$(dirname "$0")/.."

VERSION="${1:-$(git describe --tags --exact-match 2>/dev/null || git describe --tags)}"
: "${CODEBERG_TOKEN:?set CODEBERG_TOKEN (Codeberg token with repo write scope)}"

echo "==> releasing $VERSION"
scripts/build-release.sh "$VERSION"
scripts/publish-codeberg.sh "$VERSION"
