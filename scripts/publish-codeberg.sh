#!/usr/bin/env bash
# Create (or reuse) a Codeberg release for the given tag and upload every file
# in dist/ as a release asset. Forgejo/Gitea-compatible API.
#
# Usage: scripts/publish-codeberg.sh <version>
# Env:   CODEBERG_TOKEN  access token with repo write scope (CI secret).
set -euo pipefail

cd "$(dirname "$0")/.."

VERSION="${1:?usage: publish-codeberg.sh <version>}"
: "${CODEBERG_TOKEN:?CODEBERG_TOKEN is required}"

REPO="reckon-db-org/reckon-go"
API="https://codeberg.org/api/v1/repos/$REPO"
AUTH="Authorization: token $CODEBERG_TOKEN"

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing $1" >&2; exit 1; }; }
need curl
need jq

[ -d dist ] || { echo "dist/ not found — run build-release.sh first" >&2; exit 1; }

# Reuse an existing release for this tag, else create one.
id="$(curl -fsS -H "$AUTH" "$API/releases/tags/$VERSION" 2>/dev/null | jq -r '.id // empty')"
if [ -z "$id" ]; then
  id="$(curl -fsS -H "$AUTH" -H "Content-Type: application/json" \
    -X POST "$API/releases" \
    -d "$(jq -n --arg t "$VERSION" \
        '{tag_name:$t, name:$t, body:("Automated release " + $t)}')" \
    | jq -r '.id')"
  echo "created release $VERSION (id=$id)"
else
  echo "reusing release $VERSION (id=$id)"
fi
[ -n "$id" ] && [ "$id" != "null" ] || { echo "could not resolve release id" >&2; exit 1; }

for f in dist/*; do
  name="$(basename "$f")"
  # Replace an existing asset of the same name (idempotent re-runs).
  existing="$(curl -fsS -H "$AUTH" "$API/releases/$id/assets" \
    | jq -r --arg n "$name" '.[] | select(.name==$n) | .id')"
  if [ -n "$existing" ]; then
    curl -fsS -H "$AUTH" -X DELETE "$API/releases/$id/assets/$existing" >/dev/null
  fi
  curl -fsS -H "$AUTH" \
    -X POST "$API/releases/$id/assets?name=$name" \
    -F "attachment=@$f;type=application/octet-stream" >/dev/null
  echo "uploaded $name"
done
echo "done: https://codeberg.org/$REPO/releases/tag/$VERSION"
