#!/usr/bin/env bash
# Cross-compile the `reckon` CLI into dist/ as static, stripped binaries plus
# a SHA256SUMS manifest. Pure Go toolchain — no external tools.
#
# Usage: scripts/build-release.sh [version]
#   version defaults to `git describe --tags`. Assets are named
#   reckon_<version>_<os>_<arch>[.exe]; install.sh resolves the same names.
set -euo pipefail

cd "$(dirname "$0")/.."

VERSION="${1:-$(git describe --tags --always --dirty 2>/dev/null || echo dev)}"
PKG="github.com/reckon-db-org/reckon-go/cmd/reckon"
OUT="dist"
LDFLAGS="-s -w -X main.version=${VERSION}"

PLATFORMS=(
  "linux/amd64"
  "linux/arm64"
  "darwin/amd64"
  "darwin/arm64"
  "windows/amd64"
)

rm -rf "$OUT"
mkdir -p "$OUT"

for p in "${PLATFORMS[@]}"; do
  os="${p%/*}"
  arch="${p#*/}"
  asset="reckon_${VERSION}_${os}_${arch}"
  [ "$os" = "windows" ] && asset="${asset}.exe"
  echo "build ${os}/${arch} -> ${asset}"
  CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" \
    go build -trimpath -ldflags "$LDFLAGS" -o "$OUT/$asset" "$PKG"
done

# Single manifest over all assets (sha256sum -c compatible).
( cd "$OUT" && sha256sum reckon_* > SHA256SUMS )

echo "--- dist/ ---"
ls -l "$OUT"
