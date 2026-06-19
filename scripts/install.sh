#!/bin/sh
# Install the `reckon` CLI from Codeberg release assets — no Go toolchain needed.
#
#   curl -fsSL https://codeberg.org/reckon-db-org/reckon-go/raw/branch/main/scripts/install.sh | sh
#
# Environment:
#   RECKON_VERSION   release tag to install (default: latest)
#   RECKON_BIN_DIR   install directory (default: $HOME/.local/bin)
#   RECKON_TOKEN     Codeberg access token — OPTIONAL. The repo is public, so no
#                    token is needed; set one only to raise API rate limits or if
#                    the repo is ever made private (also read from CODEBERG_TOKEN).
#                    Generate at Codeberg → Settings → Applications → Access Tokens (read).
set -eu

REPO="reckon-db-org/reckon-go"
HOST="codeberg.org"
BIN_DIR="${RECKON_BIN_DIR:-$HOME/.local/bin}"
VERSION="${RECKON_VERSION:-latest}"
TOKEN="${RECKON_TOKEN:-${CODEBERG_TOKEN:-}}"

say() { printf '%s\n' "$*" >&2; }
die() { say "install: $*"; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }

need curl
need sha256sum || need shasum

# curl auth header (optional — only used if a token is set).
AUTH=""
[ -n "$TOKEN" ] && AUTH="Authorization: token $TOKEN"
fetch() { # fetch <url> <outfile|->
  if [ -n "$AUTH" ]; then curl -fsSL -H "$AUTH" -o "$2" "$1"
  else curl -fsSL -o "$2" "$1"; fi
}

# Detect OS/arch and map to release asset naming.
os="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$os" in
  linux) os=linux ;;
  darwin) os=darwin ;;
  *) die "unsupported OS: $os (use go install, or build from source)" ;;
esac
arch="$(uname -m)"
case "$arch" in
  x86_64|amd64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) die "unsupported arch: $arch" ;;
esac

API="https://$HOST/api/v1/repos/$REPO/releases"

# Resolve "latest" to a concrete tag via the API.
if [ "$VERSION" = "latest" ]; then
  tmp_rel="$(mktemp)"
  fetch "$API/latest" "$tmp_rel" || die "could not query latest release (network issue, or rate-limited — set RECKON_TOKEN to raise the limit)"
  VERSION="$(grep -o '"tag_name":"[^"]*"' "$tmp_rel" | head -1 | cut -d'"' -f4)"
  rm -f "$tmp_rel"
  [ -n "$VERSION" ] || die "could not determine latest version"
fi

asset="reckon_${VERSION}_${os}_${arch}"
base="https://$HOST/$REPO/releases/download/$VERSION"

say "==> reckon $VERSION ($os/$arch)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

fetch "$base/$asset" "$work/$asset" || die "download failed: $base/$asset"
fetch "$base/SHA256SUMS" "$work/SHA256SUMS" || die "download failed: SHA256SUMS"

# Verify checksum (sha256sum if present, else shasum -a 256).
want="$(grep " $asset\$" "$work/SHA256SUMS" | awk '{print $1}')"
[ -n "$want" ] || die "no checksum for $asset in SHA256SUMS"
if command -v sha256sum >/dev/null 2>&1; then
  got="$(sha256sum "$work/$asset" | awk '{print $1}')"
else
  got="$(shasum -a 256 "$work/$asset" | awk '{print $1}')"
fi
[ "$want" = "$got" ] || die "checksum mismatch: want $want got $got"
say "==> checksum ok"

mkdir -p "$BIN_DIR"
install -m 0755 "$work/$asset" "$BIN_DIR/reckon"
say "==> installed $BIN_DIR/reckon"

case ":$PATH:" in
  *":$BIN_DIR:"*) ;;
  *) say "note: $BIN_DIR is not on PATH — add it, e.g.:"
     say "      export PATH=\"$BIN_DIR:\$PATH\"" ;;
esac
say "==> run: reckon --version"
