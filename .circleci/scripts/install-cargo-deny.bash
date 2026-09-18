#!/usr/bin/env bash
#
# Install cargo-deny from the pinned prebuilt release binary, verified
# against a checksum recorded here. No-op if the pinned version is
# already on PATH (the CI image ships one). Falls back to building from
# source with `cargo install` if the download or verification fails, so
# audit jobs survive GitHub release-download outages.
#
# To bump the version: update VERSION and both SHA256_* values from the
# .sha256 assets at
# https://github.com/EmbarkStudios/cargo-deny/releases
# and keep the version pinned in ent/docker/Dockerfile.ci and
# oss/docker/Dockerfile.ci in sync.
#
set -euo pipefail

VERSION="0.20.2"
SHA256_X86_64="9f12ed4c49936e09b48bf862b595cde2fe64fcbd9d74dfacac6131ca824c8d5f"
SHA256_AARCH64="995c82be0defc7a025cae49a2aa2644ce8245c9a3318fc4103907c6a285e8c7d"

if command -v cargo-deny >/dev/null &&
  [ "$(cargo deny --version 2>/dev/null || true)" = "cargo-deny $VERSION" ]; then
  echo "cargo-deny $VERSION is already installed."
  exit 0
fi

# Install next to cargo itself, which is already on PATH.
BIN_DIR="${CARGO_DENY_BIN_DIR:-$(dirname "$(command -v cargo)")}"

case "$(uname -m)" in
  x86_64)
    ARCH="x86_64"
    SHA256="$SHA256_X86_64"
    ;;
  aarch64 | arm64)
    ARCH="aarch64"
    SHA256="$SHA256_AARCH64"
    ;;
  *)
    ARCH=""
    SHA256=""
    ;;
esac

NAME="cargo-deny-${VERSION}-${ARCH}-unknown-linux-musl"
URL="https://github.com/EmbarkStudios/cargo-deny/releases/download/${VERSION}/${NAME}.tar.gz"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

if [ -n "$SHA256" ] &&
  curl -fLsS --retry 3 --retry-delay 2 -o "$TMP/$NAME.tar.gz" "$URL" &&
  echo "$SHA256  $TMP/$NAME.tar.gz" | sha256sum -c - &&
  tar -xzf "$TMP/$NAME.tar.gz" -C "$TMP" &&
  install -m 755 "$TMP/$NAME/cargo-deny" "$BIN_DIR/cargo-deny"; then
  echo "Installed prebuilt cargo-deny $VERSION to $BIN_DIR."
else
  echo "Prebuilt cargo-deny install failed; building from source instead." >&2
  # --force: without it, a stale registration in .crates.toml makes this
  # a no-op that cannot repair a broken or outdated binary.
  cargo install cargo-deny --locked --force --version "$VERSION"
fi

cargo deny --version
