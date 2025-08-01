#!/usr/bin/env bash
# Script to apply platform-specific crypto patches for CircleCI builds

set -euo pipefail

# Get the target platform from the first argument
TARGET="${1:-}"

echo "Applying crypto patch for target: ${TARGET}"

# Function to uncomment the rustls patch lines in Cargo.toml
apply_ring_patch() {
    echo "Applying ring crypto patch for non-Linux platform"
    # Use sed to uncomment the rustls patch lines
    sed -i \
        -e 's/^# rustls = { version = "0.23"/rustls = { version = "0.23"/' \
        -e 's/^# rustls-webpki = { version = "0.102"/rustls-webpki = { version = "0.102"/' \
        Cargo.toml
    echo "Ring crypto patch applied"
}

# Check the target and apply patches accordingly
case "${TARGET}" in
    *linux*)
        echo "Linux target detected - keeping default aws-lc-rs crypto"
        ;;
    *windows* | *darwin* | *macos*)
        echo "Non-Linux target detected - applying ring crypto patch"
        apply_ring_patch
        ;;
    *)
        echo "Unknown target: ${TARGET} - keeping default crypto configuration"
        ;;
esac

# Show the relevant part of Cargo.toml for verification
echo "Current crypto configuration in Cargo.toml:"
grep -A 2 "Platform-specific crypto backend" Cargo.toml || true
grep "rustls.*0.23" Cargo.toml || true
grep "rustls-webpki.*0.102" Cargo.toml || true