#!/usr/bin/env bash
# Alternative approach using environment variables
# This script sets up environment variables that can control the crypto backend

set -euo pipefail

TARGET="${1:-}"

echo "Setting up crypto environment for target: ${TARGET}"

case "${TARGET}" in
    *linux*)
        echo "Linux target - using aws-lc-rs (default)"
        export INFLUXDB_CRYPTO_BACKEND="aws-lc-rs"
        ;;
    *windows* | *darwin* | *macos*)
        echo "Non-Linux target - configuring for ring"
        export INFLUXDB_CRYPTO_BACKEND="ring"
        # Set environment variable that rustls respects
        export CARGO_FEATURE_AWS_LC_RS=""
        export CARGO_FEATURE_RING="1"
        ;;
    *)
        echo "Unknown target: ${TARGET}"
        export INFLUXDB_CRYPTO_BACKEND="default"
        ;;
esac

echo "INFLUXDB_CRYPTO_BACKEND=${INFLUXDB_CRYPTO_BACKEND}"