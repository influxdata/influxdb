#!/bin/bash

# Use rustup to install rust tool chain after first
# comparing to a known checksum
set -eu -o pipefail

RUSTUP_VERSION=1.24.3
RUSTUP_HASH=a3cb081f88a6789d104518b30d4aa410009cd08c3822a1226991d6cf0442a0f8

# Notice --location to follow redirects -- important on Ubuntu 20.04
curl --proto '=https' --tlsv1.2 -sSf --location --max-redirs 1 \
  https://raw.githubusercontent.com/rust-lang/rustup/${RUSTUP_VERSION}/rustup-init.sh -O

if [ -x "$(command -v shasum)" ]; then
  hashcmd="shasum --algorithm 256"
elif [ -x "$(command -v sha256sum)" ]; then
  hashcmd="sha256sum"
else
  echo "no SHA hash command found"
  exit 1
fi

# Notice two spaces between hash and filename -- important on Darwin
echo "${RUSTUP_HASH}  rustup-init.sh" | ${hashcmd} --check -- \
  || { echo "checksum error!"; exit 1; }

sh rustup-init.sh -y --default-toolchain none
