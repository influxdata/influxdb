#!/bin/bash

set -e

# This script regenerates the flag list and checks for differences to ensure flags
# have been regenerated in case of changes to flags.yml.

make flags

if ! git --no-pager diff --exit-code -- ./kit/feature/list.go
then
  echo "Differences detected! Run 'make flags' to regenerate feature flag list."
  exit 1
fi
