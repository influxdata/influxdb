#!/bin/bash

set -e

make clean
make generate

status=$(git status --porcelain)
if [ -n "$status" ]; then
  >&2 echo "generated code is not accurate, please run make generate"
  >&2 echo -e "Files changed:\n$status"
  exit 1
fi
