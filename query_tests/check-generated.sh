#!/bin/bash -eu

# Change to the query_tests crate directory, where this script is located
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
pushd $DIR

echo "Regenerating query_tests..."

(cd generate && cargo run)

echo "Checking for uncommitted changes..."

if ! git diff --quiet HEAD --; then
  echo "git diff found:"
  git diff HEAD
  echo "************************************************************"
  echo "* Found uncommitted changes to generated flatbuffers code! *"
  echo "* Please do:"
  echo "*  cd query_tests/generate"
  echo "*  cargo run"
  echo "* to regenerate the query_tests code and check it in!      *"
  echo "************************************************************"
  exit 1
else
  echo "No uncommitted changes; everything is awesome."
fi
