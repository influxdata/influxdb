#!/bin/bash -eu

# Change to the generated_types crate directory, where this script is located
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
pushd $DIR

echo "Regenerating flatbuffers code..."

./regenerate-flatbuffers.sh

echo "Checking for uncommitted changes..."

if ! git diff --quiet HEAD --; then
  echo "git diff found:"
  git diff HEAD
  echo "************************************************************"
  echo "* Found uncommitted changes to generated flatbuffers code! *"
  echo "* Please run \`generated_types/regenerate-flatbuffers.sh\`   *"
  echo "* to regenerate the flatbuffers code and check it in!      *"
  echo "************************************************************"
  exit 1
else
  echo "No uncommitted changes; this is fine."
fi
