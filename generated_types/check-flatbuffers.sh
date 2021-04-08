#!/bin/bash -eu

BEFORE_SHA=$1
AFTER_SHA=$2

# Change to the generated_types crate directory, where this script is located
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
pushd $DIR

echo "Checking for changes to fbs files between ${BEFORE_SHA} and ${AFTER_SHA}..."

FBS_CHANGES=$(git rev-list "${BEFORE_SHA}".."${AFTER_SHA}" "**/*.fbs" | wc -l)

if [[ $FBS_CHANGES -gt 0 ]]; then
  echo "Changes to fbs files detected, regenerating flatbuffers code..."

  # Will move this to docker if it works
  sudo apt install g++
  curl -Lo bazel-4.0.0-installer-linux-x86_64.sh https://github.com/bazelbuild/bazel/releases/download/4.0.0/bazel-4.0.0-installer-linux-x86_64.sh
  ls -lrta
  chmod +x bazel-4.0.0-installer-linux-x86_64.sh
  ./bazel-4.0.0-installer-linux-x86_64.sh --user

  ./regenerate-flatbuffers.sh

  echo "Checking for uncommitted changes..."

  if ! git diff-index --quiet HEAD --; then
    echo "************************************************************"
    echo "* Found uncommitted changes to generated flatbuffers code! *"
    echo "* Please run \`generated_types/regenerate-flatbuffers.sh\`   *"
    echo "* to regenerate the flatbuffers code and check it in!      *"
    echo "************************************************************"
    exit 1
  else
    echo "No uncommitted changes; this is fine."
  fi
else
  echo "No changes to fbs files detected."
fi
