#!/bin/bash -eu

# Change to the generated_types crate directory, where this script is located
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
pushd $DIR

echo "Regenerating flatbuffers code..."

# Will move this to docker if it works
sudo apt install g++
curl -Lo bazel-4.0.0-installer-linux-x86_64.sh https://github.com/bazelbuild/bazel/releases/download/4.0.0/bazel-4.0.0-installer-linux-x86_64.sh
ls -lrta
chmod +x bazel-4.0.0-installer-linux-x86_64.sh
./bazel-4.0.0-installer-linux-x86_64.sh --user

./regenerate-flatbuffers.sh

echo "Checking for uncommitted changes..."

if ! git diff-index --quiet HEAD --; then
  echo "git diff-index HEAD found:"
  git diff-index HEAD --
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
