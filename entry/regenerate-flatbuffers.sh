#!/bin/bash -e

# Instructions
#
# If you have changed some `*.fbs` files:
#
# - Run this script to regenerate the corresponding Rust code.
# - Run `cargo test` to make sure everything works as you would expect.
# - Check in the changes to the generated code along with your changes to the `*.fbs` files.
# - You should not need to edit this script.
#
# If you are updating the version of the `flatbuffers` crate in `Cargo.lock`:
#
# - The `flatbuffers` crate gets developed in sync with the `flatc` compiler in the same repo,
#   so when updating the `flatbuffers` crate we also need to update the `flatc` compiler we're
#   using.
# - Go to https://github.com/google/flatbuffers/blame/master/rust/flatbuffers/Cargo.toml and find
#   the commit SHA where the `version` metadata was updated to the version of the `flatbuffers`
#   crate we now want to have in our `Cargo.lock`.
# - Put that commit SHA in this variable:
FB_COMMIT="86401e078d0746d2381735415f8c2dfe849f3f52"
# - Run this script to regenerate the corresponding Rust code.
# - Run `cargo test` to make sure everything works as you would expect.
# - Check in the changes to the generated code along with your changes to the `Cargo.lock` file and
#   this script.

# By default, this script will run a Docker container that uses the same image we use in CI that
# will have all the necessary dependencies. If you don't want to run Docker, run this script with
# INFLUXDB_IOX_INTEGRATION_LOCAL=1.
if [ -z "${INFLUXDB_IOX_INTEGRATION_LOCAL}" ]; then
  echo "Running in Docker..."

  CI_IMAGE=quay.io/influxdb/rust:ci

  DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
  pushd "$DIR"

  DOCKER_IOX_DIR=/home/rust/influxdb_iox

  docker rm --force flatc || true

  docker pull ${CI_IMAGE} || true

  docker run \
    -it \
    --detach \
    --name=flatc \
    --volume "${DIR}/..:${DOCKER_IOX_DIR}" \
    ${CI_IMAGE}

  docker exec -e INFLUXDB_IOX_INTEGRATION_LOCAL=1 flatc .${DOCKER_IOX_DIR}/entry/regenerate-flatbuffers.sh

  docker rm --force flatc || true
else
  echo "Running locally..."

  DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
  pushd "$DIR"

  echo "Building flatc from source ..."

  FB_URL="https://github.com/google/flatbuffers"
  FB_DIR=".flatbuffers"
  FLATC="$FB_DIR/bazel-bin/flatc"

  if [ -z "$(which bazel)" ]; then
      echo "bazel is required to build flatc"
      exit 1
  fi

  echo "Bazel version: $(bazel version | head -1 | awk -F':' '{print $2}')"

  if [ ! -e $FB_DIR ]; then
      echo "git clone $FB_URL ..."
      git clone -b master --no-tag $FB_URL $FB_DIR
  else
      echo "git pull $FB_URL ..."
      git -C $FB_DIR pull --ff-only
  fi

  echo "hard reset to $FB_COMMIT"
  git -C $FB_DIR reset --hard $FB_COMMIT

  pushd $FB_DIR
  echo "run: bazel build :flatc ..."
  bazel build :flatc
  popd

  RUST_DIR="$DIR/src"
  while read -r FBS_FILE; do
      echo "Compiling ${FBS_FILE}"
      $FLATC --rust -o "$RUST_DIR" "$FBS_FILE"
  done < <(git ls-files "$DIR"/*.fbs)

  cargo fmt
  popd

  echo "DONE! Please run 'cargo test' and check in any changes."
fi
