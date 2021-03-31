#!/bin/bash -e

# The commit where the Rust `flatbuffers` crate version was changed to the version in `Cargo.lock`
# Update this, rerun this script, and check in the changes in the generated code when the
# `flatbuffers` crate version is updated.
FB_COMMIT="86401e078d0746d2381735415f8c2dfe849f3f52"

# Change to the generated_types crate directory, where this script is located
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
pushd $DIR

echo "Building flatc from source ..."

FB_URL="https://github.com/google/flatbuffers"
FB_DIR=".flatbuffers"
FLATC="$FB_DIR/bazel-bin/flatc"

if [ -z $(which bazel) ]; then
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

WAL_FBS="$DIR/protos/wal.fbs"
WAL_RS_DIR="$DIR/src"

$FLATC --rust -o $WAL_RS_DIR $WAL_FBS

ENTRY_FBS="$DIR/protos/influxdata/iox/write/v1/entry.fbs"
ENTRY_RS_DIR="$DIR/src"

$FLATC --rust -o $ENTRY_RS_DIR $ENTRY_FBS

cargo fmt
popd

echo "DONE! Please run 'cargo test' and check in any changes."
