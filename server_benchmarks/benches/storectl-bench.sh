#!/bin/bash

# This script was based on the benchmark script in
# https://github.com/influxdata/idpe/blob/b304e751e1/cmd/storectl/benchmark/bench.sh

# TODO: Better management of relationship of this checkout to the idpe checkout
idpe_checkout_dir=~/Go/idpe

org_id=0000111100001111
bucket_id=1111000011110000

tmp_dir=$(mktemp -d -t bench)
mkdir -p $tmp_dir/data/0
mkdir -p $tmp_dir/data/object/0
echo "Working in temporary directory $tmp_dir."
echo "Run:"
echo "  rm -rf $tmp_dir"
echo "to clean up in the end."

echo "Building binaries..."
cargo build --release && \

# TODO: Teach storectl to generate line protocol instead of using inch?
  cd $idpe_checkout_dir/cmd/inch && \
    go build -o $tmp_dir/bin/inch . && \
        cd - > /dev/null && \

# TODO: Figure out how to build storectl from outside the idpe checkout
  cd $idpe_checkout_dir/cmd/storectl/benchmark && \
    go build -o $tmp_dir/bin/storectl ../../storectl && \
        cd - > /dev/null

if [[ $? -ne 0 ]]; then
  exit 1
fi

# Once IOx can ingest what `storectl generate` creates, this section will be needed.
# cat > $tmp_dir/data.toml << EOL
# title = "CLI schema"
#
# [[measurements]]
# name   = "m0"
# sample = 1.0
# tags = [
#     { name = "tag0", source = { type = "sequence", format = "value%s", start = 0, count = 30 } },
#     { name = "tag1", source = { type = "sequence", format = "value%s", start = 0, count = 20 } },
#     { name = "tag2", source = { type = "sequence", format = "value%s", start = 0, count = 10 } },
# ]
# fields = [
#     { name = "v0", count = 10, source = 1.0 },
# ]
# EOL
#
# $tmp_dir/bin/storectl generate --base-dir $tmp_dir/data --org-id=$org_id --bucket-id=$bucket_id $tmp_dir/data.toml --clean=none

sess=influxdb-iox-rpc-bench
tmux new-session -t $sess -d
tmux rename-window -t $sess 'bench'
tmux send-keys "./target/release/influxdb_iox" 'C-m'
tmux split-window -t $sess -v
tmux send-keys "sleep 5; curl 'http://localhost:8080/api/v2/create_bucket' -d org=$org_id -d bucket=$bucket_id; $tmp_dir/bin/inch -bucket $bucket_id -org $org_id -host http://localhost:8080" 'C-m'
tmux select-pane -t $sess -R
tmux split-window -t $sess -v
tmux send-keys "$tmp_dir/bin/storectl query -b $bucket_id -o $org_id --silent -c 1 --csv-out --expr \"tag0='value0'\""
tmux attach -t $sess
