#!/usr/bin/env bash
#
# This script takes a git sha as input, checks out and runs cargo
# benchmarks at that sha, and appends the results, as line protocol to
# iox_bench.lp
#
# Example
# ./run_bench.sh /Users/alamb/Software/influxdb_iox2 a6ed8d59dc0465ff3a605b1fd4783faa9edb8560
#
# A list of commits can be found via
# git rev-list main
#
# So to run this thing on the last 10 commits, you can use a command such as
# git rev-list main | head | sed -e 's|^|./run_bench.sh /Users/alamb/Software/influxdb_iox2 |'

read -r -d '' USAGE << EOF
Usage:
   $0 <source directory> gitsha

Example:
   $0 /Users/alamb/Software/influxdb_iox2 a6ed8d59dc0465ff3a605b1fd4783faa9edb8560

Log is written to stdout
Performance results are appended to iox_bench.lp
EOF

# Location of this script
# https://stackoverflow.com/questions/59895/how-to-get-the-source-directory-of-a-bash-script-from-within-the-script-itself
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

SOURCE_DIR=$1
if [ -z "$SOURCE_DIR" ] ; then
    echo "Error: no source directory specified"
    echo "$USAGE"
    exit 1
fi

if [ ! -d "$SOURCE_DIR" ] ; then
    echo "Error: Not a directory: $SOURCE_DIR"
    echo "$USAGE"
    exit 1
fi


GITSHA=$2
if [ -z "$GITSHA" ] ; then
    echo "Error: no gitsha specified"
    echo "$USAGE"
    exit 1
fi

RESULTS_FILE=$SCRIPT_DIR/iox_bench.lp

echo "**************************"
echo "InfluxDB IOx Benchmark Tool"
echo "**************************"
echo "Run starting at $(date)"
echo "Running on host $(hostname)"
echo "Source Directory: $SOURCE_DIR"
echo "Git SHA: $GITSHA"
echo "Results file: $RESULTS_FILE"

pushd $SOURCE_DIR
set -x
git checkout $GITSHA || (echo "Error checking out code. Aborting." && exit 2)
rm -rf target/criterion # remove any old results
# Debugging tip: use a command like this to run a subset of the benchmarks:
#cargo bench -- float_encode_sequential/100000 || (echo "Error running benchmarks. Aborting." && exit 3)
cargo bench || (echo "Error running benchmarks. Aborting." && exit 3)
# now, run the scraper and append results
$SCRIPT_DIR/scrape_benches.sh $SOURCE_DIR | tee -a $RESULTS_FILE
set +x
popd
