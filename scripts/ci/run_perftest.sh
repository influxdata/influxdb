#!/usr/bin/sh -ex

# Install Telegraf
wget -qO- https://repos.influxdata.com/influxdb.key | apt-key add -
echo "deb https://repos.influxdata.com/ubuntu focal stable" | tee /etc/apt/sources.list.d/influxdb.list

DEBIAN_FRONTEND=noninteractive apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y git jq telegraf awscli

# we need libc6 version 2.32 (released in ubuntu for 20.10 and later) for influx_tools
cp /etc/apt/sources.list /etc/apt/sources.list.d/groovy.list
sed -i 's/focal/groovy/g' /etc/apt/sources.list.d/groovy.list
DEBIAN_FRONTEND=noninteractive apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y libc6 -t groovy

root_branch="$(echo "${INFLUXDB_VERSION}" | rev | cut -d '-' -f1 | rev)"
log_date=$(date +%Y%m%d%H%M%S)
cleanup() {
  aws s3 cp /home/ubuntu/perftest_log.txt s3://perftest-logs-influxdb/oss/$root_branch/${TEST_COMMIT}-${log_date}.log
  if [ "${CIRCLE_TEARDOWN}" = true ]; then
    curl --request POST \
      --url https://circleci.com/api/v2/project/github/influxdata/influxdb/pipeline \
      --header "Circle-Token: ${CIRCLE_TOKEN}" \
      --header 'content-type: application/json' \
      --data "{\"branch\":\"${INFLUXDB_VERSION}\", \"parameters\":{\"aws_teardown\": true, \"aws_teardown_branch\":\"${INFLUXDB_VERSION}\", \"aws_teardown_sha\":\"${TEST_COMMIT}\", \"aws_teardown_datestring\":\"${CIRCLE_TEARDOWN_DATESTRING}\", \"aws_teardown_query_format\":\"${TEST_FORMAT}\"}}"
  fi
}
trap "cleanup" EXIT KILL

working_dir=$(mktemp -d)
mkdir -p /etc/telegraf
cat << EOF > /etc/telegraf/telegraf.conf
[[outputs.influxdb_v2]]
  urls = ["https://us-west-2-1.aws.cloud2.influxdata.com"]
  token = "${DB_TOKEN}"
  organization = "${CLOUD2_ORG}"
  bucket = "${CLOUD2_BUCKET}"

[[inputs.file]]
  name_override = "ingest"
  files = ["$working_dir/test-ingest-*.json"]
  data_format = "json"
  json_strict = true
  json_string_fields = [
    "branch",
    "commit",
    "i_type",
    "time",
    "use_case"
  ]
  tagexclude = ["host"]
  json_time_key = "time"
  json_time_format = "unix"
  tag_keys = [
    "i_type",
    "use_case",
    "branch"
  ]

[[inputs.file]]
  name_override = "query"
  files = ["$working_dir/test-query-*.json"]
  data_format = "json"
  json_strict = true
  json_string_fields = [
    "branch",
    "commit",
    "i_type",
    "query_format",
    "query_type",
    "time",
    "use_case"
  ]
  tagexclude = ["host"]
  json_time_key = "time"
  json_time_format = "unix"
  tag_keys = [
    "i_type",
    "query_format",
    "use_case",
    "query_type",
    "branch"
  ]
EOF
systemctl restart telegraf

cd $working_dir

# install golang latest version
go_version=$(curl https://golang.org/VERSION?m=text)
go_endpoint="$go_version.linux-amd64.tar.gz"

wget "https://dl.google.com/go/$go_endpoint" -O "$working_dir/$go_endpoint"
rm -rf /usr/local/go
tar -C /usr/local -xzf "$working_dir/$go_endpoint"

# set env variables necessary for go to work during cloud-init
if [ `whoami` = root ]; then
  mkdir -p /root/go/bin
  export HOME=/root
  export GOPATH=/root/go
  export PATH=$PATH:/usr/local/go/bin:$GOPATH/bin
fi
go version

# clone influxdb comparisons
git clone https://github.com/influxdata/influxdb-comparisons.git $working_dir/influxdb-comparisons
cd $working_dir/influxdb-comparisons

# install cmds
go get \
  github.com/influxdata/influxdb-comparisons/cmd/bulk_data_gen \
  github.com/influxdata/influxdb-comparisons/cmd/bulk_load_influx \
  github.com/influxdata/influxdb-comparisons/cmd/bulk_query_gen \
  github.com/influxdata/influxdb-comparisons/cmd/query_benchmarker_influxdb

# hack to get the daemon to start up again until https://github.com/influxdata/influxdb/issues/21757 is resolved
systemctl stop influxdb
sed -i 's/User=influxdb/User=root/g' /lib/systemd/system/influxdb.service
sed -i 's/Group=influxdb/Group=root/g' /lib/systemd/system/influxdb.service
systemctl daemon-reload
systemctl unmask influxdb.service
systemctl start influxdb

# Common variables used across all tests
datestring=${TEST_COMMIT_TIME}
seed=$datestring
db_name="benchmark_db"

# Controls the cardinality of generated points. Cardinality will be scale_var * scale_var.
scale_var=1000

# How many queries to generate.
queries=500

# Lines to write per request during ingest
batch=5000
# Concurrent workers to use during ingest/query
workers=4

# How long to run each set of query tests. Specify a duration to limit the maximum amount of time the queries can run,
# since individual queries can take a long time.
duration=30s

# Helper functions containing common logic
force_compaction() {
  # stop daemon and force compaction
  systemctl stop influxdb
  set +e
  shards=$(find /var/lib/influxdb/engine/data/$db_name/autogen/ -maxdepth 1 -mindepth 1)

  set -e
  for shard in $shards; do
    if [ -n "$(find $shard -name *.tsm)" ]; then
      /home/ubuntu/influx_tools compact-shard -force -verbose -path $shard
    fi
  done

  # restart daemon
  systemctl unmask influxdb.service
  systemctl start influxdb
}

# The time range controls both the span over which data is generated, and the
# span over which queries will be performed. timestamp-start and timestamp-end
# must be provided to both the data generation and query generation commands
# and must be the same to ensure that the queries cover the data range.
start_time() {
  # All queries and datasets can start at the same time. Certain queries and
  # datasets will use case-dependent end-times.
  echo 2018-01-01T00:00:00Z
}

end_time() {
  case $1 in
    iot|window-agg|group-agg|bare-agg|group-window-transpose-low-card)
      echo 2018-01-01T12:00:00Z
      ;;
    multi-measurement|metaquery|group-window-transpose-high-card)
      echo 2019-01-01T00:00:00Z
      ;;
    *)
      echo "unknown use-case: $1"
      exit 1
      ;;
  esac
}

query_types() {
  case $1 in
    window-agg|group-agg|bare-agg|group-window-transpose-low-card|group-window-transpose-high-card)
      echo min mean max first last count sum
      ;;
    iot)
      echo fast-query-small-data standalone-filter aggregate-keep aggregate-drop sorted-pivot
      ;;
    metaquery)
      echo field-keys tag-values
      ;;
    multi-measurement)
      echo multi-measurement-or
      ;;
    *)
      echo "unknown use-case: $1"
      exit 1
      ;;
  esac
}

# Many of the query generator use-cases have aliases to make reporting more
# clear. This function will translate the aliased query use cases to their
# dataset use cases. Effectively this means "for this query use case, run the
# queries against this dataset use case".
query_usecase_alias() {
  case $1 in
    window-agg|group-agg|bare-agg|group-window-transpose|iot|group-window-transpose-low-card)
      echo iot
      ;;
    metaquery|group-window-transpose-high-card)
      echo metaquery
      ;;
    multi-measurement)
      echo multi-measurement
      ;;
    *)
      echo "unknown use-case: $1"
      exit 1
      ;;
  esac
}

##########################
## Run and record tests ##
##########################

# Generate queries to test.
query_files=""
for usecase in window-agg group-agg bare-agg group-window-transpose-low-card group-window-transpose-high-card iot metaquery multi-measurement; do
  for type in $(query_types $usecase); do
    query_fname="${TEST_FORMAT}_${usecase}_${type}"
    $GOPATH/bin/bulk_query_gen \
        -use-case=$usecase \
        -query-type=$type \
        -format=influx-${TEST_FORMAT} \
        -timestamp-start=$(start_time $usecase) \
        -timestamp-end=$(end_time $usecase) \
        -queries=$queries \
        -scale-var=$scale_var > \
      ${DATASET_DIR}/$query_fname
    query_files="$query_files $query_fname"
  done
done

# Generate and ingest bulk data. Record the time spent as an ingest test if
# specified, and run the query performance tests for each dataset.
for usecase in iot metaquery multi-measurement; do
  data_fname="influx-bulk-records-usecase-$usecase"
  $GOPATH/bin/bulk_data_gen \
      -seed=$seed \
      -use-case=$usecase \
      -scale-var=$scale_var \
      -timestamp-start=$(start_time $usecase) \
      -timestamp-end=$(end_time $usecase) > \
    ${DATASET_DIR}/$data_fname

  load_opts="-file=${DATASET_DIR}/$data_fname -batch-size=$batch -workers=$workers -urls=http://${NGINX_HOST}:8086 -do-abort-on-exist=false -do-db-create=true -backoff=1s -backoff-timeout=300m0s"
  if [ -z $INFLUXDB2 ] || [ $INFLUXDB2 = true ]; then
    load_opts="$load_opts -organization=$TEST_ORG -token=$TEST_TOKEN"
  fi

  # Run ingest tests. Only write the results to disk if this run should contribute to ingest-test results.
  out=/dev/null
  if [ "${TEST_RECORD_INGEST_RESULTS}" = true ]; then
    out=$working_dir/test-ingest-$usecase.json
  fi
  $GOPATH/bin/bulk_load_influx $load_opts | \
    jq ". += {branch: \"$INFLUXDB_VERSION\", commit: \"$TEST_COMMIT\", time: \"$datestring\", i_type: \"$DATA_I_TYPE\", use_case: \"$usecase\"}" > ${out}

  # Cleanup from the data generation and loading.
  force_compaction
  rm ${DATASET_DIR}/$data_fname
 
  # Run the query tests applicable to this dataset.
  for query_file in $query_files; do 
    format=$(echo $query_file | cut -d '_' -f1)
    query_usecase=$(echo $query_file | cut -d '_' -f2)
    type=$(echo $query_file | cut -d '_' -f3)

    # Only run the query tests for queries applicable to this dataset.
    if [ "$usecase" != "$(query_usecase_alias $query_usecase)" ]; then
      continue
    fi

    ${GOPATH}/bin/query_benchmarker_influxdb \
        -file=${DATASET_DIR}/$query_file \
        -urls=http://${NGINX_HOST}:8086 \
        -debug=0 \
        -print-interval=0 \
        -json=true \
        -workers=$workers \
        -benchmark-duration=$duration | \
      jq '."all queries"' | \
      jq -s '.[-1]' | \
      jq ". += {use_case: \"$query_usecase\", query_type: \"$type\", branch: \"$INFLUXDB_VERSION\", commit: \"$TEST_COMMIT\", time: \"$datestring\", i_type: \"$DATA_I_TYPE\", query_format: \"$format\"}" > \
        $working_dir/test-query-$format-$query_usecase-$type.json

      # Restart daemon between query tests.
      systemctl stop influxdb
      systemctl unmask influxdb.service
      systemctl start influxdb
  done

  # Delete DB to start anew.
  influx -database $db_name -execute "DROP DATABASE $db_name"
done

echo "Using Telegraph to report results from the following files:"
ls $working_dir

telegraf --debug --once
