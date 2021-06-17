#!/usr/bin/sh -ex

# Install Telegraf
wget -qO- https://repos.influxdata.com/influxdb.key | apt-key add -
echo "deb https://repos.influxdata.com/ubuntu focal stable" | tee /etc/apt/sources.list.d/influxdb.list

DEBIAN_FRONTEND=noninteractive apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y git jq telegraf awscli

root_branch="$(echo "${INFLUXDB_VERSION}" | rev | cut -d '-' -f1 | rev)"
trap "aws s3 cp /home/ubuntu/perftest_log.txt s3://perftest-logs-influxdb/oss/$root_branch/${TEST_COMMIT}-$(date +%Y%m%d%H%M%S).log" EXIT KILL

working_dir=$(mktemp -d)
mkdir -p /etc/telegraf
cat << EOF > /etc/telegraf/telegraf.conf
[[outputs.influxdb_v2]]
  urls = ["https://us-west-2-1.aws.cloud2.influxdata.com"]
  token = "${DB_TOKEN}"
  organization = "${CLOUD2_ORG}"
  bucket = "${CLOUD2_BUCKET}"

[[inputs.file]]
  files = ["$working_dir/*.json"]
  file_tag = "test_name"
  data_format = "json"
  json_strict = true
  json_string_fields = [
    "branch",
    "commit",
    "i_type",
    "time"
  ]
  json_time_key = "time"
  json_time_format = "unix"
  tag_keys = [
    "i_type",
    "branch",
    "commit"
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
  export GOPATH=/root/go/bin
  export PATH=$PATH:/usr/local/go/bin:$GOPATH
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

# Run and record tests
datestring=$(date +%s)
seed=$datestring
for scale in 50 100 500; do
  # generate bulk data
  scale_string="scalevar-$scale"
  scale_seed_string="$scale_string-seed-$seed"
  data_fname="influx-bulk-records-usecase-devops-$scale_seed_string.txt"
  $GOPATH/bin/bulk_data_gen --seed=$seed --use-case=devops --scale-var=$scale --format=influx-bulk > ${DATASET_DIR}/$data_fname

  # run ingest tests
  test_type=ingest
  for parseme in "5000:2" "5000:20" "15000:2" "15000:20"; do
    batch=$(echo $parseme | cut -d: -f1)
    workers=$(echo $parseme | cut -d: -f2)
    load_opts="-batch-size=$batch -workers=$workers -urls=http://${NGINX_HOST}:8086 -do-abort-on-exist=false -do-db-create=true -backoff=1s -backoff-timeout=300m0s"
    if [ -z $INFLUXDB2 ] || [ $INFLUXDB2 = true ]; then
      load_opts="$load_opts -organization=$TEST_ORG -token=$TEST_TOKEN"
    fi

    cat ${DATASET_DIR}/$data_fname | $GOPATH/bin/bulk_load_influx $load_opts | jq ". += {branch: \"${INFLUXDB_VERSION}\", commit: \"${TEST_COMMIT}\", time: \"$datestring\", i_type: \"${DATA_I_TYPE}\"}" > $working_dir/test-$test_type-$scale_string-batchsize-$batch-workers-$workers.json
  done
done

echo "Using Telegraph to report results from the following files:"
ls $working_dir

telegraf --debug --once

if [ "${CIRCLE_TEARDOWN}" = "true" ]; then
  curl --request POST \
    --url https://circleci.com/api/v2/project/github/influxdata/influxdb/pipeline \
    --header "Circle-Token: ${CIRCLE_TOKEN}" \
    --header 'content-type: application/json' \
    --data "{\"branch\":\"${INFLUXDB_VERSION}\", \"parameters\":{\"aws_teardown\": true, \"aws_teardown_branch\":\"${INFLUXDB_VERSION}\", \"aws_teardown_sha\":\"${TEST_COMMIT}\", \"aws_teardown_datestring\":\"${CIRCLE_TEARDOWN_DATESTRING}\"}}"
fi
