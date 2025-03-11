#!just

test_data := env("LOADTEST_DATA_DIR", "./test-data")
cargo_run_profile := env("LOADTEST_CARGO_RUN_PROFILE", "quick-release")
export TRACES_EXPORTER := env("TRACES_EXPORTER", "none")

[doc('Run a compactor, writer, and load generator instance in parallel.')]
[group('workloads')]
compaction-workload testid wcount:
  #!/usr/bin/env bash
  set -euxo pipefail

  if [ "${TRACES_EXPORTER}x" == "jaegerx" ] ;then
    just run-jaeger || true
    # If using Jaeger < 2.0, then use port 14268
    export TRACES_EXPORTER_JAEGER_AGENT_PORT="${TRACES_EXPORTER_JAEGER_AGENT_PORT:-6831}"
    export TRACES_EXPORTER_JAEGER_SERVICE_NAME="${TRACES_EXPORTER_JAEGER_SERVICE_NAME:-influxdb3-loadtest}"
  fi

  pueue kill -g {{testid}} || true
  pueue group add {{testid}} --parallel 5 || true
  cargo build --profile {{cargo_run_profile}} \
    --no-default-features \
    -F no_license \
    -F aws \
    -p influxdb3
  cargo build --profile {{cargo_run_profile}} -p influxdb3_load_generator
  mkdir -p {{test_data}}/logs
  pueue add -g {{testid}} just run-writer writer {{test_data}}/{{testid}}
  pueue add -g {{testid}} just run-compactor writer {{test_data}}/{{testid}}
  pueue add -g {{testid}} just with-retries run-load-generator {{wcount}}

[doc('Run a compaction workload with periodic compactor restarts.')]
[group('workloads')]
restarting-compaction-workload testid wcount:
  #!/usr/bin/env bash
  set -euxo pipefail

  if [ "${TRACES_EXPORTER}x" == "jaegerx" ] ;then
    just run-jaeger || true
    # If using Jaeger < 2.0, then use port 14268
    export TRACES_EXPORTER_JAEGER_AGENT_PORT="${TRACES_EXPORTER_JAEGER_AGENT_PORT:-6831}"
    export TRACES_EXPORTER_JAEGER_SERVICE_NAME="${TRACES_EXPORTER_JAEGER_SERVICE_NAME:-influxdb3-loadtest}"
  fi

  pueue kill -g {{testid}} || true
  pueue group add {{testid}} --parallel 5 || true
  cargo build --profile {{cargo_run_profile}} \
    --no-default-features \
    -F no_license \
    -F aws \
    -p influxdb3
  cargo build --profile {{cargo_run_profile}} -p influxdb3_load_generator
  mkdir -p {{test_data}}/logs
  pueue add -g {{testid}} just run-writer writer {{test_data}}/{{testid}}
  pueue add -g {{testid}} just with-restarts 1200 run-compactor writer {{test_data}}/{{testid}}
  pueue add -g {{testid}} just with-retries run-load-generator {{wcount}}

[doc('Run a single influxdb3 instance in "ingest" mode.')]
[group('components')]
run-writer node_id data_dir:
  #!/usr/bin/env bash
  set -euxo pipefail

  export INFLUXDB3_NODE_IDENTIFIER_PREFIX={{node_id}}
  export INFLUXDB3_GEN1_DURATION=${INFLUXDB3_GEN1_DURATION:-1m}
  export INFLUXDB3_WAL_SNAPSHOT_SIZE=${INFLUXDB3_WAL_SNAPSHOT_SIZE:-10}
  export INFLUXDB3_NUM_WAL_FILES_TO_KEEP=${INFLUXDB3_NUM_WAL_FILES_TO_KEEP:-5}

  if [ -z ${LOADTEST_USE_MINIO+x} ] ;then
    export INFLUXDB3_OBJECT_STORE=file
    export INFLUXDB3_DB_DIR=${INFLUXDB3_DB_DIR:-{{data_dir}}}
  else
    export INFLUXDB3_OBJECT_STORE=s3
    export INFLUXDB3_BUCKET="influxdb3"
    export INFLUXDB3_DEFAULT_REGION=us-east-1
    export AWS_ENDPOINT=http://localhost:9061
    export AWS_ALLOW_HTTP=true
    export AWS_ACCESS_KEY_ID=minioadmin
    export AWS_SECRET_ACCESS_KEY=minioadmin
  fi

  cargo run -p influxdb3 \
    -F no_license \
    -F aws \
    --no-default-features \
    --profile {{cargo_run_profile}} \
    -- serve \
      --http-bind 127.0.0.1:8756 \
      --disable-telemetry-upload \
      --mode ingest \
      2>&1 > {{test_data}}/logs/writer.log

[doc('Run a single influxdb3 instance in "compact" mode.')]
[group('components')]
run-compactor node_id data_dir:
  #!/usr/bin/env bash
  set -euxo pipefail

  export INFLUXDB3_NODE_IDENTIFIER_PREFIX={{node_id}}
  export INFLUXDB3_ENTERPRISE_COMPACTION_GEN2_DURATION=${INFLUXDB3_ENTERPRISE_COMPACTION_GEN2_DURATION:-1m}
  export INFLUXDB3_ENTERPRISE_COMPACTION_MULTIPLIERS=${INFLUXDB3_ENTERPRISE_COMPACTION_MULTIPLIERS:-1,1,1,1}
  export INFLUXDB3_ENTERPRISE_COMPACTION_CLEANUP_WAIT=1m

  export LOG_FILTER="debug,reqwest=info,object_store=off,hyper_util=info,hyper::proto::h1=info,h2=info,datafusion_optimizer=info,influxdb3_wal=info,iox_query=info,datafusion=info"

  if [ -z ${LOADTEST_USE_MINIO+x} ] ;then
    export INFLUXDB3_OBJECT_STORE=file
    export INFLUXDB3_DB_DIR=${INFLUXDB3_DB_DIR:-{{data_dir}}}
  else
    export INFLUXDB3_OBJECT_STORE=s3
    export INFLUXDB3_BUCKET="influxdb3"
    export INFLUXDB3_DEFAULT_REGION=us-east-1
    export AWS_ENDPOINT=http://localhost:9061
    export AWS_ALLOW_HTTP=true
    export AWS_ACCESS_KEY_ID=minioadmin
    export AWS_SECRET_ACCESS_KEY=minioadmin
  fi

  cargo run -p influxdb3 \
    -F no_license \
    -F aws \
    --no-default-features \
    --profile {{cargo_run_profile}} \
    -- serve \
      --http-bind 127.0.0.1:8757 \
      --disable-telemetry-upload \
      --mode compact \
      --compact-from-node-ids {{node_id}} \
      --compactor-id {{node_id}}-compactor-id \
      --run-compactions \
      2>&1 >> {{test_data}}/logs/compactor.log

[doc('Run a single influxdb3_load_generator instance with the specified db name and writer count.')]
[group('components')]
run-load-generator wcount:
  #!/usr/bin/env bash
  set -euxo pipefail

  export INFLUXDB3_LOAD_WRITER_COUNT={{wcount}}
  export INFLUXDB3_DATABASE_NAME=${INFLUXDB3_DATABASE_NAME:-testdb}
  export INFLUXDB3_LOAD_BUILTIN_SPEC=${INFLUXDB3_LOAD_BUILTIN_SPEC:-one_mil}

  cargo run -p influxdb3_load_generator \
    --profile {{cargo_run_profile}} \
    -- write \
      --host http://127.0.0.1:8756 \
      2>&1 > {{test_data}}/logs/load-generator.log


[doc('Run minio in a container.')]
[group('dependencies')]
minio testid:
  #!/usr/bin/env bash
  set -euxo pipefail

  docker volume create influxdb3-minio-config

  docker run \
    -d \
    --name influxdb3-minio \
    --volume {{test_data}}/{{testid}}:/data \
    -p 9061:9000 \
    -p 9099:9090 \
    quay.io/minio/minio:latest \
      server /data --console-address :9090 || true

  sleep 5

  docker run \
    -it \
    --rm \
    --network host \
    --mount type=volume,src=influxdb3-minio-config,dst=/root/.mc \
    quay.io/minio/mc:latest \
      config host add myminio http://localhost:9061 minioadmin minioadmin

  docker run \
    -it \
    --rm \
    --network host \
    --mount type=volume,src=influxdb3-minio-config,dst=/root/.mc \
    quay.io/minio/mc:latest \
      mb -p myminio/influxdb3

[doc('Run Jaeger in a docker container.')]
[group('utility')]
run-jaeger:
  docker run -d \
    --name jaeger \
    -p 14268:14268 \
    -p 6831:6831/udp \
    -p 16686:16686 \
    jaegertracing/jaeger:2.3.0 || true

[doc('Kill all running processes.')]
[group('utility')]
kill testid="default":
  pueue kill -g {{testid}}

[doc('Archive the contents of {{test_data}}/{{testid}}.')]
[group('utility')]
archive testid="default":
  tar -czf {{test_data}}/{{testid}}.tar.gz -C {{test_data}} {{testid}} logs

[doc('Kill all processes, clean up the pueue status list, remove all test data for {{testid}}.')]
[group('utility')]
clean testid="default":
  pueue kill -g {{testid}} || true
  pueue clean -g {{testid}} || true
  pueue group remove {{testid}} || true
  docker run \
    -v {{test_data}}:/test-data \
    -it \
    busybox:stable-musl \
      rm -rf /test-data/{{testid}}

[doc('Kill all processes, clean up the pueue status list, remove all test data and logs.')]
[group('utility')]
clean-all:
  pueue reset -f
  # NOTE: the following doesn't work -- seems like the busybox container
  # doesn't use a shell with glob expansion when running commands
  docker run \
    -v {{test_data}}:/test-data \
    -it \
    busybox:stable-musl \
      rm -rf /test-data/*

[doc('Kill and remove minio container')]
[group('utility')]
kill-minio:
  docker kill influxdb3-minio && docker rm influxdb3-minio || true
  sleep 1
  docker volume rm influxdb3-minio-config || true

[doc('List all tasks in a better sort order than the default --list')]
[group('utility')]
list:
  just --list --unsorted

[private]
with-retries *JUSTARGS:
  #!/usr/bin/env bash
  set -x

  while ! just {{JUSTARGS}} || true
  do
    echo "failed to start load-generator, retrying in 1 second"
    sleep 1
  done

[doc('Run a single influxdb3_load_generator instance with the specified db name and writer count.')]
[group('utility')]
[private]
with-restarts restart_interval task *args:
  #!/usr/bin/env bash
  set -x
  set -m

  #kill -9 -$(ps -o pgid= $PID | grep -o '[0-9]*')
    #( sleep 300 ; kill -9 "-$(ps -o pgid= ${q_pid} | grep -o '[0-9]*')" ) & s_pid="${!}"
  while true; do
    setpgid just {{task}} {{args}} & q_pid="${!}"
    ( sleep {{restart_interval}} ; pkill -9 -g ${q_pid} ) & s_pid="${!}"
    wait "${q_pid}"
    kill "${s_pid}"
    wait "${s_pid}"
  done
