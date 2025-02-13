#!just

test_data := env("LOADTEST_DATA_DIR", "./test-data")
cargo_run_profile := env("LOADTEST_CARGO_RUN_PROFILE", "quick-release")

[doc('Run a compactor, writer, and load generator instance in parallel.')]
[group('workloads')]
compaction-workload testid wcount:
  pueue kill -g {{testid}} || true
  pueue group add {{testid}} --parallel 5 || true
  cargo build --profile {{cargo_run_profile}} -F no_license -p influxdb3
  cargo build --profile {{cargo_run_profile}} -p influxdb3_load_generator
  mkdir -p {{test_data}}/logs
  pueue add -g {{testid}} just run-writer writer {{test_data}}/{{testid}}
  pueue add -g {{testid}} just run-compactor compactor {{test_data}}/{{testid}}
  pueue add -g {{testid}} just with-retries run-load-generator {{wcount}}

# NOTE: after https://github.com/influxdata/influxdb_pro/issues/299 is
# implemented, we should chane this to be `--mode=write`
[doc('Run a single influxdb3 instance in read_write mode.')]
[group('components')]
run-writer node_id data_dir:
  #!/usr/bin/env bash
  set -euxo pipefail

  export INFLUXDB3_DB_DIR=${INFLUXDB3_DB_DIR:-{{data_dir}}}
  export INFLUXDB3_NODE_IDENTIFIER_PREFIX={{node_id}}
  export INFLUXDB3_GEN1_DURATION=${INFLUXDB3_GEN1_DURATION:-1m}
  export INFLUXDB3_WAL_SNAPSHOT_SIZE=${INFLUXDB3_WAL_SNAPSHOT_SIZE:-10}
  export INFLUXDB3_NUM_WAL_FILES_TO_KEEP=${INFLUXDB3_NUM_WAL_FILES_TO_KEEP:-5}

  cargo run -p influxdb3 \
    -F no_license \
    --profile {{cargo_run_profile}} \
    -- serve \
      --http-bind 127.0.0.1:8756 \
      --disable-telemetry-upload \
      --mode read_write \
      --object-store=file \
      2>&1 > {{test_data}}/logs/writer.log

[doc('Run a single influxdb3 instance in compactor mode.')]
[group('components')]
run-compactor node_id data_dir:
  #!/usr/bin/env bash
  set -euxo pipefail

  export INFLUXDB3_DB_DIR=${INFLUXDB3_DB_DIR:-{{data_dir}}}
  export INFLUXDB3_NODE_IDENTIFIER_PREFIX={{node_id}}
  export INFLUXDB3_ENTERPRISE_COMPACTION_GEN2_DURATION=${INFLUXDB3_ENTERPRISE_COMPACTION_GEN2_DURATION:-1m}
  export INFLUXDB3_ENTERPRISE_COMPACTION_MULTIPLIERS=${INFLUXDB3_ENTERPRISE_COMPACTION_MULTIPLIERS:-1,1,1,1}

  cargo run -p influxdb3 \
    -F no_license \
    --profile {{cargo_run_profile}} \
    -- serve \
      --http-bind 127.0.0.1:8757 \
      --disable-telemetry-upload \
      --mode compactor \
      --object-store=file \
      --compact-from-node-ids {{node_id}} \
      --compactor-id {{node_id}}-compactor-id \
      --run-compactions \
      2>&1 > {{test_data}}/logs/compactor.log

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
  pueue kill -g {{testid}}
  pueue clean -g {{testid}}
  pueue group remove {{testid}}
  rm -rf {{test_data}}/{{testid}}

[doc('Kill all processes, clean up the pueue status list, remove all test data and logs.')]
[group('utility')]
clean-all:
  pueue reset -f
  rm -rf {{test_data}}/*

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
