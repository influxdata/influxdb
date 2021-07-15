#!/bin/bash

set -euxo pipefail

cargo test -p write_buffer kafka -- --nocapture
cargo test -p influxdb_iox --test end_to_end write_buffer -- --nocapture
