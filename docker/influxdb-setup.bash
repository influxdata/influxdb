#!/bin/bash

# script used for docker-compose to initialize InfluxDB OSS instance
if [ -z "$INFLUXDB_USER" ] ; then
  echo "INFLUXDB_USER env variable is not set. Please set it before running this script. Aborting setup."
  exit 1
fi

if [ -z "$INFLUXDB_PASSWORD" ] ; then
  echo "INFLUXDB_PASSWORD env variable is not set. Please set it before running this script. Aborting setup."
  exit 1
fi

if [ -z "$INFLUXDB_ORG" ] ; then
  echo "INFLUXDB_ORG env variable is not set. Please set it before running this script. Aborting setup."
  exit 1
fi

if [ -z "$INFLUXDB_BUCKET" ] ; then
  echo "INFLUXDB_BUCKET env variable is not set. Please set it before running this script. Aborting setup."
  exit 1
fi

set -eu -o pipefail

echo "Sleeping for 10 seconds to avoid connectivity issues..."
sleep 10

echo "Setting up InfluxDB with the following parameters:"
echo "Username: $INFLUXDB_USER"
echo "Password: <redacted>"
echo "Organization: $INFLUXDB_ORG"
echo "Bucket: $INFLUXDB_BUCKET"

echo "Initializing InfluxDB..."
influx setup \
  --force \
  --host "http://influxdb:9999" \
  --username "$INFLUXDB_USER" \
  --password "$INFLUXDB_PASSWORD" \
  --org "$INFLUXDB_ORG" \
  --bucket "$INFLUXDB_BUCKET" \
  --retention 14400s

if [ -f "$HOME/.influxdbv2/configs" ] ; then
  # find the line with token value, which is specified in format of:
  # token = "(token)"
  # and remove the token = as well as any quotes and spaces from the result
  INFLUXDB_TOKEN=$(
    grep -E '^[[:space:]]*token[[:space:]]*=' "$HOME/.influxdbv2/configs" | \
    sed -E 's,^[[:space:]]*token[[:space:]]*=[[:space:]],,;s,",,g;s,[[:space:]]+,,' \
    )
fi

echo "Successfully initialized InfluxDB..."
echo "Open your browser and log in at http://localhost:9999"

exit 0