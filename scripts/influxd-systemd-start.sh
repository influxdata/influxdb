#!/bin/bash -e

/usr/bin/influxd &
echo $! > /var/lib/influxdb/influxd.pid

BIND_ADDRESS=$(influxd print-config --key-name http-bind-address)
HOST=${BIND_ADDRESS%%:*}
HOST=${HOST:-"localhost"}
PORT=${BIND_ADDRESS##*:}

set +e
result=$(curl -s -o /dev/null http://$HOST:$PORT/ready -w %{http_code})
while [ "$result" != "200" ]; do
  sleep 1
  result=$(curl -s -o /dev/null http://$HOST:$PORT/ready -w %{http_code})
done
set -e
