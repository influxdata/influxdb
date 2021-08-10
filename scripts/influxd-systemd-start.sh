#!/bin/bash -e

/usr/bin/influxd -config /etc/influxdb/influxdb.conf $INFLUXD_OPTS &
PID=$!
echo $PID > /var/lib/influxdb/influxd.pid

PROTOCOL="http"
BIND_ADDRESS=$(influxd config | grep -A5 "\[http\]" | grep '^  bind-address' | cut -d ' ' -f5 | tr -d '"')
HTTPS_ENABLED_FOUND=$(influxd config | grep "https-enabled = true" | cut -d ' ' -f5)
HTTPS_ENABLED=${HTTPS_ENABLED_FOUND:-"false"}
if [ $HTTPS_ENABLED = "true" ]; then
  HTTPS_CERT=$(influxd config | grep "https-certificate" | cut -d ' ' -f5 | tr -d '"')
  if [ ! -f "${HTTPS_CERT}" ]; then
    echo "${HTTPS_CERT} not found! Exiting..."
    exit 1
  fi
  echo "$HTTPS_CERT found"
  PROTOCOL="https"
fi
HOST=${BIND_ADDRESS%%:*}
HOST=${HOST:-"localhost"}
PORT=${BIND_ADDRESS##*:}

set +e
attempts=0
url="$PROTOCOL://$HOST:$PORT/health"
result=$(curl -k -s -o /dev/null $url -w %{http_code})
while [ "${result:0:2}" != "20" ] && [ "${result:0:2}" != "40" ]; do
  attempts=$(($attempts+1))
  echo "InfluxDB API unavailable after $attempts attempts..."
  sleep 1
  result=$(curl -k -s -o /dev/null $url -w %{http_code})
done
echo "InfluxDB started"
set -e
