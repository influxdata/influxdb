#!/bin/bash -e
  
/usr/bin/influxd -config /etc/influxdb/influxdb.conf $INFLUXD_OPTS &
PID=$!
echo $PID > /var/lib/influxdb/influxd.pid

PROTOCOL="http"
BIND_ADDRESS=$(influxd config | grep -A5 "\[http\]" | grep '^  bind-address' | cut -d ' ' -f5 | tr -d '"')
HTTPS_ENABLED_FOUND=$(influxd config | grep "https-enabled = true" | sed 's/^ *//g' | cut -d ' ' -f3)
HTTPS_ENABLED=${HTTPS_ENABLED_FOUND:-"false"}
if [ $HTTPS_ENABLED = "true" ]; then
  HTTPS_CERT=$(influxd config | grep "https-certificate" | sed 's/^ *//g' | cut -d ' ' -f3 | tr -d '"')
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
max_attempts=10
url="$PROTOCOL://$HOST:$PORT/health"
result=$(curl -k -s -o /dev/null $url -w %{http_code})
while [ "$result" != "200" ]; do
  sleep 1
  result=$(curl -k -s -o /dev/null $url -w %{http_code})
  max_attempts=$(($max_attempts-1))
  if [ $max_attempts -le 0 ]; then
    echo "Failed to reach influxdb $PROTOCOL endpoint at $url"
    exit 1
  fi
done
set -e

