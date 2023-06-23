#!/bin/bash -e

# Configuration file
CONFIG=/etc/influxdb/influxdb.conf

# Retrieve configuration value from influxd.
function influxd_config() {
    local header="${1}"
    local target="${2}"

    while IFS= read -r line
    do
        # The TOML specification allows for key-value pairs to be namespaced in
        # collections. Therefore, it is not enough to grep for the desired key.
        # This filters out all values that are outside the target section.
        if [[ "${line}" =~ ^\[${header}\]$ ]]
        then
            local section_found=1
        fi

        # blank line signifies that the section has ended
        if [[ ! ${line} ]]
        then
            local section_found=
        fi

        if [[ ${section_found:-} ]]
        then
                # Once within the target section, search the key-value pairs for the
                # desired key. Currently, this only supports string values. Since
                # this is used only for wal-dir and data-dir, this should be okay.
                if [[ "${line}" =~ ^[[:space:]]*${target}[[:space:]]*=[[:space:]]\"(.*)\" ]]
                then
                    echo "${BASH_REMATCH[1]}" ; return
                fi
        fi
    done <<< "$(influxd config -config "${CONFIG}" ${INFLUXD_OPTS} 2>/dev/null)"
}

DATA_DIR="$( influxd_config data dir     )"
WAL_DIR="$(  influxd_config data wal-dir )"
if [[ ( -d "${DATA_DIR}" ) && ( -d "${WAL_DIR}" ) ]]
then
    # If this daemon is configured to run as root, influx_inspect hangs
    # waiting for confirmation before executing. Supplying "yes" allows
    # the service to continue without interruption.
    yes | /usr/bin/influx_inspect buildtsi -compact-series-file \
        -datadir "${DATA_DIR}"                                  \
        -waldir  "${WAL_DIR}"
fi

/usr/bin/influxd -config "${CONFIG}" ${INFLUXD_OPTS} &
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
