#!/bin/bash

# Environment defaults
INFLUXD_CONFIG_PATH=/etc/influxdb/config.toml
INFLUXD_BOLT_PATH=/var/lib/influxdb/influxd.bolt
INFLUXD_ENGINE_PATH=/var/lib/influxdb/engine

export INFLUXD_CONFIG_PATH INFLUXD_BOLT_PATH INFLUXD_ENGINE_PATH

# Check upgrade status
bolt_dir="/root/.influxdbv2 /var/lib/influxdb/.influxdbv2 /var/lib/influxdb"
for bolt in $bolt_dir
do
  if [[ -s ${bolt}/influxd.bolt ]]; then
    echo "An existing ${bolt}/influxd.bolt file was found indicating InfluxDB is"
    echo "already upgraded to v2.  Exiting."
    exit 1
  fi
done

# Perform upgrade
sudo /usr/bin/influxd upgrade \
  --config-file=/etc/influxdb/influxdb.conf \
  --v2-config-path=${INFLUXD_CONFIG_PATH} \
  -m $INFLUXD_BOLT_PATH -e $INFLUXD_ENGINE_PATH

if [[ $? -eq 0 ]]; then
cat << EOF

The upgrade completed successfully.  Execute the following to start InfluxDB:

sudo systemctl start influxdb
EOF
fi

chown influxdb:influxdb /var/lib/influxdb/influxd.bolt /var/lib/influxdb/configs
chown -R influxdb:influxdb /var/lib/influxdb/engine
