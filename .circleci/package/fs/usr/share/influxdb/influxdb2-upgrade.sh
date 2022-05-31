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
  --log-path=/var/log/influxdb/upgrade.log \
  --continuous-query-export-path=/var/lib/influxdb/continuous_queries.txt \
  --v2-config-path=${INFLUXD_CONFIG_PATH} \
  -m $INFLUXD_BOLT_PATH -e $INFLUXD_ENGINE_PATH

if [[ $? -eq 0 ]]; then

cat << EOF

The upgrade completed successfully.  Execute the following to start InfluxDB:

sudo systemctl start influxdb

A complete copy of v1 data was created as part of the upgrade process.
After confirming v2 is functioning as intended, removal of v1 data can be
performed by executing:

sudo /var/tmp/influxdbv1-remove.sh

NOTE: This script will erase all previous v1 data and config files!

EOF

cat << EOF > /var/tmp/influxdbv1-remove.sh
#!/bin/bash
sudo rm -f /etc/influxdb/influxdb.conf
sudo rm -rf /var/lib/influxdb/data
sudo rm -rf /var/lib/influxdb/wal
sudo rm -rf /var/lib/influxdb/meta
EOF
sudo chmod +x /var/tmp/influxdbv1-remove.sh

sudo cp /root/.influxdbv2/configs /var/lib/influxdb
sudo chown influxdb:influxdb /var/lib/influxdb/influxd.bolt /var/lib/influxdb/configs
sudo chown -R influxdb:influxdb /var/lib/influxdb/engine

else

cat << EOF

The upgrade encountered an error.  Please review the
/var/log/influxdb/upgrade.log file for more information.  Before attempting
another upgrade, removal of v2 data should be performed by executing:

sudo /var/tmp/influxdbv2-remove.sh

NOTE: This script will erase all previous v2 data and config files!

EOF

cat << EOF > /var/tmp/influxdbv2-remove.sh
#!/bin/bash
sudo rm -f /etc/influxdb/config.toml /var/lib/influxdb/influxd.bolt
sudo rm -f /var/lib/influxdb/configs /root/.influxdbv2/configs
sudo rm -f /var/lib/influxdb/continuous_queries.txt
sudo rm -rf /var/lib/influxdb/engine
EOF
sudo chmod +x /var/tmp/influxdbv2-remove.sh

fi
