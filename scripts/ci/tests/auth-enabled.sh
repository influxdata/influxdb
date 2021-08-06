#!/bin/bash -eux

service influxdb start
sed -i 's/  # auth-enabled = false/  auth-enabled = true/' /etc/influxdb/influxdb.conf
sed -i 's/  # ping-auth-enabled = false/  ping-auth-enabled = true/' /etc/influxdb/influxdb.conf
influx -execute "CREATE USER admin WITH PASSWORD 'thisisnotarealpassword' WITH ALL PRIVILEGES"
service influxdb stop
service influxdb start
service influxdb stop
sed -i 's/  auth-enabled/  # auth-enabled/' /etc/influxdb/influxdb.conf
sed -i 's/  ping-auth-enabled/  # ping-auth-enabled/' /etc/influxdb/influxdb.conf
