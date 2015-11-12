#!/bin/sh

echo "Stopping the InfluxDB process..."
if which systemctl > /dev/null 2>&1; then
    systemctl stop influxdb
elif which service >/dev/null 2>&1; then
    service influxdb stop
else
    /etc/init.d/influxdb stop
fi

