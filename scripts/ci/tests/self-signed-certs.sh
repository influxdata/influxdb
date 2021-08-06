#!/bin/bash -eux

sed -i 's/  # https-enabled = false/  https-enabled = true/' /etc/influxdb/influxdb.conf
sed -i 's|  # https-certificate = "/etc/ssl/influxdb.pem"|  https-certificate = "/etc/ssl/influxdb.crt"|' /etc/influxdb/influxdb.conf
sed -i 's|  # https-private-key = ""|  https-private-key = "/etc/ssl/influxdb.key"|' /etc/influxdb/influxdb.conf
openssl req -x509 -nodes -newkey rsa:2048 -keyout /etc/ssl/influxdb.key -out /etc/ssl/influxdb.crt -days 365 -subj /C=US/ST=CA/L=sanfrancisco/O=influxdata/OU=edgeteam/CN=localhost
chown influxdb:influxdb /etc/ssl/influxdb.*
service influxdb start
service influxdb stop
sed -i 's/  https-enabled/  # https-enabled/' /etc/influxdb/influxdb.conf
sed -i 's|  https-certificate|  # https-certificate|' /etc/influxdb/influxdb.conf
sed -i 's|  https-private-key = ""|  # https-private-key|' /etc/influxdb/influxdb.conf
