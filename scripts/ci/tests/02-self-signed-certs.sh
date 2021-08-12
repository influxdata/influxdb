#!/bin/bash -eux

echo 'tls-cert = "/etc/ssl/influxdb.crt"' >> /etc/influxdb/config.toml
echo 'tls-key = "/etc/ssl/influxdb.key"' >> /etc/influxdb/config.toml
openssl req -x509 -nodes -newkey rsa:2048 -keyout /etc/ssl/influxdb.key -out /etc/ssl/influxdb.crt -days 365 -subj /C=US/ST=CA/L=sanfrancisco/O=influxdata/OU=edgeteam/CN=localhost
chown influxdb:influxdb /etc/ssl/influxdb.*
service influxdb start
service influxdb stop
contents="$(head -n -2 /etc/influxdb/config.toml)"
echo "$contents" > /etc/influxdb/config.toml
