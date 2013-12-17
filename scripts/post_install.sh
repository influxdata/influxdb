#!/usr/bin/env bash

influx_dir=/opt/influxdb
version=REPLACE_VERSION
# create some symlinks
ln -sfn $influx_dir/versions/$version $influx_dir/current
[ -e /usr/bin/influxdb ] || ln -sfn $influx_dir/current/influxdb /usr/bin/influxdb
[ -e /usr/bin/influxdb-benchmark ] || ln -sfn $influx_dir/current/influxdb-benchmark /usr/bin/influxdb-benchmark
[ -e /usr/bin/influxdb-daemon ] || ln -sfn $influx_dir/current/scripts/influxdb-daemon.sh /usr/bin/influxdb-daemon
[ -d $influx_dir/shared ] || mkdir $influx_dir/shared
[ -e $influx_dir/shared/config.toml ] || cp $influx_dir/current/config.toml $influx_dir/shared/
[ -e $influx_dir/shared/benchmark_config.toml ] || cp $influx_dir/current/benchmark_config.toml $influx_dir/shared/
touch $influx_dir/shared/log.txt
if [ ! -L /etc/init.d/influxdb ]; then
    ln -sfn $influx_dir/current/scripts/init.sh /etc/init.d/influxdb
    if which update-rc.d > /dev/null 2>&1 ; then
        update-rc.d -f influxdb remove
        update-rc.d influxdb defaults
    else
        chkconfig --add influxdb
    fi
fi
service influxdb restart
