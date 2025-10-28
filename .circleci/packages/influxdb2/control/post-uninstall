#!/bin/bash

function disable_systemd {
    systemctl disable influxdb
    rm -f /lib/systemd/system/influxdb.service
}

function disable_update_rcd {
    update-rc.d -f influxdb remove
    rm -f /etc/init.d/influxdb
}

function disable_chkconfig {
    chkconfig --del influxdb
    rm -f /etc/init.d/influxdb
}

if [[ -f /etc/redhat-release ]]; then
    # RHEL-variant logic
    if [[ "$1" = "0" ]]; then
        # InfluxDB is no longer installed, remove from init system
        rm -f /etc/default/influxdb

        if command -v systemctl &>/dev/null; then
            disable_systemd
        else
            # Assuming sysv
            disable_chkconfig
        fi
    fi
elif [[ -f /etc/lsb-release ]]; then
    # Debian/Ubuntu logic
    if [[ "$1" != "upgrade" ]]; then
        # Remove/purge
        rm -f /etc/default/influxdb

        if command -v systemctl &>/dev/null; then
            disable_systemd
        else
            # Assuming sysv
            disable_update_rcd
        fi
    fi
elif [[ -f /etc/os-release ]]; then
    source /etc/os-release
    if [[ "$ID" = "amzn" ]] && [[ "$1" = "0" ]]; then
        # InfluxDB is no longer installed, remove from init system
        rm -f /etc/default/influxdb

        if [[ "$NAME" = "Amazon Linux" ]]; then
            # Amazon Linux 2+ logic
            disable_systemd
        elif [[ "$NAME" = "Amazon Linux AMI" ]]; then
            # Amazon Linux logic
            disable_chkconfig
        fi
    fi
fi
