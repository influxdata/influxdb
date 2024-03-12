#!/bin/bash

BIN_DIR=/usr/bin
DATA_DIR=/var/lib/influxdb
LOG_DIR=/var/log/influxdb
SCRIPT_DIR=/usr/lib/influxdb/scripts
LOGROTATE_DIR=/etc/logrotate.d

function install_init {
    cp -f $SCRIPT_DIR/init.sh /etc/init.d/influxdb
    chmod +x /etc/init.d/influxdb
}

function install_systemd {
    cp -f $SCRIPT_DIR/influxdb.service /lib/systemd/system/influxdb.service
    systemctl enable influxdb
}

function install_update_rcd {
    update-rc.d influxdb defaults
}

function install_chkconfig {
    chkconfig --add influxdb
}

# Add defaults file, if it doesn't exist
if [[ ! -f /etc/default/influxdb ]]; then
    touch /etc/default/influxdb
fi

# Remove legacy symlink, if it exists
if [[ -L /etc/init.d/influxdb ]]; then
    rm -f /etc/init.d/influxdb
fi

# Distribution-specific logic
if [[ -f /etc/redhat-release ]]; then
    # RHEL-variant logic
    if command -v systemctl &>/dev/null; then
        install_systemd
    else
        # Assuming sysv
        install_init
        install_chkconfig
    fi
elif [[ -f /etc/debian_version ]]; then
    # Ownership for RH-based platforms is set in build.py via the `rmp-attr` option.
    # We perform ownership change only for Debian-based systems.
    # Moving these lines out of this if statement would make `rmp -V` fail after installation.
    chown -R -L influxdb:influxdb $LOG_DIR
    chown -R -L influxdb:influxdb $DATA_DIR
    chmod 755 $LOG_DIR
    chmod 755 $DATA_DIR

    # Debian/Ubuntu logic
    if command -v systemctl &>/dev/null; then
        install_systemd
    else
        # Assuming sysv
        install_init
        install_update_rcd
    fi
elif [[ -f /etc/os-release ]]; then
    source /etc/os-release
    if [[ "$NAME" = "Amazon Linux" ]]; then
        # Amazon Linux 2+ logic
        install_systemd
    elif [[ "$NAME" = "Amazon Linux AMI" ]]; then
        # Amazon Linux logic
        install_init
        install_chkconfig
    fi
fi
