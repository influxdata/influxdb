#!/bin/bash

BIN_DIR=/usr/bin
DATA_DIR=/var/lib/influxdb
LOG_DIR=/var/log/influxdb
SCRIPT_DIR=/usr/lib/influxdb/scripts
LOGROTATE_DIR=/etc/logrotate.d
INFLUXD_CONFIG_PATH=/etc/influxdb/config.toml

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

function should_upgrade {
    if [[ ! -s /etc/influxdb/influxdb.conf ]]; then
        # No V1 config present, no upgrade needed.
        return 1
    fi

    bolt_dir="/root/.influxdbv2 /var/lib/influxdb/.influxdbv2 /var/lib/influxdb"
    for bolt in $bolt_dir; do
        if [[ -s ${bolt}/influxd.bolt ]]; then
            # Found a bolt file, assume previous v2 upgrade.
            return 1
        fi
    done

    return 0
}

function upgrade_notice {
cat << EOF

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
! Important 1.x to 2.x Upgrade Notice !
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

Thank you for installing InfluxDB v2.  Due to significant changes between
the v1 and v2 versions, upgrading to v2 requires additional steps.  If
upgrading to v2 was not intended, simply re-install the v1 package now.

An upgrade helper script is available that should be reviewed and executed
prior to starting the influxdb systemd service.  In order to start the v2
upgrade, execute the following:

sudo /usr/share/influxdb/influxdb2-upgrade.sh

Visit our website for complete details on the v1 to v2 upgrade process:
https://docs.influxdata.com/influxdb/latest/upgrade/v1-to-v2/

For new or upgrade installations, please review the getting started guide:
https://docs.influxdata.com/influxdb/latest/get-started/

EOF
}

function init_config {
    mkdir -p $(dirname ${INFLUXD_CONFIG_PATH})

    local config_path=${INFLUXD_CONFIG_PATH}
    if [[ -s ${config_path} ]]; then
        config_path=${INFLUXD_CONFIG_PATH}.defaults
        echo "Config file ${INFLUXD_CONFIG_PATH} already exists, writing defaults to ${config_path}"
    fi

    cat << EOF > ${config_path}
bolt-path = "/var/lib/influxdb/influxd.bolt"
engine-path = "/var/lib/influxdb/engine"
EOF
}

# Add defaults file, if it doesn't exist
if [[ ! -s /etc/default/influxdb2 ]]; then
cat << EOF > /etc/default/influxdb2
INFLUXD_CONFIG_PATH=${INFLUXD_CONFIG_PATH}
EOF
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

# Check upgrade status
if should_upgrade; then
    upgrade_notice
else
    init_config
fi
