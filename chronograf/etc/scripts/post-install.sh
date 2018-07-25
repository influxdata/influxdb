#!/bin/bash

BIN_DIR=/usr/bin
DATA_DIR=/var/lib/chronograf
LOG_DIR=/var/log/chronograf
SCRIPT_DIR=/usr/lib/chronograf/scripts
LOGROTATE_DIR=/etc/logrotate.d

function install_init {
    cp -f $SCRIPT_DIR/init.sh /etc/init.d/chronograf
    chmod +x /etc/init.d/chronograf
}

function install_systemd {
    # Remove any existing symlinks
    rm -f /etc/systemd/system/chronograf.service

    cp -f $SCRIPT_DIR/chronograf.service /lib/systemd/system/chronograf.service
    systemctl enable chronograf || true
    systemctl daemon-reload || true
}

function install_update_rcd {
    update-rc.d chronograf defaults
}

function install_chkconfig {
    chkconfig --add chronograf
}

id chronograf &>/dev/null
if [[ $? -ne 0 ]]; then
    useradd --system -U -M chronograf -s /bin/false -d $DATA_DIR
fi

test -d $LOG_DIR || mkdir -p $DATA_DIR
test -d $DATA_DIR || mkdir -p $DATA_DIR
chown -R -L chronograf:chronograf $LOG_DIR
chown -R -L chronograf:chronograf $DATA_DIR
chmod 755 $LOG_DIR
chmod 755 $DATA_DIR

# Remove legacy symlink, if it exists
if [[ -L /etc/init.d/chronograf ]]; then
    rm -f /etc/init.d/chronograf
fi

# Add defaults file, if it doesn't exist
if [[ ! -f /etc/default/chronograf ]]; then
    touch /etc/default/chronograf
fi

# Distribution-specific logic
if [[ -f /etc/redhat-release ]]; then
    # RHEL-variant logic
    which systemctl &>/dev/null
    if [[ $? -eq 0 ]]; then
    	install_systemd
    else
    	# Assuming sysv
    	install_init
    	install_chkconfig
    fi
elif [[ -f /etc/debian_version ]]; then
    # Debian/Ubuntu logic
    which systemctl &>/dev/null
    if [[ $? -eq 0 ]]; then
    	install_systemd
        systemctl restart chronograf || echo "WARNING: systemd not running."
    else
    	# Assuming sysv
    	install_init
    	install_update_rcd
        invoke-rc.d chronograf restart
    fi
elif [[ -f /etc/os-release ]]; then
    source /etc/os-release
    if [[ $ID = "amzn" ]]; then
    	# Amazon Linux logic
    	install_init
    	install_chkconfig
    fi
fi
