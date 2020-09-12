#!/bin/bash

DATA_DIR=/var/lib/influxdb
<<<<<<< HEAD
<<<<<<< HEAD
USER=influxdb
GROUP=influxdb
LOG_DIR=/var/log/influxdb
<<<<<<< HEAD
=======
>>>>>>> chore: add rpm script files
=======
USER=influxdb
GROUP=influxdb
>>>>>>> chore: update install scripts
=======
>>>>>>> chore: create log directory if it doez not exist

if ! id influxdb &>/dev/null; then
    useradd --system -U -M influxdb -s /bin/false -d $DATA_DIR
fi

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> chore: update install scripts
# check if DATA_DIR exists
if [ ! -d "$DATA_DIR" ]; then
    mkdir -p $DATA_DIR
    chown $USER:$GROUP $DATA_DIR
fi

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> chore: create log directory if it doez not exist
# check if LOG_DIR exists
if [ ! -d "$LOG_DIR" ]; then
    mkdir -p $LOG_DIR
    chown $USER:$GROUP $DATA_DIR
fi

<<<<<<< HEAD
=======
>>>>>>> chore: add rpm script files
=======
>>>>>>> chore: update install scripts
=======
>>>>>>> chore: create log directory if it doez not exist
if [[ -d /etc/opt/influxdb ]]; then
    # Legacy configuration found
    if [[ ! -d /etc/influxdb ]]; then
	# New configuration does not exist, move legacy configuration to new location
	echo -e "Please note, InfluxDB's configuration is now located at '/etc/influxdb' (previously '/etc/opt/influxdb')."
	mv -vn /etc/opt/influxdb /etc/influxdb

	if [[ -f /etc/influxdb/influxdb.conf ]]; then
	    backup_name="influxdb.conf.$(date +%s).backup"
	    echo "A backup of your current configuration can be found at: /etc/influxdb/$backup_name"
	    cp -a /etc/influxdb/influxdb.conf /etc/influxdb/$backup_name
	fi
    fi
fi
