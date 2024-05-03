#!/bin/bash

USER=influxdb3
GROUP=influxdb3
DATA_DIR=/var/lib/influxdb3
LOG_DIR=/var/log/influxdb3

if ! id influxdb3 &>/dev/null; then
    useradd --system -U -M influxdb3 -s /bin/false -d $DATA_DIR
fi

# check if DATA_DIR exists
if [ ! -d "$DATA_DIR" ]; then
    mkdir -p $DATA_DIR
    chown $USER:$GROUP $DATA_DIR
fi

# check if LOG_DIR exists
if [ ! -d "$LOG_DIR" ]; then
    mkdir -p $LOG_DIR
    chown $USER:$GROUP $DATA_DIR
fi
