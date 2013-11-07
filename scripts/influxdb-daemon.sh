#!/usr/bin/env bash

nohup /usr/bin/influxdb "$@" >> /opt/influxdb/shared/log.txt 2>&1 &
