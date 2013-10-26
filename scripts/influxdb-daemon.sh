#!/usr/bin/env bash

nohup /usr/bin/influxdb > /data/anomalous-agent/shared/log.txt 2>&1 &
