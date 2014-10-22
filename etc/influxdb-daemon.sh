#!/usr/bin/env bash

nohup /usr/bin/influxdb "$@" > /dev/null 2>&1 &
