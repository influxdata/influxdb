#!/bin/sh

UUID=$(uuidgen)
UUID="$(tr [A-Z] [a-z] <<< "$UUID")"
APP_FILE=$UUID.json
cat > $APP_FILE << EOF
 {
    "id": "$UUID",
 	"telegraf_measurement": "MY_MEASUREMENT",
 	"app": "User Facing Application Name",
 	"cells": [{
 		"x": 0,
 		"y": 0,
 		"w": 10,
 		"h": 10,
 		"queries": [{
 			"query": "select used_percent from disk",
 			"db": "telegraf",
 			"rp": "autogen"
		}]
 	}]
 }
EOF
