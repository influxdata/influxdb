#!/bin/sh

measurement=

# Usage info
show_help() {

cat << EOF
Usage: ${0##*/} MEASUREMENT
Generate new layout for MEASUREMENT.  File created will be named
MEASUREMENT.json with UUID being generated from the uuidgen command.

    -h          display this help and exit
EOF
}

while :; do
    case $1 in
        -h|-\?|--help)   # Call a "show_help" function to display a synopsis, then exit.
            show_help
            exit
            ;;
        *)               # Default case: If no more options then break out of the loop.
			measurement=$1
            break
    esac
    shift
done

if [ -z "$measurement" ]; then
    show_help
	exit
fi

CELLID=$(uuidgen | tr A-Z a-z)
UUID=$(uuidgen | tr A-Z a-z)
APP_FILE="$measurement".json
echo Creating measurement file $APP_FILE
cat > $APP_FILE << EOF
{
	"id": "$UUID",
	"measurement": "$measurement",
	"app": "$measurement",
			"cells": [{
		"x": 0,
		"y": 0,
		"w": 4,
		"h": 4,
		"i": "$CELLID",
		"name": "User facing cell Name",
		"queries": [{
			"query": "select mean(\"used_percent\") from disk",
			"groupbys": [],
			"wheres": []
				}]
			}]
}
EOF
