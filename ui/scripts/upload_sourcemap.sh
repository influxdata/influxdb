#!/bin/sh

INFLUX_MINIFIED_FILE=`ls build/*.js | tail -n 1 | tr -d "\n"`
INFLUX_SOURCEMAP=`ls build/*.js.map | tail -n 1 | tr -d "\n"`

CLOCKFACE_SOURCEMAP=node_modules/@influxdata/clockface/dist/index.js.map
GIRAFFE_SOURCEMAP=node_modules/@influxdata/giraffe/dist/index.js.map

GIT_SHA=$(git rev-parse HEAD)

curl https://api.honeybadger.io/v1/source_maps \
  -F api_key=$HONEYBADGER_KEY \
  -F revision=$GIT_SHA \
  -F minified_url="*" \
  -F minified_file=@$INFLUX_MINIFIED_FILE \
  -F source_map=@$INFLUX_SOURCEMAP \
  -F *=@$CLOCKFACE_SOURCEMAP \
  -F *=@$GIRAFFE_SOURCEMAP
