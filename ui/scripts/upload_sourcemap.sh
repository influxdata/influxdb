#!/bin/sh

FILE=`ls build/*.js.map | tail -n 1 | tr -d "\n"`
MIN_FILE=`ls build/*.js | tail -n 1 | tr -d "\n"`
GIT_SHA=$(git rev-parse HEAD)

curl https://api.honeybadger.io/v1/source_maps \
  -F api_key=$HONEYBADGER_KEY \
  -F revision=$GIT_SHA \
  -F minified_url="*" \
  -F source_map=@$FILE \
  -F minified_file=@$MIN_FILE
