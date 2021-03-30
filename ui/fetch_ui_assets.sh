#!/bin/sh

curl -L https://github.com/influxdata/ui/releases/download/OSS-Master/build.tar.gz --output build.tar.gz
tar -xzf build.tar.gz
rm build.tar.gz
