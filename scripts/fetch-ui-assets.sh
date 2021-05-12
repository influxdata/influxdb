#!/bin/sh

# This script is used to download built UI assets from the "influxdata/ui"
# repository. The built UI assets are attached to a release in "influxdata/ui",
# which is linked here.
# 
# The master branch of "influxdata/influxdb" (this repository) downloads from the
# release tagged as "OSS-Master" in "influxdata/ui". That release is kept up-to-date
# with the most recent changes in "influxdata/ui". 
# 
# Feature branches of "influxdata/influxdb" (2.0, 2.1, etc) download from their
# respective releases in "influxdata/ui" (OSS-2.0, OSS-2.1, etc). Those releases
# are updated only when a bug fix needs included for the UI of that OSS release.

curl -L https://github.com/influxdata/ui/releases/download/OSS-Master/build.tar.gz --output ui/build.tar.gz
tar -xzf ui/build.tar.gz -C ui
rm ui/build.tar.gz
