#!/bin/bash -e

# Extract tarball
WORKDIR=/influxdata
mkdir -p $WORKDIR
tar xz -C ${WORKDIR} -f /influxdb-src.tar.gz

(
	cd ${WORKDIR}/influxdb
	go test -v ./... 2>&1 | tee /out/tests.log
	go-junit-report </out/tests.log > /out/influxdb.junit.xml
)
