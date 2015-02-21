# go get -u github.com/kisielk/errcheck
errcheck:
	@errcheck github.com/influxdb/influxdb \
		github.com/influxdb/influxdb/admin \
		github.com/influxdb/influxdb/client \
		github.com/influxdb/influxdb/collectd \
		github.com/influxdb/influxdb/graphite \
		github.com/influxdb/influxdb/httpd \
		github.com/influxdb/influxdb/influxql \
		github.com/influxdb/influxdb/messaging \
		github.com/influxdb/influxdb/raft
