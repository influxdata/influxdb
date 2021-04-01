package prometheus

import (
	"bytes"
	"sort"
	"testing"

	"github.com/influxdata/influxdb/models"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validMetrics = `# HELP cadvisor_version_info A metric with a constant '1' value labeled by kernel version, OS version, docker version, cadvisor version & cadvisor revision.
# TYPE cadvisor_version_info gauge
cadvisor_version_info{cadvisorRevision="",cadvisorVersion="",dockerVersion="1.8.2",kernelVersion="3.10.0-229.20.1.el7.x86_64",osVersion="CentOS Linux 7 (Core)"} 1
# HELP get_token_fail_count Counter of failed Token() requests to the alternate token source
# TYPE get_token_fail_count counter
get_token_fail_count 0
# HELP http_request_duration_microseconds The HTTP request latencies in microseconds.
# TYPE http_request_duration_microseconds summary
http_request_duration_microseconds{handler="prometheus",quantile="0.5"} 552048.506
http_request_duration_microseconds{handler="prometheus",quantile="0.9"} 5.876804288e+06
http_request_duration_microseconds{handler="prometheus",quantile="0.99"} 5.876804288e+06
http_request_duration_microseconds_sum{handler="prometheus"} 1.8909097205e+07
http_request_duration_microseconds_count{handler="prometheus"} 9
# HELP apiserver_request_latencies Response latency distribution in microseconds for each verb, resource and client.
# TYPE apiserver_request_latencies histogram
apiserver_request_latencies_bucket{resource="bindings",verb="POST",le="125000"} 1994
apiserver_request_latencies_bucket{resource="bindings",verb="POST",le="250000"} 1997
apiserver_request_latencies_bucket{resource="bindings",verb="POST",le="500000"} 2000
apiserver_request_latencies_bucket{resource="bindings",verb="POST",le="1e+06"} 2005
apiserver_request_latencies_bucket{resource="bindings",verb="POST",le="2e+06"} 2012
apiserver_request_latencies_bucket{resource="bindings",verb="POST",le="4e+06"} 2017
apiserver_request_latencies_bucket{resource="bindings",verb="POST",le="8e+06"} 2024
apiserver_request_latencies_bucket{resource="bindings",verb="POST",le="+Inf"} 2025
apiserver_request_latencies_sum{resource="bindings",verb="POST"} 1.02726334e+08
apiserver_request_latencies_count{resource="bindings",verb="POST"} 2025
`

func TestParseValidPrometheus(t *testing.T) {

	// Build a sorted list of metric families
	var parser expfmt.TextParser
	metricFamiliesMap, err := parser.TextToMetricFamilies(bytes.NewBufferString(validMetrics))
	metricFamilies := make([]*io_prometheus_client.MetricFamily, 0, len(metricFamiliesMap))
	require.NoError(t, err)
	metricFamiliesNames := make([]string, 0, len(metricFamiliesMap))
	for k, _ := range metricFamiliesMap {
		metricFamiliesNames = append(metricFamiliesNames, k)
	}
	sort.Strings(metricFamiliesNames)
	for _, name := range metricFamiliesNames {
		metricFamilies = append(metricFamilies, metricFamiliesMap[name])
		i := len(metricFamilies) - 1
		assert.NotNil(t, metricFamilies[i])
		newName := name
		metricFamilies[i].Name = &newName
	}

	// Actual test - parse the list into statistics
	statistics := PrometheusToStatistics(metricFamilies, map[string]string{"testtag": "test_value"})
	expected := []models.Statistic{
		{
			Name:   "apiserver_request_latencies",
			Tags:   map[string]string{"testtag": "test_value"},
			Values: map[string]interface{}{"+Inf": 2025., "125000": 1994., "1e+06": 2005., "250000": 1997., "2e+06": 2012., "4e+06": 2017., "500000": 2000., "8e+06": 2024., "count": 2025., "sum": 1.02726334e+08},
		},
		{
			Name:   "cadvisor_version_info",
			Tags:   map[string]string{"testtag": "test_value"},
			Values: map[string]interface{}{"gauge": 1.},
		},
		{
			Name:   "get_token_fail_count",
			Tags:   map[string]string{"testtag": "test_value"},
			Values: map[string]interface{}{"counter": 0.},
		},
		{
			Name:   "http_request_duration_microseconds",
			Tags:   map[string]string{"testtag": "test_value"},
			Values: map[string]interface{}{"0.5": 552048.506, "0.9": 5.876804288e+06, "0.99": 5.876804288e+06, "count": 9., "sum": 1.8909097205e+07},
		},
	}
	assert.Equal(t, expected, statistics)
}
