package prometheus

import (
	"runtime"
	"strconv"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/prometheus/client_golang/prometheus"
)

type influxCollector struct {
	influxInfoDesc   *prometheus.Desc
	influxUptimeDesc *prometheus.Desc
	start            time.Time
}

// NewInfluxCollector returns a collector which exports influxdb process metrics.
func NewInfluxCollector(procID platform.IDGenerator, build platform.BuildInfo) prometheus.Collector {
	id := procID.ID().String()

	return &influxCollector{
		influxInfoDesc: prometheus.NewDesc(
			"influxdb_info",
			"Information about the influxdb environment.",
			nil, prometheus.Labels{
				"version":    build.Version,
				"commit":     build.Commit,
				"build_date": build.Date,
				"os":         runtime.GOOS,
				"arch":       runtime.GOARCH,
				"cpus":       strconv.Itoa(runtime.NumCPU()),
			},
		),
		influxUptimeDesc: prometheus.NewDesc(
			"influxdb_uptime_seconds",
			"influxdb process uptime in seconds",
			nil, prometheus.Labels{
				"id": id,
			},
		),
		start: time.Now(),
	}
}

// Describe returns all descriptions of the collector.
func (c *influxCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.influxInfoDesc
	ch <- c.influxUptimeDesc
}

// Collect returns the current state of all metrics of the collector.
func (c *influxCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(c.influxInfoDesc, prometheus.GaugeValue, 1)

	uptime := time.Since(c.start).Seconds()
	ch <- prometheus.MustNewConstMetric(c.influxUptimeDesc, prometheus.GaugeValue, float64(uptime))
}
