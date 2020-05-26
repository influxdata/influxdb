package label_test

import (
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/label"
	"go.uber.org/zap"

	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestLabelMetricsService(t *testing.T) {
	influxdbtesting.LabelService(initBoltLabelMetricsService, t)
}

func initBoltLabelMetricsService(f influxdbtesting.LabelFields, t *testing.T) (influxdb.LabelService, string, func()) {
	svc, s, closer := initBoltLabelService(f, t)
	reg := prom.NewRegistry(zap.NewNop())
	return label.NewLabelMetrics(reg, svc), s, closer
}
