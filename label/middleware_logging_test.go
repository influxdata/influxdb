package label_test

import (
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/label"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestLabelLoggingService(t *testing.T) {
	influxdbtesting.LabelService(initBoltLabelLoggingService, t)
}

func initBoltLabelLoggingService(f influxdbtesting.LabelFields, t *testing.T) (influxdb.LabelService, string, func()) {
	svc, s, closer := initBoltLabelService(f, t)
	return label.NewLabelLogger(zaptest.NewLogger(t), svc), s, closer
}
