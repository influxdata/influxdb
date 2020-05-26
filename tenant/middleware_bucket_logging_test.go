package tenant_test

import (
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBucketLoggingService(t *testing.T) {
	influxdbtesting.BucketService(initBoltBucketLoggingService, t, influxdbtesting.WithoutHooks())
}

func initBoltBucketLoggingService(f influxdbtesting.BucketFields, t *testing.T) (influxdb.BucketService, string, func()) {
	svc, s, closer := initBoltBucketService(f, t)
	return tenant.NewBucketLogger(zaptest.NewLogger(t), svc), s, closer
}
