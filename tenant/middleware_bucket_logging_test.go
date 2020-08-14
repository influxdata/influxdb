package tenant_test

import (
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBucketLoggingService(t *testing.T) {
	influxdbtesting.BucketService(initInmemBucketLoggingService, t)
}

func initInmemBucketLoggingService(f influxdbtesting.BucketFields, t *testing.T) (influxdb.BucketService, string, func()) {
	svc, s, closer := initInmemBucketService(f, t)
	return tenant.NewBucketLogger(zaptest.NewLogger(t), svc), s, closer
}
