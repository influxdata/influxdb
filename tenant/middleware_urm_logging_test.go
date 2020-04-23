package tenant_test

import (
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestUserResourceMappingLoggingService(t *testing.T) {
	influxdbtesting.UserResourceMappingService(initBoltUserResourceMappingLoggingService, t)
}

func initBoltUserResourceMappingLoggingService(f influxdbtesting.UserResourceFields, t *testing.T) (influxdb.UserResourceMappingService, func()) {
	svc, closer := initBoltUserResourceMappingService(f, t)
	return tenant.NewURMLogger(zaptest.NewLogger(t), svc), closer
}
