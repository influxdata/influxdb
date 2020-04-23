package tenant_test

import (
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestUserLoggingService(t *testing.T) {
	influxdbtesting.UserService(initBoltUserLoggingService, t)
}

func initBoltUserLoggingService(f influxdbtesting.UserFields, t *testing.T) (influxdb.UserService, string, func()) {
	svc, s, closer := initBoltUserService(f, t)
	return tenant.NewUserLogger(zaptest.NewLogger(t), svc), s, closer
}
