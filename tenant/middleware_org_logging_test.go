package tenant_test

import (
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestOrganizationLoggingService(t *testing.T) {
	influxdbtesting.OrganizationService(initBoltOrganizationLoggingService, t)
}

func initBoltOrganizationLoggingService(f influxdbtesting.OrganizationFields, t *testing.T) (influxdb.OrganizationService, string, func()) {
	orgSvc, s, closer := initBoltOrganizationService(f, t)
	return tenant.NewOrgLogger(zaptest.NewLogger(t), orgSvc), s, closer
}
