package inmem

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
)

func initOrganizationService(f platformtesting.OrganizationFields, t *testing.T) (platform.OrganizationService, string, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	s.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		s.TimeGenerator = platform.RealTimeGenerator{}
	}
	ctx := context.TODO()
	for _, o := range f.Organizations {
		if err := s.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate organizations")
		}
	}
	return s, OpPrefix, func() {}
}

func TestOrganizationService(t *testing.T) {
	t.Skip("organization service no longer used.  Remove all of this inmem stuff")
	platformtesting.OrganizationService(initOrganizationService, t)
}
