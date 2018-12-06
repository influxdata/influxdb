package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initOrganizationService(f platformtesting.OrganizationFields, t *testing.T) (platform.OrganizationService, string, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	ctx := context.TODO()
	for _, o := range f.Organizations {
		if err := s.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate organizations")
		}
	}
	return s, OpPrefix, func() {}
}

func TestOrganizationService(t *testing.T) {
	platformtesting.OrganizationService(initOrganizationService, t)
}
