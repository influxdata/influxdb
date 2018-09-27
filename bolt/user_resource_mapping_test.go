package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initUserResourceMappingService(f platformtesting.UserResourceFields, t *testing.T) (platform.UserResourceMappingService, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	ctx := context.Background()
	for _, m := range f.UserResourceMappings {
		if err := c.CreateUserResourceMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate mappings")
		}
	}

	return c, func() {
		defer closeFn()
		for _, m := range f.UserResourceMappings {
			if err := c.DeleteUserResourceMapping(ctx, m.ResourceID, m.UserID); err != nil {
				t.Logf("failed to remove user resource mapping: %v", err)
			}
		}
	}
}

func TestUserResourceMappingService_FindUserResourceMappings(t *testing.T) {
	platformtesting.FindUserResourceMappings(initUserResourceMappingService, t)
}

func TestUserResourceMappingService_CreateUserResourceMapping(t *testing.T) {
	platformtesting.CreateUserResourceMapping(initUserResourceMappingService, t)
}

func TestUserResourceMappingService_DeleteUserResourceMapping(t *testing.T) {
	platformtesting.DeleteUserResourceMapping(initUserResourceMappingService, t)
}
