package mock

import (
	"context"

	platform "github.com/influxdata/influxdb"
)

// LookupService provides field lookup for the resource and ID.
type LookupService struct {
	NameFn func(ctx context.Context, resource platform.ResourceType, id platform.ID) (string, error)
}

// Name returns the name for the resource and ID.
func (s *LookupService) Name(ctx context.Context, resource platform.ResourceType, id platform.ID) (string, error) {
	return s.NameFn(ctx, resource, id)
}
