package mock

import (
	"context"

	platform "github.com/influxdata/influxdb/v2"
)

// LookupService provides field lookup for the resource and ID.
type LookupService struct {
	NameFn func(ctx context.Context, resource platform.ResourceType, id platform.ID) (string, error)
}

// NewLookupService returns a mock of LookupService where its methods will return zero values.
func NewLookupService() *LookupService {
	return &LookupService{
		NameFn: func(ctx context.Context, resource platform.ResourceType, id platform.ID) (string, error) {
			return "", nil
		},
	}
}

// Name returns the name for the resource and ID.
func (s *LookupService) Name(ctx context.Context, resource platform.ResourceType, id platform.ID) (string, error) {
	return s.NameFn(ctx, resource, id)
}
