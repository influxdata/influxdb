package influxdb

import (
	"context"
)

// LookupService provides field lookup for the resource and ID.
type LookupService interface {
	// Name returns the name for the resource and ID.
	Name(ctx context.Context, resource ResourceType, id ID) (string, error)
}
