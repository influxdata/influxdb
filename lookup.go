package influxdb

import (
	"context"
)

// LookupService provides field lookup for the resource and ID.
type LookupService interface {
	// FindResourceName returns the name for the resource and ID.
	FindResourceName(ctx context.Context, resource ResourceType, id ID) (string, error)
}
