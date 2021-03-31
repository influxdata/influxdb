package influxdb

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

// LookupService provides field lookup for the resource and ID.
type LookupService interface {
	// FindResourceName returns the name for the resource and ID.
	FindResourceName(ctx context.Context, resource ResourceType, id platform.ID) (string, error)
}
