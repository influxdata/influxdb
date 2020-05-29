package metric

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

// EventRecorder records meta-data associated with http requests.
type EventRecorder interface {
	Record(ctx context.Context, e Event)
}

// Event represents the meta data associated with an API request.
type Event struct {
	OrgID         influxdb.ID
	Endpoint      string
	RequestBytes  int
	ResponseBytes int
	Status        int
}

// NopEventRecorder never records events.
type NopEventRecorder struct{}

// Record never records events.
func (n *NopEventRecorder) Record(ctx context.Context, e Event) {}
