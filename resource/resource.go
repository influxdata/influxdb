// Package resource defines an interface for recording changes to InfluxDB resources.
//
// A resource is an entity in our system, e.g. an organization, task or bucket.
// A change includes the creation, update or deletion of a resource.
package resource

import (
	"time"

	"github.com/influxdata/influxdb/v2"
)

// Logger records changes to resources.
type Logger interface {
	// Log a change to a resource.
	Log(Change) error
}

// Change to a resource.
type Change struct {
	// Type of change.
	Type ChangeType
	// ResourceID of the changed resource.
	ResourceID influxdb.ID
	// ResourceType that was changed.
	ResourceType influxdb.ResourceType
	// OrganizationID of the organization owning the changed resource.
	OrganizationID influxdb.ID
	// UserID of the user changing the resource.
	UserID influxdb.ID
	// ResourceBody after the change.
	ResourceBody []byte
	// Time when the resource was changed.
	Time time.Time
}

// Type of  change.
type ChangeType string

const (
	// Create a resource.
	Create ChangeType = "create"
	// Put a resource.
	Put = "put"
	// Update a resource.
	Update = "update"
	// Delete a resource
	Delete = "delete"
)
