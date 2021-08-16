package influxdb

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

type RemoteConnectionService interface {
	// ListRemoteConnections returns all info about registered remote InfluxDB instances matching a filter.
	ListRemoteConnections(context.Context, RemoteConnectionListFilter) (*RemoteConnections, error)

	// CreateRemoteConnection registers a new remote InfluxDB instance.
	// Before persisting the new connection info, the service verifies that the remote instance can be reached.
	CreateRemoteConnection(context.Context, CreateRemoteConnectionRequest) (*RemoteConnection, error)

	// ValidateNewRemoteConnection validates that a remote InfluxDB instance can be reached via the given connection
	// info, but doesn't persist the info.
	ValidateNewRemoteConnection(context.Context, CreateRemoteConnectionRequest) error

	// GetRemoteConnection returns metadata about the remote InfluxDB instance with the given ID.
	GetRemoteConnection(context.Context, platform.ID) (*RemoteConnection, error)

	// UpdateRemoteConnection updates the connection details for the remote InfluxDB instance with the given ID.
	UpdateRemoteConnection(context.Context, platform.ID, UpdateRemoteConnectionRequest) (*RemoteConnection, error)

	// ValidateUpdatedRemoteConnection validates that a remote InfluxDB instance can be reached after applying the
	// given update, but doesn't persist the new info.
	ValidateUpdatedRemoteConnection(context.Context, platform.ID, UpdateRemoteConnectionRequest) error

	// DeleteRemoteConnection deletes all connection info for the remote InfluxDB instance with the given ID.
	DeleteRemoteConnection(context.Context, platform.ID) error

	// ValidateRemoteConnection checks that the remote InfluxDB instance with the given ID is still reachable
	// using its persisted settings.
	ValidateRemoteConnection(context.Context, platform.ID) error
}

// RemoteConnection contains all info about a remote InfluxDB instance that should be returned to users.
// Note that the auth token used by the request is *not* included here.
type RemoteConnection struct {
	ID               platform.ID `json:"id"`
	OrgID            platform.ID `json:"orgID"`
	Name             string      `json:"name"`
	Description      *string     `json:"description,omitempty"`
	RemoteURL        string      `json:"remoteURL"`
	RemoteOrgID      platform.ID `json:"remoteOrgID"`
	AllowInsecureTLS bool        `json:"allowInsecureTLS"`
}

// RemoteConnectionListFilter is a selection filter for listing remote InfluxDB instances.
type RemoteConnectionListFilter struct {
	OrgID     platform.ID
	Name      *string
	RemoteURL *string
}

// RemoteConnections is a collection of metadata about remote InfluxDB instances.
type RemoteConnections struct {
	Remotes []RemoteConnection `json:"remotes"`
}

// CreateRemoteConnectionRequest contains all info needed to establish a new connection to a remote
// InfluxDB instance.
type CreateRemoteConnectionRequest struct {
	OrgID            platform.ID `json:"orgID"`
	Name             string      `json:"name"`
	Description      *string     `json:"description,omitempty"`
	RemoteURL        string      `json:"remoteURL"`
	RemoteToken      string      `json:"remoteAPIToken"`
	RemoteOrgID      platform.ID `json:"remoteOrgID"`
	AllowInsecureTLS bool        `json:"allowInsecureTLS"`
}

// UpdateRemoteConnectionRequest contains a partial update to existing info about a remote InfluxDB instance.
type UpdateRemoteConnectionRequest struct {
	Name             *string      `json:"name,omitempty"`
	Description      *string      `json:"description,omitempty"`
	RemoteURL        *string      `json:"remoteURL,omitempty"`
	RemoteToken      *string      `json:"remoteAPIToken,omitempty"`
	RemoteOrgID      *platform.ID `json:"remoteOrgID,omitempty"`
	AllowInsecureTLS *bool        `json:"allowInsecureTLS,omitempty"`
}
