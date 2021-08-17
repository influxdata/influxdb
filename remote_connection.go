package influxdb

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

type RemoteConnectionService interface {
	// ListRemoteConnections returns all info about registered remote InfluxDB connections matching a filter.
	ListRemoteConnections(context.Context, RemoteConnectionListFilter) (*RemoteConnections, error)

	// CreateRemoteConnection registers a new remote InfluxDB connection.
	CreateRemoteConnection(context.Context, CreateRemoteConnectionRequest) (*RemoteConnection, error)

	// ValidateNewRemoteConnection validates that the given settings for a remote InfluxDB connection are usable,
	// without persisting the connection info.
	ValidateNewRemoteConnection(context.Context, CreateRemoteConnectionRequest) error

	// GetRemoteConnection returns metadata about the remote InfluxDB connection with the given ID.
	GetRemoteConnection(context.Context, platform.ID) (*RemoteConnection, error)

	// UpdateRemoteConnection updates the settings for the remote InfluxDB connection with the given ID.
	UpdateRemoteConnection(context.Context, platform.ID, UpdateRemoteConnectionRequest) (*RemoteConnection, error)

	// ValidateUpdatedRemoteConnection validates that a remote InfluxDB connection is still usable after applying the
	// given update, without persisting the new info.
	ValidateUpdatedRemoteConnection(context.Context, platform.ID, UpdateRemoteConnectionRequest) error

	// DeleteRemoteConnection deletes all info for the remote InfluxDB connection with the given ID.
	DeleteRemoteConnection(context.Context, platform.ID) error

	// ValidateRemoteConnection checks that the remote InfluxDB connection with the given ID is still usable
	// with its persisted settings.
	ValidateRemoteConnection(context.Context, platform.ID) error
}

// RemoteConnection contains all info about a remote InfluxDB instance that should be returned to users.
// Note that the auth token used by the request is *not* included here.
type RemoteConnection struct {
	ID               platform.ID `json:"id" db:"id"`
	OrgID            platform.ID `json:"orgID" db:"org_id"`
	Name             string      `json:"name" db:"name"`
	Description      *string     `json:"description,omitempty" db:"description"`
	RemoteURL        string      `json:"remoteURL" db:"remote_url"`
	RemoteOrgID      platform.ID `json:"remoteOrgID" db:"remote_org_id"`
	AllowInsecureTLS bool        `json:"allowInsecureTLS" db:"allow_insecure_tls"`
}

// RemoteConnectionHTTPConfig contains all info needed by a client to make HTTP requests against a
// remote InfluxDB API.
type RemoteConnectionHTTPConfig struct {
	RemoteURL        string      `db:"remote_url"`
	RemoteToken      string      `db:"remote_api_token"`
	RemoteOrgID      platform.ID `db:"remote_org_id"`
	AllowInsecureTLS bool        `db:"allow_insecure_tls"`
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
